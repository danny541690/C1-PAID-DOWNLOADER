"""Reconcile selected OTC history against original official reports and paid coverage evidence."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from .drive_pilot import token as drive_token
from .exchange_scope import price_value
from .history_pilot import iso_date, months
from .market_lifetimes import digest, earlier_evidence, name_key, nonoverlapping, official_date, tpex_events
from .test_archive import json_bytes
from .tpex_calendar import ANCHORS, SOURCE_ROOT, Source, fetch_official, monthly_rows
from .tpex_history import (
    EVIDENCE_RUN, FACTBOOK, GAP_DAY, positive_integer, private_folder, save_private, verified_archive_files,
)


CALENDAR_RUN = 45665323287
LIFETIME_RUN = 45663054564
YEARBOOK = "tpex-2000-annual-stock-trades.xls"
CHRONICLE = "twsa-market-origins.pdf"
FULL_QUOTES = "tpex-all-sessions-2010-11-24.json"
FIXED_QUOTES = "tpex-fixed-price-2010-11-24.json"
EARLY_LIVE_CODES = {"4502", "4702", "5601"}
PRICE_FIELDS = ("開盤", "最高", "最低", "收盤")
REPORT = Path("r11-tpex-historical-closeout.json")


def official_annual_rows(raw):
    import xlrd

    sheet = xlrd.open_workbook(file_contents=raw).sheet_by_index(0)
    if "表八、歷年上櫃股票交易統計表" not in sheet.row_values(0) or sheet.ncols != 9:
        raise ValueError("unexpected OTC annual workbook")
    fields, units = sheet.row_values(2), sheet.row_values(3)
    if [x.strip() for x in fields[1:4]] != ["成交量", "成交值", "成交筆數"] or "千股" not in units[1]:
        raise ValueError("unexpected OTC annual units")
    result = {}
    for index in range(4, sheet.nrows):
        year, volume, turnover, transactions, companies = sheet.row_values(index)[:5]
        if not isinstance(year, float):
            break
        calendar_year = 1911 + int(year)
        integers = [positive_integer(x) for x in (volume, transactions, companies)]
        if not year.is_integer() or calendar_year in result or any(value is None for value in integers):
            raise ValueError("invalid annual trading report row")
        result[calendar_year] = {"thousand_shares": integers[0], "amount_ntd_100m": str(Decimal(str(turnover))),
                                 "trades": integers[1], "year_end_companies": integers[2],
                                 "source": f"{YEARBOOK}#{index + 1}"}
    if not result or list(result) != list(range(min(result), 2001)):
        raise ValueError("annual report series incomplete")
    return result


def first_listed_session(raw, annual):
    text = subprocess.run(["pdftotext", "-enc", "UTF-8", "-f", "25", "-l", "25", "-", "-"], input=raw,
                          capture_output=True, check=True, timeout=15).stdout.decode()
    compact = "".join(text.split())
    match = re.search(r"第一家掛牌公司(建弘投信)於(\d{2})年(\d{1,2})(?:週年)?月(\d{1,2})正式上櫃買賣", compact)
    if not match:
        raise ValueError("association publication did not verify a dated first listing")
    name, y, m, d = match.groups()
    day = official_date(f"{y}/{m}/{d}")
    if day.year != min(annual) or not annual[day.year]["trades"]:
        raise ValueError("first listing year does not match the official annual trading series")
    return {"date": day.isoformat(), "issuer": name, "proof": [f"{CHRONICLE}#page=25", annual[day.year]["source"]],
            "kind": "FIRST_STOCK_LISTING_AND_TRADING_EVENT"}


def archived_events(files, summary, as_of):
    starts, exits = defaultdict(list), defaultdict(list)
    for name, meta in sorted(summary["sources"].items()):
        if "events" not in meta or not re.fullmatch(r"tpex-(listed|ended)-\d{4}\.json", name):
            continue
        is_start = name.startswith("tpex-listed-")
        year = int(name[-9:-5])
        for event in tpex_events(files[f"official/{name}"], year, as_of, start=is_start):
            (starts if is_start else exits)[event.code].append(event)
    return starts, exits


def aliases_match(short_name, legal_name, profile):
    legal = name_key(legal_name)
    short = name_key(short_name)
    return (legal == name_key(profile["issuer"]) and bool(re.fullmatch(r"[0-9]{8}", profile["company_id"]))
            and profile["company_id"] != "00000000"
            and (short == name_key(profile["alias"]) or legal == short + "股份有限公司"))


def verified_handover_intervals(starts, exits, profiles, summary):
    confirmed = {(row["code"], row["effective"]): row for row in summary["handover_events"]}
    intervals = summary["listing_intervals"]
    active = {(row["code"], row["start"]): row for row in intervals
              if row["market"] == "TWSE" and row["kind"] == "OFFICIAL_ACTIVE_THROUGH_PUBLICATION"}
    company = {(row["code"], row["start"]): row for row in profiles}
    pending = {row["source"] for row in summary["held_intervals_detail"]
               if row["market"] == "TPEx" and row["reason"] == "HOLD_UNPROVEN_HISTORICAL_START_OR_IDENTITY"}
    proposed = []
    for code, events in exits.items():
        for end in events:
            handover = code, end.day.isoformat()
            if end.source not in pending or handover not in confirmed or handover not in company or handover not in active:
                continue
            move, profile = confirmed[handover], company[handover]
            if end.source not in move["proof"] or profile["source"] not in active[handover]["proof"]:
                continue
            prior = max((row.day for row in events if row.day < end.day), default=date.min)
            begins = [start for start in starts.get(code, []) if prior <= start.day < end.day]
            if len(begins) != 1 or not aliases_match(begins[0].issuer, end.issuer, profile):
                continue
            start = begins[0]
            proposed.append({"market": "TPEx", "code": code, "issuer": end.issuer, "company_id": profile["company_id"],
                             "start": start.day.isoformat(), "end_exclusive": end.day.isoformat(),
                             "kind": "OFFICIAL_CLOSED_INTERVAL_WITH_LISTING_HANDOVER",
                             "proof": [start.source, *move["proof"], profile["source"]], "exiting_source": end.source})
    combined, _ = nonoverlapping(intervals + proposed)
    return [row for row in combined if row["kind"] == "OFFICIAL_CLOSED_INTERVAL_WITH_LISTING_HANDOVER"]


def market_table(raw, title):
    payload = json.loads(raw)
    if payload.get("stat") != "ok" or payload.get("date") != GAP_DAY.strftime("%Y%m%d"):
        raise ValueError("wrong source date for official daily report")
    tables = [table for table in payload["tables"] if table["title"] == title]
    if len(tables) != 1:
        raise ValueError("ambiguous official report scope")
    rows = {}
    fields = [field.strip() for field in tables[0]["fields"]]
    for index, row in enumerate(tables[0]["data"], 1):
        record = dict(zip(fields, row, strict=True))
        code = record["代號"].strip()
        if code in rows:
            raise ValueError("duplicate security in daily report")
        rows[code] = index, record, digest(json_bytes(row))
    return rows


def monthly_gap_row(raw, source):
    day = monthly_rows(raw, source).get(GAP_DAY)
    if not day or not day["traded"] or not day["positive_price"]:
        raise ValueError("individual monthly report does not contain a positive trade")
    doc = json.loads(raw)
    table = doc["tables"][0]
    if not any("不含上櫃股票鉅額交易" in note for note in table["notes"]):
        raise ValueError("monthly report scope note missing")
    index = int(day["source"].rsplit("#", 1)[1])
    row = table["data"][index - 1]
    return index, dict(zip(table["fields"], row, strict=True)), digest(json_bytes(row))


def market_amounts(row):
    result = [positive_integer(row[field]) for field in ("成交股數", "成交金額(元)", "成交筆數")]
    if any(num is None for num in result):
        raise ValueError("nonnumeric native turnover on official daily row")
    return result


def same_prices(first, second):
    result = [price_value(first[field]) for field in PRICE_FIELDS]
    return all(value is not None for value in result) and result == [price_value(second[field]) for field in PRICE_FIELDS]


def managed_stock_reconciliation(archive, responses):
    baseline = json.loads(archive["reconciliation.json"])
    source_member = f"official/TPEx-{GAP_DAY}.json"
    raw_regular = archive[source_member]
    source = next(entry for entry in baseline["backer_requests"]
                  if entry["request"] == {"dataset": "TaiwanStockPrice", "start_date": str(GAP_DAY), "end_date": str(GAP_DAY)})
    if source["status"] != "VALID_SAMPLE":
        raise ValueError("Backer bulk historical reference missing")
    vendor = json.loads(archive[source["original"]])["data"]
    if any(row["date"] != str(GAP_DAY) for row in vendor):
        raise ValueError("Backer evidence contained another trading date")
    vendor_codes = {row["stock_id"] for row in vendor}
    gaps = {gap["code"] for gap in baseline["historical_price_gaps"] if gap["window_status"] == "HOLD_PROVIDER_WINDOW_EMPTY"}
    raw_total = responses.get(FULL_QUOTES)
    raw_fixed = responses.get(FIXED_QUOTES)
    if not raw_total or not raw_fixed:
        return {"status": "HOLD_OFFICIAL_SCOPE_SOURCE"}, {"managed_stock_receipts": []}
    managed = market_table(raw_total, "管理股票")
    regular = market_table(raw_regular, "上櫃股票每日收盤行情(不含定價)")
    fixed = market_table(raw_fixed, "盤後定價行情")
    active = {code for code, (_, row, _) in managed.items() if market_amounts(row)[2] > 0 and price_value(row["收盤"]) is not None}
    receipts = []
    for code in sorted(gaps):
        src = Source(GAP_DAY.replace(day=1), code)
        raw_month = responses.get(src.name)
        if code not in active or code not in regular or code not in fixed or not raw_month:
            continue
        _, all_row, _ = managed[code]
        _, normal_row, _ = regular[code]
        _, fixed_row, _ = fixed[code]
        row_index, monthly, row_hash = monthly_gap_row(raw_month, src)
        shares, amount, trades = market_amounts(all_row)
        normalized = ((shares + 500) // 1000, (amount + 500) // 1000, trades)
        displayed = tuple(positive_integer(monthly[name]) for name in ("成交仟股", "成交仟元", "筆數"))
        if code in vendor_codes or not same_prices(all_row, monthly) or not same_prices(all_row, normal_row) or displayed != normalized:
            continue
        baseline_amounts = market_amounts(normal_row)
        deltas = [total - normal for total, normal in zip((shares, amount, trades), baseline_amounts, strict=True)]
        if any(value < 0 for value in deltas):
            continue
        receipts.append({"code": code, "date": str(GAP_DAY), "scope": "TPEX_DAILY_QUOTES_MANAGED_STOCK",
                         "native": {"shares": shares, "amount_ntd": amount, "trades": trades},
                         "displayed_thousands": list(displayed[:2]), "differences_from_regular": deltas,
                         "after_hours_fixed_trades": positive_integer(fixed_row["成交筆數"]),
                         "scope_matches": {"ohlc": True, "turnover_after_thousands_rounding": True},
                         "source_rows": {FULL_QUOTES: managed[code][::2], source_member: regular[code][::2],
                                         FIXED_QUOTES: fixed[code][::2], src.name: (row_index, row_hash)},
                         "backer_status": "HOLD_PROVIDER_SCOPE_GAP"})
    public = {"status": "PASS_OFFICIAL_FALLBACK_SCOPE" if gaps and len(receipts) == len(gaps) else "HOLD_OFFICIAL_SCOPE",
              "managed_symbols": len(managed), "managed_positive_trade_symbols": len(active),
              "managed_symbols_in_backer": len(managed.keys() & vendor_codes), "six_gap_scope_covers_all_positive_trades": gaps == active,
              "official_fallback_rows": len(receipts), "turnover_differs_from_regular_rows": sum(any(row["differences_from_regular"]) for row in receipts),
              "fixed_price_traded_rows": sum(bool(row["after_hours_fixed_trades"]) for row in receipts),
              "backer_gap_count": len(gaps)}
    evidence = {"managed_stock_receipts": receipts, "regular_report_sha256": digest(raw_regular),
                "backer_all_market_sha256": digest(archive[source["original"]])}
    return public, evidence


def early_quote_windows(tpex_profiles, exits):
    windows = {}
    sample_end = date(1995, 12, 31)
    for profile in tpex_profiles:
        if profile["code"] in EARLY_LIVE_CODES:
            start = iso_date(profile["start"])
            for month in months(start.replace(day=1), sample_end.replace(day=1)):
                windows[Source(month, profile["code"])] = (start, date(1996, 1, 1))
    for code, events in exits.items():
        early = [event for event in events if event.day <= sample_end]
        if len(early) != 1:
            continue
        end = early[0].day
        first = min(date(1994, 1, 1), end.replace(day=1))
        for month in months(first, end.replace(day=1)):
            windows[Source(month, code)] = (None, end)
    return windows


def early_trade_evidence(calendar, windows, responses, annual):
    baseline = json.loads(calendar["reconciliation.json"])
    confirmed = {}
    for year in ("1994", "1995"):
        confirmed[year] = {day: [f"run:{CALENDAR_RUN}/{row['source']}" for row in rows
                                 if row["traded"] and row["positive_price"]]
                           for day, rows in baseline["early_stock_dates"][year].items()}
        confirmed[year] = {day: proof for day, proof in confirmed[year].items() if proof}
    original = {year: set(days) for year, days in confirmed.items()}
    statuses = {}
    for src, (lower, upper) in windows.items():
        raw = responses.get(src.name)
        if not raw:
            statuses[src.name] = "HOLD_SOURCE_UNAVAILABLE"
            continue
        try:
            rows = monthly_rows(raw, src)
        except (KeyError, ValueError, IndexError, TypeError):
            statuses[src.name] = "HOLD_OFFICIAL_CONTRACT"
            continue
        statuses[src.name] = "DATED_QUOTE_ROWS" if rows else "EMPTY_OFFICIAL_REPORT"
        for day, row in rows.items():
            if row["traded"] and row["positive_price"] and (lower is None or day >= lower) and day < upper:
                confirmed.setdefault(str(day.year), {}).setdefault(str(day), []).append(row["source"])
    early = {year: {"status": "HOLD_CALENDAR_COMPLETENESS", "prior_trade_dates": len(original[year]),
                    "additional_trade_dates": len(confirmed[year].keys() - original[year]),
                    "confirmed_trade_dates": len(confirmed[year]),
                    "official_year_end_companies": annual.get(int(year), {}).get("year_end_companies")}
             for year in ("1994", "1995")}
    report = {"sampled_ticker_count": len(EARLY_LIVE_CODES | set(ANCHORS) | {src.symbol for src in windows}),
              "early_trading_days": early,
              "new_pre_1994_probe_outcomes": dict(Counter(statuses[src.name] for src in windows if src.month.year < 1994))}
    evidence = {"early_trade_date_rows": confirmed, "early_source_statuses": statuses}
    return report, evidence


def inputs():
    return earlier_evidence(), earlier_evidence(CALENDAR_RUN), earlier_evidence(LIFETIME_RUN)


def prepare(lifetimes, baseline, as_of):
    ledger = json.loads(lifetimes["reconciliation.json"])
    starts, exits = archived_events(lifetimes, ledger, as_of)
    projected = json.loads(lifetimes["official/tpex-active-projection.json"])
    windows = early_quote_windows(projected, exits)
    base_summary = json.loads(baseline["reconciliation.json"])
    gaps = [Source(GAP_DAY.replace(day=1), item["code"]) for item in base_summary["historical_price_gaps"]
            if item["window_status"] == "HOLD_PROVIDER_WINDOW_EMPTY"]
    date_query = urlencode({"date": GAP_DAY.strftime("%Y/%m/%d"), "response": "json"})
    urls = {FULL_QUOTES: f"{SOURCE_ROOT}/dailyQuotes?{date_query}", FIXED_QUOTES: f"{SOURCE_ROOT}/fixPricing?{date_query}",
            YEARBOOK: FACTBOOK + quote("89/08.表8歷年股票交易統計表.xls"), CHRONICLE: "https://www.twsa.org.tw/B01/doc/198-247.pdf"}
    urls.update({src.name: src.url for src in sorted([*gaps, *windows], key=lambda x: x.month, reverse=True)})
    return urls, windows, starts, exits, ledger


def reconcile(baseline, calendar, lifetimes, as_of, responses):
    _, windows, starts, exits, ledger = prepare(lifetimes, baseline, as_of)
    annual = official_annual_rows(responses[YEARBOOK]) if responses.get(YEARBOOK) else {}
    event = {"status": "HOLD_FIRST_LISTING_EVIDENCE"}
    raw_chronicle = responses.get(CHRONICLE)
    if raw_chronicle and annual:
        event = first_listed_session(raw_chronicle, annual) | {"status": "PASS_OFFICIAL_FIRST_LISTING"}
    public_early, early = early_trade_evidence(calendar, windows, responses, annual)
    public_prices, prices = managed_stock_reconciliation(baseline, responses)
    profiles = json.loads(lifetimes["official/twse-active-projection.json"])
    additions = verified_handover_intervals(starts, exits, profiles, ledger)
    original_holds = ledger["held_intervals"]["HOLD_UNPROVEN_HISTORICAL_START_OR_IDENTITY"]
    result = {"as_of_taipei": as_of.isoformat(), "finmind_requests": 0, "production": "HOLD",
              "first_official_stock_listing": event.get("date"), "first_listing_status": event["status"],
              "earliest_annual_trade_count": annual.get(min(annual, default=0), {}).get("trades"),
              "pre_1994_daily_quote_archive": "HOLD_DAILY_ORIGINALS",
              "managed_fallback": public_prices, "new_verified_closed_intervals": len(additions),
              "historical_intervals_still_held": original_holds - len(additions),
              "baseline_accepted_intervals": len(ledger["listing_intervals"]),
              "active_snapshot_conflicts_still_held": ledger["held_intervals"].get("HOLD_ACTIVE_SNAPSHOT_CONFLICTS_WITH_TERMINATION", 0)}
    evidence = {"official_first_listing_event": event, "official_annual_trades": annual,
                "additional_closed_intervals": additions,
                "input_archives": {str(run): {"ledger_sha256": digest(archive["reconciliation.json"])}
                                   for run, archive in ((EVIDENCE_RUN, baseline), (CALENDAR_RUN, calendar), (LIFETIME_RUN, lifetimes))}}
    return result | public_early, evidence | prices | early


def run():
    baseline, calendar, lifetimes = inputs()
    today = datetime.now(ZoneInfo("Asia/Taipei")).date()
    urls, *_ = prepare(lifetimes, baseline, today)
    sources = list(urls)
    fetched = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        for offset in range(0, len(sources), 6):
            names = sources[offset:offset + 6]
            values = list(pool.map(fetch_official, [urls[name] for name in names]))
            fetched.update(zip(names, values))
            if any(status == "HOLD_OFFICIAL_HTTP_429" for status, _ in values):
                break
    public, evidence = reconcile(baseline, calendar, lifetimes, today, {name: body for name, (_, body) in fetched.items()})
    report = public | {"status": "PASS_DIAGNOSTIC", "private_archive": "STAGED",
                       "source_statuses": dict(Counter(status for status, _ in fetched.values())),
                       "sources_skipped": len(sources) - len(fetched)}
    save_private(fetched, urls, {}, report | evidence)
    return report


def verify():
    baseline, calendar, lifetimes = inputs()
    access = drive_token()
    files = verified_archive_files(access, private_folder(access, create=False))
    expected = json.loads(files["reconciliation.json"])
    today = iso_date(expected["as_of_taipei"])
    urls, *_ = prepare(lifetimes, baseline, today)
    responses = {name: files.get(f"official/{name}") for name in urls}
    public, evidence = reconcile(baseline, calendar, lifetimes, today, responses)
    result = public | evidence
    if json_bytes(result) != json_bytes({key: expected[key] for key in result}):
        raise ValueError("archived historical results are inconsistent with the verified original sources")
    print(f"tpex_closeout_drive_readback=PASS files={len(files)} "
          f"official_fallback_rows={public['managed_fallback'].get('official_fallback_rows', 0)} "
          f"new_closed_intervals={public['new_verified_closed_intervals']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.verify_only:
            verify()
            return 0
        report = run()
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
        if args.verify_only:
            print(json.dumps(report, sort_keys=True))
            return 1
    REPORT.write_bytes(json_bytes(report))
    print(json.dumps(report, sort_keys=True))
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as output:
            output.write(f"archive={report.get('private_archive', 'HOLD')}\n")
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
