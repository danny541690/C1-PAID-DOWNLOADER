"""Verify early TPEx trading dates from dated official market and stock reports."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import hashlib
from html import unescape
import json
import os
import re
import time
from urllib.parse import urlencode

from .drive_pilot import token as drive_token
from .exchange_scope import price_value
from .history_pilot import iso_date, months
from .market_lifetimes import earlier_evidence, official_date
from .test_archive import json_bytes
from .tpex_history import (
    EVIDENCE_RUN, REPORT, positive_integer, private_folder, save_private, source_bytes, verified_archive_files,
    yearbook_days,
)


SOURCE_ROOT = "https://www.tpex.org.tw/www/zh-tw/afterTrading"
ANNUAL_SOURCE = "https://www.tpex.org.tw/epaper/monthly/nweb/GTSM112/Statistics-a-1.htm"
ANNUAL_MEMBER = "tpex-annual-turnover.html"
RETRYABLE = {"HOLD_OFFICIAL_NETWORK", *(f"HOLD_OFFICIAL_HTTP_{code}" for code in (408, 500, 502, 503, 504, 520))}
ANCHORS = ("5301", "5302")  # TPEx's issuer snapshot dates both listings before the 1994 quote archive.
YEARBOOK_TYPO = date(1999, 5, 30)
CANONICAL_SESSION = date(1999, 5, 31)


@dataclass(frozen=True)
class Source:
    month: date
    symbol: str = ""

    @property
    def name(self):
        return f"tpex-{self.symbol or 'market'}-{self.month:%Y-%m}.json"

    @property
    def url(self):
        query = {"date": self.month.strftime("%Y/%m/%d"), "response": "json"}
        if self.symbol:
            query["code"] = self.symbol
        return f"{SOURCE_ROOT}/{'tradingStock' if self.symbol else 'tradingIndex'}?{urlencode(query)}"


def source_plan():
    plan = [Source(month) for month in months(date(1998, 12, 1), date(2000, 12, 1))]
    plan += [Source(month, symbol) for month in months(date(1994, 1, 1), date(1998, 12, 1)) for symbol in ANCHORS]
    plan += [Source(date(1999, 5, 1), ANCHORS[0]), Source(date(1990, 6, 1), ANCHORS[0]),
             Source(date(1991, 1, 1), ANCHORS[1]), *(Source(date(1993, 12, 1), symbol) for symbol in ANCHORS)]
    return sorted(plan, key=lambda source: source.month, reverse=True)


def monthly_rows(raw, source):
    payload = json.loads(raw)
    if (payload.get("date") != source.month.strftime("%Y%m%d") or payload.get("stat") != "ok"
            or len(payload["tables"]) != 1 or (source.symbol and payload.get("code") != source.symbol)):
        raise ValueError("official report did not identify the requested month and security")
    table = payload["tables"][0]
    fields = ["".join(field.split()) for field in table["fields"]]
    if source.symbol:
        volume = "成交仟股"
    else:
        volume = "成交張數" if "成交張數" in fields else "成交股數（仟股）"
    value = "收盤" if source.symbol else "櫃買指數"
    date_col, quantity_col, trades_col, value_col = (fields.index(field) for field in ("日期", volume, "筆數", value))
    result = {}
    for index, row in enumerate(table["data"], 1):
        day = official_date(row[date_col].strip().rstrip("*＊").strip())
        if day.replace(day=1) != source.month or day in result:
            raise ValueError("duplicate or out-of-month official trading date")
        quantity, trades = positive_integer(row[quantity_col]), positive_integer(row[trades_col])
        if quantity is None or trades is None:
            raise ValueError("nonnumeric official turnover on a dated row")
        result[day] = {"source": f"{source.name}#{index}", "traded": trades > 0,
                       "positive_price": price_value(row[value_col]) is not None,
                       "reported_value": row[value_col]}
    return result


def verified_date_correction(yearbook, vendor, market, stock, old_value):
    new_row, stock_row = market.get(CANONICAL_SESSION, {}), stock.get(CANONICAL_SESSION, {})
    if (YEARBOOK_TYPO not in yearbook or CANONICAL_SESSION in yearbook
            or YEARBOOK_TYPO in vendor or CANONICAL_SESSION not in vendor
            or YEARBOOK_TYPO in market or YEARBOOK_TYPO in stock
            or not new_row.get("traded") or not stock_row.get("traded") or not stock_row.get("positive_price")
            or old_value is None or old_value != price_value(new_row["reported_value"])):
        return {"status": "HOLD_DATE_CONFLICT"}
    return {"status": "PASS_YEARBOOK_DATE_CORRECTION", "yearbook_date": YEARBOOK_TYPO.isoformat(),
            "confirmed_session": CANONICAL_SESSION.isoformat(), "matched_index_value": str(old_value),
            "evidence": [new_row["source"], stock_row["source"], f"yearbook-1999.xls#May/{YEARBOOK_TYPO.day}",
                         f"FinMind calendar from private run {EVIDENCE_RUN}"]}


def annual_turnover(raw):
    if not raw:
        return {}
    html = raw.decode("cp950", errors="replace")
    result = {}
    matched_header = False
    for table_row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", html, re.I | re.S):
        fields = re.findall(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", table_row, re.I | re.S)
        cells = ["".join(unescape(re.sub(r"<[^>]*>", "", field)).split()) for field in fields]
        if len(cells) < 3:
            continue
        if cells[:3] == ["上櫃股票交易統計概要年月", "總成交金額(億元)", "日平均成交金額(億元)"]:
            matched_header = True
        elif matched_header and re.fullmatch(r"\d{4}", cells[0]):
            year = int(cells[0])
            if year >= 1999:
                break
            if year in result or (total := price_value(cells[1])) is None:
                raise ValueError("ambiguous annual TPEx turnover result")
            result[year] = (total, price_value(cells[2]))
    if matched_header and {year for year in range(1994, 1999)} <= result.keys():
        return result
    raise ValueError("official annual TPEx statistics contract not recognized")


def supported_session_counts(total, average):
    if average is None:
        return []
    margin = Decimal("0.01")
    return [count for count in range(1, 367) if (average - margin) * count <= total + margin
            and (average + margin) * count >= total - margin]


def reconcile(archived, sources, responses):
    import xlrd

    baseline = json.loads(archived["reconciliation.json"])
    calendar = next(item for item in baseline["backer_requests"] if item["request"]["dataset"] == "TaiwanStockTradingDate")
    if calendar["status"] != "VALID_SAMPLE":
        raise ValueError("previous FinMind date evidence is unavailable")
    vendor = {iso_date(row["date"]) for row in json.loads(archived[calendar["original"]])["data"]}
    yearbooks = {year: yearbook_days(archived[f"official/yearbook-{year}.xls"], year) for year in (1999, 2000)}
    records, statuses, receipt = {}, {}, {}
    for source in sources:
        if not (raw := responses.get(source.name)):
            continue
        try:
            records[source] = monthly_rows(raw, source)
        except (KeyError, ValueError, TypeError, IndexError):
            statuses[source.name] = "HOLD_OFFICIAL_CONTRACT"
            continue
        statuses[source.name] = "VALID_OFFICIAL_ROWS" if records[source] else "EMPTY_OFFICIAL_REPORT"
    may = CANONICAL_SESSION.replace(day=1)
    sheet = xlrd.open_workbook(file_contents=archived["official/yearbook-1999.xls"]).sheet_by_index(0)
    yearbook_value = price_value(sheet.cell_value(YEARBOOK_TYPO.day + 3, YEARBOOK_TYPO.month))
    correction = verified_date_correction(yearbooks[1999], vendor, records.get(Source(may), {}),
                                          records.get(Source(may, ANCHORS[0]), {}), yearbook_value)
    if correction["status"] == "PASS_YEARBOOK_DATE_CORRECTION":
        yearbooks[1999].remove(YEARBOOK_TYPO)
        yearbooks[1999].add(CANONICAL_SESSION)
    aligned = {}
    for year, book_dates in yearbooks.items():
        reports = [records.get(Source(date(year, month, 1))) for month in range(1, 13)]
        official = {day for rows in reports if rows for day, row in rows.items() if row["traded"] and row["positive_price"]}
        reference = {day for day in vendor if day.year == year}
        common = official & reference & book_dates
        verified = all(reports) and official == reference == book_dates
        aligned[str(year)] = {"status": "PASS_THREE_SOURCES" if verified else "HOLD_INCOMPLETE_ALIGNMENT",
                              "official_months": sum(bool(rows) for rows in reports), "confirmed_dates": len(common)}
        receipt[str(year)] = {"confirmed_dates": [day.isoformat() for day in sorted(common, reverse=True)],
                              "market_only": sorted(str(day) for day in official - (reference | book_dates)),
                              "market_missing": sorted(str(day) for day in (reference | book_dates) - official)}

    try:
        annual = annual_turnover(responses.get(ANNUAL_MEMBER))
    except ValueError:
        annual = {}
        statuses[ANNUAL_MEMBER] = "HOLD_OFFICIAL_CONTRACT"
    if annual:
        statuses[ANNUAL_MEMBER] = "VALID_OFFICIAL_ROWS"
    early, early_dates, annual_receipt = {}, {}, {}
    for year in range(1994, 1999):
        dates = defaultdict(list)
        for source, rows in records.items():
            if source.symbol and source.month.year == year:
                for day, row in rows.items():
                    dates[day].append(row)
        traded = sorted(day for day, rows in dates.items() if any(row["traded"] and row["positive_price"] for row in rows))
        counts = supported_session_counts(*annual[year]) if year in annual else []
        confirmed = counts == [len(traded)] and len({day.month for day in traded}) == 12
        early[str(year)] = {"reported_dates": len(dates), "positive_trade_dates": len(traded),
                            "dates_reported_for_both_stocks": sum(len(rows) > 1 for rows in dates.values()),
                            "months_with_data": len({day.month for day in dates}),
                            "annual_reference_days": counts[0] if len(counts) == 1 else None,
                            "status": "PASS_SESSION_COUNT_RECONCILED" if confirmed else "HOLD_CALENDAR_COVERAGE"}
        early_dates[str(year)] = {day.isoformat(): dates[day] for day in sorted(dates, reverse=True)}
        if year in annual:
            annual_receipt[str(year)] = {"annual_amount_ntd_100m": str(annual[year][0]),
                                         "daily_average_ntd_100m": str(annual[year][1]),
                                         "compatible_session_count_range": [counts[0], counts[-1]] if counts else None}
    public = {"correction": {key: value for key, value in correction.items() if key != "evidence"},
              "calendars": aligned, "early_stock_coverage": early,
              "earliest_official_stock_report_date": min((day for rows in early_dates.values() for day in rows), default=None),
              "pre_1994_probe_outcomes": dict(Counter(statuses.get(source.name, "UNAVAILABLE") for source in sources
                                                      if source.month.year < 1994)),
              "first_official_annual_turnover_year": min(annual, default=None),
              "pre_1999_calendar": "HOLD_EARLY_YEARS_WITHOUT_COMPLETE_DATE_EVIDENCE"}
    evidence = {"correction_proof": correction, "calendar_receipt": receipt, "early_stock_dates": early_dates,
                "official_contract_statuses": statuses, "annual_turnover_reconciliation": annual_receipt,
                "earlier_private_digests": {name: hashlib.sha256(archived[name]).hexdigest() for name in (
                    calendar["original"], "official/yearbook-1999.xls", "official/yearbook-2000.xls")}}
    return public, evidence


def fetch_official(url):
    for delay in (0, 2, 4):
        if delay:
            time.sleep(delay)
        status, body = source_bytes(url)
        if status not in RETRYABLE:
            return status, body
    return status, body


def resume_sources(run_id, origins):
    archived = earlier_evidence(run_id)
    summary = json.loads(archived["reconciliation.json"])
    if summary["official_sources"] != origins or summary["source_run"] != EVIDENCE_RUN:
        raise ValueError("previous calendar archive belongs to a different source plan")
    return {name: ("REUSED_VERIFIED_SOURCE", archived[f"official/{name}"]) for name, status in
            summary["official_contract_statuses"].items() if status in {"VALID_OFFICIAL_ROWS", "EMPTY_OFFICIAL_REPORT"}}


def run(resume_from):
    archived = earlier_evidence()
    plan = source_plan()
    origins = {source.name: source.url for source in plan} | {ANNUAL_MEMBER: ANNUAL_SOURCE}
    fetched = resume_sources(resume_from, origins) if resume_from else {}
    names = [name for name in origins if name not in fetched]
    with ThreadPoolExecutor(max_workers=6) as pool:
        for offset in range(0, len(names), 6):
            batch = names[offset:offset + 6]
            results = list(pool.map(fetch_official, [origins[name] for name in batch]))
            fetched.update(zip(batch, results))
            if any(status == "HOLD_OFFICIAL_HTTP_429" for status, _ in results):
                break
    public, evidence = reconcile(archived, plan, {name: body for name, (_, body) in fetched.items()})
    report = public | {"status": "PASS_DIAGNOSTIC", "private_archive": "STAGED", "production": "HOLD",
                       "finmind_requests": 0, "source_run": EVIDENCE_RUN,
                       "source_fetch_outcomes": dict(Counter(status for status, _ in fetched.values())),
                       "source_contract_outcomes": dict(Counter(evidence["official_contract_statuses"].values())),
                       "requests_skipped": len(origins) - len(fetched), "reused_calendar_run": resume_from}
    save_private(fetched, origins, {}, report | evidence)
    return report


def verify():
    archived = earlier_evidence()
    access = drive_token()
    files = verified_archive_files(access, private_folder(access, create=False))
    plan = source_plan()
    responses = {source.name: files.get(f"official/{source.name}") for source in plan}
    responses[ANNUAL_MEMBER] = files.get(f"official/{ANNUAL_MEMBER}")
    public, evidence = reconcile(archived, plan, responses)
    expected = json.loads(files["reconciliation.json"])
    if any(expected[key] != value for key, value in (public | evidence).items()):
        raise ValueError("private trading calendar receipt did not reproduce from the original evidence")
    print(f"tpex_official_calendar_readback=PASS files={len(files)} correction={public['correction']['status']}")


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--verify-only", action="store_true")
    mode.add_argument("--resume-from", type=int)
    args = parser.parse_args()
    try:
        if args.verify_only:
            verify()
            return 0
        report = run(args.resume_from)
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
