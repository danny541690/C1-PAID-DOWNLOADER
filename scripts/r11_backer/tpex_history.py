"""Reconcile TPEx sessions, market transfers and historical Backer price gaps."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from zipfile import ZIP_DEFLATED, ZipFile
from zoneinfo import ZoneInfo

from .contract_closeout import dated_market_members
from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .exchange_scope import EXCHANGES, parse_official_close, price_value
from .history_pilot import iso_date
from .live_preflight import read_json
from .release_gaps import BULK_MAX_BYTES
from .release_probes import MAX_BYTES, RECOVERY_ROOT, Sample, Task, read_private_file, recovery_location, request_sample, run_parallel
from .release_remediation import direct_close_result, pinned_contracts
from .test_archive import confirmed_drive_zip, json_bytes


GAP_DAY = date(2010, 11, 24)
TRANSFERS = {
    "1597": date(2020, 12, 23), "5306": date(2022, 3, 8),
    "6446": date(2024, 1, 25), "8908": date(2026, 10, 16),
}
YEARBOOKS = {
    1999: "88/88指數.xls",
    2000: "89/35.圖1每日收市股價指數.xls",
}
EARLY_SAMPLES = [date.fromisoformat(day) for day in (
    "1995-11-21", "1996-11-21", "1998-12-02", "2003-08-04", "2007-01-02", "2007-07-02",
)]
EARLY_HISTORY = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/mi-pricing.html"
FACTBOOK = "https://www.tpex.org.tw/storage/publish/factbook/"
DELIST = "https://www.tpex.org.tw/www/zh-tw/company/deListed"
ARCHIVE = "market-identity-evidence.zip"
REPORT = Path("r11-tpex-history.json")


def close_url(venue, day):
    base, options, _, _ = EXCHANGES[venue]
    style = "%Y%m%d" if venue == "TWSE" else "%Y/%m/%d"
    return f"{base}?{urlencode(options | {'date': day.strftime(style), 'response': 'json'})}"


def source_bytes(url):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
    try:
        with urlopen(request, timeout=15) as result:
            data = result.read(MAX_BYTES + 1)
        if not 0 < len(data) <= MAX_BYTES:
            return "HOLD_OFFICIAL_SIZE", b""
        return "AVAILABLE", data
    except HTTPError as error:
        return f"HOLD_OFFICIAL_HTTP_{error.code}", b""
    except (URLError, TimeoutError):
        return "HOLD_OFFICIAL_NETWORK", b""


def close_rows(data, day, market):
    doc = json.loads(data)
    if doc.get("date") != day.strftime("%Y%m%d") or doc.get("stat", "").lower() != "ok":
        raise ValueError("wrong official report date")
    if not any(table.get("data") for table in doc.get("tables", [])):
        return "EMPTY_OFFICIAL_ENDPOINT", {}, {}
    closes = parse_official_close(doc, day, market)
    volume = {}
    if market == "TPEx":
        candidates = []
        for table in doc["tables"]:
            index = {name.strip(): i for i, name in enumerate(table.get("fields", []))}
            if {"成交股數", "成交筆數", "代號"} <= index.keys():
                candidates.append((index, table["data"]))
        if len(candidates) != 1:
            raise ValueError("TPEx report lacks a unique turnover table")
        index, rows = candidates[0]
        volume = {row[index["代號"]].strip(): positive_integer(row[index["成交股數"]])
                  for row in rows if row[index["代號"]].strip() in closes}
    return "VALID_OFFICIAL_DAY", closes, volume


def positive_integer(value):
    try:
        number = Decimal(str(value).replace(",", "").strip())
    except InvalidOperation:
        return None
    if number.is_finite() and 0 <= number == number.to_integral_value():
        return int(number)
    return None


def yearbook_days(data, year):
    import xlrd

    book = xlrd.open_workbook(file_contents=data, on_demand=True)
    sheet = book.sheet_by_index(0)
    caption = "".join(str(value) for value in sheet.row_values(0))
    label = {1999: "88年", 2000: "八十九年"}[year]
    if sheet.nrows < 35 or sheet.ncols != 13 or label not in caption or "每日收市股價指數" not in caption:
        raise ValueError("unexpected yearly index workbook")
    if sheet.row_values(2)[1:] != [f"{month}月" for month in range(1, 13)]:
        raise ValueError("missing or displaced month headers")
    days = set()
    for day in range(1, 32):
        row = sheet.row_values(day + 3)
        if row[0] != day:
            raise ValueError("invalid daily table")
        for month, value in enumerate(row[1:], 1):
            # A positive official closing index is a dated observation of a session.
            if price_value(value) is not None:
                days.add(date(year, month, day))
    if {day.month for day in days} != set(range(1, 13)):
        raise ValueError("yearbook has an unverified month")
    return days


def termination_rows(data, year):
    content = json.loads(data)
    if str(content.get("date")) != str(year) or content.get("stat", "").lower() != "ok" or len(content["tables"]) != 1:
        raise ValueError("wrong official termination year")
    table = content["tables"][0]
    columns = {key.strip(): pos for pos, key in enumerate(table["fields"])}
    result = []
    for row in table["data"]:
        code = str(row[columns["股票代號"]]).strip()
        if not re.fullmatch(r"[0-9]{4}", code):
            continue
        raw = str(row[columns["終止上櫃日期"]]).strip()
        match = re.fullmatch(r"(\d{2,4})[./-](\d{1,2})[./-](\d{1,2})", raw)
        if not match:
            raise ValueError("termination date missing an exact day")
        source_year, month, day = map(int, match.groups())
        effective = date(source_year if source_year >= 1911 else source_year + 1911, month, day)
        if effective.year != year:
            raise ValueError("termination year does not match filter")
        result.append({"code": code, "end": effective.isoformat(), "official_reason": row[columns["終止上櫃原因"]]})
    return result


def transfer_proof(code, day, quotes, sample):
    before = day - timedelta(days=1)
    observations = [(before, "TPEx", "TWSE"), (day, "TWSE", "TPEx")]
    if any(quotes.get((venue, effective), ("HOLD", {}, {}))[0] != "VALID_OFFICIAL_DAY"
           for effective, market, other in observations for venue in (market, other)):
        return "HOLD_TRANSFER_OFFICIAL_REFERENCE", []
    if any(quotes[market, effective][1].get(code) is None or code in quotes[other, effective][1]
           for effective, market, other in observations):
        return "HOLD_TRANSFER_MARKET_MEMBERSHIP", []
    if sample.status != "VALID_SAMPLE" or len(sample.rows) != 2 or {
        (row.get("stock_id"), row.get("date")) for row in sample.rows
    } != {(code, before.isoformat()), (code, day.isoformat())}:
        return "HOLD_TRANSFER_BACKER_IDENTITY", []
    rows = {row["date"]: row for row in sample.rows}
    for effective, venue, _ in observations:
        if price_value(rows[effective.isoformat()]["close"]) != quotes[venue, effective][1][code]:
            return "HOLD_TRANSFER_BACKER_PRICE", []
    periods = [(code, "tpex", before, day), (code, "twse", day, day + timedelta(days=1))]
    for effective, venue, _ in observations:
        if dated_market_members(periods, effective).get(code) != venue.lower():
            raise ValueError("verified dates route to wrong venue")
    evidence = [{"symbol": key, "market": venue, "start": start.isoformat(), "end_exclusive": end.isoformat()}
                for key, venue, start, end in periods]
    return "PASS_TRANSFER_PRICE_AND_VENUE_SAMPLE", evidence


def gap_status(task, sample, closing):
    if sample.status != "VALID_SAMPLE":
        return "HOLD_PROVIDER_WINDOW_EMPTY" if sample.status == "EMPTY_WINDOW" else sample.status
    if any(row.get("stock_id") != task.data_id for row in sample.rows):
        return "HOLD_GAP_UNEXPECTED_IDENTITY"
    rows = tuple(row for row in sample.rows if row.get("date") == GAP_DAY.isoformat())
    if not rows:
        return "HOLD_PROVIDER_DAY_MISSING_IN_WINDOW"
    exact = Task(task.dataset, GAP_DAY.isoformat(), GAP_DAY.isoformat(), task.data_id)
    status = direct_close_result(exact, Sample("VALID_SAMPLE", rows=rows), closing)
    return "PASS_RECOVERED_FROM_RANGE" if status == "PASS_DIRECT_CLOSE" else status


def private_folder(access, *, create):
    name = recovery_location(access, prefix="R11_TPEX_HISTORY")
    folder = ensure(RECOVERY_ROOT, name, FOLDER, access, create=create)
    if read_json(f"{BASE}/files/{folder}?fields=shared", access).get("shared") is not False:
        raise ValueError("private historical evidence folder is not verified")
    return folder


def save_private(public_payloads, source_urls, paid, details):
    files = {f"official/{name}": raw for name, (_, raw) in public_payloads.items() if raw}
    paid_index = []
    for number, (task, sample) in enumerate(paid.items(), 1):
        name = f"backer/response-{number}.json" if sample.raw else None
        paid_index.append({"request": task.parameters(), "status": sample.status, "original": name})
        if name:
            files[name] = sample.raw
    files["reconciliation.json"] = json_bytes(details | {"official_sources": source_urls, "backer_requests": paid_index})
    payload = BytesIO()
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive:
        for name, body in files.items():
            archive.writestr(name, body)
        archive.writestr("hashes.json", json_bytes({name: hashlib.sha256(body).hexdigest() for name, body in files.items()}))
    access = drive_token()
    confirmed_drive_zip(access, private_folder(access, create=True), ARCHIVE, payload.getvalue())


def verify_archive():
    access = drive_token()
    root = private_folder(access, create=False)
    file_id = ensure(root, ARCHIVE, "application/zip", access, create=False)
    if read_json(f"{BASE}/files/{file_id}?fields=shared", access).get("shared") is not False:
        raise ValueError("private evidence file is shared")
    with ZipFile(BytesIO(read_private_file(file_id, access))) as archive:
        expected = json.loads(archive.read("hashes.json"))
        if set(archive.namelist()) != set(expected) | {"hashes.json"} or archive.testzip() is not None:
            raise ValueError("incomplete historical evidence archive")
        if any(hashlib.sha256(archive.read(name)).hexdigest() != digest for name, digest in expected.items()):
            raise ValueError("historical evidence digest mismatch")
    print(f"tpex_identity_drive_readback=PASS files={len(expected)}")


def run():
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    today = datetime.now(ZoneInfo("Asia/Taipei")).date()
    info = read_json("https://api.web.finmindtrade.com/v2/user_info", auth)
    limit, used = info.get("api_request_limit"), info.get("user_count")
    if limit != 1600 or type(used) is not int or not 0 <= used <= limit - 7:
        raise RuntimeError("Backer account evidence unavailable")
    contracts = pinned_contracts()
    by_day = {(venue, day + timedelta(days=offset)) for day in TRANSFERS.values()
              for venue in EXCHANGES for offset in (-1, 0)}
    by_day |= {("TPEx", day) for day in [*EARLY_SAMPLES, *(GAP_DAY + timedelta(days=offset) for offset in (-1, 0, 1))]}
    quotes_sources = {key: f"{key[0]}-{key[1]}.json" for key in sorted(by_day)}
    years = range(GAP_DAY.year, today.year + 1)
    source_urls = {name: close_url(*key) for key, name in quotes_sources.items()}
    source_urls.update({f"yearbook-{year}.xls": FACTBOOK + quote(path) for year, path in YEARBOOKS.items()})
    source_urls.update({f"termination-{year}.json": f"{DELIST}?{urlencode({'date': year, 'response': 'json'})}" for year in years})
    market = Task("TaiwanStockPrice", GAP_DAY.isoformat(), GAP_DAY.isoformat())
    calendar = Task("TaiwanStockTradingDate", None, None)
    info_task = Task("TaiwanStockInfo", None, None)
    transfer_tasks = {code: Task("TaiwanStockPrice", (day - timedelta(days=1)).isoformat(), day.isoformat(), code)
                      for code, day in TRANSFERS.items()}
    initial = [market, calendar, info_task, *transfer_tasks.values()]

    def provider(task, contract, token):
        return request_sample(task, contract, token, max_bytes=BULK_MAX_BYTES if task == market else MAX_BYTES)

    with ThreadPoolExecutor(max_workers=8) as pool:
        public = {name: pool.submit(source_bytes, url) for name, url in source_urls.items()}
        paid = run_parallel(initial, contracts, auth, workers=len(initial), reader=provider)
        payloads = {name: future.result() for name, future in public.items()}

    quotes = {}
    for (venue, day), name in quotes_sources.items():
        status, data = payloads[name]
        try:
            quotes[venue, day] = close_rows(data, day, venue) if data else (status, {}, {})
        except (ValueError, KeyError, TypeError, IndexError):
            quotes[venue, day] = ("HOLD_OFFICIAL_LAYOUT", {}, {})
    before, after = (quotes["TPEx", GAP_DAY + timedelta(days=offset)] for offset in (-1, 1))
    official_status, old_closes, volumes = quotes["TPEx", GAP_DAY]
    got_reference = official_status == "VALID_OFFICIAL_DAY" and paid[market].status == "VALID_SAMPLE"
    existing = {row.get("stock_id") for row in paid[market].rows}
    gaps = sorted(code for code, close in old_closes.items() if code not in existing and close is not None) if got_reference else []
    pending = [Task("TaiwanStockPrice", (GAP_DAY - timedelta(days=15)).isoformat(),
                    (GAP_DAY + timedelta(days=15)).isoformat(), code) for code in gaps]
    throttled = any(sample.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for sample in paid.values())
    if pending and not throttled and used + len(initial) + len(pending) <= limit:
        paid.update(run_parallel(pending, contracts, auth))

    events = defaultdict(list)
    years_verified = []
    for year in years:
        _, document = payloads[f"termination-{year}.json"]
        try:
            rows = termination_rows(document, year)
        except (TypeError, ValueError, KeyError, IndexError):
            continue
        years_verified.append(year)
        for event in rows:
            events[event["code"]].append(event)

    snapshot_codes = {row.get("stock_id") for row in paid[info_task].rows} if paid[info_task].status == "VALID_SAMPLE" else None
    gap_details = []
    for task in pending:
        code = task.data_id
        later = [event for event in events[code] if event["end"] > GAP_DAY.isoformat()]
        gap_details.append({"code": code, "official_positive_volume": (volumes.get(code) or 0) > 0,
                            "neighbor_session_matches": sum((quoteset[2].get(code) or 0) > 0 for quoteset in (before, after)),
                            "termination_events_after_gap": later, "in_finmind_snapshot": code in snapshot_codes if snapshot_codes is not None else None,
                            "window_status": gap_status(task, paid.get(task, Sample("NOT_SENT")), old_closes[code])})

    transfers, periods = {}, []
    for code, day in TRANSFERS.items():
        verdict, intervals = transfer_proof(code, day, quotes, paid[transfer_tasks[code]])
        transfers[code] = {"transfer_date": day.isoformat(), "status": verdict}
        periods.extend(intervals)

    vendor_days = {iso_date(row["date"]) for row in paid[calendar].rows} if paid[calendar].status == "VALID_SAMPLE" else None
    sessions, calendars = {}, {}
    for year in YEARBOOKS:
        try:
            dates = yearbook_days(payloads[f"yearbook-{year}.xls"][1], year)
        except Exception:
            calendars[str(year)] = {"status": "HOLD_TPEX_YEARBOOK"}
            continue
        official_only = dates - vendor_days if vendor_days is not None else None
        sessions[str(year)] = [session.isoformat() for session in sorted(dates, reverse=True)]
        calendars[str(year)] = {"status": "PASS_OFFICIAL_TPEX_INDEX_SESSIONS", "sessions": len(dates),
                                "official_only_vs_finmind": len(official_only) if official_only is not None else None}

    save_private(payloads, source_urls, paid, {"tpex_yearbook_sessions": sessions, "confirmed_transfer_periods": periods,
                                             "historical_price_gaps": gap_details, "official_termination_years": years_verified})
    return {"status": "PASS_DIAGNOSTIC", "production": "HOLD", "private_archive": "STAGED",
            "calendars": calendars, "modern_quote_day_probes": {day.isoformat(): quotes["TPEx", day][0] for day in EARLY_SAMPLES},
            "quote_documentation": EARLY_HISTORY, "pre_1999_calendar": "HOLD_DAILY_OFFICIAL_EVIDENCE",
            "transfer_samples": transfers, "full_effective_dated_roster": "HOLD_UNVERIFIED_COVERAGE",
            "historical_gaps": {"day": GAP_DAY.isoformat(), "status": "GAPS_OBSERVED" if got_reference else "HOLD_BASELINE_REFERENCE",
                                "identified": len(gaps), "official_positive_volume": sum(item["official_positive_volume"] for item in gap_details),
                                "positive_neighbor_day": sum(item["neighbor_session_matches"] > 0 for item in gap_details),
                                "official_later_terminations": sum(bool(item["termination_events_after_gap"]) for item in gap_details),
                                "absent_from_finmind_snapshot": sum(item["in_finmind_snapshot"] is False for item in gap_details),
                                "termination_years_checked": len(years_verified),
                                "outcomes": dict(Counter(item["window_status"] for item in gap_details))},
            "finmind_requests": 1 + sum(sample.status != "NOT_SENT_RATE_LIMIT" for sample in paid.values())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.verify_only:
            verify_archive()
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
