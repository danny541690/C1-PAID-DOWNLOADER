"""Check official listing intervals and preserve independent TPEx fallback evidence."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .history_pilot import iso_date
from .live_preflight import read_json
from .release_probes import RECOVERY_ROOT
from .test_archive import json_bytes
from .tpex_history import (
    DELIST, EVIDENCE_RUN, GAP_DAY, close_rows, private_folder, save_private, source_bytes,
    verified_archive_files,
)


TWSE_START = "https://www.twse.com.tw/rwd/zh/company/newlisting?response=json"
TWSE_END = "https://www.twse.com.tw/company/suspendListingCsvAndHtml?lang=zh&startYear=&type=csv"
TPEX_START = "https://www.tpex.org.tw/www/zh-tw/company/latest"
ACTIVE_SOURCES = {
    "TWSE": ("https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
             ("出表日期", "公司代號", "公司名稱", "公司簡稱", "上市日期", "營利事業統一編號")),
    "TPEx": ("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
             ("Date", "SecuritiesCompanyCode", "CompanyName", "CompanyAbbreviation", "DateOfListing", "UnifiedBusinessNo.")),
}
FIRST_TPEX_TEST_YEAR = 1990  # The TPEx historical yearbook has records for this year.
REPORT = Path("r11-market-lifetimes.json")


@dataclass(frozen=True)
class Event:
    market: str
    code: str
    issuer: str
    day: date
    source: str
    transfer: bool = False


def digest(body):
    return hashlib.sha256(body).hexdigest()


def name_key(value):
    return "".join(unicodedata.normalize("NFKC", str(value)).split()).casefold()


def stock_code(value):
    return bool(re.fullmatch(r"[0-9]{4}", str(value)))


def official_date(value):
    value = str(value).strip().removeprefix("民國")
    if re.fullmatch(r"\d{7,8}", value):
        y, m, d = value[:-4], value[-4:-2], value[-2:]
    else:
        match = re.fullmatch(r"(\d{2,4})(?:年|[./-])(\d{1,2})(?:月|[./-])(\d{1,2})日?", value)
        if not match:
            raise ValueError("official calendar date is not exact")
        y, m, d = match.groups()
    return date(int(y) if len(y) == 4 else int(y) + 1911, int(m), int(d))


def event_rows(rows, fields, market, keys, source, today, *, year=None, transfer_field=None):
    fields = [field.strip() for field in fields]
    code_col, name_col, day_col = (fields.index(field) for field in keys)
    note = fields.index(transfer_field) if transfer_field else None
    result = []
    for position, row in enumerate(rows, 1):
        if not row:
            continue
        code, issuer, raw = str(row[code_col]).strip(), str(row[name_col]).strip(), row[day_col]
        if not stock_code(code) or not issuer or not raw:
            continue
        day = official_date(raw)
        if year is not None and day.year != year:
            raise ValueError("source report returned events from a different year")
        if day <= today:
            result.append(Event(market, code, issuer, day, f"{source}#{position}",
                                note is not None and "櫃轉市" in str(row[note])))
    return result


def twse_events(raw, today, *, start):
    if start:
        doc = json.loads(raw)
        if doc.get("stat", "").lower() != "ok":
            raise ValueError("new TWSE listing report unavailable")
        return event_rows(doc["data"], doc["fields"], "TWSE", ("公司代號", "公司簡稱", "股票上市買賣日期"),
                          "twse-listed.json", today, transfer_field="備註")
    rows = list(csv.reader(StringIO(raw.decode("cp950"))))
    if rows[0] != ["終止上市公司"]:
        raise ValueError("incorrect TWSE terminal event report")
    return event_rows(rows[2:], rows[1], "TWSE",
                      ("上市編號", "公司名稱", "終止上市日期"), "twse-ended.csv", today)


def tpex_events(raw, year, today, *, start):
    doc = json.loads(raw)
    if str(doc.get("date")) != str(year) or doc.get("stat", "").lower() != "ok" or len(doc["tables"]) != 1:
        raise ValueError("TPEx event report did not identify the requested year")
    table = doc["tables"][0]
    key = "上櫃日期" if start else "終止上櫃日期"
    source = f"tpex-listed-{year}.json" if start else f"tpex-ended-{year}.json"
    return event_rows(table["data"], table["fields"], "TPEx", ("股票代號", "公司名稱", key), source, today, year=year)


def active_rows(raw, market, today):
    fields = ACTIVE_SOURCES[market][1]
    if not (table := json.loads(raw)) or not isinstance(table, list):
        raise ValueError("active company snapshot missing")
    result = []
    for position, row in enumerate(table, 1):
        observed, code, name, alias, start, identifier = (str(row[field]).strip() for field in fields)
        if not stock_code(code) or not start or not name or not observed:
            continue
        begin, as_of = official_date(start), official_date(observed)
        if begin <= as_of <= today:
            result.append({"market": market, "code": code, "issuer": name, "alias": alias,
                           "company_id": identifier, "start": begin.isoformat(), "as_of": as_of.isoformat(),
                           "source": f"{market.lower()}-active-projection.json#{position}"})
    if not result:
        raise ValueError("active company snapshot has no usable date evidence")
    return result


def identity_intervals(starts, exits, active):
    beginning, ending = defaultdict(list), defaultdict(list)
    for event in starts:
        beginning[event.market, event.code].append(event)
    for event in exits:
        ending[event.market, event.code].append(event)
    rows, holds = [], []
    for key, events in ending.items():
        boundaries = sorted({event.day for event in events})
        for end in events:
            prior = max((day for day in boundaries if day < end.day), default=date.min)
            possible = [start for start in beginning[key] if prior <= start.day < end.day]
            identities = {(start.day, name_key(start.issuer)) for start in possible}
            matches = [start for start in possible if name_key(start.issuer) == name_key(end.issuer)]
            if len(identities) != 1 or not matches:
                holds.append({"market": end.market, "code": end.code, "source": end.source,
                              "reason": "HOLD_UNPROVEN_HISTORICAL_START_OR_IDENTITY"})
                continue
            first = matches[0]
            rows.append({"market": end.market, "code": end.code, "issuer": end.issuer,
                         "start": first.day.isoformat(), "end_exclusive": end.day.isoformat(),
                         "kind": "OFFICIAL_CLOSED_INTERVAL", "proof": [first.source, end.source]})
    for row in active:
        key = row["market"], row["code"]
        begin, as_of = iso_date(row["start"]), iso_date(row["as_of"])
        if any(begin <= event.day <= as_of for event in ending[key]):
            holds.append({"market": key[0], "code": key[1], "source": row["source"],
                          "reason": "HOLD_ACTIVE_SNAPSHOT_CONFLICTS_WITH_TERMINATION"})
            continue
        rows.append({"market": key[0], "code": key[1], "issuer": row["issuer"], "start": row["start"],
                     "end_exclusive": (as_of + timedelta(days=1)).isoformat(),
                     "kind": "OFFICIAL_ACTIVE_THROUGH_PUBLICATION", "proof": [row["source"]]})
    accepted, overlaps = nonoverlapping(rows)
    return accepted, holds + overlaps


def nonoverlapping(intervals):
    by_code = defaultdict(list)
    for interval in intervals:
        by_code[interval["code"]].append(interval)
    accepted, held = [], []
    for rows in by_code.values():
        rows.sort(key=lambda row: (row["start"], row["end_exclusive"], row["market"]))
        conflicts = set()
        for index, first in enumerate(rows):
            for later in range(index + 1, len(rows)):
                if rows[later]["start"] >= first["end_exclusive"]:
                    break
                conflicts.update((index, later))
        for index, row in enumerate(rows):
            if index in conflicts:
                held.append(row | {"reason": "HOLD_OVERLAPPING_MARKET_OR_ISSUER_INTERVALS"})
            else:
                accepted.append(row)
    return sorted(accepted, key=lambda row: (row["code"], row["start"], row["market"])), held


def handovers(starts, exits):
    closing = defaultdict(list)
    for event in exits:
        if event.market == "TPEx":
            closing[event.code, event.day].append(event)
    result = {}
    for event in starts:
        match = closing.get((event.code, event.day), [])
        if event.market == "TWSE" and event.transfer and len(match) == 1:
            result[event.code, event.day] = {"code": event.code, "effective": event.day.isoformat(),
                                             "source_market": "TPEx", "destination_market": "TWSE",
                                             "proof": [match[0].source, event.source]}
    return result


def fallback_receipts(raw, archived_gaps):
    status, closes, volumes = close_rows(raw, GAP_DAY, "TPEx")
    if status != "VALID_OFFICIAL_DAY":
        raise ValueError("original TPEx day is unavailable")
    tables = json.loads(raw)["tables"]
    table = next(item for item in tables if {"代號", "收盤", "成交股數"} <= {name.strip() for name in item["fields"]})
    headers = [name.strip() for name in table["fields"]]
    code_col = headers.index("代號")
    positions = defaultdict(list)
    for index, row in enumerate(table["data"]):
        positions[row[code_col].strip()].append(index)
    receipts = {}
    for gap in archived_gaps:
        code = gap["code"]
        if gap["window_status"] != "HOLD_PROVIDER_WINDOW_EMPTY" or not volumes.get(code) or closes.get(code) is None:
            continue
        if len(positions[code]) != 1:
            raise ValueError("official fallback has an ambiguous row locator")
        identity = {"source": "TPEx", "dataset": "official_daily_close", "symbol": code, "date": GAP_DAY.isoformat()}
        key = digest(json_bytes(identity))
        position = positions[code][0]
        row = table["data"][position]
        manifest = identity | {"key": key, "official_status": "VERIFIED_FALLBACK", "backer_status": "HOLD_SOURCE_GAP",
                               "source_run": EVIDENCE_RUN, "original_member": f"official/TPEx-{GAP_DAY}.json",
                               "original_sha256": digest(raw), "source_row_index": position,
                               "source_row_sha256": digest(json_bytes(row))}
        if key in receipts and receipts[key] != manifest:
            raise ValueError("same official fallback key has different evidence")
        receipts[key] = manifest
    return sorted(receipts.values(), key=lambda row: row["symbol"])


def earlier_evidence():
    access = drive_token()
    parent = ensure(RECOVERY_ROOT, f"R11_TPEX_HISTORY_{EVIDENCE_RUN}_1", FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{parent}?fields=shared", access).get("shared") is not False:
        raise ValueError("previous test evidence folder is shared")
    return verified_archive_files(access, parent)


def official_payloads(today, archived):
    urls = {"twse-listed.json": TWSE_START, "twse-ended.csv": TWSE_END}
    known = {}
    for market, (url, _) in ACTIVE_SOURCES.items():
        urls[f"{market.lower()}-active-projection.json"] = url
    for year in range(FIRST_TPEX_TEST_YEAR, today.year + 1):
        stated = f"tpex-ended-{year}.json"
        urls[f"tpex-listed-{year}.json"] = f"{TPEX_START}?{urlencode({'date': year, 'response': 'json'})}"
        urls[stated] = f"{DELIST}?{urlencode({'date': year, 'response': 'json'})}"
        if original := archived.get(f"official/termination-{year}.json"):
            known[stated] = ("REUSED_VERIFIED_SOURCE", original)
    queue = [(name, url) for name, url in urls.items() if name not in known]
    outcomes = dict(known)
    with ThreadPoolExecutor(max_workers=6) as pool:
        for start in range(0, len(queue), 6):
            batch = queue[start:start + 6]
            values = list(pool.map(source_bytes, [url for _, url in batch]))
            outcomes.update(zip((name for name, _ in batch), values))
            if any(status == "HOLD_OFFICIAL_HTTP_429" for status, _ in values):
                break
    return urls, outcomes, known.keys()


def run():
    today = datetime.now(ZoneInfo("Asia/Taipei")).date()
    previous = earlier_evidence()
    baseline = json.loads(previous["reconciliation.json"])
    raw_close = previous[f"official/TPEx-{GAP_DAY}.json"]
    gaps = baseline["historical_price_gaps"]
    receipts = fallback_receipts(raw_close, gaps)
    expected_codes = {gap["code"] for gap in gaps if gap["window_status"] == "HOLD_PROVIDER_WINDOW_EMPTY"}
    if not expected_codes or {row["symbol"] for row in receipts} != expected_codes:
        raise ValueError("official fallback coverage differs from the archived provider gaps")
    if receipts != fallback_receipts(raw_close, list(reversed(gaps))):
        raise ValueError("fallback IDs depend on input order")
    urls, received, reused = official_payloads(today, previous)
    starts, exits, current = [], [], []
    stored, sources = {}, {}
    for name in urls:
        status, raw = received.get(name, ("NOT_SENT_OFFICIAL_RATE_LIMIT", b""))
        if not raw:
            sources[name] = {"status": status}
            continue
        try:
            if name.endswith("-active-projection.json"):
                market = "TWSE" if name.startswith("twse") else "TPEx"
                observations = active_rows(raw, market, today)
                payload = json_bytes(observations)
                current.extend(observations)
            else:
                is_start = "listed" in name
                if name.startswith("twse"):
                    observations = twse_events(raw, today, start=is_start)
                else:
                    year = int(name.rsplit("-", 1)[1].split(".", 1)[0])
                    observations = tpex_events(raw, year, today, start=is_start)
                (starts if is_start else exits).extend(observations)
                payload = raw
        except (UnicodeError, ValueError, TypeError, IndexError, KeyError):
            sources[name] = {"status": "HOLD_OFFICIAL_EVENT_CONTRACT", "original_sha256": digest(raw)}
            if not name.endswith("-active-projection.json"):
                stored[name] = (status, raw)
            continue
        sources[name] = {"status": status if observations else "EMPTY_OFFICIAL_EVENT_REPORT", "events": len(observations),
                         "original_sha256": digest(raw), "stored_sha256": digest(payload)}
        stored[name] = (status, payload)
    accepted, held = identity_intervals(starts, exits, current)
    moves = handovers(starts, exits)
    matched = sum((item["symbol"], iso_date(item["start"])) in moves
                  for item in baseline["confirmed_transfer_periods"] if item["market"] == "twse")
    interval_counts = Counter((row["market"], row["kind"]) for row in accepted)
    report = {"status": "PASS_DIAGNOSTIC", "private_archive": "STAGED", "production": "HOLD",
              "source_run": EVIDENCE_RUN, "finmind_requests": 0,
              "official_reports_requested": len(received) - len(reused), "official_reports_reused": len(reused),
              "source_outcomes": dict(Counter(item["status"] for item in sources.values())),
              "first_observed_official_active_listing": {market: min((row["start"] for row in current if row["market"] == market), default=None)
                                                         for market in ACTIVE_SOURCES},
              "accepted_intervals": {market: {kind: count for (venue, kind), count in interval_counts.items() if venue == market}
                                     for market in ACTIVE_SOURCES},
              "held_intervals": dict(Counter(row["reason"] for row in held)),
              "corroborated_transfer_dates": len(moves), "prior_live_transfers_reconciled": matched,
              "official_fallback_receipts": len(receipts), "backer_gaps_kept_open": len(baseline["historical_price_gaps"]),
              "full_listing_history": "HOLD_UNVERIFIED_BOUNDARIES"}
    save_private(stored, urls, {}, report | {"sources": sources, "listing_intervals": accepted, "held_intervals_detail": held,
                                              "handover_events": list(moves.values()), "official_fallback": receipts})
    return report


def verify():
    previous = earlier_evidence()
    baseline = json.loads(previous["reconciliation.json"])
    access = drive_token()
    files = verified_archive_files(access, private_folder(access, create=False))
    current = json.loads(files["reconciliation.json"])
    expected = fallback_receipts(previous[f"official/TPEx-{GAP_DAY}.json"], baseline["historical_price_gaps"])
    if not expected or expected != current["official_fallback"]:
        raise ValueError("official fallback readback does not match original evidence")
    intervals, collisions = nonoverlapping(current["listing_intervals"])
    if not intervals or collisions or intervals != current["listing_intervals"]:
        raise ValueError("dated interval readback contains unresolved overlaps")
    print(f"market_lifetime_drive_readback=PASS intervals={len(intervals)} fallback_rows={len(expected)}")


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
    if out := os.environ.get("GITHUB_OUTPUT"):
        with open(out, "a", encoding="utf-8") as stream:
            stream.write(f"archive={report.get('private_archive', 'HOLD')}\n")
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
