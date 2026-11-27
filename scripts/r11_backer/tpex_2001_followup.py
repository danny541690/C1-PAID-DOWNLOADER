"""Verify the 2001 market transition and cross-year TPEx cases from official captures."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import os
from pathlib import Path
import re
import subprocess

from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .history_pilot import TWSE_CALENDAR, iso_date, parse_twse_sessions
from .listing_crosscheck import trade_rows
from .live_preflight import read_json
from .market_lifetimes import digest, earlier_evidence, name_key, twse_events
from .release_probes import RECOVERY_ROOT
from .test_archive import json_bytes
from .tpex_calendar import Source, monthly_rows
from .tpex_closeout import LIFETIME_RUN
from .tpex_delisting import complete_rows, delisting_url
from .tpex_history import private_folder, save_private, verified_archive_files


PRIOR_RUN = 45742569508
INPUT_FOLDER = f"R11_TPEX_CUTOVER_INPUT_{PRIOR_RUN}_1"
INPUT_HASH = "9e809950d22978a74cddccbf758c289bf735f3fd0ae0fa2dc702a787cffa03ea"
REPORT = Path("r11-tpex-2001-followup.json")
ANNUAL = "twse-2001-listing-annual.pdf"
ANNUAL_URL = "https://wwwc.twse.com.tw/downloads/zh/about/company/Annual/90/23.pdf"
TWSE_SESSIONS = "twse-2001-09-index.json"
TERMINATIONS = "tpex-termination-2001-0.json"
SEPTEMBER = date(2001, 9, 1)
DECEMBER_CLOSE = date(2001, 12, 31)
JANUARY_OPEN = date(2002, 1, 2)
SAMPLE_TRANSFERS = ("1534", "1731", "2459", "2489", "2492")
SAMPLE_CONTINUING = ("4531", "5496", "6105")
GAP = "HOLD_DELISTING_YEAR_CONTRADICTION"
LISTING_ROW = re.compile(r"普通股股票\s+((?:\d\s*){4})\s+(\d(?: ?\d)?)\s*月\s*(\d(?: ?\d)?)\s*日")


def listed_cohort(directory, layout):
    annual = {}
    for position, line in enumerate(layout.splitlines(), 1):
        for match in LISTING_ROW.finditer(line):
            code, mm, dd = ("".join(field.split()) for field in match.groups())
            key = (code, date(2001, int(mm), int(dd)))
            if key in annual:
                raise ValueError("duplicate annual listing entry")
            annual[key] = f"{ANNUAL}#layout-line={position}"
    listings = [event for event in twse_events(directory, date(2001, 12, 31), start=True) if event.day.year == 2001]
    keys = {(event.code, event.day) for event in listings}
    if len(listings) != 69 or len(keys) != len(listings) or keys != annual.keys():
        raise ValueError("annual and directory listing records disagree")
    transfers = [event for event in listings if event.transfer]
    if len(transfers) != 48 or len({event.day for event in transfers}) != 1:
        raise ValueError("2001 TWSE transfer cohort does not reconcile")
    return transfers, annual


def report_bytes(packet, name, url):
    raw = packet[f"official/{name}"]
    receipt = json.loads(packet["reconciliation.json"])["captures"][name]
    if receipt != {"url": url, "sha256": digest(raw)}:
        raise ValueError("official capture provenance does not reconcile")
    return raw


def official_quote(packet, source):
    raw = report_bytes(packet, source.name, source.url)
    status, _, dated = trade_rows(raw, source)
    document = json.loads(raw)
    if (document["name"] == "" and len(document["tables"]) == 1
            and document["tables"][0]["title"] == "個股日成交資訊"
            and document["tables"][0]["totalCount"] == 0 and not monthly_rows(raw, source)):
        return "", {}
    if status != "VALID":
        raise ValueError("official stock quote contract changed")
    return name_key(document["name"]), {day: record["source"] for day, record in dated}


def cross_year_case(packet, row, decision):
    starts = {iso_date(item["date"]) for item in decision["listing_evidence"]}
    if len(starts) != 1 or not decision["terminal_proof"]:
        raise ValueError("pending case has no single dated listing or terminal evidence")
    listed, = starts
    if listed.year != 2001 or iso_date(row["end_exclusive"]) <= JANUARY_OPEN:
        raise ValueError("pending case is outside the cross-year test window")
    months = sorted({day.replace(day=1) for day in (listed, DECEMBER_CLOSE, JANUARY_OPEN)})
    quotes = {month: official_quote(packet, Source(month, row["code"])) for month in months}
    names = {name for name, _ in quotes.values()}
    first_quotes = quotes[listed.replace(day=1)][1]
    if len(names) != 1 or "" in names:
        raise ValueError("the publisher's security name changed within the cross-year probe")
    return row | {
        "listing_evidence": decision["listing_evidence"],
        "first_trade_observed": str(min(first_quotes)) if first_quotes else None,
        "listing_day_quote": first_quotes.get(listed),
        "december_close_quote": quotes[DECEMBER_CLOSE.replace(day=1)][1].get(DECEMBER_CLOSE),
        "january_open_quote": quotes[JANUARY_OPEN.replace(day=1)][1].get(JANUARY_OPEN),
        "continuity_evidence": "DATED_QUOTES_ONLY",
    }


def reconcile(previous, lifetime, packet):
    ledger = json.loads(previous["reconciliation.json"])
    manifest = json.loads(packet["reconciliation.json"])
    pending = ledger["pending_listing_ledger"]
    by_market = dict(Counter(row["market"] for row in pending))
    if (digest(packet["reconciliation.json"]) != INPUT_HASH or manifest["prior_run"] != PRIOR_RUN
            or ledger["status"] != "PASS_DIAGNOSTIC"
            or ledger["production"] != "HOLD" or ledger["pending_by_market"] != by_market
            or ledger["historic_intervals_pending_after"] != len(pending)
            or len({row["end_source"] for row in pending}) != len(pending)):
        raise ValueError("historical input ledgers do not reconcile")
    annual = report_bytes(packet, ANNUAL, ANNUAL_URL)
    layout = subprocess.run(["pdftotext", "-layout", "-enc", "UTF-8", "-", "-"], input=annual,
                            capture_output=True, check=True, timeout=15).stdout.decode("utf-8")
    transfers, annual_rows = listed_cohort(lifetime["official/twse-listed.json"], layout)
    listed_day = transfers[0].day
    twse_calendar = report_bytes(packet, TWSE_SESSIONS, f"{TWSE_CALENDAR}?response=json&date=20010901")
    tpex_month = Source(SEPTEMBER)
    tpex_calendar = report_bytes(packet, tpex_month.name, tpex_month.url)
    twse_days = parse_twse_sessions(json.loads(twse_calendar), SEPTEMBER)
    tpex_days = {day for day, row in monthly_rows(tpex_calendar, tpex_month).items() if row["traded"]}
    next_session = min(day for day in twse_days & tpex_days if day >= listed_day)
    if listed_day in twse_days | tpex_days or next_session != date(2001, 9, 19):
        raise ValueError("calendar archives disagree with the reported September cutover")
    if not re.search(r"九月\s+十九日：\s*店頭市場\s*48\s*家上櫃公司轉至證券集中市場上市", layout):
        raise ValueError("annual market chronology does not identify the cohort's trading transition")
    terminations = report_bytes(packet, TERMINATIONS, delisting_url(2001))
    _, terminal_rows = complete_rows({0: terminations}, 2001)
    if terminal_rows or 2001 not in ledger["historical_coverage_contradiction_years"]:
        raise ValueError("the 2001 TPEx termination gap requires new reconciliation")
    if not set(SAMPLE_TRANSFERS) <= {event.code for event in transfers}:
        raise ValueError("September quote control is not in the documented cohort")
    september_checks = {}
    for code in (*SAMPLE_TRANSFERS, *SAMPLE_CONTINUING):
        _, quotes = official_quote(packet, Source(SEPTEMBER, code))
        september_checks[code] = {"quote_count": len(quotes), "cutover_session_proof": quotes.get(next_session)}
    gap_rows = [row for row in pending if row["disposition"] == GAP]
    if not gap_rows or any(row["market"] != "TPEx" for row in gap_rows):
        raise ValueError("verified TPEx gap cases are unavailable")
    cases = [cross_year_case(packet, row, ledger["case_decisions"][row["end_source"]]) for row in gap_rows]
    report = {
        "status": "PASS_DIAGNOSTIC", "production": "HOLD", "finmind_requests": 0,
        "historical_coverage_contradiction_years": ledger["historical_coverage_contradiction_years"],
        "historic_intervals_pending_after": len(pending), "pending_by_market": by_market,
        "verified_twse_listing_rows": len(annual_rows), "verified_transfer_listing_rows": len(transfers),
        "twse_directory_listing_day": str(listed_day), "first_trading_session_at_cutover": str(next_session),
        "tpex_full_year_termination_rows": len(terminal_rows),
        "transfer_monthly_probes_empty": sum(not september_checks[code]["quote_count"] for code in SAMPLE_TRANSFERS),
        "continuing_symbols_trading_at_cutover": sum(bool(september_checks[code]["cutover_session_proof"])
                                                     for code in SAMPLE_CONTINUING),
        "cross_year_cases_checked": len(cases),
        "cross_year_cases_with_both_quotes": sum(bool(row["december_close_quote"] and row["january_open_quote"])
                                               for row in cases),
        "cross_year_cases_with_listing_day_quote": sum(bool(row["listing_day_quote"]) for row in cases),
    }
    details = {
        "pending_listing_ledger": pending, "prior_history_run": PRIOR_RUN, "twse_lifetime_run": LIFETIME_RUN,
        "prior_history_sha256": digest(previous["reconciliation.json"]),
        "twse_directory_sha256": digest(lifetime["official/twse-listed.json"]),
        "private_input_folder": INPUT_FOLDER, "capture_manifest_sha256": digest(packet["reconciliation.json"]),
        "cohort": [{"code": event.code, "issuer": event.issuer, "listing_day": str(event.day),
                    "proof": [event.source, annual_rows[event.code, event.day]],
                    "tpex_terminal_date": None, "tpex_terminal_disposition": "HOLD_2001_TPEX_TERMINATION_NOTICE"}
                   for event in transfers],
        "cross_year_cases": cases, "september_quote_checks": september_checks,
    }
    return report, details


def staged_packet():
    access = drive_token()
    root = ensure(RECOVERY_ROOT, INPUT_FOLDER, FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{root}?fields=shared", access).get("shared") is not False:
        raise ValueError("the 2001 input folder is shared")
    return verified_archive_files(access, root)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            previous = pool.submit(earlier_evidence, PRIOR_RUN)
            lifetime = pool.submit(earlier_evidence, LIFETIME_RUN)
            packet = pool.submit(staged_packet)
            report, details = reconcile(previous.result(), lifetime.result(), packet.result())
        result = report | details
        if args.verify_only:
            access = drive_token()
            saved = verified_archive_files(access, private_folder(access, create=False))
            expected = json.loads(saved["reconciliation.json"])
            if json_bytes(result) != json_bytes({key: expected[key] for key in result}):
                raise ValueError("private readback did not reproduce the 2001 cutover test")
            print("tpex_2001_followup_drive_readback=PASS")
        else:
            save_private({}, {}, {}, result)
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
    if not args.verify_only:
        REPORT.write_bytes(json_bytes(report))
        if path := os.environ.get("GITHUB_OUTPUT"):
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(f"archive={'STAGED' if report['status'] == 'PASS_DIAGNOSTIC' else 'HOLD'}\n")
    print(json.dumps(report, sort_keys=True))
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
