"""Reconcile pending market listings with archived yearbooks and documented code changes."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from html import unescape
import json
import os
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from urllib.parse import quote
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

from .drive_pilot import token as drive_token
from .history_pilot import iso_date
from .market_lifetimes import digest, earlier_evidence, name_key, nonoverlapping, official_date, stock_code, twse_events
from .test_archive import json_bytes
from .tpex_calendar import fetch_official
from .tpex_closeout import LIFETIME_RUN, archived_events
from .tpex_history import FACTBOOK, private_folder, save_private, verified_archive_files


PREVIOUS_RUN = 45716698773
YEARBOOKS = {
    1999: ("tpex-new-listings-1999.doc", "88/T1.doc", "表一、八十八年度新上櫃公司資料", ("編號", "公司名稱", "上櫃日期")),
    2000: ("tpex-new-listings-2000.doc", "89/01.表1新上櫃公司資料.doc", "表一、89年度一般類股及第二類股新上櫃公司資料",
           ("股票代號", "公司名稱", "掛牌日期")),
}
TWSE_CHRONICLE = "twse-anniversary-listings.pdf"
ISSUER_CHAIN = "tpex-industry-chain-2301.html"
MERGER_NOTICE = "liteon-merger-announcement.html"
TPEX_CHRONICLE = "tpex-market-chronology.html"
REPORT = Path("r11-market-chronology.json")
SOURCE_URLS = {
    TWSE_CHRONICLE: "https://wwwc.twse.com.tw/staticFiles/product/publication/twse60/P17.pdf",
    ISSUER_CHAIN: "https://ic.tpex.org.tw/company_data.php?column=vision_story&table=company_vision&userId=2301",
    MERGER_NOTICE: "https://www.liteon.com/zh-tw/news/press-center/content/413",
    TPEX_CHRONICLE: "https://www.tpex.org.tw/storage/tpex30/history.html",
} | {name: FACTBOOK + quote(path) for name, path, _, _ in YEARBOOKS.values()}
PENDING = "HOLD_UNPROVEN_HISTORICAL_START_OR_IDENTITY"
ACTIVE_CONFLICT = "HOLD_ACTIVE_SNAPSHOT_CONFLICTS_WITH_TERMINATION"


def parse_listing_book(xml, year):
    name, _, heading, labels = YEARBOOKS[year]
    book = ElementTree.fromstring(xml)
    if name_key(heading) not in name_key("".join(book.itertext())):
        raise ValueError("listing book caption does not match the selected year")
    tables = book.findall(".//informaltable")
    if len(tables) != 1:
        raise ValueError("listing book has no unique listing table")
    rows = tables[0].findall(".//row")
    headers = [name_key("".join(cell.itertext())) for cell in rows[0].findall("entry")]
    code_index, name_index, day_index = (headers.index(label) for label in labels)
    indexed = {}
    for position, row in enumerate(rows[1:], 2):
        values = ["".join(cell.itertext()).strip() for cell in row.findall("entry")]
        if len(values) <= max(code_index, name_index, day_index) or not stock_code(values[code_index]):
            continue
        code, issuer, day = values[code_index], values[name_index], official_date(values[day_index])
        if code in indexed or not issuer or day.year != year:
            raise ValueError("ambiguous or undated official listing row")
        indexed[code] = {"code": code, "issuer": issuer, "start": day.isoformat(), "source": f"{name}#{position}"}
    if not indexed:
        raise ValueError("listing book has no verified securities")
    return indexed


def read_listing_book(raw, year):
    if not raw.startswith(bytes.fromhex("d0cf11e0a1b11ae1")):
        raise ValueError("unexpected Word document format")
    with TemporaryDirectory() as directory:
        path = Path(directory) / "official-listing.doc"
        path.write_bytes(raw)
        # DocBook preserves table-cell boundaries. Plain text wraps two-digit dates.
        xml = subprocess.run(["antiword", "-x", "db", str(path)], capture_output=True,
                             check=True, timeout=25).stdout
    return parse_listing_book(xml, year)


def anniversary_events(raw):
    text = subprocess.run(["pdftotext", "-enc", "UTF-8", "-raw", "-", "-"], input=raw,
                          capture_output=True, check=True, timeout=25).stdout.decode()
    year = None
    entries = []
    for row in text.splitlines():
        line = "".join(row.split())
        if match := re.fullmatch(r"(\d{2,3})年", line):
            year = int(match[1]) + 1911
            continue
        match = re.match(r"(\d{1,2})月(\d{1,2})日[︰：:](.+)", line)
        if not match or not year:
            continue
        day = date(year, int(match[1]), int(match[2]))
        for segment in re.split(r"[，；;]", match[3]):
            if segment.endswith("終止上市"):
                action = "ENDED"
                names = segment.removesuffix("終止上市")
            elif segment.endswith("上市"):
                action = "LISTED"
                names = segment.removesuffix("上市")
            else:
                continue
            if re.fullmatch(r"[\u3400-\u9fffA-Za-z0-9、－\-]+", names):
                entries.extend((day, action, name) for name in names.split("、") if name)
    return entries


def unique_chronicle_day(events, action, company):
    dates = {day for day, operation, issuer in events if operation == action and issuer == company}
    if len(dates) != 1:
        raise ValueError("anniversary source has no unique dated event")
    return dates.pop()


def page_text(raw):
    return "".join(unescape(re.sub(r"<[^>]*>", " ", raw.decode("utf-8"))).split())


def verify_liteon(ledger, twse_profiles, exits, chronicle, chain_raw, notice_raw):
    timeline, notice = page_text(chain_raw), page_text(notice_raw)
    if not all(text in timeline for text in ("以源興科技為存續公司", "同時沿用股票代號2301",
                                             "股票代號為2346", "上市公司，股票代號2301")):
        raise ValueError("issuer timeline lacks both original code assignments and the merger")
    if not all(text in notice for text in ("2002/10/06", "以源興為存續公司", "十一月四日為合併基準日",
                                           "新股權利証書上市交易日")):
        raise ValueError("issuer announcement lacks the dated exchange event")
    begin = unique_chronicle_day(chronicle, "LISTED", "光寶電子")
    survivor = unique_chronicle_day(chronicle, "LISTED", "源興科技")
    handover = unique_chronicle_day(chronicle, "ENDED", "光寶電子")
    boundary = date(2002, 11, 4)
    end = next(event for event in exits if event.code == "2301" and event.issuer == "光寶電子" and event.day == handover)
    profile = next(row for row in twse_profiles if row["code"] == "2301" and row["issuer"] == "光寶科技股份有限公司")
    holds = {row["source"] for row in ledger["held_intervals_detail"] if row["reason"] == ACTIVE_CONFLICT}
    if (handover != boundary or iso_date(profile["start"]) != survivor or profile["source"] not in holds
            or not begin < survivor < boundary <= iso_date(profile["as_of"])):
        raise ValueError("issuer profile or effective date disagrees with the preserved sources")
    date_bound = boundary.isoformat()
    witness = [ISSUER_CHAIN, TWSE_CHRONICLE, MERGER_NOTICE, end.source, profile["source"]]
    return [
        {"code": "2301", "market": "TWSE", "issuer": "光寶電子", "start": begin.isoformat(),
         "end_exclusive": date_bound, "kind": "OFFICIAL_PREDECESSOR_CODE_ASSIGNMENT", "proof": witness,
         "exiting_source": end.source},
        {"code": "2346", "market": "TWSE", "issuer": "源興科技", "start": survivor.isoformat(),
         "end_exclusive": date_bound, "kind": "OFFICIAL_SURVIVOR_PREVIOUS_CODE_ASSIGNMENT", "proof": witness,
         "company_id": profile["company_id"]},
        {"code": "2301", "market": "TWSE", "issuer": profile["issuer"], "start": date_bound,
         "end_exclusive": (iso_date(profile["as_of"]) + timedelta(days=1)).isoformat(),
         "kind": "OFFICIAL_SURVIVOR_CURRENT_CODE_ASSIGNMENT", "proof": witness,
         "company_id": profile["company_id"], "resolved_profile_source": profile["source"]},
    ]


def legal_stem(issuer):
    return name_key(issuer).removesuffix("(管理股票)").removesuffix("股份有限公司")


def market_events(lifetimes, ledger, as_of):
    otc_starts, otc_ends = archived_events(lifetimes, ledger, as_of)
    starts = {("TPEx", code): events for code, events in otc_starts.items()}
    ends = {("TPEx", code): events for code, events in otc_ends.items()}
    for is_start, index, member in [(True, starts, "twse-listed.json"), (False, ends, "twse-ended.csv")]:
        for event in twse_events(lifetimes[f"official/{member}"], as_of, start=is_start):
            index.setdefault(("TWSE", event.code), []).append(event)
    return starts, ends


def reconcile_pending(ledger, current, starts, ends, listings, chronology, code_changes):
    claimed = {row["exiting_source"] for row in current if "exiting_source" in row}
    held_sources = {row["source"] for row in ledger["held_intervals_detail"] if row["reason"] == PENDING} - claimed
    matched_codes = {row["exiting_source"] for row in code_changes if "exiting_source" in row}
    chronicle_names = defaultdict(set)
    for day, action, issuer in chronology:
        if action == "LISTED":
            chronicle_names[name_key(issuer)].add(day)
    remaining, proposals, baseline = {}, [], Counter()
    for key, events in ends.items():
        for end in events:
            if end.source not in held_sources:
                continue
            prior = max((row.day for row in events if row.day < end.day), default=date.min)
            opening = [row for row in starts.get(key, []) if prior <= row.day < end.day]
            reason = "HOLD_ISSUER_NAME_LINK" if opening else "HOLD_LISTING_START_MISSING"
            baseline[reason] += 1
            if end.source in matched_codes:
                continue
            candidate = {"market": end.market, "code": end.code, "issuer": end.issuer, "end_exclusive": end.day.isoformat(),
                         "disposition": reason, "end_source": end.source,
                         "existing_starts": [{"date": str(row.day), "proof": row.source} for row in opening]}
            remaining[end.source] = candidate
            if end.market == "TWSE":
                possible = [day for day in chronicle_names[name_key(end.issuer)] if prior <= day < end.day]
                if len(possible) == 1 and not opening:
                    candidate.update(disposition="HOLD_HISTORICAL_SYMBOL_LINK",
                                     dated_name_only_start=possible[0].isoformat(), start_source=TWSE_CHRONICLE)
                continue
            book = [row for row in listings.get(end.code, []) if prior <= iso_date(row["start"]) < end.day]
            if len(book) != 1 or legal_stem(book[0]["issuer"]) != legal_stem(end.issuer):
                continue
            row = book[0]
            if {str(event.day) for event in opening} - {row["start"]}:
                candidate.update(disposition="HOLD_OFFICIAL_START_DATE_CONFLICT", yearbook_candidate=row)
                continue
            proposals.append({"market": end.market, "code": end.code, "issuer": end.issuer, "start": row["start"],
                              "end_exclusive": end.day.isoformat(), "kind": "OFFICIAL_CLOSED_INTERVAL_WITH_YEARBOOK",
                              "proof": [row["source"], end.source], "exiting_source": end.source})
    if len(held_sources) != sum(baseline.values()):
        raise ValueError("archived pending listing records are missing from the comparison")
    accepted = current + code_changes
    _, collisions = nonoverlapping(accepted)
    if collisions:
        raise ValueError("code assignment overlaps an existing market interval")
    combined, overlaps = nonoverlapping(accepted + proposals)
    for row in overlaps:
        if row["kind"] == "OFFICIAL_CLOSED_INTERVAL_WITH_YEARBOOK":
            remaining[row["exiting_source"]]["disposition"] = "HOLD_OVERLAPPING_YEARBOOK_INTERVAL"
    additions = [row for row in combined if row["kind"] == "OFFICIAL_CLOSED_INTERVAL_WITH_YEARBOOK"]
    for row in additions:
        del remaining[row["exiting_source"]]
    return additions, list(remaining.values()), dict(baseline)


def cutover_check(corrections):
    events = {(row["code"], row["start"]): row for row in corrections}
    previous, former_code, current = (events[key] for key in (("2301", "1983-01-26"), ("2346", "1995-11-17"),
                                                              ("2301", "2002-11-04")))
    if previous["end_exclusive"] != current["start"] or former_code["end_exclusive"] != current["start"]:
        raise ValueError("share code cutover is not contiguous")
    if (previous["issuer"] == current["issuer"] or former_code["company_id"] != current["company_id"]
            or former_code["code"] == current["code"]):
        raise ValueError("predecessor and successor company identifiers were merged")


def early_market_eras(raw, previous):
    text = page_text(raw)
    before, system, matching = "83年11月1日櫃買中心成立", "同年12月30日股票電腦交易系統啟用", "84年9月18日"
    if not all(value in text for value in (before, system, matching, "改採電腦競價自動撮合系統")):
        raise ValueError("official chronology does not verify the market system boundaries")
    first = previous["official_first_listing_event"]
    if first["status"] != "PASS_OFFICIAL_FIRST_LISTING" or iso_date(first["date"]) >= date(1994, 11, 1):
        raise ValueError("earliest listing evidence does not predate the center")
    return {"first_stock_listing": first["date"], "center_established": "1994-11-01",
            "computer_trading_system_started": "1994-12-30", "automatic_matching_started": "1995-09-18",
            "daily_quote_coverage": previous["pre_1994_daily_quote_archive"], "source": TPEX_CHRONICLE}


def reconcile(lifetimes, earlier, responses, as_of):
    ledger = json.loads(lifetimes["reconciliation.json"])
    previous = json.loads(earlier["reconciliation.json"])
    starts, ends = market_events(lifetimes, ledger, as_of)
    book_counts, books, sources = {}, defaultdict(list), {}
    for year, (name, *_) in YEARBOOKS.items():
        if not (raw := responses.get(name)):
            book_counts[str(year)] = 0
            sources[name] = "HOLD_SOURCE_UNAVAILABLE"
            continue
        listed = read_listing_book(raw, year)
        book_counts[str(year)] = len(listed)
        sources[name] = "VALID_OFFICIAL_LISTING_ROWS"
        for code, row in listed.items():
            books[code].append(row)
    timeline = anniversary_events(responses[TWSE_CHRONICLE]) if responses.get(TWSE_CHRONICLE) else []
    profiles = json.loads(lifetimes["official/twse-active-projection.json"])
    old_exits = [event for (market, _), records in ends.items() if market == "TWSE" for event in records]
    code_changes = []
    if timeline and responses.get(ISSUER_CHAIN) and responses.get(MERGER_NOTICE):
        code_changes = verify_liteon(ledger, profiles, old_exits, timeline, responses[ISSUER_CHAIN], responses[MERGER_NOTICE])
        cutover_check(code_changes)
    approved = ledger["listing_intervals"] + previous["additional_closed_intervals"]
    additions, pending, classified = reconcile_pending(ledger, approved, starts, ends, books, timeline, code_changes)
    prior_count = previous["historical_intervals_still_held"]
    resolved_historical = len(additions) + sum("exiting_source" in row for row in code_changes)
    if sum(classified.values()) != prior_count or len(pending) + resolved_historical != prior_count:
        raise ValueError("pending listing totals did not reconcile")
    eras = early_market_eras(responses[TPEX_CHRONICLE], previous) if responses.get(TPEX_CHRONICLE) else {}
    resolved_active = sum("resolved_profile_source" in row for row in code_changes)
    report = {"status": "PASS_DIAGNOSTIC", "production": "HOLD", "finmind_requests": 0, "as_of_taipei": as_of.isoformat(),
              "previous_run": PREVIOUS_RUN, "yearbook_valid_listing_rows": book_counts,
              "yearbook_closed_intervals": len(additions), "issuer_code_intervals": len(code_changes),
              "issuer_code_cutover": "PASS_VERIFIED" if code_changes else "HOLD_OFFICIAL_SOURCE",
              "oldest_verified_liteon_assignment": code_changes[0]["start"] if code_changes else None,
              "active_snapshot_conflicts_open": previous["active_snapshot_conflicts_still_held"] - resolved_active,
              "historic_intervals_open_before": prior_count, "historic_intervals_open_after": len(pending),
              "pending_before_evidence": classified, "pending_after_evidence": dict(Counter(x["disposition"] for x in pending)),
              "official_early_market_milestones": eras}
    details = {"additional_yearbook_intervals": additions, "code_assignment_intervals": code_changes,
               "pending_listing_ledger": pending, "yearbook_rows": dict(books), "yearbook_source_status": sources,
               "input_archive_hashes": {str(LIFETIME_RUN): digest(lifetimes["reconciliation.json"]),
                                        str(PREVIOUS_RUN): digest(earlier["reconciliation.json"])}}
    return report, details


def input_archives():
    return earlier_evidence(LIFETIME_RUN), earlier_evidence(PREVIOUS_RUN)


def run():
    archived = input_archives()
    with ThreadPoolExecutor(max_workers=6) as pool:
        downloaded = dict(zip(SOURCE_URLS, pool.map(fetch_official, SOURCE_URLS.values())))
    today = datetime.now(ZoneInfo("Asia/Taipei")).date()
    report, details = reconcile(*archived, {name: body for name, (_, body) in downloaded.items()}, today)
    report.update(private_archive="STAGED", source_results=dict(Counter(status for status, _ in downloaded.values())))
    save_private(downloaded, SOURCE_URLS, {}, report | details)
    return report


def verify():
    archived = input_archives()
    access = drive_token()
    originals = verified_archive_files(access, private_folder(access, create=False))
    expected = json.loads(originals["reconciliation.json"])
    responses = {name: originals.get(f"official/{name}") for name in SOURCE_URLS}
    report, details = reconcile(*archived, responses, iso_date(expected["as_of_taipei"]))
    computed = report | details
    if json_bytes(computed) != json_bytes({key: expected[key] for key in computed}):
        raise ValueError("verified Drive sources did not reproduce the accepted intervals")
    print(f"market_chronology_drive_readback=PASS files={len(originals)} "
          f"yearbook_intervals={report['yearbook_closed_intervals']} code_intervals={report['issuer_code_intervals']} "
          f"pending={report['historic_intervals_open_after']}")


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
