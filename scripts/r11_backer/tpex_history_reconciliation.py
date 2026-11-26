"""Replay a complete official TPEx delisting capture against the listing evidence."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re

from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .history_pilot import iso_date
from .listing_crosscheck import INPUT_RUNS, MANAGED, current_ledger
from .live_preflight import read_json
from .market_chronology import legal_stem, market_events
from .market_lifetimes import Event, digest, earlier_evidence, name_key, nonoverlapping, official_date, stock_code
from .release_probes import RECOVERY_ROOT
from .test_archive import json_bytes
from .tpex_delisting import PAGE_SIZE, complete_rows, delisting_url, first_table
from .tpex_history import private_folder, save_private, verified_archive_files


PRIOR_RUN = 45735630132
BASELINE_RUNS = (*INPUT_RUNS, PRIOR_RUN)
INPUT_FOLDER = f"R11_TPEX_LISTING_INPUT_{PRIOR_RUN}_1"
INPUT_HASH = "fe8b29f73eaeb8e1701e36bc2d14010f57ed8a0bc337b269b5ef5cb9c94e518f"
REPORT = Path("r11-tpex-history-reconciliation.json")
MISSING_START = "HOLD_LISTING_START_MISSING"
NEW_KIND = "OFFICIAL_TPEX_INTERVAL_WITH_COMPLETE_ANNUAL_PAGES"


def terminal_member(year, offset=0):
    return f"tpex-termination-{year}-{offset}.json"


def terminal_key(event):
    return event.code, event.day, name_key(event.issuer)


def delisting_events(fields, data, year, as_of):
    code_col, issuer_col, date_col = (fields.index(key) for key in ("股票代號", "公司名稱", "終止上櫃日期"))
    events = {}
    for index, row in enumerate(data):
        code, issuer = str(row[code_col]).strip(), str(row[issuer_col]).strip()
        day = official_date(row[date_col])
        if day.year != year:
            raise ValueError("TPEx delisting response includes a different calendar year")
        if not stock_code(code) or day > as_of:
            continue
        if not issuer:
            raise ValueError("dated TPEx stock termination has no issuer")
        offset = index // PAGE_SIZE * PAGE_SIZE
        event = Event("TPEx", code, issuer, day, f"{terminal_member(year, offset)}#{index - offset + 1}")
        if terminal_key(event) in events:
            raise ValueError("TPEx termination identity occurs more than once")
        events[terminal_key(event)] = event
    return events


def complete_delistings(packet, manifest, previous, first_year, as_of):
    grouped = defaultdict(dict)
    for name, url in manifest["delisting_urls"].items():
        match = re.fullmatch(r"tpex-termination-(\d{4})-(\d+)\.json", name)
        if not match:
            raise ValueError("invalid archived TPEx report name")
        year, offset = map(int, match.groups())
        raw = packet[f"official/{name}"]
        if url != delisting_url(year, offset) or digest(raw) != manifest["delisting_sha256"][name]:
            raise ValueError("official TPEx report provenance does not match the captured input")
        grouped[year][offset] = raw
    if set(grouped) != set(range(first_year, as_of.year + 1)):
        raise ValueError("official delisting capture has an uncovered year")
    full, pagination = {}, {}
    for year, pages in sorted(grouped.items(), reverse=True):
        fields, data = complete_rows(pages, year)
        old = previous.get(f"official/tpex-ended-{year}.json")
        old_total = len(first_table(old, year)["data"]) if old else 0
        pagination[str(year)] = {"previous_rows": old_total, "verified_rows": len(data), "pages": len(pages)}
        full.update(delisting_events(fields, data, year, as_of))
    if not full:
        raise ValueError("official delisting capture contains no valid stock events")
    return full, pagination


def yearbook_listings(packet, manifest):
    pages = manifest["retained_pages"]
    for name, meta in pages.items():
        raw = packet[f"official/{name}"]
        book = manifest["factbooks"][str(meta["year"])]
        if (not raw.startswith(b"%PDF-") or meta["sha256"] != digest(raw)
                or not re.fullmatch(r"[a-f0-9]{64}", book["sha256"])
                or not book["url"].startswith("https://www.tpex.org.tw/storage/publish/factbook/")
                or name != f"tpex-factbook-{meta['year']}-page-{meta['physical_page']}.pdf"):
            raise ValueError("audited factbook page has invalid official provenance")
    listed, managed, seen = [], [], set()
    for row in manifest["audited_listing_rows"]:
        code, issuer, day, page = row["code"], row["issuer"], iso_date(row["start"]), row["page"]
        if not stock_code(code) or not issuer or day.year != pages[page]["year"] or (code, day) in seen:
            raise ValueError("invalid or duplicate audited factbook listing row")
        seen.add((code, day))
        event = Event("TPEx", code, issuer, day, f"{page}#code={code}")
        if row["classification"] == "LISTING_TABLE":
            listed.append(event)
        elif row["classification"] == "MANAGED_OR_BOARD_UNCONFIRMED":
            managed.append(event)
        else:
            raise ValueError("unrecognized factbook listing classification")
    return listed, managed


def coverage_gaps(manifest, pagination):
    gaps = set()
    for year, proof in manifest["historical_year_contradictions"].items():
        if (len(proof["pages"]) < 2
                or any(manifest["retained_pages"][p]["year"] != int(year) for p in proof["pages"])
                or pagination[year]["verified_rows"] != proof["api_total"]):
            raise ValueError("historic delisting coverage contradiction is inconsistent")
        gaps.add(int(year))
    return gaps


def evaluate_case(end, endings, starts, managed, gaps):
    lower = max((event.day for event in endings if event.day < end.day), default=None)
    choices = [event for event in starts if (lower is None or lower <= event.day) and event.day < end.day]
    same_day_issuers = {name_key(event.issuer) for event in endings if event.day == end.day}
    distinct = {(event.day, legal_stem(event.issuer)) for event in choices}
    if len(distinct) > 1 or len(same_day_issuers) > 1:
        return "HOLD_CONFLICTING_OFFICIAL_EVENTS", choices, None
    if not choices:
        relevant = [event for event in managed if (lower is None or lower <= event.day) and event.day < end.day]
        return ("HOLD_MANAGED_OR_BOARD_UNCONFIRMED", relevant, None) if relevant else (MISSING_START, [], None)
    first = choices[0]
    if legal_stem(first.issuer) != legal_stem(end.issuer):
        return "HOLD_ISSUER_NAME_LINK", choices, None
    if any(first.day.year <= year <= end.day.year for year in gaps):
        return "HOLD_DELISTING_YEAR_CONTRADICTION", choices, None
    return "PASS", choices, {
        "code": end.code, "market": end.market, "issuer": end.issuer,
        "start": str(first.day), "end_exclusive": str(end.day), "kind": NEW_KIND,
        "proof": sorted({event.source for event in choices}) + [end.source],
        "exiting_source": end.source,
    }


def reconcile(inputs, packet):
    manifest = json.loads(packet["reconciliation.json"])
    if digest(packet["reconciliation.json"]) != INPUT_HASH or manifest["prior_run"] != PRIOR_RUN:
        raise ValueError("pinned TPEx input packet does not match this replay")
    as_of = iso_date(manifest["as_of_taipei"])
    previous, closeout = [json.loads(inputs[index]["reconciliation.json"]) for index in (0, 1)]
    baseline, _, _, _ = current_ledger(inputs[:-1], as_of)
    latest = json.loads(inputs[-1]["reconciliation.json"])
    baseline = baseline + latest["new_verified_intervals"]
    _, baseline_collisions = nonoverlapping(baseline)
    if baseline_collisions:
        raise ValueError("the accepted baseline contains overlapping intervals")
    pending = {row["end_source"]: row for row in latest["pending_listing_ledger"]}
    if len(pending) != latest["historical_intervals_after"]:
        raise ValueError("the historical pending ledger does not reconcile")
    first = closeout["official_first_listing_event"]
    if first["status"] != "PASS_OFFICIAL_FIRST_LISTING":
        raise ValueError("the earliest official TPEx listing year was not established")
    prior_starts, prior_ends = market_events(inputs[0], previous, as_of)
    old_events = {terminal_key(event): event for (market, _), events in prior_ends.items()
                  if market == "TPEx" for event in events}
    full, pagination = complete_delistings(packet, manifest, inputs[0], iso_date(first["date"]).year, as_of)
    unexpected_missing = old_events.keys() - full.keys()
    if unexpected_missing:
        raise ValueError("a previously verified event is absent from the complete report capture")
    gaps = coverage_gaps(manifest, pagination)
    omitted = {key: event for key, event in full.items() if key not in old_events}
    targets = {row["end_source"]: row for row in pending.values()
               if row["market"] == "TPEx" and row["disposition"] == MISSING_START}
    remap = {event.source: full[key] for key, event in old_events.items() if event.source in targets}
    if remap.keys() != targets.keys():
        raise ValueError("a pending termination could not be matched back to its publisher row")
    new_endings, starts, managed = defaultdict(list), defaultdict(list), defaultdict(list)
    for event in full.values():
        new_endings[event.code].append(event)
    for (market, _), events in prior_starts.items():
        if market != "TPEx":
            continue
        for event in events:
            index = managed if name_key(event.issuer).endswith(MANAGED) else starts
            index[event.code].append(event)
    factbook, factbook_managed = yearbook_listings(packet, manifest)
    for event in factbook:
        starts[event.code].append(event)
    for event in factbook_managed:
        managed[event.code].append(event)
    for event in omitted.values():
        if event.source in pending:
            raise ValueError("newly captured row conflicts with an existing pending identifier")
        pending[event.source] = {"market": event.market, "code": event.code, "issuer": event.issuer,
                                 "end_exclusive": str(event.day), "end_source": event.source,
                                 "disposition": MISSING_START}
    decisions, proposals = {}, []
    queue = remap | {event.source: event for event in omitted.values()}
    for source, end in sorted(queue.items(), key=lambda item: (item[1].day, item[1].code), reverse=True):
        decision, choices, proposal = evaluate_case(end, new_endings[end.code], starts[end.code], managed[end.code], gaps)
        decisions[source] = {"disposition": decision, "newly_observed_terminal": source not in targets,
                             "terminal_proof": end.source,
                             "listing_evidence": [{"date": str(e.day), "issuer": e.issuer, "source": e.source}
                                                  for e in choices]}
        if proposal:
            proposals.append(proposal | {"pending_source": source})
    merged, collisions = nonoverlapping(baseline + proposals)
    for row in collisions:
        if row["kind"] == NEW_KIND:
            decisions[row["pending_source"]]["disposition"] = "HOLD_OVERLAPPING_OFFICIAL_INTERVAL"
    additions = [row for row in merged if row["kind"] == NEW_KIND]
    for row in additions:
        del pending[row["pending_source"]]
    for source in decisions.keys() & pending.keys():
        pending[source] = pending[source] | {"disposition": decisions[source]["disposition"]}
    if len(pending) + len(additions) != latest["historical_intervals_after"] + len(omitted):
        raise ValueError("repaired pending counts do not reconcile with the recovered records")
    conflicts = [
        {"prior_interval": row, "terminal_proof": event.source, "terminal_date": str(event.day)}
        for event in omitted.values() for row in baseline
        if row["market"] == "TPEx" and row["code"] == event.code
        and row["start"] <= str(event.day) < row["end_exclusive"]
    ]
    report = {
        "status": "PASS_DIAGNOSTIC", "production": "HOLD", "finmind_requests": 0,
        "previous_run": PRIOR_RUN, "as_of_taipei": str(as_of), "complete_annual_page_totals": len(pagination),
        "annual_totals_with_truncated_prior_pages": sum(
            row["verified_rows"] > row["previous_rows"] for row in pagination.values()
        ),
        "delisting_rows_previous": sum(row["previous_rows"] for row in pagination.values()),
        "delisting_rows_verified": sum(row["verified_rows"] for row in pagination.values()),
        "newly_observed_stock_terminations": len(omitted),
        "historical_coverage_contradiction_years": sorted(gaps),
        "audited_factbook_rows": len(factbook) + len(factbook_managed),
        "old_missing_start_cases_checked": len(targets),
        "old_missing_start_cases_closed": sum(row["pending_source"] in targets for row in additions),
        "new_delisting_cases_closed": sum(row["pending_source"] not in targets for row in additions),
        "historic_intervals_pending_before": latest["historical_intervals_after"],
        "historic_intervals_pending_after": len(pending),
        "pending_by_market": dict(Counter(row["market"] for row in pending.values())),
        "previous_tpex_intervals_with_new_internal_terminations": len(conflicts),
        "verdicts": dict(Counter(row["disposition"] for row in decisions.values())),
    }
    details = {
        "private_staged_input_folder": INPUT_FOLDER, "staged_input_manifest_sha256": INPUT_HASH,
        "input_archive_sha256": {str(run): digest(archive["reconciliation.json"])
                                 for run, archive in zip(BASELINE_RUNS, inputs, strict=True)},
        "oldest_official_tpex_listing": first,
        "pagination_by_year": pagination,
        "historical_year_contradictions": manifest["historical_year_contradictions"],
        "new_verified_intervals": additions, "pending_listing_ledger": list(pending.values()),
        "case_decisions": decisions, "prior_interval_conflicts": conflicts,
    }
    return report, details


def staged_packet():
    access = drive_token()
    root = ensure(RECOVERY_ROOT, INPUT_FOLDER, FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{root}?fields=shared", access).get("shared") is not False:
        raise ValueError("the TPEx original input folder is shared")
    return verified_archive_files(access, root)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        with ThreadPoolExecutor(max_workers=len(BASELINE_RUNS) + 1) as executor:
            input_task = executor.submit(staged_packet)
            inputs = list(executor.map(earlier_evidence, BASELINE_RUNS))
            report, details = reconcile(inputs, input_task.result())
        if args.verify_only:
            access = drive_token()
            files = verified_archive_files(access, private_folder(access, create=False))
            expected = json.loads(files["reconciliation.json"])
            result = report | details
            if json_bytes(result) != json_bytes({key: expected[key] for key in result}):
                raise ValueError("Drive readback did not reproduce the historical TPEx reconciliation")
            print(f"tpex_history_reconciliation_drive_readback=PASS rows={report['delisting_rows_verified']} "
                  f"pending={report['historic_intervals_pending_after']}")
            return 0
        save_private({}, {}, {}, report | details)
        report["private_archive"] = "STAGED"
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
        if args.verify_only:
            print(json.dumps(report, sort_keys=True))
            return 1
    REPORT.write_bytes(json_bytes(report))
    print(json.dumps(report, sort_keys=True))
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(f"archive={report.get('private_archive', 'HOLD')}\n")
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
