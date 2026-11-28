"""Dispatch independently evidenced Backer windows from a private, resumable campaign."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date, timedelta
from itertools import islice
import json
from math import ceil
import os
from time import sleep, time

from . import release_batch, release_rollout
from .cross_run_ledger import DriveJournal
from .dispatch import Window
from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .drive_retry import operation_scope
from .finish_preflight import MANIFEST as PROBE_MANIFEST, PREFIX as PROBE_PREFIX
from .history_pilot import next_month, twse_month
from .integration_pilot import digest, encode, verify_private_folder
from .live_bronze_admission import advance
from .live_preflight import drive_health
from .live_recovery_canary import read_json_file, write_json
from .manifest_pilot import serial_window
from .market_lifetimes import nonoverlapping, stock_code
from .production_preflight import reconciled_catalog
from .quota import NORMAL_LIMIT, next_reservation_at
from .release_batch import CUTOFF, MANIFEST_NAME, MAX_ATTEMPTS, WINDOW_LIMIT
from .release_rehearsal import RehearsalError, require, root_folder
from .release_remediation import pinned_contracts
from .tpex_calendar import Source, fetch_official, monthly_rows
from .tpex_closeout import LIFETIME_RUN
from .tpex_history import verified_archive_files


CAMPAIGN = "R12_1_PRODUCTION_STOCK_2026_01"
PLAN_VERSION = 1
PLAN = "DATASET_MANIFEST.json"
TPEX_PLAN = "TPEX_PRICE_MANIFEST.json"
PREVIOUS_ROLLOUT = 45850929455
PROBE_RUN = 45831567041
PROBE_ACCEPT = "B10_SUMMARY_45832107974_1.json"
PRICE = "TaiwanStockPrice"
BATCH_SIZE = 96
WORKERS = 32
TERMINAL = {"VERIFIED", "BLOCKED", "REPAIR"}


def management(access, *, create):
    parent = ROOT
    verify_private_folder(parent, access)
    for title in ("00_COMMAND", "R12_BACKER", CAMPAIGN):
        child = ensure(parent, title, FOLDER, access, create=create and title == CAMPAIGN)
        verify_private_folder(child, access)
        if title == CAMPAIGN:
            return parent, child
        parent = child


def prior_receipts(access):
    base = release_batch.private_campaign(access, create=False)
    manifest, record = release_batch.load(base, DriveJournal(base, access), access)
    _, prior = release_rollout.campaign(access, PREVIOUS_ROLLOUT, create=False)
    last = read_json_file(prior, MANIFEST_NAME, access)
    ledger = DriveJournal(prior, access).version()
    require(ledger["state"]["manifest_sha"] == digest(encode(last)), "PREVIOUS_MANIFEST_DIGEST")
    result = []
    for plan, state in ((manifest, record["state"]), (last, ledger["state"])):
        for row in plan["items"]:
            task = state["tasks"][row["task_id"]]
            if task["status"] == "VERIFIED":
                require(task["receipt"]["file_id"] == row["destination_id"], "PREVIOUS_RECEIPT_ID")
                result.append([row["dataset"], row["scope"], row["start"], row["end"]])
    probes = root_folder(access, prefix=PROBE_PREFIX, run_id=PROBE_RUN, attempt=1, create=False)
    tests = read_json_file(probes, PROBE_MANIFEST, access)
    require(DriveJournal(probes, access).version()["state"]["manifest_sha"] == digest(encode(tests)),
            "PROBE_MANIFEST_DIGEST")
    for key, receipt in read_json_file(probes, PROBE_ACCEPT, access)["receipts"].items():
        row = tests["tests"][key]
        require(receipt["file_id"] == row["object_id"], "PROBE_RECEIPT_ID")
        params = row["params"]
        result.append([params["dataset"], params["data_id"], params["start_date"], row["end"]])
    require(len(result) == len(set(map(tuple, result))), "DUPLICATE_PREVIOUS_COVERAGE")
    return sorted(result)


def sessions(venue, month):
    if venue == "TWSE":
        status, dates = twse_month(month)
        return status, sorted(str(day) for day in dates if day <= CUTOFF)
    source = Source(month)
    status, raw = fetch_official(source.url)
    if not raw:
        return status, []
    try:
        dates = [str(day) for day, row in monthly_rows(raw, source).items()
                 if day <= CUTOFF and row["traded"] and row["positive_price"]]
    except (KeyError, IndexError, TypeError, ValueError):
        return "HOLD_OFFICIAL_CALENDAR_FORMAT", []
    return "PASS", sorted(dates)


def candidate_windows(plan):
    previous = defaultdict(set)
    for dataset, scope, start, end in plan["reused"]:
        previous[dataset, scope].add((start, end))
    rules = sorted(plan["contracts"].items(), key=lambda item: item[1]["lane"])
    for month, calendars in sorted(plan["months"].items(), reverse=True):
        first = date.fromisoformat(month)
        last = min(CUTOFF, next_month(first) - timedelta(days=1)).isoformat()
        for entry in plan["listings"]:
            market, code = entry["market"], entry["code"]
            start = max(month, entry["start"])
            end = min(last, str(date.fromisoformat(entry["end_exclusive"]) - timedelta(days=1)))
            if start > end or market not in calendars:
                continue
            days = [day for day in calendars[market] if start <= day <= end]
            if not days:
                continue
            for dataset, rule in rules:
                if market not in rule["venues"]:
                    continue
                covered = previous.get((dataset, code), ())
                if (start, end) in covered:
                    continue
                require(not any(start <= old_end and old_start <= end for old_start, old_end in covered),
                        "RECEIPT_WINDOW_OVERLAP")
                yield dataset, code, market, start, end, days


def prepare_native_segment(index, read, journal, access, stage, *, resolve_destination=None,
                           known_requests=(), known_coverage=()):
    """Load a hashed native manifest into the existing B01–B10 journal interface."""
    from .bulk_batch import prepare

    return prepare(index, read, journal, access, stage, resolve_destination or release_batch.destination,
                   known_requests=known_requests, known_coverage=known_coverage)


def storage_bytes(windows):
    # Four possible staged gzip files, the final file, and bounded journals per batch.
    return windows * (MAX_ATTEMPTS + 1) * (WINDOW_LIMIT + 1024) + ceil(windows / BATCH_SIZE) * 16 * 1024 * 1024


def available_capacity(access, window_count):
    health = drive_health(access)
    require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
            health["available_bytes"] >= storage_bytes(window_count), "DRIVE_BATCH_CAPACITY")
    return health["available_bytes"]


def plan_budget(plan, access):
    counts = Counter(dataset for dataset, *_ in candidate_windows(plan))
    count = sum(counts.values())
    headroom = available_capacity(access, min(count, BATCH_SIZE))
    return {"windows": count, "windows_by_dataset": dict(counts), "base_source_requests": count,
            "max_source_requests": count * MAX_ATTEMPTS, "storage_bound_bytes": storage_bytes(count),
            "segment_task_limit": BATCH_SIZE, "storage_bound_fits_at_launch": headroom >= storage_bytes(count),
            "minimum_hours_at_hourly_limit": ceil(count / NORMAL_LIMIT)}


def freeze(access, parent, root, journal, run):
    if find(root, PLAN, access) is None:
        registry = pinned_contracts()
        catalog, _ = reconciled_catalog(access, parent)
        roster = {row["dataset"]: row for row in catalog}
        rules = {}
        for dataset, lane in release_rollout.LANES.items():
            entry, contract = roster[dataset], registry[dataset]
            require(entry["state"] == "SAMPLE_ONLY" and entry["routing"] == lane and
                    contract.tier in {"Free(w/ data_id)", "Backer"} and "`end_date`" in contract.optional and
                    {"stock_id", "date"} <= set(contract.fields), "PRODUCTION_QUERY_CONTRACT")
            rules[dataset] = {"lane": lane, "contract": digest(encode(asdict(contract))),
                              "fields": list(contract.fields),
                              "venues": ["TWSE", "TPEx"] if dataset == PRICE else ["TWSE"]}
        source = root_folder(access, prefix="R11_TPEX_HISTORY", run_id=LIFETIME_RUN, attempt=1, create=False)
        raw = verified_archive_files(access, source)["reconciliation.json"]
        listings, conflicts = nonoverlapping(json.loads(raw)["listing_intervals"])
        require(listings and not conflicts, "LISTING_IDENTITY_OVERLAP")
        listings = [{key: row[key] for key in ("market", "code", "start", "end_exclusive", "proof")}
                    for row in listings if stock_code(row["code"]) and row["proof"] and
                    row["end_exclusive"] > "2026-01-01" and row["start"] <= str(CUTOFF)]
        require({row["market"] for row in listings} == {"TWSE", "TPEx"}, "MARKET_UNIVERSE")
        months = {str(date(2026, month, 1)): {} for month in range(CUTOFF.month, 0, -1)}
        queries = [(venue, date.fromisoformat(month)) for month in months for venue in ("TWSE", "TPEx")]
        held = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            for (venue, month), (status, days) in zip(queries, pool.map(lambda q: sessions(*q), queries), strict=True):
                if status == "PASS" and days:
                    months[str(month)][venue] = days
                else:
                    held.append({"venue": venue, "month": str(month), "reason": status})
        require(any(months.values()), "NO_VERIFIED_MARKET_SESSIONS")
        plan = {"campaign": CAMPAIGN, "plan_version": PLAN_VERSION, "origin_run": run, "commit": os.environ["GITHUB_SHA"],
                "months": months, "listings": listings, "contracts": rules, "reused": prior_receipts(access),
                "held_calendar_months": held, "listing_evidence_run": LIFETIME_RUN, "listing_sha": digest(raw),
                "catalog": [{"dataset": row["dataset"], "lane": row["routing"], "state": row["state"],
                             "admitted_venues": rules.get(row["dataset"], {}).get("venues", [])} for row in catalog]}
        plan["budget"] = plan_budget(plan, access)
        require(plan["budget"]["windows"] > 0, "NO_NEW_ELIGIBLE_WINDOWS")
        write_json(root, "B09_SCOPE_HOLDS.json", {
            "held_calendar_months": held, "unconnected_datasets": [row for row in plan["catalog"] if
                not row["admitted_venues"] and not row["state"].startswith("EXCLUDED")],
            "boundaries": {"first_year": 2026, "verified_cutoff": str(CUTOFF),
                           "unsupported_venues_by_rule": {name: ["TPEx"] for name in rules if name != PRICE}},
        }, access)
        write_json(root, PLAN, plan, access)
    plan = read_json_file(root, PLAN, access)
    journal.initialize({"plan_sha": digest(encode(plan)), "cursor": 0, "pause": 0, "events": [],
                        "totals": {}, "rows": 0, "bytes": 0, "source_requests": 0, "account_requests": 0})


def checked_otc_plan(plan, master, journal, root, access, *, create):
    state = master["state"]
    if "otc_plan_sha" not in state and create:
        if find(root, TPEX_PLAN, access) is None:
            pending = [date.fromisoformat(month) for month, venues in plan["months"].items() if "TPEx" not in venues]
            months, held = {}, []
            with ThreadPoolExecutor(max_workers=6) as pool:
                values = pool.map(lambda month: sessions("TPEx", month), pending)
                for month, (status, dates) in zip(pending, values, strict=True):
                    if status == "PASS" and dates:
                        months[str(month)] = {"TPEx": dates}
                    else:
                        held.append({"venue": "TPEx", "month": str(month), "reason": status})
            supplement = {"months": months, "listings": [row for row in plan["listings"] if row["market"] == "TPEx"],
                          "contracts": {PRICE: plan["contracts"][PRICE]}, "reused": plan["reused"],
                          "held_calendar_months": held, "base_plan_sha": state["plan_sha"]}
            supplement["budget"] = plan_budget(supplement, access)
            combined = plan["budget"]["storage_bound_bytes"] + supplement["budget"]["storage_bound_bytes"]
            supplement["combined_storage_bound_fits"] = available_capacity(access, 0) >= combined
            write_json(root, TPEX_PLAN, supplement, access)
        supplement = read_json_file(root, TPEX_PLAN, access)
        prior_batch = find(root, f"BATCH_{state['cursor']:06d}", access)
        master = advance(journal, master, otc_plan_sha=digest(encode(supplement)), otc_cursor=0,
                         next_group="BASE" if prior_batch else "TPEX")
        state = master["state"]
    if "otc_plan_sha" not in state:
        return master, None
    supplement = read_json_file(root, TPEX_PLAN, access)
    require(supplement["base_plan_sha"] == state["plan_sha"] and
            digest(encode(supplement)) == state["otc_plan_sha"], "TPEX_PLAN_DIGEST")
    return master, supplement


def pending_group(base, supplement, state):
    groups = {"BASE": state["cursor"] < base["budget"]["windows"],
              "TPEX": supplement is not None and state.get("otc_cursor", 0) < supplement["budget"]["windows"]}
    preferred = state.get("next_group", "BASE")
    if groups[preferred]:
        return preferred
    return next((group for group, pending in groups.items() if pending), None)


def year_budget(base, supplement):
    total = base["budget"].copy()
    if supplement:
        extra = supplement["budget"]
        for name in ("windows", "base_source_requests", "max_source_requests", "storage_bound_bytes"):
            total[name] += extra[name]
        total["windows_by_dataset"] = dict(Counter(total["windows_by_dataset"]) + Counter(extra["windows_by_dataset"]))
        total["minimum_hours_at_hourly_limit"] = ceil(total["base_source_requests"] / NORMAL_LIMIT)
        total["storage_bound_fits_at_launch"] = supplement["combined_storage_bound_fits"]
    return total


def segment(plan, master, root, access, group, *, create):
    otc = group == "TPEX"
    position = master["state"]["otc_cursor" if otc else "cursor"]
    plan_sha = master["state"]["otc_plan_sha" if otc else "plan_sha"]
    count = plan["budget"]["windows"]
    name = f"{'TPEX_' if otc else ''}BATCH_{position:06d}"
    folder = ensure(root, name, FOLDER, access, create=create)
    verify_private_folder(folder, access)
    journal = DriveJournal(folder, access)
    if create and find(folder, MANIFEST_NAME, access) is None:
        end = min(position + BATCH_SIZE, count)
        available_capacity(access, end - position)
        stage = ensure(folder, "STAGING", FOLDER, access, create=True)
        verify_private_folder(stage, access)
        candidates = list(islice(candidate_windows(plan), position, end))
        ids = journal.new_ids(len(candidates))
        rows, parents = [], {}
        for (dataset, code, market, start, last, dates), file_id in zip(candidates, ids, strict=True):
            rule = plan["contracts"][dataset]
            item = serial_window(Window(dataset, code, date.fromisoformat(start), date.fromisoformat(last),
                                        rule["contract"], rule["lane"]))
            month = start[:7]
            if month not in parents:
                parents[month] = release_batch.destination(item, access, create=True)
            rows.append(item | {"market": market, "fields": rule["fields"], "dates": dates,
                                "destination": parents[month], "destination_id": file_id,
                                "name": f"{dataset}__{code}__{start}__{last}__r12_1.json.gz"})
        require(len(rows) == end - position and len({row["task_id"] for row in rows}) == len(rows), "BATCH_INVENTORY")
        write_json(folder, MANIFEST_NAME, {"items": rows, "stage": stage, "cursor": position, "end": end,
                                         "held_calendar_months": [], "capacity_bound": storage_bytes(len(rows)),
                                         "plan_sha": plan_sha}, access)
    manifest = read_json_file(folder, MANIFEST_NAME, access)
    require(manifest["cursor"] == position and manifest["plan_sha"] == plan_sha, "BATCH_PLAN_DIGEST")
    if create and journal.version(0) is None:
        initial = master["state"]
        journal.initialize({"manifest_sha": digest(encode(manifest)), "pause": initial["pause"],
                            "events": initial["events"], "inherited_events": len(initial["events"]),
                            "tasks": {row["task_id"]: {"status": "READY", "attempts": 0} for row in manifest["items"]}})
    record = journal.version()
    require(record and record["state"]["manifest_sha"] == digest(encode(manifest)), "BATCH_MANIFEST_DIGEST")
    return folder, manifest, journal, record


def next_request_time(state, now):
    events = [(event["at"], event.get("kind", "download")) for event in state["events"]]
    due = next_reservation_at(events, now, "download", paused_until=state["pause"])
    if account := state.get("account"):
        if now - account["at"] <= 150 and account["headroom"] <= len(state["events"]) - account["first_event"]:
            due = max(due, account["at"] + 151)
    tasks_due = [task.get("retry_after", now) if task["status"] == "READY" else task["lease_until"]
                 for task in state.get("tasks", {}).values() if task["status"] in {"READY", "REQUESTING"}]
    return max(due, min(tasks_due, default=now))


def wait_for_request(state):
    now = int(time())
    due = next_request_time(state, now)
    require(due - now <= 3600, "PROVIDER_PAUSE_OVER_ONE_HOUR")
    if due > now:
        print(f"seconds_until_next_quota_window={due - now}", flush=True)
    while (remaining := due - int(time())) > 0:
        sleep(min(remaining, 50))
    return due > now


def accept_segment(folder, manifest, journal, record, master_journal, master, access, run, group):
    record = release_batch.accept(manifest, record, journal, access, workers=16, reread_verified=False)
    state = record["state"]
    tasks = state["tasks"]
    counts = Counter(task["status"] for task in tasks.values())
    pending = len(tasks) - sum(counts[key] for key in TERMINAL)
    tag = f"{run}_{os.environ['GITHUB_RUN_ATTEMPT']}"
    events = state["events"][state["inherited_events"]:]
    own = [event for event in events if event["run"] == run]
    with operation_scope("B10_REPORT"):
        for lane in sorted({row["lane"] for row in manifest["items"]}):
            assigned = {row["task_id"]: tasks[row["task_id"]] for row in manifest["items"] if row["lane"] == lane}
            write_json(folder, f"{lane}_{tag}.json", {"tasks": assigned, "events": [e for e in own if e["task_id"] in assigned]}, access)
        write_json(folder, f"B09_{tag}.json", {key: task for key, task in tasks.items()
                                              if task["status"] in {"BLOCKED", "REPAIR"}}, access)
        receipts = {key: task["receipt"] for key, task in tasks.items() if task["status"] == "VERIFIED"}
        write_json(folder, f"B10_{tag}.json", {"receipts": receipts, "states": dict(counts), "pending": pending}, access)
    if not pending:
        previous = master["state"]
        otc = group == "TPEX"
        progress = {"otc_cursor" if otc else "cursor": manifest["end"], "next_group": "BASE" if otc else "TPEX"}
        with operation_scope("B10_MASTER"):
            master = advance(master_journal, master, **progress,
                             totals=dict(Counter(previous["totals"]) + counts),
                             rows=previous["rows"] + sum(r["proof"]["rows"] for r in receipts.values()),
                             bytes=previous["bytes"] + sum(r["proof"]["bytes"] for r in receipts.values()),
                             source_requests=previous["source_requests"] + sum(e["kind"] == "download" for e in events),
                             account_requests=previous["account_requests"] + sum(e["kind"] == "probe" for e in events),
                             events=[event for event in state["events"] if event["at"] > int(time()) - 3600],
                             pause=state["pause"], auth_hold=state.get("auth_hold", False))
    return master, state, {"segment_states": dict(counts), "segment_pending": pending,
                           "source_requests_this_run": sum(e["kind"] == "download" for e in own),
                           "account_requests_this_run": sum(e["kind"] == "probe" for e in own),
                           "verification_finmind_requests": 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("launch", "continue", "accept", "validate-native"))
    phase = parser.parse_args().phase
    if phase == "validate-native":
        from .official_bulk_v24 import run_job

        run_job("G06")
        return
    try:
        run = int(os.environ["GITHUB_RUN_ID"])
        access = token()
        parent, root = management(access, create=phase == "launch")
        journal = DriveJournal(root, access)
        master = journal.head()
        if phase == "launch" and master is None:
            freeze(access, parent, root, journal, run)
            master = journal.head()
        plan = read_json_file(root, PLAN, access)
        require(master and master["state"]["plan_sha"] == digest(encode(plan)) and
                plan["plan_version"] == PLAN_VERSION and plan["campaign"] == CAMPAIGN, "CAMPAIGN_MANIFEST_DIGEST")
        master, supplement = checked_otc_plan(plan, master, journal, root, access, create=phase != "accept")
        state, result = master["state"], {}
        group = pending_group(plan, supplement, state)
        if group is not None and not state.get("auth_hold"):
            active_plan = plan if group == "BASE" else supplement
            folder, manifest, batch, record = segment(active_plan, master, root, access, group, create=phase != "accept")
            if phase == "accept":
                master, state, result = accept_segment(folder, manifest, batch, record, journal, master, access, run, group)
            else:
                state = record["state"]
                if not state.get("auth_hold") and any(t["status"] in {"READY", "REQUESTING"} for t in state["tasks"].values()):
                    if wait_for_request(state):
                        access = token()
                        batch = DriveJournal(folder, access)
                    record = release_batch.dispatch(manifest, record, batch, access, run,
                                                    request_ceiling=BATCH_SIZE, workers=WORKERS, lane_limit=WORKERS)
                state = record["state"]
                result = release_batch.report(manifest, record, run)
        totals = master["state"]
        resume = pending_group(plan, supplement, totals) is not None and not state.get("auth_hold")
        if phase == "accept" and (path := os.environ.get("GITHUB_OUTPUT")):
            with open(path, "a", encoding="utf-8") as output:
                print(f"resume={'true' if resume else 'false'}", file=output)
        print(json.dumps(result | {"campaign": CAMPAIGN, "budget": year_budget(plan, supplement),
                                   "finished_windows": totals["cursor"] + totals.get("otc_cursor", 0), "batch_group": group,
                                   "tpex_new_windows": supplement["budget"]["windows"] if supplement else 0,
                                   "total_states": totals["totals"], "total_rows": totals["rows"],
                                   "total_source_requests": totals["source_requests"],
                                   "reused_receipts": len(plan["reused"]), "automatic_resume": resume,
                                   "scope": "2026_CERTIFIED_STOCK_WINDOWS", "cutoff": str(CUTOFF)}, sort_keys=True))
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_PRODUCTION_HOLD phase={phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
