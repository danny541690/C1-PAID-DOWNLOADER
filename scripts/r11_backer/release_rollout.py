"""Run a small admitted Bronze rollout through two independent Actions runs."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import os
from time import time

from .cross_run_ledger import DriveJournal
from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .finish_preflight import PUBLISH
from .history_pilot import twse_month
from .integration_pilot import digest, encode, verify_private_folder
from .live_preflight import drive_health
from .live_recovery_canary import read_json_file, write_json
from .production_preflight import reconciled_catalog
from .release_batch import (
    MANIFEST_NAME, MAX_ATTEMPTS, PRICE_LANES, WINDOW_LIMIT, accept, compile_daily,
    destination, dispatch, report,
)
from .release_rehearsal import RehearsalError, require, root_folder
from .release_remediation import pinned_contracts


PREFIX = "R12_1_ROLLOUT"
MODE = "TWO_RUN_RELEASE_GATE"
MONTHS = (date(2026, 6, 1), date(2026, 7, 1))
CUTOFF = date(2026, 7, 31)
LANES = PRICE_LANES | PUBLISH
PER_RUN = len(LANES)
TERMINAL = {"VERIFIED", "BLOCKED", "REPAIR"}


def budget_for(items):
    # Reserve for all potential staging copies, the final gzip and bounded journal/receipt overhead.
    packed_limit = WINDOW_LIMIT + 1024
    return {
        "source_requests_base": len(items),
        "source_requests_including_retries": len(items) * MAX_ATTEMPTS,
        "source_requests_per_workflow_run": PER_RUN,
        "storage_bytes": len(items) * (MAX_ATTEMPTS + 1) * packed_limit + 16 * 1024 * 1024,
    }


def campaign(access, origin, *, create):
    parent = ROOT
    verify_private_folder(parent, access)
    for name in ("00_COMMAND", "R12_BACKER"):
        parent = ensure(parent, name, FOLDER, access, create=False)
        verify_private_folder(parent, access)
    root = ensure(parent, f"{PREFIX}_{origin}", FOLDER, access, create=create)
    verify_private_folder(root, access)
    return parent, root


def freeze(access, parent, root, journal, origin):
    if find(root, MANIFEST_NAME, access) is None:
        catalog, _ = reconciled_catalog(access, parent)
        roster = {row["dataset"]: row for row in catalog}
        with ThreadPoolExecutor(max_workers=len(MONTHS)) as pool:
            calendar = {month: days for month, (status, days) in
                        zip(MONTHS, pool.map(twse_month, MONTHS), strict=True) if status == "PASS" and days}
        require(calendar.keys() == set(MONTHS), "OFFICIAL_CALENDAR_INCOMPLETE")
        rows = compile_daily(calendar, pinned_contracts(), roster, dataset_lanes=LANES, cutoff=CUTOFF)
        rows.sort(key=lambda row: (row["end"], row["lane"]), reverse=True)
        require(len(rows) == len(MONTHS) * PER_RUN, "EXPECTED_MANIFEST_COUNT")
        budget = budget_for(rows)
        health = drive_health(access)
        require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
                health["available_bytes"] >= budget["storage_bytes"], "ROLLOUT_CAPACITY")
        stage = root_folder(access, prefix=PREFIX, run_id=origin, attempt=1, create=True)
        parents = {}
        for row in rows:
            month = row["start"][:7]
            if month not in parents:
                parents[month] = destination(row, access, create=True)
            row["destination"] = parents[month]
            row["name"] = f"{row['dataset']}__{row['scope']}__{row['start']}__{row['end']}__r12_1.json.gz"
            require(find(row["destination"], row["name"], access) is None, "UNTRACKED_DESTINATION")
            row["destination_id"] = journal.new_id()
        dispositions = [
            {"dataset": row["dataset"], "lane": row["routing"], "evidence_run": row["evidence_run"],
             "state": row["state"], "admitted_windows": sum(item["dataset"] == row["dataset"] for item in rows)}
            for row in catalog
        ]
        write_json(root, "DATASET_MANIFEST.json", dispositions, access)
        write_json(root, MANIFEST_NAME, {
            "mode": MODE, "origin_run": origin, "commit": os.environ["GITHUB_SHA"], "stage": stage,
            "capacity_bound": budget["storage_bytes"], "budget": budget, "items": rows,
            "held_calendar_months": [], "dataset_manifest_sha": digest(encode(dispositions)),
        }, access)
    manifest = read_json_file(root, MANIFEST_NAME, access)
    journal.initialize({"manifest_sha": digest(encode(manifest)), "events": [], "pause": 0,
                        "tasks": {row["task_id"]: {"status": "READY", "attempts": 0} for row in manifest["items"]}})


def context(root, journal, access, origin):
    manifest = read_json_file(root, MANIFEST_NAME, access)
    record = journal.version()
    require(manifest["mode"] == MODE and manifest["origin_run"] == origin and
            manifest["commit"] == os.environ["GITHUB_SHA"], "ROLLOUT_IDENTITY")
    require(record and record["state"]["manifest_sha"] == digest(encode(manifest)), "ROLLOUT_MANIFEST_DIGEST")
    roster = read_json_file(root, "DATASET_MANIFEST.json", access)
    require(manifest["dataset_manifest_sha"] == digest(encode(roster)), "DATASET_MANIFEST_DIGEST")
    require(manifest["budget"] == budget_for(manifest["items"]) and
            manifest["capacity_bound"] == manifest["budget"]["storage_bytes"], "ROLLOUT_BUDGET_DIGEST")
    return manifest, record


def reconcile(manifest, record, journal, root, access, run, origin):
    record = accept(manifest, record, journal, access)
    state = record["state"]
    tasks = state["tasks"]
    tag = f"{run}_{os.environ['GITHUB_RUN_ATTEMPT']}"
    for lane in LANES.values():
        rows = {row["task_id"]: tasks[row["task_id"]] for row in manifest["items"] if row["lane"] == lane}
        events = [event for event in state["events"] if event["run"] == run and event["task_id"] in rows]
        write_json(root, f"{lane}_{tag}.json", {"tasks": rows, "source_events": events}, access)
    repairs = {key: task for key, task in tasks.items() if task["status"] in {"BLOCKED", "REPAIR"}}
    write_json(root, f"B09_{tag}.json", repairs, access)
    counts = Counter(task["status"] for task in tasks.values())
    pending = len(tasks) - sum(counts[status] for status in TERMINAL)
    result = report(manifest, record, run) | {
        "gate": "TERMINAL" if not pending else "CHECKPOINTED", "pending": pending, "origin_run": origin,
        "source_run": run, "verification_finmind_requests": 0, "budget": manifest["budget"],
        "automatic_resume": run == origin and pending > 0 and not state.get("auth_hold") and state["pause"] <= int(time()),
    }
    write_json(root, f"B10_{tag}.json", result | {"receipts": {
        key: task["receipt"] for key, task in tasks.items() if task["status"] == "VERIFIED"
    }}, access)
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as output:
            print(f"resume={'true' if result['automatic_resume'] else 'false'}", file=output)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("dispatch", "accept"))
    parser.add_argument("--origin", type=int)
    args = parser.parse_args()
    run = int(os.environ["GITHUB_RUN_ID"])
    origin = run if args.origin is None else args.origin
    try:
        require(origin > 0 and (args.origin is None or origin != run), "INDEPENDENT_RUN_REQUIRED")
        access = token()
        seed = args.origin is None and args.phase == "dispatch"
        parent, root = campaign(access, origin, create=seed)
        journal = DriveJournal(root, access)
        if seed and journal.version(0) is None:
            freeze(access, parent, root, journal, origin)
        manifest, record = context(root, journal, access, origin)
        if args.phase == "dispatch":
            if args.origin is not None:
                require(any(event["run"] == origin for event in record["state"]["events"]), "ORIGIN_CHECKPOINT")
            record = dispatch(manifest, record, journal, access, run, request_ceiling=PER_RUN)
            result = report(manifest, record, run) | {"budget": manifest["budget"], "origin_run": origin}
        else:
            result = reconcile(manifest, record, journal, root, access, run, origin)
        print(json.dumps(result, sort_keys=True))
        if args.phase == "accept" and args.origin is not None:
            require(result["pending"] == 0, "ROLLOUT_PENDING_AT_CHECKPOINT")
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_ROLLOUT_HOLD phase={args.phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
