"""Dispatch verified R12.1 windows and independently accept them in private Bronze."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict
from datetime import date, timedelta
import gzip
from io import BytesIO
import json
import os
from time import sleep, time
from urllib.error import HTTPError
from uuid import uuid4

from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .dispatch import Window
from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .history_pilot import BUSINESS, BUSINESS_START, NDC_PERIOD, read_verified_history_archive, twse_month
from .integration_pilot import digest, encode, verify_private_folder
from .live_bronze_admission import advance, fetch_original
from .live_preflight import drive_health, read_json
from .live_recovery_canary import month_folder, read_json_file, write_json
from .manifest_pilot import manifest_windows, sample_data, serial_window
from .quota import TOTAL_LIMIT, can_reserve, retry_deadline
from .release_probes import MAX_BYTES, Task, read_private_file
from .release_readiness import ACCOUNT
from .release_rehearsal import RehearsalError, require, root_folder
from .release_remediation import pinned_contracts

CAMPAIGN = "R12_1_VERIFIED_BATCH_20261126"
MANIFEST_NAME = "TASK_MANIFEST.json"
PRICE_LANES = {"TaiwanStockPrice": "B01", "TaiwanStockMarketValue": "B04"}
MONTHS = tuple(date(2026, month, 1) for month in (8, 9, 10, 11))
CUTOFF = date(2026, 11, 23)
WINDOW_LIMIT = 262144
MAX_ATTEMPTS = 4
HISTORY_RUN = 45651295300


def task_for(item):
    return Task(item["dataset"], item["start"], item["end"], item["scope"] if item["dataset"] != BUSINESS else None)


def next_month(day):
    return (day.replace(day=28) + timedelta(days=4)).replace(day=1)


def compile_daily(calendar, contracts, roster):
    rows = []
    for dataset, lane in PRICE_LANES.items():
        contract = contracts[dataset]
        require(roster[dataset]["state"] == "SAMPLE_ONLY" and roster[dataset]["routing"] == lane
                and contract.tier in {"Free(w/ data_id)", "Backer"} and "`end_date`" in contract.optional
                and {"date", "stock_id"} <= set(contract.fields), "DAILY_SCOPE_CONTRACT")
        for month, sessions in calendar.items():
            end = min(next_month(month) - timedelta(days=1), CUTOFF)
            days = sorted(day.isoformat() for day in sessions if month <= day <= end)
            require(bool(days), "CALENDAR_EMPTY")
            contract_id = digest(encode(asdict(contract)))
            window, = manifest_windows([{
                "dataset": dataset, "lane": lane, "scope": "2330", "start": month.isoformat(),
                "end": end.isoformat(), "listing_start": month.isoformat(), "listing_end": end.isoformat(),
                "contract": contract_id,
                "evidence": {"entitlement": "Documented scope checked against the current account",
                             "coverage_bounds": "Published TWSE session roster",
                             "dataset_contract": contract_id,
                             "market_identity": "TWSE 2330",
                             "query_mode": "One stock and a closed monthly interval"},
            }])
            rows.append(serial_window(window) | {"fields": list(contract.fields), "dates": days})
    return rows


def proof_for(item, packed, raw=None):
    if raw is None:
        bound = MAX_BYTES if item["dataset"] == BUSINESS else WINDOW_LIMIT
        with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
            raw = stream.read(bound + 1)
    bound = MAX_BYTES if item["dataset"] == BUSINESS else WINDOW_LIMIT
    require(len(raw) <= bound, "SIZE_LIMIT")
    reply = json.loads(raw)
    require(isinstance(reply, dict) and reply.get("status") == 200 and
            set(reply) <= {"msg", "status", "data"}, "STATUS_OR_PAGINATION")
    rows = reply.get("data")
    require(isinstance(rows, list) and all(isinstance(row, dict) and
            set(item["fields"]) <= row.keys() for row in rows), "SOURCE_FIELDS")
    observed = [row["date"] for row in rows]
    require(all(isinstance(day, str) for day in observed) and sorted(observed) == item["dates"]
            and len(observed) == len(set(observed)) and bool(observed), "DATE_KEY_COVERAGE")
    if item["dataset"] != BUSINESS:
        require(all(row["stock_id"] == item["scope"] for row in rows), "SYMBOL_COVERAGE")
    return {"rows": len(rows), "bytes": len(packed), "raw_sha": digest(raw), "packed_sha": digest(packed),
            "first_sha": digest(encode(rows[0])), "last_sha": digest(encode(rows[-1]))}


def history_item(access, contracts):
    root = root_folder(access, prefix="R11_HISTORY_PILOT", run_id=HISTORY_RUN, attempt=1, create=False)
    receipt, raw, packed = read_verified_history_archive(access, root)
    period = BUSINESS_START
    days = []
    while period <= NDC_PERIOD:
        days.append(period.isoformat())
        period = next_month(period)
    contract = contracts[BUSINESS]
    require(contract.tier == "Backer" and "date" in contract.fields, "MONTHLY_SCOPE_CONTRACT")
    window = Window(BUSINESS, "TW", BUSINESS_START, NDC_PERIOD, digest(encode(asdict(contract))), "B07")
    item = serial_window(window) | {"fields": list(contract.fields), "dates": days}
    proof = proof_for(item, packed, raw)
    require(proof["raw_sha"] == receipt["raw_sha256"] and proof["packed_sha"] == receipt["gzip_sha256"],
            "ARCHIVED_SOURCE_CHANGED")
    return item | {"source_file_id": receipt["file_id"], "archived_proof": proof}


def private_campaign(access, *, create):
    parent = ROOT
    verify_private_folder(parent, access)
    for name in ("00_COMMAND", "R12_BACKER", CAMPAIGN):
        parent = ensure(parent, name, FOLDER, access, create=create and name == CAMPAIGN)
        verify_private_folder(parent, access)
    return parent


def destination(item, access, *, create):
    bronze = ensure(ROOT, "20_BRONZE", FOLDER, access, create=False)
    verify_private_folder(bronze, access)
    return month_folder(bronze, task_for(item), access, create=create)


def freeze(root, journal, access, run):
    if find(root, MANIFEST_NAME, access) is None:
        contracts = pinned_contracts()
        roster = {row["dataset"]: row for row in sample_data()}
        calendar, held = {}, []
        with ThreadPoolExecutor(max_workers=4) as pool:
            for month, (status, dates) in zip(MONTHS, pool.map(twse_month, MONTHS), strict=True):
                if status == "PASS" and any(day <= CUTOFF for day in dates):
                    calendar[month] = dates
                else:
                    held.append({"month": month.strftime("%Y-%m"), "status": status})
        rows = compile_daily(calendar, contracts, roster)
        try:
            rows.append(history_item(access, contracts))
        except (RehearsalError, OSError, ValueError, RuntimeError):
            held.append({"dataset": BUSINESS, "status": "HOLD_ARCHIVED_SOURCE"})
        require(bool(rows), "NO_EVIDENCED_WINDOWS")
        rows.sort(key=lambda row: (row["end"], row["task_id"]), reverse=True)
        health = drive_health(access)
        worst_case = sum(2 * WINDOW_LIMIT if "archived_proof" not in row else row["archived_proof"]["bytes"]
                         for row in rows)
        worst_case += 1048576
        require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
                health["available_bytes"] >= worst_case, "BATCH_CAPACITY")
        stage = root_folder(access, prefix="R12_RELEASE_BATCH", run_id=run, attempt=1, create=True)
        parents = {}
        for row in rows:
            month = row["start"][:7]
            if month not in parents:
                parents[month] = destination(row, access, create=True)
            row["destination"] = parents[month]
            row["name"] = f"{row['dataset']}__{row['scope']}__{row['start']}__{row['end']}__r12_1.json.gz"
            require(find(row["destination"], row["name"], access) is None, "EXISTING_UNTRACKED_DESTINATION")
            row["destination_id"] = journal.new_id()
        manifest = {"mode": "VERIFIED_SCOPE_BATCH", "origin_run": run, "stage": stage,
                    "capacity_bound": worst_case, "items": rows, "held_calendar_months": held}
        write_json(root, MANIFEST_NAME, manifest, access)
    manifest = read_json_file(root, MANIFEST_NAME, access)
    journal.initialize({"manifest_sha": digest(encode(manifest)), "events": [], "pause": 0,
                        "tasks": {row["task_id"]: {"status": "STAGED" if "source_file_id" in row else "READY",
                                                  "attempts": 0} for row in manifest["items"]}})
    return manifest


def load(root, journal, access):
    manifest = read_json_file(root, MANIFEST_NAME, access)
    record = journal.version()
    require(record is not None and manifest["mode"] == "VERIFIED_SCOPE_BATCH" and
            digest(encode(manifest)) == record["state"]["manifest_sha"], "MANIFEST_DIGEST")
    return manifest, record


def stage_name(key, attempt):
    return f"{key}-{attempt}.json.gz"


def stage_read(manifest, item, state, access):
    key = item["task_id"]
    if "source_file_id" in item:
        file_id = item["source_file_id"]
    else:
        file_id = ensure(manifest["stage"], stage_name(key, state["attempts"]), "application/gzip",
                         access, create=False, file_id=state["stage_id"])
    verify_private_folder(file_id, access)
    packed = read_private_file(file_id, access)
    proof = proof_for(item, packed)
    expected = item.get("archived_proof", state.get("proof"))
    require(expected is None or proof == expected, "STAGED_PROOF")
    return packed, proof


def retry_state(task, outcome, now):
    state = task | {"reason": outcome}
    if task["attempts"] >= MAX_ATTEMPTS:
        return state | {"status": "REPAIR"}
    return state | {"status": "READY", "retry_after": now + 2 ** min(task["attempts"], 3)}


def recover_pending(manifest, state, access):
    updated = deepcopy(state["tasks"])
    now = int(time())
    for row in manifest["items"]:
        key = row["task_id"]
        task = updated[key]
        if task["status"] != "REQUESTING" or task["lease_until"] > now:
            continue
        if find(manifest["stage"], stage_name(key, task["attempts"]), access) is None:
            updated[key] = retry_state(task, "INTERRUPTED_REQUEST", now)
            continue
        _, proof = stage_read(manifest, row, task, access)
        updated[key] = task | {"status": "STAGED", "proof": proof, "recovered": True}
    return updated


def stored_event(run, kind, now, key=None):
    return {"kind": kind, "at": now, "run": run, "task_id": key, "request_id": uuid4().hex}


def account_check(journal, record, run, auth):
    now = int(time())
    state = record["state"]
    if state.get("auth_hold") or now < state["pause"]:
        return record
    account = state.get("account")
    if account is not None and 0 <= now - account["at"] <= 150:
        return record
    events = state["events"]
    if not can_reserve([(e["at"], e["kind"]) for e in events], now, "probe", total_limit=TOTAL_LIMIT,
                       normal_limit=TOTAL_LIMIT, paused_until=state["pause"]):
        return record
    record = advance(journal, record, events=events + [stored_event(run, "probe", now)])
    print("finmind_account_request=1")
    try:
        reply = read_json(ACCOUNT, auth)
    except HTTPError as error:
        if error.code == 429:
            pause = retry_deadline(error.headers.get("Retry-After") if error.headers else None, now)
            return advance(journal, record, pause=pause, account=None)
        raise
    limit, used = reply.get("api_request_limit"), reply.get("user_count")
    require(type(limit) is int and limit >= TOTAL_LIMIT and type(used) is int and 0 <= used <= limit,
            "BACKER_ACCOUNT")
    return advance(journal, record, account={"at": now, "limit": min(TOTAL_LIMIT, limit),
                                             "headroom": max(0, limit - used - 1),
                                             "first_event": len(record["state"]["events"])})


def ready_wave(items, state, now):
    account = state.get("account")
    if not account or state.get("auth_hold") or now < state["pause"] or not 0 <= now - account["at"] <= 150:
        return []
    remaining = account["headroom"] - (len(state["events"]) - account["first_event"])
    events = [(entry["at"], entry["kind"]) for entry in state["events"]]
    lanes, selected = set(), []
    for row in items:
        task = state["tasks"][row["task_id"]]
        if (len(selected) >= remaining or row["lane"] in lanes or task["status"] != "READY"
                or task.get("retry_after", 0) > now):
            continue
        if not can_reserve(events, now, "download", normal_limit=account["limit"],
                           total_limit=account["limit"], paused_until=state["pause"]):
            break
        selected.append(row)
        lanes.add(row["lane"])
        events.append((now, "download"))
    return selected


def receive(manifest, item, task, access, auth):
    try:
        raw = fetch_original(auth, task_for(item), byte_limit=WINDOW_LIMIT)
        require(len(raw) <= WINDOW_LIMIT, "SIZE_LIMIT")
        result = json.loads(raw)
        if isinstance(result, dict) and result.get("status") in (401, 403, 429):
            return ("HTTP", result["status"], None)
        packed, _, _ = compress_original(raw)
        proof = proof_for(item, packed, raw)
    except HTTPError as error:
        return ("HTTP", error.code, error.headers.get("Retry-After") if error.headers else None)
    except RehearsalError as error:
        return ("HOLD", str(error), None)
    except OSError:
        return ("RETRY", "NETWORK_OR_TIMEOUT", None)
    except ValueError:
        return ("HOLD", "INVALID_REPLY", None)
    name = stage_name(item["task_id"], task["attempts"])
    file_id = ensure(manifest["stage"], name, "application/gzip", access, packed,
                     create=True, file_id=task["stage_id"])
    verify_private_folder(file_id, access)
    require(read_private_file(file_id, access) == packed, "STAGING_BYTES")
    return ("STAGED", proof, None)


def dispatch(manifest, record, journal, access, run):
    health = drive_health(access)
    require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
            health["available_bytes"] >= manifest["capacity_bound"], "BATCH_CAPACITY_CHANGED")
    verify_private_folder(manifest["stage"], access)
    tasks = recover_pending(manifest, record["state"], access)
    if tasks != record["state"]["tasks"]:
        record = advance(journal, record, tasks=tasks)
    if not any(task["status"] == "READY" for task in record["state"]["tasks"].values()):
        return record
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    record = account_check(journal, record, run, auth)
    while True:
        now = int(time())
        current = record["state"]
        wave = ready_wave(manifest["items"], current, now)
        if not wave:
            retry = min((t["retry_after"] for t in current["tasks"].values()
                         if t["status"] == "READY" and 0 < t.get("retry_after", 0) - now <= 8), default=0)
            if retry and current["pause"] <= now and not current.get("auth_hold"):
                sleep(retry - now)
                continue
            return record
        tasks = deepcopy(current["tasks"])
        events = list(current["events"])
        for row in wave:
            key = row["task_id"]
            tasks[key] = tasks[key] | {"status": "REQUESTING", "attempts": tasks[key]["attempts"] + 1,
                                       "stage_id": journal.new_id(), "lease_until": now + 90}
            events.append(stored_event(run, "download", now, key))
        record = advance(journal, record, tasks=tasks, events=events)
        print(f"finmind_source_requests={len(wave)} active_lanes={len(wave)}")
        pause, auth_hold = current["pause"], current.get("auth_hold", False)
        with ThreadPoolExecutor(max_workers=len(wave)) as pool:
            results = [pool.submit(receive, manifest, row, tasks[row["task_id"]], access, auth) for row in wave]
            for row, result in zip(wave, results, strict=True):
                key = row["task_id"]
                try:
                    status, outcome, after = result.result()
                except (OSError, ValueError, RuntimeError):
                    # Keep the reserved ID for recovery when upload success could not be determined.
                    tasks[key] = tasks[key] | {"reason": "STAGING_RESULT_UNCERTAIN"}
                    continue
                if status == "STAGED":
                    tasks[key] = tasks[key] | {"status": "STAGED", "proof": outcome}
                elif status == "RETRY" or status == "HTTP" and outcome in (408, 429, 500, 502, 503, 504):
                    if outcome == 429:
                        pause = max(pause, retry_deadline(after, int(time())))
                    tasks[key] = retry_state(tasks[key], str(outcome), int(time()))
                else:
                    auth_hold |= outcome == 401
                    tasks[key] = tasks[key] | {"status": "BLOCKED", "reason": str(outcome)}
        record = advance(journal, record, tasks=tasks, pause=pause, auth_hold=auth_hold)


def accept(manifest, record, journal, access, *, audit=False):
    tasks = deepcopy(record["state"]["tasks"])
    parents = {}
    for item in manifest["items"]:
        key = item["task_id"]
        task = tasks[key]
        if task["status"] != "VERIFIED" and (audit or task["status"] != "STAGED"):
            continue
        month = item["start"][:7]
        if month not in parents:
            parents[month] = destination(item, access, create=False)
        parent = parents[month]
        require(parent == item["destination"], "DESTINATION_CHANGED")
        if task["status"] == "VERIFIED":
            receipt = task["receipt"]
            file_id = ensure(parent, item["name"], "application/gzip", access, create=False,
                             file_id=item["destination_id"])
            verify_private_folder(file_id, access)
            require(receipt["file_id"] == file_id and proof_for(item, read_private_file(file_id, access)) == receipt["proof"],
                    "EXISTING_RECEIPT_CHANGED")
            continue
        try:
            packed, proof = stage_read(manifest, item, task, access)
            file_id = ensure(parent, item["name"], "application/gzip", access, packed, create=True,
                             file_id=item["destination_id"])
            verify_private_folder(file_id, access)
            saved = read_private_file(file_id, access)
            require(proof_for(item, saved) == proof and saved == packed, "BRONZE_READBACK")
            tasks[key] = task | {"status": "VERIFIED", "receipt": {"file_id": file_id, "proof": proof}}
        except (RehearsalError, ValueError, RuntimeError):
            tasks[key] = task | {"status": "BLOCKED", "reason": "BRONZE_VALIDATION"}
        except OSError:
            tasks[key] = task | {"reason": "BRONZE_RETRY_PENDING"}
    if tasks != record["state"]["tasks"]:
        record = advance(journal, record, tasks=tasks)
    return record


def report(manifest, record, run):
    tasks = record["state"]["tasks"]
    counts = Counter(task["status"] for task in tasks.values())
    receipts = [task["receipt"]["proof"] for task in tasks.values() if task["status"] == "VERIFIED"]
    events = [event for event in record["state"]["events"] if event["run"] == run]
    current = Counter(event["kind"] for event in events)
    return {"batch": "VERIFIED" if len(receipts) == len(tasks) and
            not manifest["held_calendar_months"] else "PARTIAL",
            "scope_only": True, "full_volume": "HOLD", "planned": len(tasks), "states": dict(counts),
            "verified_rows": sum(row["rows"] for row in receipts), "verified_bytes": sum(row["bytes"] for row in receipts),
            "verified_lanes": sorted({item["lane"] for item in manifest["items"]
                                      if tasks[item["task_id"]]["status"] == "VERIFIED"}),
            "account_reservations_this_run": current.get("probe", 0),
            "source_reservations_this_run": current.get("download", 0),
            "held_calendar_months": len(manifest["held_calendar_months"])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("dispatch", "accept", "audit"))
    phase = parser.parse_args().phase
    try:
        access = token()
        run = int(os.environ["GITHUB_RUN_ID"])
        root = private_campaign(access, create=phase == "dispatch")
        journal = DriveJournal(root, access)
        if phase == "dispatch" and journal.version(0) is None:
            freeze(root, journal, access, run)
        manifest, record = load(root, journal, access)
        if phase == "dispatch":
            record = dispatch(manifest, record, journal, access, run)
        else:
            record = accept(manifest, record, journal, access, audit=phase == "audit")
            if phase == "accept":
                private = {row["task_id"]: record["state"]["tasks"][row["task_id"]]
                           for row in manifest["items"]
                           if record["state"]["tasks"][row["task_id"]]["status"] in {"REPAIR", "BLOCKED"}}
                write_json(root, f"B09_RUN_{run}_{os.environ['GITHUB_RUN_ATTEMPT']}.json", private, access)
        print(json.dumps(report(manifest, record, run), sort_keys=True))
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_RELEASE_BATCH_HOLD phase={phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
