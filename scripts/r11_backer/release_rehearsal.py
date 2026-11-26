"""Rehearse a manifest-driven Backer batch using previously accepted originals."""

import argparse
from collections import defaultdict
import gzip
from io import BytesIO
import json
import os
from urllib.parse import urlencode

from .drive_pilot import BASE, FOLDER, ensure, find, token
from .integration_pilot import digest, encode, verify_private_folder
from .live_preflight import drive_health, read_json
from .live_recovery_canary import TASKS, evidence, month_folder, read_json_file, write_json
from .manifest_pilot import manifest_windows, serial_window
from .quota import can_reserve, retry_deadline
from .release_probes import MAX_BYTES, RECOVERY_ROOT, read_private_file, recovery_location

SOURCE_RUN = 45797252783
SOURCE_ATTEMPT = 1
PREFIX = "R12_RELEASE_REHEARSAL"
MANIFEST = "TASK_MANIFEST.json"
CLAIMS = "MASTER_CLAIMS.json"
CHECKPOINT = "WORKER_CHECKPOINT.json"
RECOVERY = "MASTER_RECOVERY.json"


class RehearsalError(ValueError):
    """A static label safe for GitHub logs."""


def require(condition, label):
    if not condition:
        raise RehearsalError(label)


def root_folder(access, *, prefix, run_id, attempt, create):
    name = recovery_location(access, prefix=prefix, run_id=run_id, attempt=attempt)
    root = ensure(RECOVERY_ROOT, name, FOLDER, access, create=create)
    verify_private_folder(root, access)
    return root


def read_original(task, file_id, proof, access):
    verify_private_folder(file_id, access)
    packed = read_private_file(file_id, access)
    require(len(packed) == proof["bytes"] and digest(packed) == proof["packed_sha"], "PACKED_SHA")
    with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
        raw = stream.read(MAX_BYTES + 1)
    require(evidence(task, raw, packed) == proof, "SOURCE_PROOF")
    return packed


def connected_manifest(access):
    """Use the previous accepted receipts as evidence for these exact rehearsal windows."""
    prior = root_folder(access, prefix="R12_LIVE_RECOVERY_CANARY", run_id=SOURCE_RUN,
                        attempt=SOURCE_ATTEMPT, create=False)
    approved, originals = [], {}
    lanes = dict(zip((task.dataset for task in TASKS), ("B01", "B07", "B06"), strict=True))
    for task in TASKS:
        receipt = read_json_file(prior, f"{task.dataset}-accepted.json", access)
        proof = receipt["source"]
        parent = month_folder(prior, task, access, create=False)
        source_id = ensure(parent, task.dataset + ".json.gz", "application/gzip", access, create=False)
        require(source_id == receipt["file_id"], "BASELINE_FILE_ID")
        packed = read_original(task, source_id, proof, access)
        contract = digest(encode({"version": "archived-response-v1", "parameters": task.parameters()}))
        approved.append({
            "dataset": task.dataset, "scope": task.data_id or "TW", "lane": lanes[task.dataset],
            "start": task.start, "end": task.end, "listing_start": task.start, "listing_end": task.end,
            "contract": contract,
            "evidence": {
                "entitlement": "previous successful Backer receipt",
                "coverage_bounds": "rehearsal window; historical and pagination completeness not inferred",
                "dataset_contract": "exact source proof and stored payload",
                "market_identity": "exact scope in the archived request and tested rows",
                "query_mode": "archived response replay",
            },
        })
        originals[task.dataset] = {"source_id": source_id, "proof": proof, "packed": packed}
    return [serial_window(window) for window in manifest_windows(approved)], originals


def run_owner(ledger, task_id, owner, generation):
    claim = ledger["active"][task_id]
    return claim["owner"] == owner and claim["generation"] == generation


def initial_ledger(manifest):
    return {
        "manifest_sha": digest(encode(manifest)),
        "active": {row["task_id"]: {"owner": row["lane"], "generation": 1} for row in manifest["tasks"]},
        "quota": {
            "finmind_requests": 0,
            "simulation": {"events": [[100, "download"], [100, "retry"]],
                           "paused_until": retry_deadline("180", 100)},
        },
    }


def prepare(root, access):
    windows, originals = connected_manifest(access)
    packed_bytes = sum(len(value["packed"]) for value in originals.values())
    health = drive_health(access)
    # Reserve space for the exact copies and all rehearsal journals before staging.
    headroom = health.get("available_bytes")
    require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
            isinstance(headroom, int) and headroom >= packed_bytes + MAX_BYTES, "REHEARSAL_CAPACITY")
    manifest_file = find(root, MANIFEST, access)
    if manifest_file is None:
        ids = read_json(f"{BASE}/files/generateIds?count={len(windows)}&space=drive", access)["ids"]
        require(len(set(ids)) == len(windows), "ALLOCATED_FILE_IDS")
        manifest = {
            "kind": "ARCHIVED_SOURCE_REHEARSAL", "source_run": SOURCE_RUN,
            "tasks": [row | {"destination_id": dest} | {
                key: value for key, value in originals[row["dataset"]].items() if key != "packed"
            } for row, dest in zip(windows, ids, strict=True)],
        }
        write_json(root, MANIFEST, manifest, access)
    else:
        manifest = read_json_file(root, MANIFEST, access)
        expected = [
            row | {key: value for key, value in originals[row["dataset"]].items() if key != "packed"}
            for row in windows
        ]
        actual = [
            {key: value for key, value in row.items() if key != "destination_id"}
            for row in manifest["tasks"]
        ]
        require(manifest["kind"] == "ARCHIVED_SOURCE_REHEARSAL" and manifest["source_run"] == SOURCE_RUN
                and actual == expected, "EXISTING_MANIFEST_MISMATCH")
    ledger = initial_ledger(manifest)
    write_json(root, CLAIMS, ledger, access)

    stopped = []
    archive = ensure(root, "ARCHIVE", FOLDER, access, create=True)
    verify_private_folder(archive, access)
    for row in manifest["tasks"]:
        name = row["dataset"]
        task = next(task for task in TASKS if task.dataset == name)
        require(run_owner(ledger, row["task_id"], row["lane"], 1), "CLAIM")
        parent = month_folder(archive, task, access, create=True)
        fixed_id = row["destination_id"]
        ensure(parent, name + ".json.gz", "application/gzip", access,
               originals[name]["packed"], create=True, file_id=fixed_id)
        read_original(task, fixed_id, row["proof"], access)
        if name == "TaiwanBusinessIndicator":
            stopped.append(row["task_id"])
        else:
            write_json(root, f"{row['lane']}-{row['task_id']}-upload.json",
                       {"task_id": row["task_id"], "file_id": fixed_id, "generation": 1}, access)
    # A controlled interruption relinquishes the lease after the archive commits and before the worker logs it.
    write_json(root, CHECKPOINT, {"manifest_sha": ledger["manifest_sha"], "relinquished": stopped}, access)
    print(json.dumps({"prepared": len(windows), "finmind_requests": 0,
                      "rehearsal_copy_bytes": packed_bytes, "production": "HOLD"}))


def read_state(root, access):
    manifest = read_json_file(root, MANIFEST, access)
    claims = read_json_file(root, CLAIMS, access)
    checkpoint = read_json_file(root, CHECKPOINT, access)
    require(manifest["kind"] == "ARCHIVED_SOURCE_REHEARSAL" and manifest["source_run"] == SOURCE_RUN,
            "REHEARSAL_MODE")
    require(claims == initial_ledger(manifest) and checkpoint["manifest_sha"] == claims["manifest_sha"],
            "MANIFEST_OR_LEDGER_MISMATCH")
    quota = claims["quota"]["simulation"]
    limits = {"normal_limit": 1, "total_limit": 2, "paused_until": quota["paused_until"]}
    require(not can_reserve([], 101, "retry", **limits)
            and not can_reserve(quota["events"], 280, "retry", **limits)
            and can_reserve(quota["events"], 3700, "download", **limits), "QUOTA_REPLAY")
    known = {row["task_id"] for row in manifest["tasks"] if row["dataset"] == "TaiwanBusinessIndicator"}
    require(known and set(checkpoint["relinquished"]) == known, "CHECKPOINT_SCOPE")
    for key in known:
        claims["active"][key] = {"owner": "B09", "generation": 2}
    return manifest, claims


def validate_inventory(inventory, access):
    for parent, names in inventory.items():
        query = urlencode({"q": f"'{parent}' in parents and trashed = false", "pageSize": 100,
                           "fields": "nextPageToken,incompleteSearch,files(name)"})
        listing = read_json(f"{BASE}/files?{query}", access)
        require(not listing.get("nextPageToken") and not listing.get("incompleteSearch")
                and sorted(row["name"] for row in listing["files"]) == sorted(names),
                "DRIVE_INVENTORY_MISMATCH")


def acceptance_receipt(ledger, row, file_id, *, owner, generation):
    require(run_owner(ledger, row["task_id"], owner, generation), "STALE_LEASE")
    return {
        "task_id": row["task_id"], "manifest_sha": ledger["manifest_sha"],
        "claim": {"owner": owner, "generation": generation},
        "file_id": file_id, "proof": row["proof"],
    }


def reconcile(root, access, *, recover):
    manifest, ledger = read_state(root, access)
    if recover:
        write_json(root, RECOVERY, ledger, access)
    else:
        require(read_json_file(root, RECOVERY, access) == ledger, "RECOVERY_LEDGER_MISMATCH")

    archive = ensure(root, "ARCHIVE", FOLDER, access, create=False)
    verify_private_folder(archive, access)
    inventory = defaultdict(set, {root: {MANIFEST, CLAIMS, CHECKPOINT, RECOVERY, "ARCHIVE"}})
    seen, verified_rows, repaired = set(), 0, 0
    for row in manifest["tasks"]:
        task = next(task for task in TASKS if task.dataset == row["dataset"])
        task_id = row["task_id"]
        claim = ledger["active"][task_id]
        parent = month_folder(archive, task, access, create=False, inventory=inventory)
        archive_name = task.dataset + ".json.gz"
        inventory[parent].add(archive_name)
        file_id = ensure(parent, archive_name, "application/gzip", access, create=False,
                         file_id=row["destination_id"])
        require(file_id not in seen and file_id != row["source_id"], "DUPLICATE_FILE_ID")
        seen.add(file_id)
        read_original(task, file_id, row["proof"], access)
        if claim["generation"] == 2:
            try:
                acceptance_receipt(ledger, row, file_id, owner=row["lane"], generation=1)
            except RehearsalError as error:
                require(str(error) == "STALE_LEASE", "STALE_LEASE_WRONG_ERROR")
            else:
                raise RehearsalError("STALE_LEASE_ACCEPTED")
            repaired += 1
        upload_name = f"{row['lane']}-{task_id}-upload.json"
        logged_upload = {"task_id": task_id, "file_id": file_id, "generation": claim["generation"]}
        if recover and claim["generation"] == 2:
            write_json(root, upload_name, logged_upload, access)
        require(read_json_file(root, upload_name, access) == logged_upload, "WORKER_UPLOAD_MISMATCH")
        receipt_name = f"B10-{task_id}.json"
        receipt = acceptance_receipt(ledger, row, file_id, **claim)
        if recover:
            write_json(root, receipt_name, receipt, access)
        require(read_json_file(root, receipt_name, access) == receipt, "ACCEPTANCE_RECEIPT_MISMATCH")
        inventory[root].update((upload_name, receipt_name))
        verified_rows += row["proof"]["rows"]

    validate_inventory(inventory, access)
    result = {"verified": len(seen), "verified_rows": verified_rows, "recovered_leases": repaired,
              "private_folders_reconciled": len(inventory), "quota_replay": "PASS_SIMULATED",
              "finmind_requests": 0, "production": "HOLD"}
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "recover", "accept"))
    phase = parser.parse_args().phase
    try:
        access = token()
        run = int(os.environ["GITHUB_RUN_ID"])
        # Failed-job reruns continue the same frozen rehearsal. Archive jobs record each Run Attempt.
        root = root_folder(access, prefix=PREFIX, run_id=run, attempt=1, create=phase == "prepare")
        if phase == "prepare":
            prepare(root, access)
        else:
            reconcile(root, access, recover=phase == "recover")
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_RELEASE_REHEARSAL_FAILED phase={phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
