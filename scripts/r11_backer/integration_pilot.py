"""Exercise B00–B10 together against a private Drive recovery sandbox."""

import argparse
from collections import defaultdict
from datetime import date, timedelta
import gzip
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
from time import monotonic, sleep
from urllib.parse import urlencode

from .archive import archive_plan, compress_original
from .dispatch import Window, assignments
from .drive_pilot import BASE, FOLDER, ensure, find, token
from .live_preflight import read_json
from .manifest_pilot import serial_window
from .pilot_ledger import PilotFailure, PilotLedger, require
from .quota import can_reserve
from .release_probes import RECOVERY_ROOT, read_private_file, recovery_location

MAX_FIXTURE_BYTES = 65536
GATES = tuple(f"G{number}" for number in range(6))


def digest(body):
    return hashlib.sha256(body).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def fixtures():
    windows = []
    for number in range(1, 9):
        start, end = (date(1982, 12, 31), date(1983, 1, 2))
        if number == 8:
            start, end = date(2026, 11, 23), date(2026, 11, 24)
        windows.append(Window(f"SyntheticLane{number:02}", "fixture", start, end,
                              "fixture-v1", f"B{number:02}"))
    return assignments(windows, {window.dataset: window.end for window in windows})


HELD_TASK = digest(b"synthetic-missing-coverage-evidence")


def manifest_hash(windows):
    return digest(encode({"tasks": [serial_window(item) for item in windows], "held": HELD_TASK}))


def sample(item):
    rows = [
        {"date": day.isoformat(), "published": (day + timedelta(days=7)).isoformat(),
         "symbol": item.scope, "value": number, "unit": "fixture-native"}
        for number, day in enumerate((item.start, item.end), 1)
    ]
    return encode({"status": 200, "data": rows}) + b"\n"


def archive_names(item):
    prefix = f"FinMind_Backer__{item.dataset}__{item.scope}__{item.start}__{item.end}__{item.contract}"
    return prefix + ".json.gz", prefix + "__manifest.json"


def make_receipt(item, raw, packed, parent, file_id):
    require(len(raw) <= MAX_FIXTURE_BYTES, "G1_ORIGINAL_SIZE")
    response = json.loads(raw)
    require(response.get("status") == 200 and isinstance(response.get("data"), list)
            and response["data"], "G1_BUSINESS_STATUS")
    rows = response["data"]
    required = {"date", "published", "symbol", "value", "unit"}
    require(all(isinstance(row, dict) and required <= row.keys() and type(row["value"]) is int
                and row["unit"] == "fixture-native" for row in rows), "G2_NATIVE_CONTRACT")
    try:
        dates = [date.fromisoformat(row["date"]) for row in rows]
        published = [date.fromisoformat(row["published"]) for row in rows]
    except (ValueError, TypeError):
        raise PilotFailure("G3_DATE_PARSE") from None
    keys = {(row["symbol"], day) for row, day in zip(rows, dates)}
    require(len(keys) == len(rows), "G3_DUPLICATE_KEY")
    require(dates == [item.start, item.end] and all(row["symbol"] == item.scope for row in rows),
            "G3_DATE_OR_SCOPE_COVERAGE")
    require(all(day <= announcement for day, announcement in zip(dates, published)), "G4_PUBLICATION")
    plan = archive_plan(dates)
    monthly_index = defaultdict(list)
    for number, day in enumerate(dates):
        monthly_index[day.isoformat()[:7]].append(number)
    return {
        "partition": serial_window(item), "path": plan.anchor, "file": archive_names(item)[0],
        "parent": parent, "file_id": file_id, "rows": len(rows), "bytes": len(packed),
        "raw_sha256": digest(raw), "gzip_sha256": digest(packed), "months": dict(monthly_index),
        "earliest_pit": [(announcement + timedelta(days=1)).isoformat() for announcement in published],
    }, plan


def verify_receipt(item, receipt, packed, parent, file_id):
    require(len(packed) == receipt["bytes"] and digest(packed) == receipt["gzip_sha256"], "G1_GZIP_SHA")
    with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
        raw = stream.read(MAX_FIXTURE_BYTES + 1)
    require(digest(raw) == receipt["raw_sha256"], "G1_ORIGINAL_SHA")
    expected, _ = make_receipt(item, raw, packed, parent, file_id)
    require(expected == receipt, "G4_PIT_OR_BIDIRECTIONAL_LINEAGE")


def verify_private_folder(folder, access):
    require(read_json(f"{BASE}/files/{folder}?fields=shared", access).get("shared") is False,
            "DRIVE_NOT_PRIVATE")


def recovery_root(access, *, create=False):
    name = recovery_location(access, prefix="R11_INTEGRATION")
    folder = ensure(RECOVERY_ROOT, name, FOLDER, access, create=create)
    verify_private_folder(folder, access)
    return folder


def archive_folder(item, root, access, ledger, owner):
    anchor = archive_plan([item.start, item.end]).anchor
    key = digest(anchor.encode())
    creating = owner != "B09" and ledger.reserve_folder(key, owner)
    if not creating:
        expiry = monotonic() + 45
        while ledger.read()[1]["folders"].get(key, {}).get("status") != "READY":
            require(monotonic() < expiry, "FOLDER_CREATE_OUTCOME_UNVERIFIED")
            sleep(1)
    parent = root
    for name in anchor.split("/"):
        parent = ensure(parent, name, FOLDER, access, create=creating)
    verify_private_folder(parent, access)
    if creating:
        ledger.folder_ready(key, owner)
    return parent


def verify_private_archive(item, parent, access):
    archive, manifest = archive_names(item)
    file_id = ensure(parent, archive, "application/gzip", access, create=False)
    manifest_id = ensure(parent, manifest, "application/json", access, create=False)
    receipt = json.loads(read_private_file(manifest_id, access))
    verify_receipt(item, receipt, read_private_file(file_id, access), parent, file_id)
    return file_id, manifest_id


def write_archive(item, parent, access, *, interrupt_before_receipt=False):
    original = sample(item)
    compressed, _, _ = compress_original(original)
    # Contract validation happens before the first Drive write.
    make_receipt(item, original, compressed, parent, "pending")
    archive, manifest = archive_names(item)
    file_id = ensure(parent, archive, "application/gzip", access, compressed, create=True)
    if interrupt_before_receipt:
        require(read_private_file(file_id, access) == compressed, "STAGED_BYTES_MISMATCH")
        require(find(parent, manifest, access) is None, "INTERRUPT_RECEIPT_ALREADY_PRESENT")
        return
    receipt, _ = make_receipt(item, original, compressed, parent, file_id)
    ensure(parent, manifest, "application/json", access, encode(receipt), create=True)
    verify_private_archive(item, parent, access)


def run_seed(ledger, windows):
    recovery_root(token(), create=True)
    ledger.seed(manifest_hash(windows), [item.task_id for item in windows], HELD_TASK)
    print("B00_fixture_manifest_frozen=PASS")


def run_worker(ledger, item, owner):
    generation = ledger.claim(item.task_id, owner)
    if generation is None:
        require(item.lane == "B01", "DISPATCH_UNEXPECTED_HOLD")
        print("duplicate_claim_blocked=PASS")
        return
    access = token()
    parent = archive_folder(item, recovery_root(access), access, ledger, owner)
    write_archive(item, parent, access, interrupt_before_receipt=item.lane == "B01")
    require(ledger.finalize(item.task_id, owner, generation, interrupt=item.lane == "B01"), "FINALIZE")
    print("fixture_staged_for_takeover=PASS" if item.lane == "B01" else "fixture_write_and_readback=PASS")


def run_recovery(ledger, item):
    key = item.task_id
    staged = ledger.read()[1]["tasks"][key]
    require(staged["status"] == "STAGED", "RECOVERY_MISSING_STAGE")
    ledger.simulate_retry_after()
    require(ledger.claim(HELD_TASK, "B09") is None, "G0_HELD_TASK_LEASED")
    generation = ledger.claim(key, "B09", recovery=True)
    require(generation == staged["generation"] + 1, "RECOVERY_LEASE")
    require(not ledger.finalize(key, staged["owner"], staged["generation"]), "STALE_LEASE_ACCEPTED")
    access = token()
    parent = archive_folder(item, recovery_root(access), access, ledger, "B09")
    archive, manifest = archive_names(item)
    require(find(parent, manifest, access) is None, "RECOVERY_RECEIPT_ALREADY_PRESENT")
    file_id = ensure(parent, archive, "application/gzip", access, create=False)
    original = sample(item)
    packed, _, _ = compress_original(original)
    require(read_private_file(file_id, access) == packed, "RECOVERY_FIXTURE_BYTES_MISMATCH")
    receipt, _ = make_receipt(item, original, packed, parent, file_id)
    ensure(parent, manifest, "application/json", access, encode(receipt), create=True)
    verified_id, _ = verify_private_archive(item, parent, access)
    require(verified_id == file_id, "RECOVERY_FILE_ID_CHANGED")
    require(ledger.finalize(key, "B09", generation), "RECOVERY_FINALIZE")
    require(not ledger.finalize(key, "B09", generation), "DUPLICATE_FINALIZE")
    print("cross_runner_missing_receipt_recovered_and_stale_writer_rejected=PASS")


def drive_inventory(windows, root, access):
    """Reconcile both directions, including unexpected Drive children and duplicate names."""
    folders = {"": root}
    expected = defaultdict(set)
    file_ids = set()
    for item in windows:
        parent, path = root, ""
        for name in archive_plan([item.start, item.end]).anchor.split("/"):
            expected[parent].add(name)
            path += "/" + name
            if path not in folders:
                folders[path] = ensure(parent, name, FOLDER, access, create=False)
                verify_private_folder(folders[path], access)
            parent = folders[path]
        expected[parent].update(archive_names(item))
        for file_id in verify_private_archive(item, parent, access):
            require(file_id not in file_ids, "G5_DUPLICATE_FILE_ID")
            file_ids.add(file_id)
    for parent, names in expected.items():
        query = urlencode({"q": f"'{parent}' in parents and trashed = false",
                           "fields": "nextPageToken,incompleteSearch,files(name)", "pageSize": 100})
        listing = read_json(f"{BASE}/files?{query}", access)
        require(not (listing.get("nextPageToken") or listing.get("incompleteSearch"))
                and sorted(entry["name"] for entry in listing["files"]) == sorted(names),
                "G5_UNEXPECTED_OR_DUPLICATE_CHILD")
    return len(folders) - 1, len(file_ids)


def negative_cases(item):
    original = sample(item)
    packed, _, _ = compress_original(original)
    receipt, _ = make_receipt(item, original, packed, "fixture-parent", "fixture-file")
    tests = [("G1_GZIP_SHA", lambda: verify_receipt(item, receipt, packed + b"x", "fixture-parent", "fixture-file")),
             ("G1_ORIGINAL_SHA", lambda: verify_receipt(item, receipt | {"raw_sha256": "bad"}, packed,
                                                        "fixture-parent", "fixture-file")),
             ("G4_PIT_OR_BIDIRECTIONAL_LINEAGE", lambda: verify_receipt(
                 item, receipt | {"earliest_pit": [item.start.isoformat()]}, packed, "fixture-parent", "fixture-file")),
             ("G4_PIT_OR_BIDIRECTIONAL_LINEAGE", lambda: verify_receipt(
                 item, receipt | {"file_id": "different-file"}, packed, "fixture-parent", "fixture-file"))]
    for label, field, value in (("G2_NATIVE_CONTRACT", "unit", "unverified"),
                                ("G3_DUPLICATE_KEY", "date", item.end.isoformat()),
                                ("G3_DATE_PARSE", "date", "unknown")):
        changed = json.loads(original)
        changed["data"][0][field] = value
        changed_raw = encode(changed)
        tests.append((label, lambda body=changed_raw: make_receipt(item, body, packed, "fixture-parent", "fixture-file")))
    for label, check in tests:
        try:
            check()
        except PilotFailure as failure:
            require(str(failure) == label, "WRONG_NEGATIVE_CASE_RESULT")
        else:
            raise PilotFailure("NEGATIVE_CASE_ACCEPTED")
    return len(tests)


def run_acceptance(ledger, windows):
    state = ledger.read()[1]
    require(state["manifest"] == manifest_hash(windows), "G0_MANIFEST_SHA")
    tasks = state["tasks"]
    require(set(tasks) == {item.task_id for item in windows} | {HELD_TASK}, "G5_MANIFEST_COVERAGE")
    require(tasks[HELD_TASK] == {"status": "HOLD", "generation": 0, "reason": "G0_MISSING_COVERAGE"},
            "G0_MISSING_EVIDENCE_ACCEPTED")
    for item in windows:
        task = tasks[item.task_id]
        recovered = item.lane == "B01"
        require(task["status"] == "PASS" and task["generation"] == (2 if recovered else 1)
                and task["owner"] in ({"B09"} if recovered else {item.lane}), "G5_TASK_RECONCILIATION")
    require(len(state["events"]) == len(windows), "G5_RESERVATION_COUNT")
    anchors = {digest(archive_plan([item.start, item.end]).anchor.encode()) for item in windows}
    require(set(state["folders"]) == anchors and all(folder["status"] == "READY" for folder in state["folders"].values()),
            "G5_FOLDER_REGISTRY")
    cooldown = state["paused_until"]
    require(cooldown > 0 and not can_reserve(state["events"], cooldown - 1, "download", paused_until=cooldown)
            and can_reserve(state["events"], cooldown, "download", paused_until=cooldown), "SHARED_429_COOLDOWN")
    access = token()
    folder_count, file_count = drive_inventory(windows, recovery_root(access), access)
    require(file_count == 2 * len(windows), "G5_FILE_COUNT")
    report = {
        "status": "PASS", "production": "HOLD", "gates_fixture": dict.fromkeys(GATES, "PASS"),
        "workstreams": 11, "archives_read_back": file_count // 2, "private_receipts": file_count // 2,
        "archive_folders_reconciled": folder_count, "recovered_tasks": 1, "held_tasks": 1,
        "negative_cases": negative_cases(windows[0]), "mocked_retry_after": "PASS", "finmind_requests": 0,
    }
    Path("r11-integration-verdict.json").write_bytes(encode(report) + b"\n")
    print(json.dumps(report))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("seed", "work", "recover", "accept", "self-check"))
    args = parser.parse_args()
    try:
        windows = fixtures()
        worker = os.environ.get("R11_WORKER", "B01")
        item = next(task for task in windows if task.lane == worker.split("-", 1)[0])
        if args.phase == "self-check":
            print(f"negative_cases={negative_cases(item)}")
            return
        pilot = PilotLedger()
        if args.phase == "seed":
            run_seed(pilot, windows)
        elif args.phase == "work":
            run_worker(pilot, item, worker)
        elif args.phase == "recover":
            run_recovery(pilot, item)
        else:
            run_acceptance(pilot, windows)
    except Exception as failure:
        label = str(failure) if isinstance(failure, PilotFailure) else type(failure).__name__
        print(f"integration={args.phase}:FAIL:{label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
