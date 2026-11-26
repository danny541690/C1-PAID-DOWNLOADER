"""Bounded Backer recovery rehearsal using real originals in private Drive."""

import argparse
from collections import Counter, defaultdict
import gzip
from io import BytesIO
import json
import os
from urllib.parse import urlencode

from .archive import compress_original
from .drive_pilot import BASE, FOLDER, ensure, find, token
from .integration_pilot import digest, encode, verify_private_folder
from .live_preflight import finmind_status, read_json
from .release_probes import (
    MAX_BYTES, RECOVERY_ROOT, Task, observation_date, read_private_file,
    recovery_location, request_sample,
)
from .release_remediation import pinned_contracts

TASKS = (
    Task("TaiwanStockPrice", "2026-10-30", "2026-10-30", "2330"),
    Task("TaiwanBusinessIndicator", "1982-01-01", "1982-01-31"),
    Task("TaiwanOptionDaily", "2026-11-20", "2026-11-20", "TXO"),
)


def require(condition, label):
    if not condition:
        raise ValueError(label)


def write_json(parent, name, value, access):
    file_id = ensure(parent, name, "application/json", access, encode(value), create=True)
    require(read_json_file(parent, name, access) == value, "LEDGER_READBACK")
    return file_id


def read_json_file(parent, name, access):
    file_id = ensure(parent, name, "application/json", access, create=False)
    verify_private_folder(file_id, access)
    return json.loads(read_private_file(file_id, access))


def month_folder(root, task, access, *, create, inventory=None):
    parent = root
    for name in (task.start[:4], "FinMind_Backer", task.start[:7]):
        if inventory is not None:
            inventory[parent].add(name)
        parent = ensure(parent, name, FOLDER, access, create=create)
        verify_private_folder(parent, access)
    return parent


def evidence(task, raw, packed):
    require(len(raw) <= MAX_BYTES, "RESPONSE_SIZE")
    response = json.loads(raw)
    rows = response["data"]
    require(response.get("status") == 200 and rows, "SOURCE_STATUS")
    dates = [observation_date(task.dataset, row["date"]).isoformat() for row in rows]
    require(min(dates) == task.start and max(dates) == task.start, "CANARY_DATE")
    if task.data_id:
        key = "stock_id" if task.dataset == "TaiwanStockPrice" else "option_id"
        require(all(row.get(key) == task.data_id for row in rows), "CANARY_SCOPE")
    if task.dataset != "TaiwanOptionDaily":
        require(len(rows) == 1, "SINGLE_OBSERVATION_EXPECTED")
    return {
        "parameters": task.parameters(), "rows": len(rows), "bytes": len(packed),
        "first_sha": digest(encode(rows[0])), "last_sha": digest(encode(rows[-1])),
        "raw_sha": digest(raw), "packed_sha": digest(packed),
    }


def prepare(root, access):
    require(finmind_status() == "ACCOUNT_HEADROOM_OBSERVED", "ACCOUNT_HEADROOM")
    contracts = pinned_contracts()
    outcomes = {}
    for task in TASKS:
        sample = request_sample(task, contracts[task.dataset], os.environ["FINMIND_BACKER_TOKEN"])
        if sample.status not in {"VALID_SAMPLE", "FIELD_DIFFERENCES"}:
            outcomes[task.dataset] = sample.status
            continue
        packed, _, _ = compress_original(sample.raw)
        proof = evidence(task, sample.raw, packed)
        write_json(root, f"{task.dataset}-source.json", proof, access)
        parent = month_folder(root, task, access, create=True)
        file_id = ensure(parent, task.dataset + ".json.gz", "application/gzip", access, packed, create=True)
        verify_private_folder(file_id, access)
        require(read_private_file(file_id, access) == packed, "INITIAL_UPLOAD_READBACK")
        # Also omit the upload journal for the historical sample. Its source proof is durable.
        if task.dataset != "TaiwanBusinessIndicator":
            write_json(root, f"{task.dataset}-upload.json", {"file_id": file_id}, access)
        # Deliberately stop before issuing the acceptance receipt.
        outcomes[task.dataset] = "UPLOADED_WITHOUT_RECEIPT"
    write_json(root, "outcomes.json", outcomes, access)
    print(json.dumps(outcomes, sort_keys=True))


def reconcile(root, access, *, recover):
    outcomes = read_json_file(root, "outcomes.json", access)
    require(outcomes.keys() == {task.dataset for task in TASKS}, "OUTCOME_INVENTORY")
    verified = Counter()
    inventory = defaultdict(set, {root: {"outcomes.json"}})
    repaired_upload_journals = 0
    file_ids = set()
    for task in TASKS:
        status = outcomes[task.dataset]
        if status != "UPLOADED_WITHOUT_RECEIPT":
            verified[status] += 1
            continue
        parent = month_folder(root, task, access, create=False, inventory=inventory)
        archive_name = task.dataset + ".json.gz"
        inventory[parent].add(archive_name)
        original = ensure(parent, archive_name, "application/gzip", access, create=False)
        verify_private_folder(original, access)
        require(original not in file_ids, "DUPLICATE_FILE_ID")
        file_ids.add(original)
        packed = read_private_file(original, access)
        proof = read_json_file(root, f"{task.dataset}-source.json", access)
        require(len(packed) == proof["bytes"] and digest(packed) == proof["packed_sha"], "PACKED_SHA")
        with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
            raw = stream.read(MAX_BYTES + 1)
        require(evidence(task, raw, packed) == proof, "SOURCE_PROOF_MISMATCH")
        upload_name = f"{task.dataset}-upload.json"
        if recover and find(root, upload_name, access) is None:
            write_json(root, upload_name, {"file_id": original}, access)
            repaired_upload_journals += 1
        require(read_json_file(root, upload_name, access)["file_id"] == original, "FILE_ID_CHANGED")
        receipt_name = f"{task.dataset}-accepted.json"
        inventory[root].update((f"{task.dataset}-source.json", upload_name, receipt_name))
        receipt = {"file_id": original, "source": proof}
        if recover and find(root, receipt_name, access) is None:
            write_json(root, receipt_name, receipt, access)
        require(read_json_file(root, receipt_name, access) == receipt, "RECEIPT_MISMATCH")
        verified["VERIFIED"] += 1
    for parent, names in inventory.items():
        query = urlencode({"q": f"'{parent}' in parents and trashed = false",
                           "fields": "nextPageToken,incompleteSearch,files(name)", "pageSize": 100})
        listing = read_json(f"{BASE}/files?{query}", access)
        require(not (listing.get("nextPageToken") or listing.get("incompleteSearch"))
                and sorted(entry["name"] for entry in listing["files"]) == sorted(names),
                "UNEXPECTED_OR_DUPLICATE_CHILD")
    print(json.dumps({"recovered" if recover else "independently_verified": dict(verified),
                      "source_requests": 0, "production": "HOLD",
                      "repaired_upload_journals": repaired_upload_journals,
                      "folders_reconciled": len(inventory)}))
    if not recover:
        require(verified["VERIFIED"] == len(TASKS), "REQUIRED_WINDOWS_MISSING")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "recover", "accept"))
    parser.add_argument("--prior-run", type=int)
    parser.add_argument("--prior-attempt", type=int, default=1)
    args = parser.parse_args()
    phase = args.phase
    if args.prior_run is not None and (phase != "accept" or args.prior_run < 1 or args.prior_attempt < 1):
        parser.error("A prior run may only be opened for acceptance")
    try:
        access = token()
        name = recovery_location(
            access, prefix="R12_LIVE_RECOVERY_CANARY", run_id=args.prior_run,
            attempt=args.prior_attempt if args.prior_run is not None else None,
        )
        root = ensure(RECOVERY_ROOT, name, FOLDER, access, create=phase == "prepare")
        verify_private_folder(root, access)
        if phase == "prepare":
            prepare(root, access)
        else:
            reconcile(root, access, recover=phase == "recover")
    except Exception as error:
        # Do not print request URLs, Drive IDs or private source values.
        print(f"R12_LIVE_RECOVERY_HOLD phase={phase} class={type(error).__name__}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
