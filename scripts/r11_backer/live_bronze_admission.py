"""Admit one new Backer-only partition through the private ledger and Bronze."""

import argparse
from dataclasses import asdict
import gzip
from io import BytesIO
import json
import math
import os
from time import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener
from uuid import uuid4

from .archive import compress_original
from .cross_run_ledger import DriveJournal, until
from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .history_pilot import BENCHMARK_DAY
from .integration_pilot import digest, encode, verify_private_folder
from .live_preflight import NoRedirect, drive_health, read_json
from .live_recovery_canary import month_folder, read_json_file, write_json
from .manifest_pilot import manifest_windows, sample_data, serial_window
from .quota import NORMAL_LIMIT, TOTAL_LIMIT, can_reserve, retry_deadline
from .release_probes import Task, read_private_file
from .release_readiness import ACCOUNT
from .release_rehearsal import RehearsalError, require, root_folder
from .release_remediation import pinned_contracts

DATE = BENCHMARK_DAY.isoformat()
TASK = Task("TaiwanStockMarketValue", DATE, DATE, "2330")
LEDGER_FOLDER = "R12_1_LIVE_BRONZE_LEDGER_20261124"
MANIFEST_NAME = "TASK_MANIFEST.json"
RAW_LIMIT = 65536
STAGED_NAME = "finmind-source.json.gz"


def planned_window():
    contract = pinned_contracts()[TASK.dataset]
    row = next(row for row in sample_data() if row["dataset"] == TASK.dataset)
    require(contract.tier == "Backer" and "`end_date`" in contract.optional and
            {"date", "stock_id", "market_value"} <= set(contract.fields) and
            row["state"] == "SAMPLE_ONLY" and row["routing"] == "B04", "QUERY_NOT_SUPPORTED")
    window, = manifest_windows([{
        "dataset": TASK.dataset, "scope": TASK.data_id, "lane": row["routing"],
        "start": DATE, "end": DATE, "listing_start": DATE, "listing_end": DATE,
        "contract": digest(encode(asdict(contract))),
        "evidence": {"entitlement": "Backer-only documented tier with live account validation",
                     "coverage_bounds": "One observed Taiwan trading day; one stock/date key",
                     "dataset_contract": "Pinned FinMind field list",
                     "market_identity": "TWSE 2330 on the verified benchmark session",
                     "query_mode": "Explicit start and end, one symbol, one day"},
    }])
    return serial_window(window), list(contract.fields)


def source_proof(raw, packed, fields):
    require(len(raw) <= RAW_LIMIT, "RESPONSE_SIZE")
    reply = json.loads(raw)
    require(isinstance(reply, dict) and reply.get("status") == 200 and
            set(reply) <= {"status", "msg", "data"}, "SOURCE_STATUS_OR_PAGINATION")
    rows = reply.get("data")
    require(isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict), "EXPECTED_ONE_KEY")
    row = rows[0]
    value = row.get("market_value")
    valid_value = type(value) is int or (type(value) is float and math.isfinite(value))
    require(set(fields) <= row.keys() and row["date"] == DATE and row["stock_id"] == TASK.data_id and
            valid_value and value > 0, "SOURCE_KEY_OR_CONTRACT")
    row_sha = digest(encode(row))
    return {"parameters": TASK.parameters(), "raw_sha": digest(raw), "packed_sha": digest(packed),
            "bytes": len(packed), "rows": 1, "first_sha": row_sha, "last_sha": row_sha,
            "coverage": "EXACT_SINGLE_KEY"}


def fetch_original(auth, task=TASK, *, byte_limit=RAW_LIMIT):
    query = urlencode(task.parameters())
    request = Request(f"https://api.finmindtrade.com/api/v4/data?{query}", headers={
        "Authorization": f"Bearer {auth}", "Accept": "application/json", "Accept-Encoding": "identity",
    })
    with build_opener(NoRedirect()).open(request, timeout=25) as response:
        require(response.status == 200 and not response.headers.get("Content-Range")
                and not response.headers.get("Link"), "PARTIAL_HTTP_RESPONSE")
        encoding = response.headers.get("Content-Encoding", "identity").lower()
        if encoding == "gzip":
            with gzip.GzipFile(fileobj=response) as stream:
                return stream.read(byte_limit + 1)
        require(encoding == "identity", "UNEXPECTED_ENCODING")
        return response.read(byte_limit + 1)


def management_folder(access, *, create):
    location = ROOT
    verify_private_folder(location, access)
    for name in ("00_COMMAND", "R12_BACKER", LEDGER_FOLDER):
        location = ensure(location, name, FOLDER, access, create=create and name == LEDGER_FOLDER)
        verify_private_folder(location, access)
    return location


def bronze_parent(access, *, create):
    bronze = ensure(ROOT, "20_BRONZE", FOLDER, access, create=False)
    verify_private_folder(bronze, access)
    return month_folder(bronze, TASK, access, create=create)


def initialize(root, journal, access, run):
    if find(root, MANIFEST_NAME, access) is None:
        window, fields = planned_window()
        stage = root_folder(access, prefix="R12_LIVE_BRONZE", run_id=run, attempt=1, create=True)
        name = f"{TASK.dataset}__{TASK.data_id}__{DATE}__r12_1_canary.json.gz"
        parent = bronze_parent(access, create=True)
        require(find(parent, name, access) is None, "UNTRACKED_DESTINATION")
        manifest = {"mode": "ONE_WINDOW_LIVE_PREFLIGHT", "origin_run": run, "window": window,
                    "fields": fields, "parameters": TASK.parameters(), "stage": stage, "stage_id": journal.new_id(),
                    "parent": parent, "name": name, "destination_id": journal.new_id()}
        write_json(root, MANIFEST_NAME, manifest, access)
    manifest = read_json_file(root, MANIFEST_NAME, access)
    journal.initialize({"status": "READY", "manifest_sha": digest(encode(manifest)), "events": [], "paused_until": 0})


def context(root, journal, access):
    manifest = read_json_file(root, MANIFEST_NAME, access)
    record = journal.version()
    require(record is not None and record["state"]["manifest_sha"] == digest(encode(manifest))
            and manifest["mode"] == "ONE_WINDOW_LIVE_PREFLIGHT"
            and manifest["parameters"] == TASK.parameters(), "MANIFEST_IDENTITY")
    require(manifest["parent"] == bronze_parent(access, create=False), "BRONZE_PARENT_CHANGED")
    return manifest, record


def advance(journal, previous, **changes):
    state = previous["state"] | changes
    require(journal.append(previous, state), "CONCURRENT_COMMIT")
    record = until(lambda: journal.load(previous["next_id"], previous["sequence"] + 1), "LEDGER_READBACK")
    require(record["state"] == state, "LEDGER_STATE_CHANGED")
    return record


def reserve(journal, previous, run, kind):
    state = previous["state"]
    now = int(time())
    events = state["events"]
    observed = state.get("account", {})
    if kind == "probe":
        require(state["status"] == "READY" and not events, "ACCOUNT_ALREADY_RESERVED")
        limit = TOTAL_LIMIT
        next_status = "ACCOUNT_RESERVED"
    else:
        require(state["status"] == "ACCOUNT_OK" and [row["kind"] for row in events] == ["probe"],
                "SOURCE_ALREADY_RESERVED")
        require(0 <= now - observed["checked_at"] <= 180, "ACCOUNT_MEASUREMENT_STALE")
        require(observed["used"] + 2 <= observed["limit"], "ACCOUNT_HEADROOM")
        limit = min(observed["limit"], TOTAL_LIMIT)
        next_status = "SOURCE_RESERVED"

    require(can_reserve([(row["at"], row["kind"]) for row in events], now, kind,
                        normal_limit=min(NORMAL_LIMIT, limit), total_limit=limit,
                        paused_until=state["paused_until"]), "QUOTA_OR_COOLDOWN")
    reservation = {"kind": kind, "at": now, "run": run, "request_id": uuid4().hex}
    return advance(journal, previous, status=next_status, events=events + [reservation])


def verify_blob(file_id, expected, fields, access):
    verify_private_folder(file_id, access)
    packed = read_private_file(file_id, access)
    require(len(packed) == expected["bytes"] and digest(packed) == expected["packed_sha"], "DRIVE_PACKED_SHA")
    with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
        raw = stream.read(RAW_LIMIT + 1)
    require(source_proof(raw, packed, fields) == expected, "DRIVE_SOURCE_PROOF")
    return packed


def publish(manifest, proof, access):
    verify_private_folder(manifest["stage"], access)
    stage_id = ensure(manifest["stage"], STAGED_NAME, "application/gzip", access, create=False,
                      file_id=manifest["stage_id"])
    packed = verify_blob(stage_id, proof, manifest["fields"], access)
    return ensure(manifest["parent"], manifest["name"], "application/gzip", access, packed, create=True,
                  file_id=manifest["destination_id"])


def verified_file(manifest, state, access):
    file_id = ensure(manifest["parent"], manifest["name"], "application/gzip", access, create=False,
                     file_id=manifest["destination_id"])
    verify_blob(file_id, state["proof"], manifest["fields"], access)
    return file_id


def call_provider(journal, record, operation, *args):
    try:
        return operation(*args)
    except HTTPError as error:
        pause = 0
        if error.code == 429:
            pause = retry_deadline(error.headers.get("Retry-After") if error.headers else None, int(time()))
        advance(journal, record, status="HOLD", paused_until=pause)
        raise RehearsalError(f"FINMIND_HTTP_{error.code}") from None


def capture(manifest, record, journal, access, run):
    state = record["state"]
    if state["status"] == "VERIFIED":
        require(verified_file(manifest, state, access) == state["receipt"]["file_id"], "REPLAY_FILE_ID")
        print(json.dumps({"reused_receipt": True, "account_requests": 0, "source_requests": 0}))
        return
    if state["status"] == "SOURCE_PROVEN":
        publish(manifest, state["proof"], access)
        return
    require(state["status"] in {"READY", "ACCOUNT_OK"}, "OUTSTANDING_RESERVATION_OR_HOLD")
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    health = drive_health(access)
    require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
            health["available_bytes"] >= 1048576, "INSUFFICIENT_PILOT_CAPACITY")
    require(find(manifest["parent"], manifest["name"], access) is None, "UNTRACKED_DESTINATION")

    if state["status"] == "READY":
        record = reserve(journal, record, run, "probe")
        print("finmind_account_request=1")
        info = call_provider(journal, record, read_json, ACCOUNT, auth)
        limit, used = info.get("api_request_limit"), info.get("user_count")
        require(type(limit) is int and limit >= TOTAL_LIMIT and type(used) is int and
                0 <= used <= limit - 2, "ACCOUNT_HEADROOM")
        record = advance(journal, record, status="ACCOUNT_OK",
                         account={"limit": limit, "used": used, "checked_at": int(time())})
    record = reserve(journal, record, run, "download")
    print("finmind_source_request=1")
    raw = call_provider(journal, record, fetch_original, auth)
    require(len(raw) <= RAW_LIMIT, "RESPONSE_SIZE")
    envelope = json.loads(raw)
    if isinstance(envelope, dict) and envelope.get("status") == 429:
        advance(journal, record, status="HOLD", paused_until=retry_deadline(None, int(time())))
        raise RehearsalError("SOURCE_BUSINESS_RATE_LIMIT")
    packed, _, _ = compress_original(raw)
    proof = source_proof(raw, packed, manifest["fields"])

    advance(journal, record, status="SOURCE_PROVEN", proof=proof)
    verify_private_folder(manifest["stage"], access)
    ensure(manifest["stage"], STAGED_NAME, "application/gzip", access, packed, create=True,
           file_id=manifest["stage_id"])
    publish(manifest, proof, access)


def accept(manifest, record, journal, access, run):
    state = record["state"]
    require(state["status"] in {"SOURCE_PROVEN", "VERIFIED"}, "SOURCE_PROOF_MISSING")
    require([row["kind"] for row in state["events"]] == ["probe", "download"] and
            len({row["request_id"] for row in state["events"]}) == 2, "DUPLICATE_OR_MISSING_CHARGE")
    if state["status"] == "SOURCE_PROVEN" and find(manifest["parent"], manifest["name"], access) is None:
        publish(manifest, state["proof"], access)
    file_id = verified_file(manifest, state, access)
    receipt = {"file_id": file_id, "verified_run": run, "proof": state["proof"]}
    if state["status"] == "VERIFIED":
        require(state["receipt"]["file_id"] == file_id and state["receipt"]["proof"] == state["proof"],
                "RECEIPT_CONFLICT")
    else:
        advance(journal, record, status="VERIFIED", receipt=receipt)
    print(json.dumps({"verified_rows": state["proof"]["rows"], "packed_bytes": state["proof"]["bytes"],
                      "account_reservations": 1, "source_reservations": 1, "window_verified": True,
                      "production": "HOLD"}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("capture", "accept"))
    phase = parser.parse_args().phase
    try:
        access = token()
        run = int(os.environ["GITHUB_RUN_ID"])
        root = management_folder(access, create=phase == "capture")
        journal = DriveJournal(root, access)
        if phase == "capture" and journal.version(0) is None:
            initialize(root, journal, access, run)
        manifest, record = context(root, journal, access)
        if phase == "capture":
            capture(manifest, record, journal, access, run)
        else:
            accept(manifest, record, journal, access, run)
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_LIVE_BRONZE_HOLD phase={phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
