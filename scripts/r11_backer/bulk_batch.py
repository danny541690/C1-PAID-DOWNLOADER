"""Native-window B01–B10 adapter using the existing Drive journal and private archive."""

from collections import Counter
from copy import deepcopy
import gzip
from http.client import IncompleteRead
from io import BytesIO
import json
from time import time
from urllib.error import HTTPError

from .archive import compress_original
from .bulk_manifest import (
    SCHEMA, check_window, native_proof, packed_limit, page_label, planned_capacity, read_manifest, receipt_id,
)
from .drive_pilot import ensure, read_drive_bytes
from .drive_retry import error_class, operation_scope, transient
from .integration_pilot import digest, encode, verify_private_folder
from .live_bronze_admission import advance
from .quota import AUTH_BLOCK_CODES, QUOTA_BLOCK_CODES, provider_block_code, retry_deadline
from .release_rehearsal import RehearsalError, require
from .v2_registry import key_value, read_rows, validate_pages
from .v2_recovery import metadata, verified_path

ATTEMPTS = 4
GZIP = "application/gzip"


class NativeHold(RehearsalError):
    """A fact that prevents a window from being certified."""


def prepare(index, read, journal, access, stage, destination, *, known_requests=(), known_coverage=()):
    windows = read_manifest(index, read, known_requests=known_requests, known_coverage=known_coverage)
    count = sum(row["max_pages"] for row in windows)
    file_ids = iter(journal.new_ids(count)) if count else iter(())
    folders = {}
    items = []
    verify_private_folder(stage, access)
    for row in windows:
        if row["anchor_month"] not in folders:
            folders[row["anchor_month"]] = destination(row, access, create=True)
        items.append(row | {"destination": folders[row["anchor_month"]],
                            "destination_ids": [next(file_ids) for _ in range(row["max_pages"])]})
    return {"schema_version": SCHEMA, "stage": stage, "items": items, "index_sha256": digest(encode(index)),
            "capacity": planned_capacity(windows), "held_calendar_months": []}


def initial(manifest):
    return {"manifest_sha": digest(encode(manifest)), "pause": 0, "auth_hold": False, "events": [],
            "tasks": {row["task_id"]: {"status": "READY", "attempts": 0, "pages": []}
                      for row in manifest["items"]}}


def check_batch(manifest, record):
    require(manifest["schema_version"] == SCHEMA and record["state"]["manifest_sha"] == digest(encode(manifest)) and
            set(record["state"]["tasks"]) == {row["task_id"] for row in manifest["items"]}, "NATIVE_BATCH_BINDING")


def update(journal, record, key, changed, **state_changes):
    tasks = record["state"]["tasks"] | {key: changed}
    return advance(journal, record, tasks=tasks, **state_changes)


def page_params(item, pages):
    rule = check_window(item)
    params = dict(item["parameters"])
    if pages:
        cursor = pages[-1]["next"]
        require(rule.page_field and key_value(cursor) and len(pages) < item["max_pages"], "PAGE_CHAIN_LIMIT")
        require(all(cursor != page["next"] for page in pages[:-1]), "REPEATED_PAGE_CURSOR")
        params[rule.cursor_parameter] = cursor
    return params


def stored_page(item, pending, raw, packed, encoding):
    rule = check_window(item)
    _, result = read_rows(rule, pending["params"], raw)
    if rule.page_field:
        require(rule.page_field in result, "MISSING_PAGE_TERMINATOR")
        cursor = result[rule.page_field]
        require(cursor is None or key_value(cursor), "INVALID_NEXT_PAGE")
    else:
        cursor = None
    return {"position": pending["position"], "attempt": pending["attempt"], "params": pending["params"],
            "staging_id": pending["id"], "next": cursor, "raw_sha": digest(raw), "packed_sha": digest(packed),
            "raw_bytes": len(raw), "bytes": len(packed), "transport_content_encoding": encoding}


def stage_name(item, pending):
    return f"{item['task_id']}-{pending['position'] + 1:02d}-{pending['attempt']}.json.gz"


def read_blob(file_id, item, access):
    verify_private_folder(file_id, access)
    packed = read_drive_bytes(file_id, access, packed_limit(item))
    with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
        raw = stream.read(item["raw_limit"] + 1)
    require(0 < len(raw) <= item["raw_limit"], "NATIVE_RESPONSE_LIMIT")
    return raw, packed


def read_stage(manifest, item, pending, access):
    file_id = pending["id"]
    info = metadata(file_id, access)
    if info is None:
        raise HTTPError("private-drive", 404, "staging file missing", None, None)
    verified_path(info, file_id, manifest["stage"], stage_name(item, pending), GZIP)
    return read_blob(file_id, item, access)


def stage_blob(manifest, item, pending, packed, access):
    file_id = ensure(manifest["stage"], stage_name(item, pending), GZIP, access, packed, create=True, file_id=pending["id"])
    require(read_blob(file_id, item, access)[1] == packed, "NATIVE_STAGE_READBACK")


def staged_task(item, task, page):
    pages = task["pages"] + [page]
    changed = {k: v for k, v in task.items() if k not in {"pending", "reason", "retry_after"}}
    if page["next"] is None:
        return changed | {"status": "STAGED", "attempts": 0, "pages": pages}
    if len(pages) == item["max_pages"] or any(old["next"] == page["next"] for old in task["pages"]):
        return changed | {"status": "BLOCKED", "reason": "INCOMPLETE_PAGE_CHAIN", "pages": pages}
    return changed | {"status": "READY", "attempts": 0, "pages": pages}


def retry_task(task, reason, now):
    changed = {k: v for k, v in task.items() if k != "pending"}
    if task["attempts"] >= ATTEMPTS:
        return changed | {"status": "REPAIR", "reason": reason}
    return changed | {"status": "READY", "reason": reason, "retry_after": now + 2 ** task["attempts"]}


def recover(manifest, record, journal, access, *, now=None):
    """Adopt any durably staged page without issuing another provider request."""
    check_batch(manifest, record)
    now = int(time()) if now is None else now
    for item in manifest["items"]:
        key = item["task_id"]
        task = record["state"]["tasks"][key]
        if task["status"] != "REQUESTING" or task["pending"]["lease_until"] > now:
            continue
        pending = task["pending"]
        try:
            # Inspect the reserved ID directly when Drive named listing is lagging.
            raw, packed = read_stage(manifest, item, pending, access)
        except HTTPError as exc:
            if exc.code != 404:
                raise
            record = update(journal, record, key, task | {"status": "REPAIR", "reason": "UNKNOWN_REQUEST_OR_STAGE"})
            continue
        except (OSError, IncompleteRead) as exc:
            if not transient(exc):
                raise
            continue
        page = stored_page(item, pending, raw, packed, pending.get("encoding", "UNKNOWN_AFTER_INTERRUPTION"))
        record = update(journal, record, key, staged_task(item, task, page) | {"recovered": True})
    return record


def receive_one(manifest, record, journal, item, access, run, permit, fetch, now):
    state = record["state"]
    key = item["task_id"]
    task = state["tasks"][key]
    params = page_params(item, task["pages"])
    position, attempt = len(task["pages"]), task["attempts"] + 1
    grant_id = digest(encode(["R12_V24", key, position, attempt, manifest["index_sha256"]]))
    pending = {"id": journal.new_ids(1)[0], "params": params, "position": position, "attempt": attempt,
               "grant_id": grant_id, "lease_until": now + 90}
    task = task | {"status": "REQUESTING", "attempts": attempt, "pending": pending}
    record = update(journal, record, key, task)
    granted = permit(grant_id, params, item)
    if granted != "RESERVED":
        if granted in {"ALREADY_RESERVED", "RESERVATION_UNCERTAIN"}:
            return update(journal, record, key, task | {"reason": "QUOTA_RESERVATION_UNCERTAIN"})
        skipped = {k: v for k, v in task.items() if k != "pending"}
        return update(journal, record, key, skipped | {"status": "READY", "attempts": attempt - 1, "reason": granted})
    events = record["state"]["events"] + [{"kind": "download", "at": now, "run": run,
                                            "task_id": key, "request_id": grant_id}]
    record = update(journal, record, key, task, events=events)
    try:
        raw, encoding = fetch(params, item["raw_limit"])
        reply = json.loads(raw)
        code = provider_block_code(reply.get("status")) if isinstance(reply, dict) else None
        if code:
            permit.complete(grant_id, f"HTTP_{code}", code=code, retry_after=reply.get("retry_after"))
            return provider_result(journal, record, key, task, code, reply.get("retry_after"), now)
        packed, _, _ = compress_original(raw)
        page = stored_page(item, pending, raw, packed, encoding)
    except HTTPError as exc:
        delay = exc.headers.get("Retry-After") if exc.headers else None
        permit.complete(grant_id, f"HTTP_{exc.code}", code=exc.code, retry_after=delay)
        return provider_result(journal, record, key, task, exc.code, delay, now)
    except (OSError, IncompleteRead) as exc:
        permit.complete(grant_id, error_class(exc))
        outcome = retry_task(task, error_class(exc), now) if transient(exc) else task | {
            "status": "BLOCKED", "reason": error_class(exc),
        }
        return update(journal, record, key, outcome)
    except ValueError as exc:
        reason = str(exc) if isinstance(exc, RehearsalError) else "INVALID_NATIVE_PAYLOAD"
        permit.complete(grant_id, reason)
        return update(journal, record, key, task | {"status": "BLOCKED", "reason": reason})
    # Write encoding before the upload. A resumed process can reconstruct the receipt from the reserved ID.
    pending = pending | {"encoding": encoding}
    task = task | {"pending": pending}
    record = update(journal, record, key, task)
    try:
        stage_blob(manifest, item, pending, packed, access)
    except (OSError, RuntimeError, ValueError, IncompleteRead):
        permit.complete(grant_id, "STAGING_RESULT_UNCERTAIN")
        return update(journal, record, key, task | {"reason": "STAGING_RESULT_UNCERTAIN"})
    permit.complete(grant_id, "NATIVE_RESPONSE_STAGED")
    return update(journal, record, key, staged_task(item, task, page))


def provider_result(journal, record, key, task, code, retry_after, now):
    if code in QUOTA_BLOCK_CODES:
        until = max(record["state"]["pause"], retry_deadline(retry_after, now))
        return update(journal, record, key, retry_task(task, f"HTTP_{code}", now), pause=until)
    if code == 408 or 500 <= code < 600:
        return update(journal, record, key, retry_task(task, f"HTTP_{code}", now))
    return update(journal, record, key, task | {"status": "BLOCKED", "reason": f"HTTP_{code}"},
                  auth_hold=record["state"]["auth_hold"] or code in AUTH_BLOCK_CODES)


def dispatch(manifest, record, journal, access, run, *, permit, fetch, request_ceiling=None):
    require(callable(permit) and callable(getattr(permit, "complete", None)) and callable(fetch),
            "NATIVE_DISPATCH_REQUIRES_SHARED_BROKER_AND_TRANSPORT")
    record = recover(manifest, record, journal, access)
    sent = sum(event["kind"] == "download" and event["run"] == run for event in record["state"]["events"])
    for item in manifest["items"]:
        for _ in range(item["max_pages"]):
            now = int(time())
            state = record["state"]
            if state["auth_hold"] or state["pause"] > now or request_ceiling is not None and sent >= request_ceiling:
                return record
            task = state["tasks"][item["task_id"]]
            if task["status"] != "READY" or task.get("retry_after", 0) > now:
                break
            before = len(state["events"])
            record = receive_one(manifest, record, journal, item, access, run, permit, fetch, now)
            after = len(record["state"]["events"])
            sent += after - before
            if after == before:
                break
    return record


def accepted_proof(manifest, item, task, access):
    raw_pages, packed_pages = [], []
    for page in task["pages"]:
        pending = {"id": page["staging_id"], "position": page["position"], "attempt": page["attempt"]}
        raw, packed = read_stage(manifest, item, pending, access)
        require(digest(raw) == page["raw_sha"] and digest(packed) == page["packed_sha"], "NATIVE_STAGE_DIGEST")
        raw_pages.append((page["params"], raw))
        packed_pages.append(packed)
    rule = check_window(item)
    if not rule.page_field:
        require(len(raw_pages) == 1, "UNEXPECTED_NATIVE_PAGE_COUNT")
        proof = native_proof(item, packed_pages[0], raw_pages[0][1])
    else:
        validated = validate_pages(rule, raw_pages, completeness=item["completeness"])
        require(all(digest(raw) == proof["raw_sha"] and digest(packed) == proof["packed_sha"]
                    for (_, raw), packed, proof in zip(raw_pages, packed_pages, validated["pages"], strict=True)),
                "NATIVE_PAGE_GZIP_SHA")
        # Head/tail refer to the original received order, including empty intermediate pages.
        nonempty = [p for p in validated["pages"] if p["rows"]]
        proof = {"status": validated["status"], "rows": validated["rows"],
                 "bytes": sum(p["bytes"] for p in validated["pages"]),
                 "first_sha": nonempty[0]["first_sha"] if nonempty else None,
                 "last_sha": nonempty[-1]["last_sha"] if nonempty else None,
                 "pages": validated["pages"], "request_id": item["request_id"],
                 "contract_version": item["contract_version"], "coverage_sha256": digest(encode(item["coverage"]))}
    return proof, packed_pages


def accept(manifest, record, journal, access, *, destination, audit=False, reread_verified=True):
    check_batch(manifest, record)
    tasks = deepcopy(record["state"]["tasks"])
    parents = {}
    for item in manifest["items"]:
        key = item["task_id"]
        task = tasks[key]
        if task["status"] not in ({"VERIFIED"} if audit else {"STAGED", "VERIFIED"}) or (
            task["status"] == "VERIFIED" and not reread_verified
        ):
            continue
        with operation_scope("B10", key):
            try:
                if item["anchor_month"] not in parents:
                    parents[item["anchor_month"]] = destination(item, access, create=False)
                parent = parents[item["anchor_month"]]
                require(item["destination"] == parent, "NATIVE_BRONZE_FOLDER_ID")
                verify_private_folder(parent, access)
                proof, packed_pages = accepted_proof(manifest, item, task, access)
                is_verified = task["status"] == "VERIFIED"
                files = []
                for page, packed in zip(task["pages"], packed_pages, strict=True):
                    index = page["position"]
                    file_id = ensure(parent, page_label(item, index), GZIP, access, packed if not is_verified else None,
                                     create=not is_verified, file_id=item["destination_ids"][index])
                    raw, actual = read_blob(file_id, item, access)
                    require(actual == packed and digest(raw) == page["raw_sha"], "NATIVE_BRONZE_READBACK")
                    files.append({k: page[k] for k in ("position", "params", "raw_sha", "packed_sha", "raw_bytes", "bytes",
                                                       "transport_content_encoding")} | {"file_id": file_id})
                receipt = {"receipt_id": receipt_id(key, [p["file_id"] for p in files], [p["raw_sha"] for p in files],
                                                     [p["packed_sha"] for p in files]),
                           "request_id": key, "contract_version": item["contract_version"], "folder_id": parent,
                           "anchor_month": item["anchor_month"], "covered_months": item["coverage"]["requested_months"],
                           "coverage_ids": item["coverage"]["coverage_ids"], "gzip_format": "gzip-mtime-0",
                           "file_id": files[0]["file_id"], "files": files, "proof": proof}
                require(not is_verified or receipt == task["receipt"], "NATIVE_RECEIPT_CHANGED")
                tasks[key] = task | {"status": "VERIFIED", "receipt": receipt}
            except (OSError, IncompleteRead) as exc:
                if task["status"] == "VERIFIED":
                    raise
                attempt = task.get("accept_attempts", 0) + 1
                phase = "STAGED" if transient(exc) and attempt < ATTEMPTS else "REPAIR"
                tasks[key] = task | {"status": phase, "accept_attempts": attempt, "reason": error_class(exc)}
            except (ValueError, RuntimeError):
                if task["status"] == "VERIFIED":
                    raise
                tasks[key] = task | {"status": "BLOCKED", "reason": "NATIVE_B10_INCOMPLETE_OR_INVALID"}
    if tasks != record["state"]["tasks"]:
        return advance(journal, record, tasks=tasks)
    return record


def reconcile(record):
    tasks = record["state"]["tasks"]
    return {"states": dict(Counter(task["status"] for task in tasks.values())),
            "B09": {key: task["reason"] for key, task in tasks.items() if task["status"] in {"REPAIR", "BLOCKED"}},
            "B10": {key: task["receipt"] for key, task in tasks.items() if task["status"] == "VERIFIED"}}
