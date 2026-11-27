"""G02 read-only reconciliation and one isolated B10 acceptance replay."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from http.client import IncompleteRead
from urllib.error import HTTPError
from urllib.parse import urlencode

from . import production_download as production, release_batch
from .cross_run_ledger import DriveJournal
from .drive_pilot import BASE, FOLDER, ensure, find
from .drive_retry import error_class, operation_scope
from .integration_pilot import digest, encode, verify_private_folder
from .live_preflight import read_json
from .live_recovery_canary import read_json_file, write_json
from .release_probes import read_private_file
from .release_rehearsal import RehearsalError, require


IDENTITY = "id,name,parents,shared,trashed,mimeType"
GZIP = "application/gzip"
READ_ERRORS = (OSError, IncompleteRead, ValueError, RuntimeError, KeyError, TypeError, EOFError)


def list_children(parent, access):
    query = {"q": f"'{parent}' in parents and trashed=false", "pageSize": 100,
             "fields": f"nextPageToken,incompleteSearch,files({IDENTITY})"}
    result = {}
    seen_pages = set()
    while True:
        page = read_json(f"{BASE}/files?{urlencode(query)}", access)
        require(not page.get("incompleteSearch"), "AUDIT_INCOMPLETE_LIST")
        for item in page["files"]:
            require(item["name"] not in result, "AUDIT_AMBIGUOUS_CHILD_NAME")
            result[item["name"]] = item
        if not (token := page.get("nextPageToken")):
            return result
        require(token not in seen_pages, "AUDIT_PAGINATION_LOOP")
        seen_pages.add(token)
        query["pageToken"] = token


def metadata(file_id, access):
    try:
        return read_json(f"{BASE}/files/{file_id}?fields={IDENTITY}", access)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise


def verified_path(item, reserved, parent, name, mime):
    require(item == {"id": reserved, "name": name, "parents": [parent],
                     "shared": False, "trashed": False, "mimeType": mime}, "AUDIT_OBJECT_IDENTITY")


def find_object(listed, reserved, parent, name, access):
    found = listed.get(name)
    if found is None:
        found = metadata(reserved, access)
    if found is not None:
        verified_path(found, reserved, parent, name, GZIP)
    return found


def stages_for(history):
    result = defaultdict(dict)
    for row in history:
        for key, task in row["state"]["tasks"].items():
            stage_id = task.get("stage_id")
            if stage_id:
                attempt = task["attempts"]
                previous = result[key].setdefault(attempt, stage_id)
                require(previous == stage_id, "AUDIT_STAGE_RESERVATION_CONFLICT")
    return result


def batch_receipts(folder, listed, access):
    receipts = {}
    for name, item in sorted(listed.items()):
        if not (name.startswith("B10_") and name.endswith(".json")):
            continue
        verified_path(item, item["id"], folder, name, "application/json")
        for key, receipt in read_json_file(folder, name, access)["receipts"].items():
            previous = receipts.setdefault(key, receipt)
            require(previous == receipt, "AUDIT_CONFLICTING_B10_REPORT")
    return receipts


def audited_item(item, task, manifest, staged, destinations, reservations, earlier_receipt, access):
    key = item["task_id"]
    result = {name: item[name] for name in ("task_id", "dataset", "scope", "market", "start", "end")}
    result.update(recorded_status=task["status"], recorded_attempts=task["attempts"], verified_staging=[])
    try:
        with operation_scope("G02", key):
            target_id = item["destination_id"]
            target = find_object(destinations, target_id, item["destination"], item["name"], access)
            named_stages = {release_batch.stage_name(key, attempt): stage_id
                            for attempt, stage_id in reservations.items()}
            require(all(name in named_stages for name in staged if name.startswith(key + "-")),
                    "AUDIT_UNTRACKED_STAGE_NAME")
            for attempt, stage_id in sorted(reservations.items()):
                name = release_batch.stage_name(key, attempt)
                if find_object(staged, stage_id, manifest["stage"], name, access):
                    proof = release_batch.proof_for(item, read_private_file(stage_id, access))
                    result["verified_staging"].append({"attempt": attempt, "file_id": stage_id, "proof": proof})
            proofs = [entry["proof"] for entry in result["verified_staging"]]
            require(not proofs or all(proof == proofs[0] for proof in proofs), "AUDIT_CONFLICTING_STAGES")
            require(not proofs or task.get("proof", proofs[0]) == proofs[0], "AUDIT_JOURNAL_STAGE_PROOF")
            result["destination_present"] = target is not None
            observed = (release_batch.proof_for(item, read_private_file(target_id, access)) if target else None)
            if task["status"] == "VERIFIED":
                receipt = task["receipt"]
                require(observed is not None and receipt == {"file_id": target_id, "proof": observed},
                        "AUDIT_DESTINATION_RECEIPT_MISMATCH")
                require(earlier_receipt in (None, receipt) and (not proofs or observed == proofs[0]),
                        "AUDIT_SOURCE_OR_RECEIPT_MISMATCH")
                return result | {"disposition": "VERIFIED_RECEIPT", "receipt": receipt,
                                 "prior_b10_report": earlier_receipt is not None}
            require(earlier_receipt is None, "AUDIT_UNEXPECTED_B10_REPORT")
            if proofs:
                require(observed in (None, proofs[0]), "AUDIT_UNVERIFIED_DESTINATION_MISMATCH")
                return result | {"disposition": "REUSABLE_STAGED"}
            require(target is None, "AUDIT_ORPHAN_DESTINATION")
            require(task["status"] == "READY", "AUDIT_PENDING_SOURCE_UNPROVEN")
            return result | {"disposition": "DOWNLOAD_REQUIRED"}
    except READ_ERRORS as error:
        return result | {"disposition": "HOLD", "reason": str(error) if isinstance(error, RehearsalError)
                         else error_class(error)}


def read_active(ctx, group, root_children):
    access = ctx.access
    plan = ctx.plan if group == "BASE" else ctx.supplement
    folder, manifest, journal, current = production.segment(plan, ctx.master, ctx.campaign, access, group, create=False)
    history = list(journal.history())
    require(history and history[-1] == current, "ACTIVE_BATCH_ADVANCED_DURING_READ")
    require(all(version["state"]["manifest_sha"] == digest(encode(manifest)) for version in history),
            "ACTIVE_BATCH_HISTORY_MANIFEST")
    tasks = current["state"]["tasks"]
    keys = {item["task_id"] for item in manifest["items"]}
    require(len(keys) == len(manifest["items"]) and keys == tasks.keys(), "ACTIVE_BATCH_INVENTORY")
    require(any(item["id"] == folder for item in root_children.values()), "ACTIVE_BATCH_PARENT")
    stage_meta = metadata(manifest["stage"], access)
    verified_path(stage_meta, manifest["stage"], folder, "STAGING", FOLDER)
    staged = list_children(manifest["stage"], access)
    stages = stages_for(history)
    receipts = batch_receipts(folder, list_children(folder, access), access)
    require(receipts.keys() <= keys, "ACTIVE_BATCH_UNKNOWN_RECEIPT")
    parents = {}
    representatives = {item["destination"]: item for item in manifest["items"]}
    for parent, item in representatives.items():
        require(release_batch.destination(item, access, create=False) == parent, "AUDIT_CANONICAL_BRONZE_PARENT")
        parents[parent] = list_children(parent, access)

    def audit(item):
        key = item["task_id"]
        return audited_item(item, tasks[key], manifest, staged, parents[item["destination"]], stages.get(key, {}),
                            receipts.get(key), access)

    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(audit, manifest["items"]))
    return {"group": group, "journal_sequence": current["sequence"], "manifest_sha": digest(encode(manifest)),
            "tasks": rows, "recorded_states": dict(Counter(task["status"] for task in tasks.values())),
            "dispositions": dict(Counter(item["disposition"] for item in rows))}, manifest


def historical_exceptions(ctx, root_children):
    finalized = [(name, entry) for name, entry in root_children.items() if
                 (name.startswith("BATCH_") and name[6:].isdigit() and int(name[6:]) < ctx.master["state"]["cursor"]) or
                 (name.startswith("TPEX_BATCH_") and name[11:].isdigit() and
                  int(name[11:]) < ctx.master["state"].get("otc_cursor", 0))]

    def read_batch(candidate):
        name, entry = candidate
        try:
            verified_path(entry, entry["id"], ctx.campaign, name, FOLDER)
            record = DriveJournal(entry["id"], ctx.access).head()
            require(record is not None, "B09_LEDGER_MISSING")
            manifest = read_json_file(entry["id"], release_batch.MANIFEST_NAME, ctx.access)
            require(record["state"]["manifest_sha"] == digest(encode(manifest)), "B09_MANIFEST_HASH")
            scope = "otc_plan_sha" if name.startswith("TPEX_") else "plan_sha"
            prefix = "TPEX_BATCH_" if name.startswith("TPEX_") else "BATCH_"
            require(manifest["plan_sha"] == ctx.master["state"][scope] and
                    manifest["cursor"] == int(name.removeprefix(prefix)), "B09_PARENT_PLAN")
            state = record["state"]["tasks"]
            manifest_items = {item["task_id"]: item for item in manifest["items"]}
            require(state.keys() == manifest_items.keys() and len(state) == manifest["end"] - manifest["cursor"],
                    "B09_TASK_INVENTORY")
            counts = Counter(task["status"] for task in state.values())
            require(counts.keys() <= production.TERMINAL, "B09_UNFINALIZED_IN_MASTER")
            exceptions = [{"batch": name, "task_id": key, "dataset": manifest_items[key]["dataset"],
                           "lane": manifest_items[key]["lane"], "recorded_status": task["status"],
                           "reason": task.get("reason", "UNSPECIFIED"),
                           "disposition": repair_disposition(task.get("reason"))}
                          for key, task in state.items() if task["status"] != "VERIFIED"]
            return counts, exceptions, None
        except READ_ERRORS as error:
            return Counter(), [], {"batch": name, "category": error_class(error)}

    counts, holds, errors = Counter(), [], []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for counter, issues, error in pool.map(read_batch, sorted(finalized)):
            counts.update(counter)
            holds.extend(issues)
            if error:
                errors.append(error)
    expected = ctx.master["state"]["totals"]
    unique = len({row["task_id"] for row in holds}) == len(holds)
    return {"master_sequence": ctx.master["sequence"], "master_totals": expected, "batch_totals": dict(counts),
            "finalized_batches_read": len(finalized), "errors": errors, "matched_master": dict(counts) == expected,
            "unique_exception_keys": unique, "reason_counts": dict(Counter(row["reason"] for row in holds)),
            "records": holds}


def repair_disposition(reason):
    return {
        "DATE_KEY_COVERAGE": "RECONCILE_CONTRACT_DATE_SEMANTICS_AND_OFFICIAL_SESSIONS",
        "SOURCE_FIELDS": "RECONCILE_REQUIRED_NATIVE_FIELDS",
        "SYMBOL_COVERAGE": "VERIFY_MARKET_AND_SYMBOL_EFFECTIVE_INTERVAL",
        "STATUS_OR_PAGINATION": "VERIFY_SOURCE_STATUS_AND_PAGE_COMPLETENESS",
        "SIZE_LIMIT": "ESTABLISH_CONTRACT_BOUND_AND_TEST_SPLIT",
        "BRONZE_VALIDATION": "REREAD_RESERVED_OBJECT_AND_RECEIPT",
        "BRONZE_READBACK_RETRIES": "RESOLVE_READBACK_WITH_EXISTING_STAGE",
        "401": "VERIFY_EXISTING_ENTITLEMENT",
        "403": "VERIFY_EXISTING_ENTITLEMENT",
    }.get(reason, "REVIEW_EXISTING_EVIDENCE")


def isolated_accept(ctx, active, source_manifest):
    chosen = next((row for row in active["tasks"] if row["disposition"] == "VERIFIED_RECEIPT"), None)
    require(chosen is not None, "G02_NO_VERIFIED_SOURCE_FOR_REPLAY")
    verify_private_folder(ctx.root, ctx.access)
    sandbox = ensure(ctx.root, "G02_RECOVERY_SANDBOX", FOLDER, ctx.access, create=True)
    verify_private_folder(sandbox, ctx.access)
    require(sandbox != ctx.campaign, "G02_ISOLATION")
    stage = ensure(sandbox, "STAGING", FOLDER, ctx.access, create=True)
    destination = ensure(sandbox, "TEST_BRONZE", FOLDER, ctx.access, create=True)
    for child in (stage, destination):
        verify_private_folder(child, ctx.access)
    journal = DriveJournal(sandbox, ctx.access)
    key = chosen["task_id"]
    receipt = chosen["receipt"]
    item = next(row for row in source_manifest["items"] if row["task_id"] == key)
    packed = read_private_file(receipt["file_id"], ctx.access)
    require(release_batch.proof_for(item, packed) == receipt["proof"], "G02_REPLAY_SOURCE_HASH")
    if find(sandbox, "ISOLATED_INPUT.json", ctx.access) is None:
        staging_id, destination_id = journal.new_ids(2)
        copy_item = item | {"destination": destination, "destination_id": destination_id}
        cfg = {"stage": stage, "staging_id": staging_id, "item": copy_item, "original_sha": digest(packed)}
        write_json(sandbox, "ISOLATED_INPUT.json", cfg, ctx.access)
    cfg = read_json_file(sandbox, "ISOLATED_INPUT.json", ctx.access)
    require(cfg["stage"] == stage and cfg["item"]["destination"] == destination and
            cfg["item"]["task_id"] == key and cfg["original_sha"] == digest(packed), "G02_REPLAY_IDENTITY")
    replay = {"stage": stage, "items": [cfg["item"]]}
    hold = {"status": "STAGED", "attempts": 1, "stage_id": cfg["staging_id"], "proof": receipt["proof"]}
    ensure(stage, release_batch.stage_name(key, 1), GZIP, ctx.access, packed, create=True, file_id=hold["stage_id"])
    journal.initialize({"tasks": {key: hold}, "manifest_sha": digest(encode(replay)), "events": []})
    source_record = journal.version()
    require(source_record["state"]["manifest_sha"] == digest(encode(replay)), "G02_REPLAY_MANIFEST")

    def isolated_path(row, _, *, create):
        require(not create and row["destination"] == destination, "G02_REPLAY_WRONG_DESTINATION")
        return destination

    accepted = release_batch.accept(replay, source_record, journal, ctx.access, resolve_destination=isolated_path)
    redo = release_batch.accept(replay, accepted, journal, ctx.access, resolve_destination=isolated_path)
    accepted_task = accepted["state"]["tasks"][key]
    require(accepted_task["status"] == "VERIFIED" and accepted_task["receipt"]["proof"] == receipt["proof"] and
            accepted == redo and accepted["state"]["events"] == [], "G02_REPLAY_OR_DEDUPE")
    return {"status": "PASS", "original_file_id": receipt["file_id"], "receipt": accepted_task["receipt"],
            "writes_to_production": 0, "source_requests": 0, "duplicate_replay": "SAME_JOURNAL_AND_RECEIPT"}


def run(ctx):
    group = production.pending_group(ctx.plan, ctx.supplement, ctx.master["state"])
    blockers = []
    active, repair, replay = {}, {}, {"status": "NOT_RUN"}
    with operation_scope("G02"):
        root_children = list_children(ctx.campaign, ctx.access)
        try:
            require(group is not None, "NO_ACTIVE_BATCH")
            active, manifest = read_active(ctx, group, root_children)
            if active["dispositions"].get("HOLD"):
                blockers.append("ACTIVE_BATCH_UNRESOLVED_TASKS")
            replay = isolated_accept(ctx, active, manifest)
        except READ_ERRORS as error:
            blockers.append(str(error) if isinstance(error, RehearsalError) else error_class(error))
        try:
            repair = historical_exceptions(ctx, root_children)
            if repair["errors"] or not repair["matched_master"] or not repair["unique_exception_keys"]:
                blockers.append("B09_MASTER_RECONCILIATION")
        except READ_ERRORS as error:
            blockers.append("B09_" + error_class(error))
        try:
            current_master = DriveJournal(ctx.campaign, ctx.access).head()
            stable = current_master == ctx.master
        except READ_ERRORS:
            stable = False
        if not stable:
            blockers.append("MASTER_ADVANCED_OR_UNREADABLE_DURING_READONLY_AUDIT")
    status = "HOLD" if blockers else "PASS"
    result = {"status": status, "blockers": blockers, "read_only_master_sequence": ctx.master["sequence"],
              "master_snapshot_unchanged": stable, "active_batch": active, "isolated_acceptance": replay,
              "source_requests": 0, "production_mutations": 0}
    return {"status": status, "outputs": {"RECOVERY_REPORT.json": result, "B09_REPAIR_PLAN.json": repair}}
