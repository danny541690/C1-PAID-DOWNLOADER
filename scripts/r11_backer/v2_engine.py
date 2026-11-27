"""Exercise the R12 proof and acceptance path in an isolated Drive ledger."""

from collections import Counter
from dataclasses import asdict, replace
import gzip
from io import BytesIO
import json
import os

from . import release_batch
from .archive import compress_original
from .cross_run_ledger import DriveJournal, accept as fenced_accept, claim, reconcile as reconcile_cross_run
from .drive_pilot import BASE, FOLDER, ensure, find
from .integration_pilot import digest, encode, verify_private_folder
from .live_bronze_admission import advance
from .live_preflight import read_json
from .live_recovery_canary import TASKS, month_folder, read_json_file, write_json
from .quota import can_reserve, retry_deadline
from .release_rehearsal import SOURCE_ATTEMPT, SOURCE_RUN, read_original, require, root_folder
from .v2_coverage import query_key, subtract_coverage
from .v2_registry import NativeContract, validate_original, validate_pages


MANIFEST = "G07_ISOLATED_MANIFEST.json"
SANDBOX = "G07_ENGINE_SANDBOX"
FIXTURE_TIME = 100
TASK_ID = "R12-FINAL-READINESS-V2-01"
PRIOR_CROSS_RUNS = (45817707383, 45817707583)


def specimen(dataset, lane, market, start, end, grain, rows, keys, *, scope=None,
             snapshot=False, split=False, empty=False, source=None, raw=None):
    mode = "snapshot" if snapshot else "entity_range" if scope else "market_range"
    parameters = {"dataset": dataset}
    if not snapshot:
        parameters.update(start_date=start, end_date=end)
    if scope:
        parameters["data_id"] = scope
    fields = tuple(rows[0]) if rows else tuple(keys)
    contract = NativeContract(dataset=dataset, grain=grain, query_mode=mode, fields=fields,
                              key_fields=tuple(keys), identity_field="stock_id" if scope else None,
                              date_field=None if snapshot else "date")
    topology = ({"method": "split_reconciliation", "row_hashes": [digest(encode(row)) for row in rows]} if split else
                {"method": "expected_keys", "keys": [[row[key] for key in keys] for row in rows]})
    if empty:
        topology = {"method": "confirmed_empty", "evidence": "isolated verified fixture calendar",
                    "keys": []}
    item = {"task_id": query_key(parameters, mode, market=market, identity=start if snapshot else None),
            "dataset": dataset, "lane": lane, "market": market, "scope": scope,
            "start": start, "end": end, "native_contract": json.loads(encode(asdict(contract))),
            "parameters": parameters, "completeness": topology, "source": source or "fixture"}
    raw = raw if raw is not None else encode({"status": 200, "data": rows}) + b"\n"
    packed, raw_sha, packed_sha = compress_original(raw)
    item["expected_source"] = {"raw_sha": raw_sha, "packed_sha": packed_sha}
    return item, packed


def fixtures():
    """Representative query shapes, with explicit synthetic evidence boundaries."""
    options = "TaiwanOptionDaily"
    return [
        specimen("TaiwanStockPrice", "B01", "TPEx", "1994-12-30", "1994-12-30", "daily",
                 [{"date": "1994-12-30", "stock_id": "0001", "close": 11}], ("date", "stock_id"), scope="0001"),
        specimen("TaiwanStockInstitutionalInvestorsBuySell", "B02", "TWSE", "2024-05-13", "2024-05-13", "daily",
                 [{"date": "2024-05-13", "stock_id": "0002", "name": label, "buy": 5}
                  for label in ("second", "first")], ("date", "stock_id", "name"), scope="0002"),
        specimen("TaiwanStockShareholding", "B03", "TWSE", "2025-09-06", "2025-09-06", "daily",
                 [], ("date", "stock_id"), scope="0003", empty=True),
        specimen("TaiwanStockMonthRevenue", "B04", "TWSE", "2010-01-01", "2010-02-01", "monthly",
                 [{"date": day, "stock_id": "0004", "revenue": 42} for day in ("2010-01-01", "2010-02-01")],
                 ("date", "stock_id"), scope="0004"),
        specimen("TaiwanStockFinancialStatements", "B04", "TWSE", "2011-03-31", "2011-06-30", "quarterly",
                 [{"date": day, "stock_id": "0004", "type": "fixture", "value": 12}
                  for day in ("2011-03-31", "2011-06-30")], ("date", "stock_id", "type"), scope="0004"),
        specimen("TaiwanStockDividend", "B05", "TPEx", "2000-12-30", "2001-01-02", "event",
                 [{"date": "2001-01-02", "stock_id": "0005", "event_id": 6}],
                 ("date", "stock_id", "event_id"), scope="0005"),
        *[specimen(options, "B06", "TAIFEX", day, day, "daily",
                   [{"date": day, "option_id": "fixture", "contract_id": strike, "close": 5}
                    for strike in (101, 103)],
                   ("date", "option_id", "contract_id"), split=True)
          for day in ("2009-03-18", "2009-03-19")],
        specimen("TaiwanBusinessIndicator", "B07", "TW", "1982-12-01", "1983-01-01", "monthly",
                 [{"date": day, "indicator_index": 12} for day in ("1982-12-01", "1983-01-01")], ("date",)),
        specimen("TaiwanStockConvertibleBondInfo", "B08", "TPEx", "2026-11-26", "2026-11-26", "snapshot",
                 [{"cb_id": "fixture-bond", "name": "fixture"}], ("cb_id",), snapshot=True),
    ]


def archived_price(access):
    """Read one previously accepted original using its existing proof, without a source request."""
    prior = root_folder(access, prefix="R12_LIVE_RECOVERY_CANARY", run_id=SOURCE_RUN,
                        attempt=SOURCE_ATTEMPT, create=False)
    task = TASKS[0]
    receipt = read_json_file(prior, f"{task.dataset}-accepted.json", access)
    parent = month_folder(prior, task, access, create=False)
    file_id = ensure(parent, task.dataset + ".json.gz", "application/gzip", access, create=False)
    require(file_id == receipt["file_id"], "G07_ARCHIVED_FILE_ID")
    original = read_original(task, file_id, receipt["source"], access)
    raw = gzip.decompress(original)
    item, _ = specimen(task.dataset, "B01", "TWSE", task.start, task.end, "daily", json.loads(raw)["data"],
                       ("date", "stock_id"), scope=task.data_id, raw=raw,
                       source={"kind": "archived", "run": SOURCE_RUN, "attempt": SOURCE_ATTEMPT,
                               "file_id": file_id, "proof": receipt["source"]})
    # Use the original stored gzip as well as the original uncompressed bytes.
    item["expected_source"]["packed_sha"] = digest(original)
    return item, original


def frozen_inputs(access):
    records = fixtures()
    try:
        records.append(archived_price(access))
        return records, None
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        return records, type(error).__name__


def cross_run_evidence(access):
    origin, continuation = PRIOR_CROSS_RUNS
    result = {"origin_run": origin, "resume_run": continuation, "mode": "verified_reuse", "finmind_requests": 0}
    try:
        root = root_folder(access, prefix="R12_CROSS_RUN_LEDGER", run_id=origin, attempt=1, create=False)
        manifest = read_json_file(root, "TASK_MANIFEST.json", access)
        resumed = read_json_file(root, "claim-resume.json", access)
        require(manifest["origin_run"] == origin and resumed["run"] == continuation,
                "G07_PRIOR_CROSS_RUN_BINDING")
        journal = DriveJournal(root, access)
        require(journal.version(2)["state"]["manifest_sha"] == digest(encode(manifest)) and
                journal.version(4) is not None, "G07_PRIOR_CROSS_RUN_INCOMPLETE")
        reconcile_cross_run(root, journal, access, manifest, continuation, commit=False)
        return result | {"status": "PASS", "manifest_sha": digest(encode(manifest)), "terminal_sequence": 4}
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        return result | {"status": "HOLD", "reason": type(error).__name__}


def native_proof(item, packed):
    contract = NativeContract(**item["native_contract"])
    with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
        raw = stream.read(contract.byte_limit + 1)
    proof = validate_original(contract, item["parameters"], raw, completeness=item["completeness"])
    packed_sha = digest(packed)
    seen = {"raw_sha": digest(raw), "packed_sha": packed_sha}
    require(seen == item["expected_source"], "G07_SOURCE_HASH")
    result = proof | seen | {"bytes": len(packed)}
    if isinstance(item["source"], dict):
        previous = item["source"]["proof"]
        require(all(result[key] == previous[key] for key in ("rows", "bytes", "raw_sha", "packed_sha",
                                                            "first_sha", "last_sha")), "G07_ARCHIVED_RECEIPT")
    return result


def expected_failures(records):
    by_lane = {item["lane"]: (item, packed) for item, packed in records if item["source"] == "fixture"}
    original, packed = by_lane["B02"]
    raw = gzip.decompress(packed)
    data = json.loads(raw)["data"]
    empty, empty_packed = by_lane["B03"]
    option, _ = by_lane["B06"]
    option_data = [{"date": option["start"], "option_id": "fixture", "contract_id": strike, "close": 5}
                   for strike in (101, 103)]
    cases = {
        "duplicate_native_key": (original, encode({"status": 200, "data": [data[0], data[0]]})),
        "lost_native_key": (original, encode({"status": 200, "data": data[:1]})),
        "wrong_observation_date": (original, encode({"status": 200, "data": [data[0] | {"date": "2024-05-14"}]})),
        "wrong_security": (original, encode({"status": 200, "data": [data[0] | {"stock_id": "9999"}]})),
        "missing_native_field": (original, encode({"status": 200, "data": [{"date": data[0]["date"]}]})),
        "unknown_empty_response": (empty | {"completeness": {}}, gzip.decompress(empty_packed)),
        "unconfirmed_page_continuation": (option, encode({"status": 200, "data": option_data, "next_page": "2"})),
        "provider_denial_is_not_empty": (empty, encode({"status": 403, "data": []})),
    }
    for label, (item, body) in cases.items():
        try:
            validate_original(NativeContract(**item["native_contract"]), item["parameters"], body,
                              completeness=item["completeness"])
        except ValueError:
            continue
        raise ValueError(f"G06_NEGATIVE_ACCEPTED_{label}")
    return sorted(cases)


def engine_matrix(records):
    validated = [native_proof(item, packed) for item, packed in records]
    lane_set = {item["lane"] for item, _ in records}
    require(lane_set == {f"B{n:02}" for n in range(1, 9)}, "G06_LANE_COVERAGE")
    option_days = [(item["start"], item["end"]) for item, _ in records if item["lane"] == "B06"]
    require(not subtract_coverage("2009-03-18", "2009-03-19", option_days), "G06_SPLIT_UNION")
    require([list(x) for x in subtract_coverage("2009-03-18", "2009-03-20", option_days)] ==
            [["2009-03-20", "2009-03-20"]], "G06_SPLIT_GAP")
    parameters = {"dataset": "fixture", "data_id": "one", "start_date": "2000-01-01", "end_date": "2000-01-01"}
    require(query_key(parameters, "entity_range", market="TWSE") == query_key(parameters, "entity_range", market="TPEx") and
            query_key(parameters, "entity_range", market="TWSE", market_specific=True) !=
            query_key(parameters, "entity_range", market="TPEx", market_specific=True), "G06_VENUE_ALIAS")
    item, packed = next(pair for pair in records if pair[0]["lane"] == "B06")
    native_rows = json.loads(gzip.decompress(packed))["data"]
    paged_rule = replace(NativeContract(**item["native_contract"]), page_field="next_page")
    pages = [(item["parameters"], encode({"status": 200, "data": native_rows[:1], "next_page": "2"})),
             (item["parameters"] | {"page": "2"}, encode({"status": 200, "data": native_rows[1:], "next_page": None}))]
    paged_receipt = validate_pages(paged_rule, pages, completeness=item["completeness"])
    require(paged_receipt["rows"] == len(native_rows) and
            [part["raw_sha"] for part in paged_receipt["pages"]] == [digest(raw) for _, raw in pages],
            "G06_PAGE_PROOF")
    require(not can_reserve([(100, "download")], 101, "download", normal_limit=1, total_limit=2)
            and can_reserve([(100, "download")], 101, "probe", normal_limit=1, total_limit=2)
            and not can_reserve([(100, "download"), (100, "probe")], 101, "probe", normal_limit=1, total_limit=2)
            and not can_reserve([], 119, "download", paused_until=retry_deadline("20", 100)), "G06_ROLLING_LIMITS")
    state = {"tasks": {"fixture": {"status": "PENDING", "generation": 0}}, "quota": {"events": [], "paused_until": 0}}
    lease1 = claim(state, "fixture", "worker", 100, charge=True)
    require(lease1 and not claim(lease1, "fixture", "duplicate", 100, charge=True), "G06_DUPLICATE_CLAIM")
    lease2 = claim(lease1, "fixture", "B09", 131, charge=False)
    require(lease2 and not fenced_accept(lease2, "fixture", lease1["tasks"]["fixture"], 132, {}) and
            fenced_accept(lease2, "fixture", lease2["tasks"]["fixture"], 132, {}) and
            lease2["quota"]["events"] == lease1["quota"]["events"], "G06_STALE_LEASE")
    require(release_batch.retry_state({"attempts": release_batch.MAX_ATTEMPTS}, "fixture", 100)["status"] ==
            "REPAIR", "G06_RETRY_EXHAUSTION")
    return {"lanes": sorted(lane_set), "fixture_windows": sum(item["source"] == "fixture" for item, _ in records),
            "archived_originals": sum(isinstance(item["source"], dict) for item, _ in records),
            "verified_sample_rows": sum(row["rows"] for row in validated), "negative_cases": expected_failures(records),
            "split_coverage": "PASS", "fixture_page_chain": "PASS", "venue_canonical_key": "PASS",
            "quota_and_fence": "PASS", "finmind_requests": 0}


def private_child(parent, name, access, *, create):
    child = ensure(parent, name, FOLDER, access, create=create)
    verify_private_folder(child, access)
    return child


def isolated_root(ctx, *, create):
    require(ctx.task_id == TASK_ID and len(ctx.spec_sha) == 64, "G07_TASK_SCOPE")
    root_meta = read_json(f"{BASE}/files/{ctx.root}?fields=id,parents,shared,trashed,mimeType", ctx.access)
    require(root_meta == {"id": ctx.root, "parents": [ctx.task_root], "shared": False, "trashed": False,
                          "mimeType": FOLDER}, "G07_NOT_ISOLATED")
    return private_child(ctx.root, SANDBOX, ctx.access, create=create)


def parent_for(item, bronze, access, *, create):
    parent = bronze
    for name in (item["start"][:4], "FinMind_Backer", item["start"][:7]):
        parent = private_child(parent, name, access, create=create)
    return parent


def identity(ctx):
    return {"task_id": ctx.task_id, "spec_sha": ctx.spec_sha, "run": ctx.run, "attempt": ctx.attempt}


def stage(ctx):
    root = isolated_root(ctx, create=True)
    staging = private_child(root, "STAGING", ctx.access, create=True)
    bronze = private_child(root, "TEST_BRONZE", ctx.access, create=True)
    journal = DriveJournal(root, ctx.access)
    records, archive_error = frozen_inputs(ctx.access)
    packed_by_id = {item["task_id"]: packed for item, packed in records}
    valid = [item for item, _ in records]
    require(len(packed_by_id) == len(valid), "G07_DUPLICATE_CANONICAL_KEY")
    blocked = {"task_id": digest(b"r12-v2-g07-held-fixture"), "lane": "B09", "reason": "UNPROVEN_SCOPE_FIXTURE"}
    if find(root, MANIFEST, ctx.access) is None:
        ids = journal.new_ids(len(valid))
        items = [item | {"destination": parent_for(item, bronze, ctx.access, create=True),
                         "destination_id": file_id, "name": item["task_id"] + ".json.gz"}
                 for file_id, item in zip(ids, valid, strict=True)]
        manifest = identity(ctx) | {"stage": staging, "bronze": bronze, "items": items, "blocked": blocked,
                                    "archive_reuse_error": archive_error, "origin_job": os.environ["GITHUB_JOB"]}
        write_json(root, MANIFEST, manifest, ctx.access)
    else:
        manifest = read_json_file(root, MANIFEST, ctx.access)
        require([{key: val for key, val in row.items() if key not in {"destination", "destination_id", "name"}}
                 for row in manifest["items"]] == json.loads(encode(valid)), "G07_CHANGED_FIXTURE_INPUT")
    require(all(manifest.get(key) == value for key, value in identity(ctx).items()) and
            manifest["stage"] == staging and manifest["bronze"] == bronze, "G07_MANIFEST_SCOPE")
    if journal.version(0) is None:
        states = {item["task_id"]: {"status": "READY", "attempts": 0} for item in manifest["items"]}
        states[blocked["task_id"]] = {"status": "BLOCKED", "reason": blocked["reason"], "attempts": 0}
        journal.initialize({"manifest_sha": digest(encode(manifest)), "phase": "READY", "tasks": states,
                            "events": [], "pause": 0, "account": {"at": FIXTURE_TIME, "limit": 1600,
                                                                  "headroom": len(valid), "first_event": 0}})
    record = journal.version()
    require(record["state"]["manifest_sha"] == digest(encode(manifest)), "G07_MANIFEST_DIGEST")
    if record["state"]["phase"] == "READY":
        candidates = release_batch.ready_wave(manifest["items"], record["state"], FIXTURE_TIME,
                                              worker_limit=len(valid), lane_limit=len(valid))
        require(candidates == manifest["items"], "G07_WORK_PLAN")
        tasks = record["state"]["tasks"].copy()
        events = []
        for item, file_id in zip(candidates, journal.new_ids(len(candidates)), strict=True):
            key = item["task_id"]
            tasks[key] = {"status": "REQUESTING", "attempts": 1, "stage_id": file_id,
                          "lease_until": FIXTURE_TIME + 30}
            events.append(release_batch.stored_event(ctx.run, "download", FIXTURE_TIME, key))
        record = advance(journal, record, tasks=tasks, events=events, phase="REQUESTING")
    if record["state"]["phase"] == "REQUESTING":
        tasks = record["state"]["tasks"].copy()
        for index, item in enumerate(manifest["items"]):
            key = item["task_id"]
            task = tasks[key]
            packed = packed_by_id[key]
            proof = native_proof(item, packed)
            release_batch.stage_original(manifest, item, task, ctx.access, packed)
            # Leave one archive under the expired REQUESTING lease to model lost staging acknowledgement.
            if index:
                tasks[key] = task | {"status": "STAGED", "proof": proof}
        record = advance(journal, record, tasks=tasks, phase="INTERRUPTED")
    require(record["state"]["phase"] in {"INTERRUPTED", "ACCEPTED"}, "G07_STAGE_PHASE")
    return {"status": "PASS", "outputs": {"G07_STAGE_CHECKPOINT.json": identity(ctx) | {
        "job": manifest["origin_job"], "stage": record["state"]["phase"], "windows": len(valid),
        "manifest_sha": record["state"]["manifest_sha"], "finmind_requests": 0, "test_only": True}}}


def resume(ctx):
    root = isolated_root(ctx, create=False)
    staging = private_child(root, "STAGING", ctx.access, create=False)
    bronze = private_child(root, "TEST_BRONZE", ctx.access, create=False)
    journal = DriveJournal(root, ctx.access)
    manifest = read_json_file(root, MANIFEST, ctx.access)
    require(all(manifest.get(key) == value for key, value in identity(ctx).items()) and
            manifest["stage"] == staging and manifest["bronze"] == bronze and
            manifest["origin_job"] != os.environ["GITHUB_JOB"], "G07_DISTINCT_JOB_PROOF")
    record = journal.version()
    before = journal.version(1)
    require(record["state"]["manifest_sha"] == digest(encode(manifest)) and
            record["state"]["phase"] in {"INTERRUPTED", "ACCEPTED"}, "G07_CHECKPOINT_PROOF")
    stale = before["state"] | {"phase": "STALE_WRITER"}
    require(not journal.append(before, stale), "G07_STALE_WRITER_ACCEPTED")

    def isolated_destination(item, access, *, create):
        return parent_for(item, bronze, access, create=create)

    if record["state"]["phase"] == "INTERRUPTED":
        tasks = release_batch.recover_pending(manifest, record["state"], ctx.access, proof=native_proof)
        reclaimed_key = manifest["items"][0]["task_id"]
        require(tasks[reclaimed_key]["status"] in {"STAGED", "VERIFIED"} and tasks[reclaimed_key]["recovered"] and
                tasks[reclaimed_key]["attempts"] == 1, "G07_RECOVERY_REDOWNLOAD")
        if tasks != record["state"]["tasks"]:
            record = advance(journal, record, tasks=tasks)
        record = release_batch.accept(manifest, record, journal, ctx.access, proof=native_proof,
                                      resolve_destination=isolated_destination, workers=4)
        tasks = record["state"]["tasks"].copy()
        for item in manifest["items"]:
            current = tasks[item["task_id"]]
            require(current["status"] == "VERIFIED", "G07_B10_NOT_VERIFIED")
            if item["completeness"]["method"] == "confirmed_empty":
                require(current["receipt"]["proof"]["rows"] == 0, "G07_EMPTY_COUNT")
                tasks[item["task_id"]] = current | {"status": "EMPTY_CONFIRMED"}
        record = advance(journal, record, tasks=tasks, phase="ACCEPTED")
    state = record["state"]
    require(state["tasks"][manifest["items"][0]["task_id"]].get("recovered") is True, "G07_RECOVERY_NOT_PERSISTED")
    tasks = state["tasks"]
    require(tasks[manifest["blocked"]["task_id"]]["status"] == "BLOCKED" and state["events"] == before["state"]["events"] and
            len({event["task_id"] for event in state["events"]}) == len(manifest["items"]), "G07_FINAL_RECONCILIATION")
    # Revalidate all archived files including the empty one without another journal append.
    ids = set()
    for item in manifest["items"]:
        task = tasks[item["task_id"]]
        parent = isolated_destination(item, ctx.access, create=False)
        require(task["status"] in {"VERIFIED", "EMPTY_CONFIRMED"} and task["receipt"]["file_id"] not in ids,
                "G07_DUPLICATE_OR_MISSING_RECEIPT")
        ids.add(task["receipt"]["file_id"])
        checked = release_batch.accept_item(manifest, item, task | {"status": "VERIFIED"}, parent, ctx.access,
                                            proof=native_proof)
        require(checked["receipt"] == task["receipt"], "G07_RECEIPT_REPLAY")
    require(journal.version()["sequence"] == record["sequence"], "G07_REPLAY_ADVANCED_LEDGER")
    prior = cross_run_evidence(ctx.access)
    details = identity(ctx) | {
        "origin_job": manifest["origin_job"], "resume_job": os.environ["GITHUB_JOB"], "transferred_state": "Private Drive",
        "new_cross_job_same_run": True, "prior_cross_run_evidence": prior,
        "sandbox": root, "commit_sequence": record["sequence"], "manifest_sha": state["manifest_sha"],
        "states": dict(Counter(task["status"] for task in tasks.values())), "unique_receipts": len(ids),
        "case": "lost staging acknowledgement with expired lease", "recovered_tasks": 1,
        "stale_writer": "REJECTED", "replayed_receipt": "READBACK_WITHOUT_NEW_WRITE",
        "archive_reuse_error": manifest["archive_reuse_error"], "finmind_requests": 0,
        "synthetic_reservations": len(state["events"]), "production_dispatchable": 0,
    }
    return {"status": "HOLD" if manifest["archive_reuse_error"] or prior["status"] != "PASS" else "PASS", "outputs": {
        "INTEGRATION_REPORT.json": details,
        "G07_B09_EXCEPTION.json": {"scope": "fixture", "task": manifest["blocked"]},
        "G07_B10_RECEIPTS.json": {key: task["receipt"] for key, task in tasks.items() if "receipt" in task},
    }}


def run(ctx, phase="engine"):
    if phase == "stage":
        return stage(ctx)
    if phase == "resume":
        return resume(ctx)
    require(phase == "engine", "G06_UNKNOWN_PHASE")
    records, archive_error = frozen_inputs(ctx.access)
    matrix = engine_matrix(records)
    return {"status": "HOLD" if archive_error else "PASS", "outputs": {
        "ENGINE_TESTS.json": identity(ctx) | matrix | {"archive_reuse_error": archive_error, "test_only": True},
        "TEST_MATRIX.json": [{"dataset": item["dataset"], "lane": item["lane"], "market": item["market"],
                              "grain": item["native_contract"]["grain"], "method": item["completeness"]["method"],
                              "source": "fixture" if item["source"] == "fixture" else "archived_original"}
                             for item, _ in records],
    }}
