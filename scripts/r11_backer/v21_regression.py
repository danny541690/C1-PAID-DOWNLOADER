"""Recheck V2.1 deltas and replay one interrupted window on private Drive."""

from dataclasses import asdict
import gzip
from io import BytesIO
import json
import os
from pathlib import Path
import re

from . import release_batch
from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .drive_pilot import BASE, FOLDER, ensure, find
from .integration_pilot import digest, encode
from .live_bronze_admission import advance
from .live_preflight import read_json
from .live_recovery_canary import read_json_file, write_json
from .quota import can_reserve, next_reservation_at
from .release_rehearsal import require
from .v2_coverage import query_key, subtract_coverage
from .v2_engine import TASK_ID, private_child
from .v2_registry import NativeContract, validate_original


SANDBOX = "G07_V21_REGRESSION"
CASE = "CASE_MANIFEST.json"
EVIDENCE = ("DATASET_REGISTRY.json", "CONTRACT_EVIDENCE.json", "MANIFEST_INDEX.json",
            "COVERAGE_GAPS.json", "RESOURCE_BUDGET.json", "PROBE_LEDGER.json")
WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/r12-incremental-closeout.yml"


def identity(ctx):
    return {"task_id": ctx.task_id, "spec_sha256": ctx.spec_sha,
            "commit": ctx.commit, "run_id": ctx.run, "attempt": ctx.attempt}


def fixture():
    dataset = "TaiwanStockDividend"
    first, last = "1999-12-31", "2000-01-03"
    rule = NativeContract(dataset, "event", "entity_range", ("date", "stock_id", "amount"), (),
                          identity_field="stock_id", key_policy="content_multiset")
    params = {"dataset": dataset, "start_date": first, "end_date": last, "data_id": "fixture-v21"}
    late = {"date": last, "stock_id": "fixture-v21", "amount": 1.25}
    early = {"date": first, "stock_id": "fixture-v21", "amount": 0}
    rows = [late, early, late]
    raw = encode({"status": 200, "data": rows}) + b"\n"
    packed, raw_sha, packed_sha = compress_original(raw)
    complete = {"method": "expected_multiset", "row_hashes": [digest(encode(row)) for row in rows]}
    return {"task_id": query_key(params, "entity_range"), "dataset": dataset, "lane": "B05",
            "market": "TWSE", "scope": params["data_id"], "start": first, "end": last,
            "parameters": params, "rule": json.loads(encode(asdict(rule))), "completeness": complete,
            "fixture_original": {"raw_sha": raw_sha, "packed_sha": packed_sha}}, packed


def fixture_proof(item, packed):
    rule = NativeContract(**item["rule"])
    with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
        raw = stream.read(rule.byte_limit + 1)
    proof = validate_original(rule, item["parameters"], raw, completeness=item["completeness"])
    actual = {"raw_sha": digest(raw), "packed_sha": digest(packed)}
    require(actual == item["fixture_original"], "V21_FIXTURE_ORIGINAL_CHANGED")
    return proof | actual | {"bytes": len(packed)}


def test_native_and_coverage(item, packed):
    p = fixture_proof(item, packed)
    rule = NativeContract(**item["rule"])
    rows = json.loads(gzip.decompress(packed))["data"]
    require(p["rows"] == 3 and p["first_sha"] == digest(encode(rows[0])) and
            p["last_sha"] == digest(encode(rows[-1])), "V21_NATIVE_ROW_ORDER_OR_MULTIPLICITY")
    for label, body, evidence in (
        ("MULTIPLICITY", encode({"status": 200, "data": rows[:-1]}), item["completeness"]),
        ("EVIDENCE", encode({"status": 200, "data": rows}), {}),
    ):
        try:
            validate_original(rule, item["parameters"], body, completeness=evidence)
        except ValueError:
            continue
        raise ValueError(f"V21_UNPROVEN_{label}_ACCEPTED")
    remainder = subtract_coverage(item["start"], item["end"],
                                  [("1999-12-31", "1999-12-31"), ("2000-01-02", "2000-01-03")])
    require(remainder == [["2000-01-01", "2000-01-01"]], "V21_CROSS_YEAR_COVERAGE_DIFF")
    params = item["parameters"]
    require(query_key(params, "entity_range", market="TWSE") ==
            query_key(params, "symbol_range", market="TPEx") and
            query_key(params, "entity_range", market="TWSE", market_specific=True) !=
            query_key(params, "entity_range", market="TPEx", market_specific=True), "V21_NATIVE_QUERY_IDENTITY")
    return {"content_multiset": "PASS", "row_order_and_raw_digest": "PASS",
            "source_query_coverage": "PASS", "fixture_hashes": p}


def artifact_bindings(ctx):
    read, errors = {}, {}
    for name in EVIDENCE:
        try:
            value = ctx.read(name)
            require(isinstance(value, (dict, list)) and bool(value), "V21_EMPTY_DEPENDENCY")
            read[name] = value
        except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
            errors[name] = type(error).__name__
    return read, errors


def bind_hashes(artifacts):
    return {name: digest(encode(value)) for name, value in artifacts.items()}


def source_consumer_check(text):
    """Verify the current workflow exposes the existing token to its probe job alone."""
    top, jobs = text.split("\njobs:\n", 1)
    require(re.search(r"(?m)^  group: ['\"]?r12-1-bounded-preflight['\"]?\s*$", top),
            "V21_WORKFLOW_MUTEX")
    require("secrets.FINMIND_BACKER_TOKEN" not in top, "V21_TOKEN_AT_WORKFLOW_SCOPE")
    headers = list(re.finditer(r"(?m)^  ([A-Za-z][\w-]*):[ \t]*$", jobs))
    holders = []
    for i, header in enumerate(headers):
        end = headers[i + 1].start() if i + 1 < len(headers) else len(jobs)
        body = jobs[header.end():end]
        count = body.count("secrets.FINMIND_BACKER_TOKEN")
        if count:
            holders.extend([header[1]] * count)
    require(len(holders) == 1 and "probe" in holders[0].lower(), "V21_TOKEN_OUTSIDE_DEDICATED_JOB")
    require(not re.search(r"(?i)\b(?:production_download\s+(?:launch|continue)|"
                          r"r12_production\s*=\s*(?:launch|continue))\b", jobs), "V21_PRODUCTION_TRIGGER_PATH")
    return {"workflow_token_isolation": "PASS", "secret_holder": holders[0],
            "account_consumer_coverage": "SEE_G05_SHARED_LEDGER_EVIDENCE"}


def check_shard(ctx, index):
    require(index.get("production_dispatchable") == 0, "V21_PRODUCTION_MANIFEST_RELEASED")
    entries = index["historical_shards"]
    require(len({entry["name"] for entry in entries}) == len(entries), "V21_DUPLICATE_MANIFEST_SHARD")
    if not entries:
        return {"historical_shard_readback": "NOT_RUN_NO_BOUNDED_HISTORY"}
    entry = entries[0]
    name = entry["name"]
    require(re.fullmatch(r"V21_HISTORICAL_MANIFEST_[A-Za-z0-9_.-]+\.json", name), "V21_SHARD_NAME")
    shard = ctx.read(name)
    require(digest(encode(shard)) == entry["sha256"], "V21_HISTORICAL_SHARD_DIGEST")
    rows = shard["rows"]
    require(len(rows) == entry["windows"] and rows, "V21_HISTORICAL_SHARD_COUNT")
    keys = {row["query_key"] for row in rows}
    require(len(keys) == len(rows) and all(re.fullmatch(r"[a-f0-9]{64}", key) for key in keys) and
            all(row["parameters"].get("dataset") and row["mode"] for row in rows), "V21_HISTORICAL_NATIVE_WINDOW")
    return {"historical_shard_readback": "PASS", "current_historical_shard_sha256": entry["sha256"],
            "current_historical_shard_windows": len(rows)}


def inherited_checks(ctx):
    prior_engine = ctx.prior("ENGINE_TESTS.json")
    prior_run = ctx.prior("INTEGRATION_REPORT.json")
    cross = prior_run["prior_cross_run_evidence"]
    require(prior_engine["finmind_requests"] == prior_run["finmind_requests"] == 0 and
            prior_engine["lanes"] == [f"B{n:02}" for n in range(1, 9)] and
            prior_run["new_cross_job_same_run"] is True and cross["status"] == "PASS" and
            cross["origin_run"] != cross["resume_run"], "V21_INHERITED_TEST_EVIDENCE")
    return {"inherited_engine_report_sha256": digest(encode(prior_engine)),
            "inherited_integration_report_sha256": digest(encode(prior_run)),
            "inherited_cross_run": cross}


def test_quota():
    events = [(100, "download")] * 1200 + [(100, "probe")] * 399
    require(not can_reserve(events, 101, "download") and can_reserve(events, 101, "probe"),
            "V21_NORMAL_LIMIT")
    require(not can_reserve(events + [(100, "probe")], 101, "probe") and
            next_reservation_at(events, 101, "download") == 3700, "V21_SHARED_WINDOW_LIMIT")
    return {"normal_1200": "PASS", "shared_1600": "PASS", "clock_rollover": "PASS"}


def engine(ctx):
    item, packed = fixture()
    findings = test_native_and_coverage(item, packed) | test_quota()
    artifacts, errors = artifact_bindings(ctx)
    try:
        findings.update(inherited_checks(ctx))
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        errors["inherited_engine_and_cross_run_evidence"] = type(error).__name__
    try:
        findings.update(source_consumer_check(WORKFLOW.read_text(encoding="utf-8")))
    except (OSError, ValueError, RuntimeError) as error:
        errors["current_probe_workflow_contract"] = type(error).__name__
    if "MANIFEST_INDEX.json" in artifacts:
        try:
            findings.update(check_shard(ctx, artifacts["MANIFEST_INDEX.json"]))
        except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
            errors["historical_shard_readback"] = type(error).__name__
    report = identity(ctx) | {"selected_revision": "V2.1", "targeted_checks": findings,
                              "dependency_sha256": bind_hashes(artifacts), "missing_dependencies": errors,
                              "production_mutations": 0, "finmind_requests": 0,
                              "test_only": True}
    matrix = [{"case": name, "result": status, "scope": "V2.1_CHANGED_INTERFACES_ONLY"}
              for name, status in findings.items() if status == "PASS"]
    return {"status": "HOLD" if errors else "PASS", "outputs": {
        "ENGINE_TESTS.json": report, "TEST_MATRIX.json": matrix}}


def sandbox(ctx, *, create):
    require(ctx.task_id == TASK_ID and "FINMIND_BACKER_TOKEN" not in os.environ, "V21_ISOLATION_CREDENTIALS")
    meta = read_json(f"{BASE}/files/{ctx.root}?fields=id,parents,shared,trashed,mimeType", ctx.access)
    require(meta == {"id": ctx.root, "parents": [ctx.task_root], "shared": False,
                     "trashed": False, "mimeType": FOLDER}, "V21_ISOLATED_PARENT")
    return private_child(ctx.root, SANDBOX, ctx.access, create=create)


def stage(ctx):
    access = ctx.access
    report = ctx.read("ENGINE_TESTS.json")
    require(all(report[key] == value for key, value in identity(ctx).items()), "V21_ENGINE_EVIDENCE_BINDING")
    root = sandbox(ctx, create=True)
    stage_root = private_child(root, "STAGING", access, create=True)
    target = private_child(root, "TEST_BRONZE", access, create=True)
    journal = DriveJournal(root, access)
    item, packed = fixture()
    if find(root, CASE, access) is None:
        stage_id, target_id = journal.new_ids(2)
        staged = item | {"destination": target, "destination_id": target_id, "name": item["task_id"] + ".json.gz"}
        write_json(root, CASE, identity(ctx) | {"stage": stage_root, "items": [staged], "staging_id": stage_id,
                                               "origin_job": os.environ["GITHUB_JOB"],
                                               "engine_report_sha256": digest(encode(report))}, access)
    manifest = read_json_file(root, CASE, access)
    require(all(manifest[key] == value for key, value in identity(ctx).items()) and
            manifest["stage"] == stage_root and manifest["items"][0]["destination"] == target and
            manifest["engine_report_sha256"] == digest(encode(report)), "V21_STAGE_IDENTITY")
    item = manifest["items"][0]
    key = item["task_id"]
    task = {"status": "REQUESTING", "attempts": 1, "stage_id": manifest["staging_id"], "lease_until": 131}
    event = release_batch.stored_event(ctx.run, "download", 100, key)
    if journal.version(0) is None:
        journal.initialize({"manifest_sha": digest(encode(manifest)), "tasks": {key: task}, "events": [event]})
    record = journal.version(0)
    require(record["state"]["manifest_sha"] == digest(encode(manifest)) and
            record["state"]["tasks"][key] == task, "V21_STAGE_CHECKPOINT_IDENTITY")
    proof = fixture_proof(item, packed)
    release_batch.stage_original(manifest, item, task, access, packed)
    return {"status": "PASS", "outputs": {"G07_STAGE_CHECKPOINT.json": identity(ctx) | {
        "manifest_sha256": digest(encode(manifest)), "engine_report_sha256": digest(encode(report)),
        "journal_sha256": digest(encode(record)), "job": manifest["origin_job"],
        "fixture_proof": proof, "finmind_requests": 0, "test_only": True}}}


def resume(ctx):
    access = ctx.access
    root = sandbox(ctx, create=False)
    target = private_child(root, "TEST_BRONZE", access, create=False)
    stage_dir = private_child(root, "STAGING", access, create=False)
    manifest = read_json_file(root, CASE, access)
    stage_evidence = ctx.read("G07_STAGE_CHECKPOINT.json")
    require(all(manifest.get(key) == stage_evidence.get(key) == value for key, value in identity(ctx).items()) and
            manifest["stage"] == stage_dir and stage_evidence["manifest_sha256"] == digest(encode(manifest)) and
            manifest["origin_job"] == stage_evidence["job"] != os.environ["GITHUB_JOB"], "V21_CROSS_JOB_BINDING")
    engine_report = ctx.read("ENGINE_TESTS.json")
    require(digest(encode(engine_report)) == manifest["engine_report_sha256"] == stage_evidence["engine_report_sha256"],
            "V21_ENGINE_CHANGED_AFTER_STAGE")
    inputs, errors = artifact_bindings(ctx)
    require(engine_report["dependency_sha256"] == bind_hashes(inputs), "V21_DEPENDENCIES_CHANGED_AFTER_STAGE")
    journal = DriveJournal(root, access)
    initial = journal.version(0)
    require(stage_evidence["journal_sha256"] == digest(encode(initial)), "V21_STAGED_JOURNAL_CHANGED")
    record = journal.version()
    key, = record["state"]["tasks"]
    if record["state"]["tasks"][key]["status"] == "REQUESTING":
        tasks = release_batch.recover_pending(manifest, record["state"], access, proof=fixture_proof)
        require(tasks[key]["status"] == "STAGED" and tasks[key]["recovered"] and tasks[key]["attempts"] == 1 and
                tasks[key]["proof"] == stage_evidence["fixture_proof"], "V21_CHECKPOINT_NOT_REUSED")
        record = advance(journal, record, tasks=tasks)
    require(not journal.append(initial, initial["state"] | {"stale_writer": True}), "V21_STALE_WRITER_ACCEPTED")

    def resolve(item, _, *, create):
        require(not create and item["destination"] == target, "V21_DESTINATION_OUTSIDE_SANDBOX")
        return target

    record = release_batch.accept(manifest, record, journal, access, resolve_destination=resolve, proof=fixture_proof)
    state = record["state"]
    task = state["tasks"][key]
    require(task["status"] == "VERIFIED" and task["receipt"]["proof"] == stage_evidence["fixture_proof"] and
            state["events"] == initial["state"]["events"] and task["recovered"], "V21_B10_RECONCILIATION")
    replay = release_batch.accept(manifest, record, journal, access, resolve_destination=resolve, proof=fixture_proof)
    require(record == replay and journal.version()["sequence"] == record["sequence"], "V21_DUPLICATE_REPLAY_WRITE")
    evidence_errors = engine_report["missing_dependencies"] | errors
    return {"status": "HOLD" if evidence_errors else "PASS", "outputs": {"INTEGRATION_REPORT.json": identity(ctx) | {
        "revision": "V2.1", "origin_job": manifest["origin_job"], "resume_job": os.environ["GITHUB_JOB"],
        "case": "changed native multiset adapter with expired staging acknowledgement", "manifest_sha256":
        digest(encode(manifest)), "independent_cross_job_checkpoint": "PASS", "stale_writer": "REJECTED",
        "receipt_replay": "SAME_LEDGER_WITH_ZERO_NEW_EVENTS", "test_receipt": task["receipt"],
        "sequence": record["sequence"], "missing_dependencies": evidence_errors,
        "inherited_cross_run": engine_report["targeted_checks"].get("inherited_cross_run"), "production_dispatchable": 0,
        "production_mutations": 0, "finmind_requests": 0}}}


def run(ctx, phase="engine"):
    require("FINMIND_BACKER_TOKEN" not in os.environ, "V21_REGRESSION_BACKER_CREDENTIAL")
    return {"engine": engine, "stage": stage, "resume": resume}[phase](ctx)
