"""One isolated V2.4 preflight workflow, with append-only D-006 evidence."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from hashlib import sha256
from io import StringIO
import json
import os
import re
import subprocess
import sys
import unittest

from . import v24_account, v24_coverage, v24_regression, v24_sources
from .drive_pilot import ROOT, ensure, read_drive_bytes, token
from .final_readiness_v2 import Context, SOP, SOP_SHA, SPEC_SHA as LEGACY_SHA, TASK_ID, original, private_path, snapshot, write_markdown
from .incremental_closeout_v21 import production_runs
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import read_json_file, write_json
from .production_preflight import final_readiness_verdict
from .release_rehearsal import RehearsalError, require
from .test_archive import GitHub
from .v2_handover import agents_test

SPEC_SHA = "197052e312c1cb41767d94c657eac4ca1196448b5b9610fdab099f4a22c6fd94"
SPEC_NAME = "R12_1_V24_OFFICIAL_BULK_BRONZE_READINESS_20261128.md"
SPEC_ID = "1Slb9UJryLtJ84Nrtd-JYR6VEbxgPKHyn"
PRIOR_RUN = 46074623286
PRIOR_COMMIT = "b502683fc5fd9806c7277f1d7c6701821ab7adc7"
PRIOR_SPEC = "c83f8916d9559cba7638193bd5e206498f085ba8648337a685aeb2a5548b4271"
PRIOR_REPORT_SHA = "13e4ecafee084c865136dc5b873254d923a6e4b0c1334ec74d304a19b6e7555c"
LEGACY_RUN = 46030259242
LEGACY_COMMIT = "4df007ddbe6c3f25eddd6ed7f3ac1599a2ced25c"
LEGACY_REPORT_SHA = "049a250350455e819c3159731447f2f6e40548d94f664b69b9499fc9b0806611"
REUSED = ("G00", "G01", "G02", "G06", "G07", "G08")
PROBE_JOBS = {"G05_A", "G05_B"}
JOB_ARTIFACTS = {
    "LOCK": ("INPUT_LOCK.json",), "H": ("HANDOVER_EVIDENCE.json",),
    "G05_A": ("ACCOUNT_CONSUMER_INVENTORY.json", "LEASE_EVIDENCE.json", "ACCOUNT_BOOTSTRAP_RECEIPT.json"),
    "G03_OFFICIAL": ("OFFICIAL_SOURCE_LOCK.json", "ENTITLEMENT_MATRIX.json", "OFFICIAL_DATASET_REVIEW.json",
                     "OFFICIAL_QUERY_REVIEW.json", "CONTRACT_GAP_MATRIX.json"),
    "G04_OFFLINE": ("HISTORICAL_INPUT.json", "ACTIVE_BATCH_RECONCILIATION.json", "BULK_BENCHMARK_EVIDENCE.json",
                    "BRONZE_STORAGE_RULE.json", "B09_REPAIR_PLAN.json"),
    "G05_B": ("PROBE_RESULTS.json", "PROBE_LEDGER.json"),
    "G03_FINAL": ("DATASET_REGISTRY.json", "QUERY_MODE_REGISTRY.json"),
    "G04_FINAL": ("HISTORICAL_UNIVERSE.json", "HISTORICAL_COVERAGE.json", "MANIFEST_INDEX.json", "REUSE_MAP.json",
                   "COVERAGE_GAPS.json", "QUERY_MODE_BENCHMARK.json"),
    "G05_C": ("RESOURCE_BUDGET.json", "PARTITION_RESOURCE_CHECK.json"),
    "G06": ("ENGINE_TESTS.json",), "G07_STAGE": ("G07_CHECKPOINT.json",), "G07": ("INTEGRATION_REPORT.json",),
}
STATES = {"PASS", "HOLD", "FAIL", "NOT_RUN"}
ACTIONS = {"G05_A": v24_account.account_phase, "G03_OFFICIAL": v24_sources.official, "G04_OFFLINE": v24_coverage.offline,
           "G05_B": v24_account.samples_phase, "G04_FINAL": v24_coverage.finalize, "G05_C": v24_account.resource_phase,
           "G07_STAGE": v24_regression.stage, "G07": v24_regression.resume}
TEST_PATTERNS = ("test_r12_bulk_v24.py", "test_r12_v21_closeout.py", "test_r12_release_batch.py",
                 "test_r12_production_download.py", "test_r12_v2_contracts.py")
MIN_V24_TESTS = 9


class BulkContext(Context):
    def __init__(self, job):
        require(os.environ.get("R12_TASK_ID") == TASK_ID and os.environ.get("R12_SPEC_SHA") == SPEC_SHA and
                os.environ.get("GITHUB_JOB") == job, "V24_WORKFLOW_BINDING")
        require(job in PROBE_JOBS or "FINMIND_BACKER_TOKEN" not in os.environ, "V24_SECRET_OUTSIDE_PROBE")
        self.task_id, self.spec_sha, self.prior_spec = TASK_ID, SPEC_SHA, PRIOR_SPEC
        self.run, self.attempt = int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"])
        self.commit, self.access = os.environ["GITHUB_SHA"], token()
        self.shared = private_path(self.access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT")
        task = private_path(self.access, self.shared, "FINAL_READINESS", TASK_ID)
        self.prior_version = private_path(self.access, task, PRIOR_SPEC)
        self.prior_root = private_path(self.access, self.prior_version, f"{PRIOR_RUN}_1")
        self.legacy_root = private_path(self.access, task, LEGACY_SHA, f"{LEGACY_RUN}_1")
        self.task_root = private_path(self.access, task, SPEC_SHA, create=job == "LOCK")
        self.root = private_path(self.access, self.task_root, f"{self.run}_{self.attempt}", create=job == "LOCK")
        self._production = None
        self._prior_files = {}
        self._legacy_files = {}

    def binary(self, name, raw, mime):
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]+", name), "V24_BINARY_NAME")
        file_id = ensure(self.root, name, mime, self.access, raw, create=True)
        verify_private_folder(file_id, self.access)
        require(read_drive_bytes(file_id, self.access, len(raw)) == raw, "V24_BINARY_READBACK")
        return file_id

    def saved(self, name, legacy=False):
        cache, root = (self._legacy_files, self.legacy_root) if legacy else (self._prior_files, self.prior_root)
        if name not in cache:
            _, raw = original(self.access, root, name)
            body = json.loads(raw)
            if name == "FINAL_READINESS.json":
                expected = LEGACY_REPORT_SHA if legacy else PRIOR_REPORT_SHA
                require(digest(raw) == expected, "V24_PRIOR_REPORT_DIGEST")
            else:
                report = self.saved("FINAL_READINESS.json", legacy=legacy)
                outputs = [record.get("output_sha256", {}) for record in (
                    list(report["gates"].values()) + list(report.get("execution", {}).values()))]
                expected = {row[name] for row in outputs if name in row}
                # Inherited manifest subshards are verified against their content indexes by their consumers.
                require(not expected or expected == {digest(encode(body))}, "V24_PRIOR_OUTPUT_DIGEST")
            cache[name] = body
        return cache[name]

    def prior(self, name):
        return self.saved(name, legacy=name.startswith("V2_REUSE_MAP_"))

    def legacy(self, name):
        return self.saved(name, legacy=True)


def lock(ctx):
    fid, raw = original(ctx.access, ctx.shared, SPEC_NAME)
    require(fid == SPEC_ID and digest(raw) == ctx.spec_sha and TASK_ID.encode() in raw, "V24_APPROVED_SPEC_BINDING")
    global_root = private_path(ctx.access, ROOT, "00_COMMAND", "AI_SHARED_CONTEXT")
    _, global_sop = original(ctx.access, global_root, SOP)
    _, sop = original(ctx.access, ctx.shared, SOP)
    require(sop == global_sop and digest(sop) == SOP_SHA, "V24_D006_BINDING")
    inputs = {}
    for name in ("START_HERE.md", "GOALS.md", "PLANS.md", "TASK_MASTER.json", "CURRENT_STATUS.json", "HANDOVER.md",
                 "DECISION_LOG.md", "MIGRATION_REGISTER.json", "R12_1_V22_BOOTSTRAP_AND_PARTITION_READINESS_20261128.md",
                 "R12_1_V23_OFFICIAL_QUERY_OPTIMIZATION_20261128.md"):
        key, body = original(ctx.access, ctx.shared, name)
        inputs[name] = {"file_id": key, "sha256": digest(body)}
    github = GitHub()
    prior_run = github.json(f"/actions/runs/{PRIOR_RUN}/attempts/1")
    previous = ctx.prior("FINAL_READINESS.json")
    require(prior_run["status"] == "completed" and prior_run["conclusion"] == "success" and
            prior_run["head_sha"] == previous["commit"] == PRIOR_COMMIT and previous["spec_sha256"] == PRIOR_SPEC and
            previous["run_id"] == PRIOR_RUN and previous["task_id"] == TASK_ID and previous["attempt"] == 1,
            "V24_PREVIOUS_RUN_BINDING")
    prior_jobs = {job["name"]: job for job in github.pages(f"/actions/runs/{PRIOR_RUN}/attempts/1/jobs", "jobs")}
    mapped = {"G00": "LOCK", "G01": "H", "G06": "G06", "G07": "G07", "G08": "G08"}
    inherited = {}
    for gate, job in mapped.items():
        actual = prior_jobs.get(job, {})
        binding = previous["github_jobs"].get(job, {})
        require(previous["gates"][gate]["status"] == "PASS" and actual["id"] == binding["id"] and
                actual["conclusion"] == "success", "V24_INHERITED_GATE_EVIDENCE")
        inherited[gate] = {"status": "PASS", "run_id": PRIOR_RUN, "attempt": 1, "job_id": actual["id"],
                           "commit": PRIOR_COMMIT, "spec_sha256": PRIOR_SPEC,
                           "sha256": digest(encode(previous["gates"][gate]))}
    # V2.1 inherited G02 from V2.0 rather than running a second G02 job.
    recovery_proof = previous["gates"]["G02"]
    parent = recovery_proof["inherited_from"]
    legacy = ctx.legacy("FINAL_READINESS.json")
    recovery_job = next(job for job in github.pages(f"/actions/runs/{LEGACY_RUN}/attempts/1/jobs", "jobs")
                        if job["name"] == "G02")
    require(recovery_proof["status"] == legacy["gates"]["G02"]["status"] == "PASS" and
            parent["run_id"] == LEGACY_RUN and parent["spec_sha256"] == LEGACY_SHA and
            parent["commit"] == LEGACY_COMMIT and recovery_job["id"] == parent["job_id"] and
            recovery_job["conclusion"] == "success" and
            digest(encode(legacy["gates"]["G02"])) == parent["report_sha256"], "V24_INHERITED_G02_CHAIN")
    inherited["G02"] = {"status": "PASS", **parent, "via_run": PRIOR_RUN}
    before = snapshot(ctx)
    stable = before == previous["production_snapshot_after"]
    last_formal, active = production_runs(github)
    if not stable or active:
        inherited["G02"]["status"] = "HOLD"
    return ("PASS" if stable and not active else "HOLD"), {"INPUT_LOCK.json": {"production": before,
        "last_formal_run": last_formal, "active_writers": active, "prior_master_matches": stable, "inherited": inherited,
        "prior_run": PRIOR_RUN, "prior_report_sha256": PRIOR_REPORT_SHA, "spec_file_id": fid,
        "inputs": inputs, "sop_sha256": SOP_SHA}}


def handover(ctx):
    inherited = ctx.prior("H_ACK.json")
    baseline = ctx.prior("AGENTS_TEST.json")
    require(inherited["status"] == "PASS" and inherited["mode"] == "INHERITED_READ_ONLY" and
            agents_test()["agents_sha256"] == baseline["agents_sha256"], "V24_H_INHERITED_ACK")
    proof = ctx.prior("FINAL_READINESS.json")
    require(proof["full_download_release"] == "HOLD" and proof["production_dispatchable"] == 0, "V24_H_RELEASE_BOUNDARY")
    return "PASS", {"HANDOVER_EVIDENCE.json": {"reader": "Codex/GitHub", "governance": "D-006", "job": "H",
                       "verified_previous_ack_file_sha256": digest(encode(inherited)), "agents": baseline,
                       "latest_spec_binding": ctx.spec_sha, "previous_run": PRIOR_RUN,
                       "no_new_chatgpt_read_ack_inferred": True, "backer_requests": 0}}


def contract_final(ctx):
    registry = ctx.read("OFFICIAL_DATASET_REVIEW.json")
    modes = ctx.read("OFFICIAL_QUERY_REVIEW.json")
    results = optional(ctx, "PROBE_RESULTS.json")
    sources = ctx.read("OFFICIAL_SOURCE_LOCK.json")
    by_dataset = {probe["dataset"]: probe for probe in results.get("probes", ())}
    for row in registry["rows"]:
        if row["dataset"] in by_dataset:
            row["probe_evidence"] = by_dataset[row["dataset"]]
    registry |= {"gate_contract_definition": "CONTRACT_READY_IS_SCHEMA_AND_METHOD_ONLY",
                 "admission_levels": ["CONTRACT_READY", "PARTITION_READY", "WINDOW_VERIFIED"],
                 "newly_certified_production_windows": 0}
    unknown = registry["counts"].get("HOLD", 0) or sources["status"] != "PASS"
    return ("HOLD" if unknown else "PASS"), {"DATASET_REGISTRY.json": registry, "QUERY_MODE_REGISTRY.json": modes}


def units():
    checks = {}
    for pattern in TEST_PATTERNS:
        suite = unittest.defaultTestLoader.discover("tests", pattern=pattern)
        tests = suite.countTestCases()
        minimum = MIN_V24_TESTS if pattern == TEST_PATTERNS[0] else 1
        require(tests >= minimum, "V24_REQUIRED_UNIT_TESTS_MISSING")
        stream = StringIO()
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
        print(f"{pattern}, {result.testsRun} tests, {len(result.failures)} failures, {len(result.errors)} errors")
        if not result.wasSuccessful() or result.skipped or result.testsRun != tests:
            sys.stderr.write(stream.getvalue())
            raise RehearsalError("V24_REQUIRED_UNIT_ASSERTION_FAILED")
        checks[pattern] = {"tests_run": result.testsRun, "expected": tests, "named_assertions": True, "passing": True}
    return {"test_count": sum(report["tests_run"] for report in checks.values()), "suites": checks}


def resume_job(ctx, job):
    if job not in {"G03_OFFICIAL", "G07_STAGE"}:
        return None
    source = re.fullmatch(r"(\d+)_(1|2|3|4)_([0-9a-f]{40})", os.environ.get("R12_V24_RESUME_FROM", ""))
    if source is None or not int(source[1]) < ctx.run:
        return None
    run, attempt, commit = int(source[1]), int(source[2]), source[3]
    prefix = "scripts/r11_backer/"
    relevant = ("v24_sources.py", "release_probes.py", "reconcile_catalog.py", "v2_registry.py") if job == "G03_OFFICIAL" else (
        "bulk_batch.py", "bulk_manifest.py", "production_download.py", "release_batch.py", "live_bronze_admission.py",
    )
    try:
        require(subprocess.run(["git", "diff", "--quiet", commit, ctx.commit, "--",
                                *(prefix + name for name in relevant)], check=False).returncode == 0, "V24_RESUMED_CODE_UNCHANGED")
        root = private_path(ctx.access, ctx.task_root, f"{run}_{attempt}")
        record = read_json_file(root, f"{job}.json", ctx.access)
        actual = next(row for row in GitHub().pages(f"/actions/runs/{run}/attempts/{attempt}/jobs", "jobs") if row["name"] == job)
        binding = ctx.binding() | {"run_id": run, "attempt": attempt, "commit": commit, "job": job, "status": "PASS"}
        require(all(record.get(key) == value for key, value in binding.items()) and actual["conclusion"] == "success" and
                actual["head_sha"] == commit and set(JOB_ARTIFACTS[job]) <= record["outputs"].keys(), "V24_RESUME_JOB_BINDING")
        output = {}
        for name, ref in record["outputs"].items():
            fid, original_bytes = original(ctx.access, root, name)
            require(fid == ref["file_id"] and digest(original_bytes) == ref["sha256"], "V24_RESUME_OUTPUT_DIGEST")
            output[name] = json.loads(original_bytes)
        if job == "G03_OFFICIAL":
            saved = output["OFFICIAL_SOURCE_LOCK.json"]["originals"]
            fid, raw = original(ctx.access, root, "OFFICIAL_SOURCE_ORIGINALS.zip")
            require(fid == saved["file_id"] and digest(raw) == saved["sha256"], "V24_RESUMED_OFFICIAL_ARCHIVE")
        else:
            checkpoint = output["G07_CHECKPOINT.json"]
            index, _, _ = v24_regression.fixture()
            require(checkpoint["origin_run"] == run and checkpoint["origin_attempt"] == attempt and
                    checkpoint["test_only"], "V24_NATIVE_FIXTURE_STAGE_BINDING")
            sandbox_root, _, _ = v24_regression.sandbox(ctx, False, run_root=root)
            fixture_manifest = read_json_file(sandbox_root, v24_regression.MANIFEST, ctx.access)
            require(fixture_manifest["index_sha256"] == digest(encode(index)), "V24_NATIVE_FIXTURE_CHANGED")
        output["RESUME_" + job + ".json"] = {"prior_run_id": run, "prior_attempt": attempt, "prior_commit": commit,
                                              "prior_job_id": actual["id"], "prior_run_folder": root,
                                              "verified_output_sha256": record["outputs"]}
        return "PASS", output
    except (OSError, RuntimeError, ValueError, TypeError, KeyError, StopIteration, subprocess.SubprocessError):
        return None


def stage_action(ctx, job):
    if job == "LOCK":
        return lock(ctx)
    if job == "H":
        return handover(ctx)
    if job == "G03_FINAL":
        return contract_final(ctx)
    if job == "G06":
        return v24_regression.engine(ctx, units())
    if resumed := resume_job(ctx, job):
        return resumed
    if job in PROBE_JOBS:
        require(ctx.read("LOCK.json")["status"] == "PASS", "V24_PROBE_INPUT_LOCK_HOLD")
    return ACTIONS[job](ctx)


def store_job(ctx, job):
    output = {}
    try:
        status, generated = stage_action(ctx, job)
        require(status in STATES and set(JOB_ARTIFACTS[job]) <= generated.keys(), "V24_EXPECTED_JOB_OUTPUTS")
        for name, value in generated.items():
            require(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*\.json", name) and name != f"{job}.json", "V24_OUTPUT_NAME")
            file_id = ctx.write(name, value)
            output[name] = {"file_id": file_id, "sha256": digest(encode(value))}
        failure = None
    except Exception as exc:
        status = "FAIL"
        failure = {"category": type(exc).__name__,
                   **({"code": str(exc)} if re.fullmatch(r"[A-Z][A-Z0-9_]{2,99}", str(exc)) else {})}
        if isinstance(exc, KeyError) and exc.args and isinstance(exc.args[0], str) and re.fullmatch(r"[a-zA-Z_0-9]{1,45}", exc.args[0]):
            failure["missing_field"] = exc.args[0]
    evidence = ctx.binding() | {"job": job, "status": status, "outputs": output, "error": failure}
    ctx.write(f"{job}.json", evidence)
    print(f"{job} {status}" + (f" {failure}" if failure else ""))


def recorded(ctx, job, platform):
    try:
        report = ctx.read(f"{job}.json")
        require(all(report.get(k) == v for k, v in ctx.binding().items()) and report["job"] == job and
                report["status"] in STATES, "V24_GATE_BINDING")
        if report["status"] == "PASS":
            require(set(JOB_ARTIFACTS[job]) <= report["outputs"].keys(), "V24_GATE_OUTPUTS")
        require(platform[job]["conclusion"] == "success", "V24_GITHUB_JOB_FAILED")
        for name, ref in report["outputs"].items():
            fid, raw = original(ctx.access, ctx.root, name)
            require(fid == ref["file_id"] and digest(raw) == ref["sha256"], "V24_OUTPUT_DIGEST")
        return report
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        return {"status": "NOT_RUN", "job": job, "reason": "MISSING_OR_UNVERIFIED_JOB_EVIDENCE",
                "error": {"category": type(exc).__name__}}


def optional(ctx, name):
    try:
        return ctx.read(name)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        return {}


def git_jobs(ctx):
    endpoint = f"/actions/runs/{ctx.run}/attempts/{ctx.attempt}/jobs"
    fields = ("id", "status", "conclusion", "html_url", "started_at", "completed_at")
    return {row["name"]: {k: row[k] for k in fields} for row in GitHub().pages(endpoint, "jobs")}


def minutes(job):
    first, last = job["started_at"], job["completed_at"]
    if not first or not last:
        return None
    return (datetime.fromisoformat(last) - datetime.fromisoformat(first)).total_seconds() / 60


def final(ctx):
    try:
        platform = git_jobs(ctx)
    except (OSError, RuntimeError, ValueError, TypeError, KeyError):
        platform = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        run_jobs = dict(zip(JOB_ARTIFACTS, pool.map(lambda job: recorded(ctx, job, platform), JOB_ARTIFACTS), strict=True))
    inputs = optional(ctx, "INPUT_LOCK.json")
    inherited = inputs.get("inherited", {})
    states = {"G00": run_jobs["LOCK"], "G01": run_jobs["H"], "G02": inherited.get("G02", {"status": "NOT_RUN"}),
              "G03": run_jobs["G03_FINAL"], "G04": run_jobs["G04_FINAL"], "G05": run_jobs["G05_C"],
              "G06": run_jobs["G06"], "G07": run_jobs["G07"]}
    try:
        end = snapshot(ctx)
        _, writers = production_runs(GitHub())
        unchanged = end == inputs["production"] and inputs["active_writers"] == writers == []
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        end, unchanged = None, False
    ledger = optional(ctx, "PROBE_LEDGER.json")
    try:
        durable = v24_account.usage(ctx)
        counters_equal = ledger == durable
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        durable, counters_equal = {}, False
    missing = [job for job, result in run_jobs.items() if result["status"] == "NOT_RUN"]
    levels_ok = all(run_jobs[job]["status"] == "PASS" for job in ("G06", "G07_STAGE", "G07"))
    states["G08"] = {"status": "PASS" if unchanged and not missing and counters_equal and "G08" in platform else "HOLD",
                     "production_snapshot_unchanged": unchanged, "job_evidence_bound": not missing and bool(platform),
                     "unverified_jobs": missing, "backer_counters_reconciled": counters_equal,
                     "previous_pass_has_independent_evidence": inherited.get("G08", {}).get("status") == "PASS"}
    index = optional(ctx, "MANIFEST_INDEX.json")
    resources = optional(ctx, "PARTITION_RESOURCE_CHECK.json")
    partition = resources | {"ready_for_partition_release": bool(
        resources.get("base_state") == "RESOURCES_EVIDENCED_AWAITING_ENGINE" and levels_ok and unchanged and
        states["G08"]["status"] == "PASS"), "engine_and_checkpoint_passed": levels_ok}
    partition["status"] = "READY_FOR_PARTITION_RELEASE" if partition["ready_for_partition_release"] else "PARTITION_HOLD"
    partition["new_production_windows_verified"] = 0
    part_file = ctx.write("PARTITION_READINESS.json", partition)
    gates = {key: value["status"] for key, value in states.items()}
    verdict = final_readiness_verdict(gates)
    if verdict["result"] == "READY_FOR_USER_RELEASE" and not (index.get("full_history_denominator") and levels_ok):
        verdict["result"] = "HOLD_WITH_EVIDENCE"
        verdict["full_history_release_gate"] = "FULL_UNIVERSE_ENGINE_OR_RESOURCES_UNPROVEN"
    final_json = ctx.binding() | verdict | {"revision": "V2.4", "gates": states, "workflow_job_evidence": run_jobs,
        "github_jobs": platform, "prior_inherited_gates": inherited, "checkpoint_reused": PRIOR_RUN,
        "production_snapshot_before": inputs.get("production"), "production_snapshot_after": end,
        "provider_usage": durable, "full_history_denominator": index.get("full_history_denominator"),
        "prepared_native_windows": index.get("prepared_windows"), "partition_readiness": partition,
        "partition_report_id": part_file, "runner_minutes_observed_by_job": {job: minutes(row) for job, row in platform.items()}}
    published = ctx.write("FINAL_READINESS.json", final_json)
    ctx.write("G08.json", ctx.binding() | states["G08"])
    rows = [f"| {gate} | {status} |" for gate, status in sorted(gates.items())]
    text = ["# R12.1 V2.4 全量與 Bronze 前置驗收", "", f"**{verdict['result']}**", "",
            f"{TASK_ID} · `{ctx.spec_sha}`", "", f"Commit `{ctx.commit}`，Run `{ctx.run}`，Attempt `{ctx.attempt}`。",
            "", "| 關卡 | 結果 |", "|---|---|", *rows, "",
            f"新版可評估歷史窗口 {index.get('prepared_windows', '待核對')}。分區狀態 `{partition['status']}`。",
            f"全歷史分母 {index.get('full_history_denominator') or '待核證'}。", "",
            f"本任務累計資料 API {durable.get('actual_data_requests', '待核證')} 次，帳號 API "
            f"{durable.get('actual_account_requests', '待核證')} 次。", "",
            "正式放行 `HOLD`，正式派發 `0`。", ""]
    write_markdown(ctx, "FINAL_READINESS.md", "\n".join(text))
    fid, raw = original(ctx.access, ctx.root, "FINAL_READINESS.json")
    require(fid == published, "V24_FINAL_READBACK_ID")
    event = ctx.binding() | verdict | {"writer": "GitHub Actions", "event_type": "READINESS_HANDOVER",
        "decision_id": "D-006/R12-V2.4-BRONZE", "governance": "JARVIS-GOV-HANDOVER-B-001", "classification": "EVIDENCE",
        "recorded_at": platform.get("G08", {}).get("started_at"), "report_file_id": fid, "report_sha256": sha256(raw).hexdigest(),
        "gates": gates, "partition_status": partition["status"], "prior_run": PRIOR_RUN}
    suffix = f"{TASK_ID}_V2_4_{ctx.run}_{ctx.attempt}.json"
    write_json(ctx.shared, "HANDOVER_" + suffix, event, ctx.access)
    write_json(ctx.shared, "TASK_EVENT_" + suffix, event | {"event_type": "READINESS_TASK_RESULT"}, ctx.access)
    write_json(ctx.shared, "DECISION_EVENT_" + suffix, event | {"event_type": "APPROVED_STORAGE_RULE_EXECUTION",
        "authority": {"file_id": SPEC_ID, "sha256": ctx.spec_sha}, "scope": {
            "retained_original_markets": ["TWSE", "TPEx", "Emerging"], "research_venues": ["TWSE", "TPEx"],
            "previous_mixed_original_restriction_replaced": True, "production_release_still_required": True}}, ctx.access)
    print(f"{verdict['result']} gates={verdict['passed_gates']}/{verdict['expected_gates']} production_dispatchable=0")


def run_job(job):
    try:
        ctx = BulkContext(job)
        final(ctx) if job == "G08" else store_job(ctx, job)
    except Exception as exc:
        category = type(exc).__name__
        label = str(exc) if re.fullmatch(r"[A-Z][A-Z0-9_]{2,99}", str(exc)) else category
        print(f"{job} INFRASTRUCTURE_ERROR {label}")
        raise SystemExit(1) from None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=(*JOB_ARTIFACTS, "G08"))
    run_job(parser.parse_args().job)


if __name__ == "__main__":
    main()
