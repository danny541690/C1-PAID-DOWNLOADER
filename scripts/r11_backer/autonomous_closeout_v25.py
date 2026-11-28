"""Run and reconcile the V2.5 D/H preflight under the existing task identity."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import StringIO
import json
import os
import re
import sys
import unittest

from . import v25_account, v25_contracts, v25_manifest, v25_regression, v25_resources
from .drive_pilot import ROOT, ensure, read_drive_bytes, token
from .final_readiness_v2 import Context, SOP, SOP_SHA, SPEC_SHA as V20_SHA, TASK_ID, original, private_path, snapshot, write_markdown
from .incremental_closeout_v21 import production_runs
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import write_json
from .production_preflight import final_readiness_verdict
from .release_rehearsal import require
from .test_archive import GitHub
from .v2_handover import agents_test

SPEC_SHA = "1ebae4aa6c81edb3ac45ff65181065195ad3e2d99666f63f8588ae4dda2defa2"
SPEC = "R12_1_V25_AUTONOMOUS_QUOTA_AND_MANIFEST_CLOSEOUT_20261128.md"
SPEC_FILES = {
    SPEC: ("19eZ9eqTx7OFZ1n_5aJ_DvVV6DSMx9-Gb", SPEC_SHA),
    "R12_V25_quota_reference.py": ("1Ye7QB7AOHaZy2fDu5N5cs4Jw2klKNfv4", "c858f44180b2a26334e1a7a503d068f8016e386331efbb7b5864f2461ad100d2"),
    "R12_V25_quota_reference_test.txt": ("1596UufsB4L86hmQgE6dNdBFLo7Qm9LVI", "c1fa35ccb20f144cafd5a3e890a0faa4b48060e702c38c079445d3f50f282c5a"),
}
V24_SHA = "197052e312c1cb41767d94c657eac4ca1196448b5b9610fdab099f4a22c6fd94"
PRIOR_RUN = 46087858930
PRIOR_COMMIT = "1781922334a8661915dbf6bcdf22610c0ea3fb6f"
PRIOR_SHA = "e41fa62c29338898e54e01f37891611972f190f8e7e2c1c853c0ec54360b28f9"
LEGACY_EVIDENCE = "d07dc296831e151607d18850d66df5a7c09adf8cd146b6c642b038ef3e2dc218"
V20_REPORT = "049a250350455e819c3159731447f2f6e40548d94f664b69b9499fc9b0806611"
PRESERVED = {"G00": "LOCK", "G01": "H", "G06": "G06", "G07": "G07", "G08": "G08"}
ARTIFACTS = {"LOCK": ("INPUT_LOCK.json",), "H": ("HANDOVER_EVIDENCE.json", "V25_DECISION.json"),
             "G05_A": ("QUOTA_POLICY.json", "CONSUMER_INVENTORY.json", "QUOTA_MIGRATION.json"),
             "G03_OFFICIAL": ("CONTRACT_REVIEW.json", "PROBE_PLAN.json"),
             "G04_OFFLINE": ("HISTORY_AXES_INPUT.json", "ACTIVE_BATCH_RECONCILIATION.json", "B09_QUEUE.json", "BRONZE_STORAGE_RULE.json"),
             "G05_B": ("PROBE_RESULTS.json", "PROBE_LEDGER.json", "10MIN_REBALANCE.json"),
             "G03_FINAL": ("DATASET_REGISTRY.json", "CONTRACT_GAP_MATRIX.json"),
             "G04_FINAL": ("MANIFEST_INDEX.json", "HISTORICAL_UNIVERSE.json", "REUSE_MAP.json", "COVERAGE_GAPS.json", "QUERY_MODE_BENCHMARK.json"),
             "G05_C": ("RESOURCE_BUDGET.json",), "G06": ("ENGINE_TESTS.json",),
             "G07_STAGE": ("G07_QUOTA_CHECKPOINT.json",), "G07": ("INTEGRATION_REPORT.json",)}
RUNNERS = {"G05_A": v25_account.setup, "G03_OFFICIAL": v25_contracts.offline, "G04_OFFLINE": v25_manifest.offline,
           "G05_B": v25_account.probe, "G03_FINAL": v25_contracts.final, "G04_FINAL": v25_manifest.final, "G05_C": v25_resources.run,
           "G07_STAGE": v25_regression.stage, "G07": v25_regression.resume}


class V25Context(Context):
    def __init__(self, job):
        require(os.environ.get("R12_TASK_ID") == TASK_ID and os.environ.get("R12_SPEC_SHA") == SPEC_SHA and
                os.environ.get("GITHUB_JOB") == job, "V25_EXECUTION_BINDING")
        require(job == "G05_B" or "FINMIND_BACKER_TOKEN" not in os.environ, "V25_BACKER_TOKEN_ISOLATION")
        self.task_id, self.spec_sha = TASK_ID, SPEC_SHA
        self.run, self.attempt, self.commit = int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"]), os.environ["GITHUB_SHA"]
        self.access = token()
        self.shared = private_path(self.access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT")
        self.task_base = private_path(self.access, self.shared, "FINAL_READINESS", TASK_ID)
        self.prior_root = private_path(self.access, self.task_base, V24_SHA, f"{PRIOR_RUN}_1")
        self.legacy_root = private_path(self.access, self.task_base, v25_account.EARLY_JOURNAL_SPEC, "46074623286_1")
        self.v20_root = private_path(self.access, self.task_base, V20_SHA, "46030259242_1")
        self.task_root = private_path(self.access, self.task_base, SPEC_SHA, create=job == "LOCK")
        self.root = private_path(self.access, self.task_root, f"{self.run}_{self.attempt}", create=job == "LOCK")
        self._production = None
        self._prior_files = {}

    def prior(self, name):
        if name not in self._prior_files:
            _, raw = original(self.access, self.prior_root, name)
            body = json.loads(raw)
            if name == "FINAL_READINESS.json":
                require(digest(raw) == PRIOR_SHA, "V25_PREVIOUS_RESULT_SHA")
            else:
                report = self.prior("FINAL_READINESS.json")
                expected = {job["outputs"][name]["sha256"] for job in report["workflow_job_evidence"].values()
                            if name in job.get("outputs", {})}
                require(not expected or expected == {digest(encode(body))}, "V25_PREVIOUS_ARTIFACT_SHA")
            self._prior_files[name] = body
        return self._prior_files[name]

    def legacy(self, name):
        _, raw = original(self.access, self.legacy_root, name)
        require(name == "CONTRACT_EVIDENCE.json" and digest(raw) == LEGACY_EVIDENCE, "V25_LEGACY_EVIDENCE_SHA")
        return json.loads(raw)

    def v20(self, name):
        _, raw = original(self.access, self.v20_root, name)
        if name == "FINAL_READINESS.json":
            require(digest(raw) == V20_REPORT, "V25_ORIGINAL_G02_SHA")
        return json.loads(raw)

    def binary(self, name, raw, media):
        require(re.fullmatch(r"PROBE_[0-9a-f]{64}\.json\.gz", name), "V25_PROBE_RAW_FILENAME")
        key = ensure(self.root, name, media, self.access, raw, create=True)
        verify_private_folder(key, self.access)
        require(read_drive_bytes(key, self.access, len(raw)) == raw, "V25_PROBE_ORIGINAL_READBACK")
        return key


def lock(ctx):
    bindings = {}
    for name, (identity, sha) in SPEC_FILES.items():
        fid, raw = original(ctx.access, ctx.shared, name)
        require(fid == identity and digest(raw) == sha, "V25_APPROVED_SOURCE_SHA")
        bindings[name] = {"file_id": fid, "sha256": sha}
    root = private_path(ctx.access, ROOT, "00_COMMAND", "AI_SHARED_CONTEXT")
    _, global_sop = original(ctx.access, root, SOP)
    _, local_sop = original(ctx.access, ctx.shared, SOP)
    start_id, start = original(ctx.access, ctx.shared, "START_HERE.md")
    require(local_sop == global_sop and digest(local_sop) == SOP_SHA and start, "V25_D006_SOP")
    github = GitHub()
    actual = github.json(f"/actions/runs/{PRIOR_RUN}/attempts/1")
    prior = ctx.prior("FINAL_READINESS.json")
    require(actual["conclusion"] == "success" and actual["head_sha"] == prior["commit"] == PRIOR_COMMIT and
            prior["spec_sha256"] == V24_SHA and prior["task_id"] == ctx.task_id and prior["run_id"] == PRIOR_RUN,
            "V25_PREVIOUS_RUN_BINDING")
    jobs = {job["name"]: job for job in github.pages(f"/actions/runs/{PRIOR_RUN}/attempts/1/jobs", "jobs")}
    inheritance = {}
    for gate, name in PRESERVED.items():
        require(prior["gates"][gate]["status"] == "PASS" and prior["github_jobs"][name]["id"] == jobs[name]["id"] and
                jobs[name]["conclusion"] == "success", "V25_PRIOR_GATE_PROOF")
        inheritance[gate] = {"status": "PASS", "run_id": PRIOR_RUN, "job_id": jobs[name]["id"], "attempt": 1,
                              "sha256": digest(encode(prior["gates"][gate]))}
    recovered = prior["gates"]["G02"]
    require(recovered["status"] == "PASS" and recovered["run_id"] == 46030259242 and
            recovered["spec_sha256"] == V20_SHA and
            recovered["report_sha256"] == digest(encode(ctx.v20("FINAL_READINESS.json")["gates"]["G02"])),
            "V25_V20_G02_CHAIN")
    inheritance["G02"] = recovered
    before = snapshot(ctx)
    newest, busy = production_runs(github)
    stable = before == prior["production_snapshot_after"] and not busy
    return ("PASS" if stable else "HOLD"), {"INPUT_LOCK.json": {
        "inputs": bindings, "start_here": {"file_id": start_id, "sha256": digest(start)}, "sop_sha256": SOP_SHA,
        "baseline": {"run_id": PRIOR_RUN, "commit": PRIOR_COMMIT, "report_sha256": PRIOR_SHA},
        "inherited": inheritance, "production": before, "last_production_run": newest, "active_writers": busy,
        "matches_latest_previous_production_snapshot": stable}}


def handover(ctx):
    previous = ctx.prior("HANDOVER_EVIDENCE.json")
    require(previous["agents"]["agents_sha256"] == agents_test()["agents_sha256"], "V25_H_AGENTS_UNCHANGED")
    assert ctx.spec_sha == SPEC_SHA
    info = {"decision_id": "D-006/R12-V2.5-QUOTA", "membership_horizon_user_estimate_days": 21,
            "account_upper_requests_per_rolling_hour": 1600, "official_quota_snapshot_optional": True,
            "manual_account_exclusivity_evidence_required": False, "production_dispatchable": 0,
            "full_download_release": "HOLD", "retains_v24_full_native_bronze_path": True}
    return "PASS", {"HANDOVER_EVIDENCE.json": {"previous_handover_sha256": digest(encode(previous)),
        "previous_ack_status_preserved": True, "current_spec_sha256": SPEC_SHA, "H_finmind_calls": 0}, "V25_DECISION.json": info}


def units():
    suite = unittest.defaultTestLoader.discover("tests", pattern="test_r12_v25_quota.py")
    expected = suite.countTestCases()
    require(expected >= 11, "V25_MISSING_REQUIRED_ASSERTIONS")
    stream = StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    if not result.wasSuccessful():
        sys.stderr.write(stream.getvalue())
    require(result.wasSuccessful() and result.testsRun == expected and not result.skipped, "V25_REGRESSION_ASSERTION_FAILED")
    print(f"V2.5 affected unit tests={result.testsRun}, failures={len(result.failures)}, errors={len(result.errors)}")
    return {"tests": result.testsRun, "failures": 0, "errors": 0, "verified_named_cases": True}


def execute(ctx, job):
    if job == "LOCK":
        return lock(ctx)
    if job == "H":
        return handover(ctx)
    if job in {"G05_A", "G05_B"}:
        require(ctx.read("LOCK.json")["status"] == "PASS", "V25_PAID_INPUT_NOT_LOCKED")
    if job == "G06":
        return v25_regression.engine(ctx, units())
    return RUNNERS[job](ctx)


def record(ctx, job):
    outputs = {}
    try:
        status, extra = execute(ctx, job)
        require(set(ARTIFACTS[job]) <= extra.keys(), "V25_REQUIRED_GATE_ARTIFACTS")
        for name, value in extra.items():
            require(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*\.json", name) and name != f"{job}.json", "V25_INVALID_REPORT_NAME")
            key = ctx.write(name, value)
            outputs[name] = {"file_id": key, "sha256": digest(encode(value))}
        failure = None
    except Exception as error:
        status = "FAIL"
        failure = {"category": type(error).__name__}
        if re.fullmatch(r"[A-Z][A-Z0-9_]{2,99}", str(error)):
            failure["code"] = str(error)
        if isinstance(error, KeyError) and error.args and isinstance(error.args[0], str) and re.fullmatch(r"[a-zA-Z_0-9]{1,45}", error.args[0]):
            failure["missing_field"] = error.args[0]
    ctx.write(f"{job}.json", ctx.binding() | {"job": job, "status": status, "outputs": outputs, "error": failure})
    print(f"{job} {status}" + (f" {failure}" if failure else ""))


def recorded(ctx, job, platform):
    try:
        report = ctx.read(f"{job}.json")
        require(report["job"] == job and all(report.get(k) == v for k, v in ctx.binding().items()) and
                platform[job]["conclusion"] == "success", "V25_JOB_BINDING")
        for name, witness in report["outputs"].items():
            fid, raw = original(ctx.access, ctx.root, name)
            require(fid == witness["file_id"] and digest(raw) == witness["sha256"], "V25_OUTPUT_FILE_SHA")
        return report
    except (OSError, RuntimeError, ValueError, TypeError, KeyError):
        return {"job": job, "status": "NOT_RUN", "reason": "MISSING_OR_UNVERIFIED_RESULT"}


def safe(ctx, name):
    try:
        return ctx.read(name)
    except (OSError, RuntimeError, ValueError, TypeError, KeyError):
        return {}


def conclude(ctx):
    fields = ("id", "status", "conclusion", "html_url", "started_at", "completed_at")
    github = GitHub()
    jobs = {j["name"]: {k:j[k] for k in fields} for j in github.pages(f"/actions/runs/{ctx.run}/attempts/{ctx.attempt}/jobs", "jobs")}
    with ThreadPoolExecutor(max_workers=5) as pool:
        stage_jobs = dict(zip(ARTIFACTS, pool.map(lambda name: recorded(ctx, name, jobs), ARTIFACTS), strict=True))
    initial = safe(ctx, "INPUT_LOCK.json")
    _, active = production_runs(github)
    end = snapshot(ctx)
    stable = initial.get("production") == end and not (initial.get("active_writers") or active)
    try:
        ledger = v25_account.usage(ctx)
        quota_matches = safe(ctx, "PROBE_LEDGER.json") == ledger
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        ledger, quota_matches = {}, False
    states = {"G00": stage_jobs["LOCK"], "G01": stage_jobs["H"],
              "G02": initial.get("inherited", {}).get("G02", {"status": "NOT_RUN"}) if stable else {"status": "HOLD"},
              "G03": stage_jobs["G03_FINAL"], "G04": stage_jobs["G04_FINAL"], "G05": stage_jobs["G05_C"],
              "G06": stage_jobs["G06"], "G07": stage_jobs["G07"]}
    missing = [job for job, report in stage_jobs.items() if report["status"] == "NOT_RUN"]
    states["G08"] = {"status": "PASS" if quota_matches and stable and not missing and "G08" in jobs else "HOLD",
                     "formal_master_snapshot_unchanged": stable, "quota_counts_match_durable_ledger": quota_matches,
                     "unverified_jobs": missing, "full_download_release": "HOLD", "production_dispatchable": 0}
    data = safe(ctx, "MANIFEST_INDEX.json")
    reg = safe(ctx, "DATASET_REGISTRY.json")
    resources = safe(ctx, "RESOURCE_BUDGET.json")
    ready_partition = bool(resources.get("resource_ready_for_bounded_review") and stable and
                           all(states[key]["status"] == "PASS" for key in ("G06", "G07", "G08")))
    partition = {"status": "READY_FOR_PARTITION_RELEASE" if ready_partition else "PARTITION_HOLD",
                 "scope": "V25_STRICT_NATIVE_DISPATCH_SHARDS_ONLY", "windows": data.get("prepared_windows"),
                 "actual_release_authorized": False, "all_history_previews_excluded_from_partition_release": True}
    ctx.write("PARTITION_READINESS.json", partition)
    ctx.write("QUOTA_JOURNAL.json", ledger)
    ctx.write("G08.json", ctx.binding() | states["G08"])
    verdict = final_readiness_verdict({key: value["status"] for key, value in states.items()})
    if verdict["result"] == "READY_FOR_USER_RELEASE" and not data.get("full_history_denominator"):
        verdict["result"] = "HOLD_WITH_EVIDENCE"
    runner = {key: (datetime.fromisoformat(row["completed_at"]) - datetime.fromisoformat(row["started_at"])).total_seconds()
              for key, row in jobs.items() if row["completed_at"] and row["started_at"]}
    payload = ctx.binding() | verdict | {"revision": "V2.5", "gates": states, "workflow_job_evidence": stage_jobs,
        "github_jobs": jobs, "previous_valid_run": PRIOR_RUN, "previous_inherited_gates": initial.get("inherited"),
        "production_snapshot_before": initial.get("production"), "production_snapshot_after": end, "provider_usage": ledger,
        "full_history_denominator": data.get("full_history_denominator"), "prepared_native_windows": data.get("prepared_windows"),
        "planned_scope_datasets": data.get("planned_scope_datasets"), "planned_scope_http_requests": data.get("planned_scope_http_requests"),
        "contract_counts": reg.get("counts"), "partition_readiness": partition, "actual_runner_seconds": runner,
        "hold_reasons": resources.get("blockers", [])}
    fid = ctx.write("FINAL_READINESS.json", payload)
    rows = [f"| {gate} | {result['status']} |" for gate, result in sorted(states.items())]
    text = ["# R12.1 V2.5 自主額度與全歷史清冊驗收", "", f"**{verdict['result']}**", "",
            f"{TASK_ID} · `{SPEC_SHA}`", "", f"Commit `{ctx.commit}`，Run `{ctx.run}`，Attempt `{ctx.attempt}`。", "",
            "| 關卡 | 結果 |", "|---|---|", *rows, "",
            f"歷史分母 {data.get('full_history_denominator') or '待核證'}。有證據的規劃資料集 {data.get('planned_scope_datasets', '待核對')}。",
            f"前置資料請求 {ledger.get('actual_data_requests', '待核對')} 次，帳號查詢 {ledger.get('actual_account_requests', '待核對')} 次。",
            f"分區 `{partition['status']}`。正式放行 `HOLD`，正式派發 `0`。", ""]
    write_markdown(ctx, "FINAL_READINESS.md", "\n".join(text))
    report_id, raw = original(ctx.access, ctx.root, "FINAL_READINESS.json")
    require(report_id == fid, "V25_FINAL_JSON_READBACK")
    suffix = f"{TASK_ID}_V2_5_{ctx.run}_{ctx.attempt}.json"
    event = ctx.binding() | verdict | {"decision_id": "D-006/R12-V2.5-QUOTA", "classification": "EVIDENCE",
        "writer": "GitHub Actions", "report_file_id": fid, "report_sha256": digest(raw), "partition_status": partition["status"],
        "recorded_at": jobs.get("G08", {}).get("started_at"), "official_user_info_need_not_be_available": True,
        "user_confirmed_planning_horizon_days": 21}
    for prefix, operation in (("HANDOVER", "READINESS_HANDOVER"), ("TASK_EVENT", "READINESS_TASK_RESULT"),
                              ("DECISION_EVENT", "V25_AUTONOMOUS_QUOTA_GOVERNANCE")):
        write_json(ctx.shared, prefix + "_" + suffix, event | {"event_type": operation}, ctx.access)
    print(f"{verdict['result']} gates={verdict['passed_gates']}/{verdict['expected_gates']} production_dispatchable=0")


def run_job(job):
    try:
        ctx = V25Context(job)
        conclude(ctx) if job == "G08" else record(ctx, job)
    except Exception as error:
        label = str(error) if re.fullmatch(r"[A-Z][A-Z0-9_]{2,99}", str(error)) else type(error).__name__
        print(f"{job} INFRASTRUCTURE_ERROR {label}")
        raise SystemExit(1) from None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=(*ARTIFACTS, "G08"))
    run_job(parser.parse_args().job)


if __name__ == "__main__":
    main()
