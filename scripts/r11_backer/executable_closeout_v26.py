"""Single D-006 incremental schedule for executable R12.1 readiness."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import os
import re
import sys
import unittest

from . import executable_manifest_v26 as manifest, v25_account, v26_boundaries, v26_capacity, v26_regression
from .bulk_manifest import SNAPSHOT_ZONE
from .drive_pilot import ROOT, ensure, read_drive_bytes, token
from .final_readiness_v2 import Context, SOP, SOP_SHA, TASK_ID, original, private_path, snapshot, write_markdown
from .incremental_closeout_v21 import production_runs
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import write_json
from .production_preflight import final_readiness_verdict
from .release_rehearsal import require
from .test_archive import GitHub
from .v2_recovery import metadata
from .v24_coverage import active_snapshot

SPEC = "R12_1_V26_EXECUTABLE_MANIFEST_RESOURCE_CLOSEOUT_20261128.md"
SPEC_FILE = "1IHNobAR5r6FAzfekujZtKqazHG4k8Ucq"
SPEC_SHA = "861ddcd825818f2bdd36d22d5a45a7428b6ad787fbdc1ba7179bf85a4071a327"
V25_SPEC = "1ebae4aa6c81edb3ac45ff65181065195ad3e2d99666f63f8588ae4dda2defa2"
V25_RUN = 46099758774
V25_COMMIT = "33a73e0673761ea2d91b748298b6b980f7ff3534"
V25_REPORT = "c743bac5365c97da1c6ae0aed4478378098b65ac07ba3f4547c5a78264772618"
V25_EXTRA = {"ERROR_REPAIR_HISTORY.json": "fe07fb53c93a017f88eac228e570050e921797277564ba8b343caf39dbc93f9e",
             "RESOURCE_BUDGET_RUN_EVIDENCE.json": "584a8aa1afffe8b29b55150367a4375b78c2007e3c32c534d75f098c81b8641f"}
REQUIRED = {
    "LOCK": {"LOCK_INPUT.json", "BASELINE_PROOFS.json", "ACTIVE_BATCH_RECONCILIATION.json"},
    "H": {"HANDOVER_EVIDENCE.json", "V26_DECISION.json"},
    "D1": {"BOUNDARY_REGISTRY.json", "OFFICIAL_SOURCE_LOCK.json", "HISTORICAL_CALENDAR.json"},
    "D2": {manifest.INPUTS, "COMPILER_BASELINE.json"},
    "D3": {"CAPACITY_POLICY.json"},
    "G04_FINAL": {manifest.INDEX, manifest.SHARD, "QUERY_MODE_BENCHMARK.json", "REUSE_MAP.json", "COVERAGE_GAPS.json", "B09_QUEUE.json"},
    "G05_FINAL": {"RESOURCE_ESTIMATE.json"},
    "G06": {"ENGINE_TESTS.json", "CAPACITY_STAGE_CHECKPOINT.json"},
    "G07": {"CAPACITY_GUARD_TEST.json", "CONTROLLER_RECOVERY.json"},
}


class V26Context(Context):
    def __init__(self, job):
        require(os.environ.get("R12_TASK_ID") == TASK_ID and os.environ.get("R12_SPEC_SHA") == SPEC_SHA and
                job == os.environ.get("GITHUB_JOB") and "FINMIND_BACKER_TOKEN" not in os.environ, "V26_JOB_SCOPE")
        self.task_id, self.spec_sha = TASK_ID, SPEC_SHA
        self.run, self.attempt, self.commit = int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"]), os.environ["GITHUB_SHA"]
        self.access = token()
        self.shared = private_path(self.access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT")
        self.task_base = private_path(self.access, self.shared, "FINAL_READINESS", TASK_ID)
        self.prior_root = private_path(self.access, self.task_base, V25_SPEC, f"{V25_RUN}_1")
        self.task_root = private_path(self.access, self.task_base, SPEC_SHA, create=job == "LOCK")
        self.root = private_path(self.access, self.task_root, f"{self.run}_{self.attempt}", create=job == "LOCK")
        self._files, self._production = {}, None

    def previous(self, name):
        if name not in self._files:
            fid, raw = original(self.access, self.prior_root, name)
            sha = digest(raw)
            if name == "FINAL_READINESS.json":
                require(sha == V25_REPORT, "V26_V25_REPORT_SHA")
            elif name in V25_EXTRA:
                require(sha == V25_EXTRA[name], "V26_V25_SUPPLEMENT_SHA")
            else:
                refs = [gate["outputs"][name] for gate in self.previous("FINAL_READINESS.json")["workflow_job_evidence"].values()
                        if name in gate.get("outputs", {})]
                require(refs and all(ref["sha256"] == sha and ref["file_id"] == fid for ref in refs), "V26_PREDECESSOR_ARTIFACT")
            self._files[name] = json.loads(raw)
        return self._files[name]

    prior = previous

    def binary(self, name, raw, media):
        require(name == v26_boundaries.ORIGINALS and media == "application/zip", "V26_ARCHIVE_NAME")
        file_id = ensure(self.root, name, media, self.access, raw, create=True)
        verify_private_folder(file_id, self.access)
        require(read_drive_bytes(file_id, self.access, len(raw)) == raw, "V26_SOURCE_ORIGINAL_READBACK")
        return file_id


def lock(ctx):
    file_id, raw = original(ctx.access, ctx.shared, SPEC)
    require(file_id == SPEC_FILE and digest(raw) == SPEC_SHA, "V26_APPROVED_SPEC")
    sop_id, sop = original(ctx.access, ctx.shared, SOP)
    global_root = private_path(ctx.access, ROOT, "00_COMMAND", "AI_SHARED_CONTEXT")
    global_id, global_sop = original(ctx.access, global_root, SOP)
    start_id, start = original(ctx.access, ctx.shared, "START_HERE.md")
    require(digest(sop) == SOP_SHA and sop == global_sop and start, "V26_PLAN_B_GOVERNANCE")
    before = ctx.previous("FINAL_READINESS.json")
    git = GitHub()
    actual = git.json(f"/actions/runs/{V25_RUN}/attempts/1")
    require(actual["conclusion"] == "success" and actual["head_sha"] == before["commit"] == V25_COMMIT and
            before["spec_sha256"] == V25_SPEC and before["run_id"] == V25_RUN, "V26_PREVIOUS_RUN")
    prior_jobs = {row["name"]: row for row in git.pages(f"/actions/runs/{V25_RUN}/attempts/1/jobs", "jobs")}
    require(len(prior_jobs) == 13 and all(row["conclusion"] == "success" for row in prior_jobs.values()), "V26_PREVIOUS_JOB_EVIDENCE")
    inherited = {gate: before["gates"][gate] for gate in ("G00", "G01", "G02", "G03", "G06", "G07", "G08")}
    require(all(gate["status"] == "PASS" for gate in inherited.values()), "V26_PREVIOUS_GATE_PROOF")
    for name in ("HISTORICAL_UNIVERSE.json", "RESOURCE_BUDGET.json", "RESOURCE_BUDGET_RUN_EVIDENCE.json", "ERROR_REPAIR_HISTORY.json",
                 "DATASET_REGISTRY.json", "MANIFEST_INDEX.json", "ENGINE_TESTS.json", "INTEGRATION_REPORT.json"):
        ctx.previous(name)
    usage = v25_account.usage(ctx)
    prior_usage = ctx.previous("PROBE_LEDGER.json")
    caps = usage["task_limits"]
    require(caps == {"account": 4, "data": 48} and
            prior_usage["reserved_data_requests"] <= usage["reserved_data_requests"] <= caps["data"] and
            prior_usage["reserved_account_requests"] <= usage["reserved_account_requests"] <= caps["account"],
            "V26_TASK_CUMULATIVE_QUOTA")
    probe = ctx.previous("PROBE_RESULTS.json")
    prior_g05 = {name: before["workflow_job_evidence"][name]["status"] for name in ("G05_A", "G05_B")}
    require(prior_g05 == {"G05_A": "PASS", "G05_B": "PASS"} and probe["verified_backer_entitlement_response"],
            "V26_BACKER_ENTITLEMENT_EVIDENCE")
    current = snapshot(ctx)
    latest, writers = production_runs(git)
    active = active_snapshot(ctx)
    baseline_active = ctx.previous("ACTIVE_BATCH_RECONCILIATION.json")
    bronze_id = ctx.previous("BRONZE_STORAGE_RULE.json")["folder_id"]
    bronze = metadata(bronze_id, ctx.access)
    require(bronze and bronze["name"] == "20_BRONZE" and bronze["shared"] is False, "V26_BRONZE_ROOT")
    catalog = {row["id"]: row["state"] for row in git.pages("/actions/workflows", "workflows")}
    known = ctx.previous("CONSUMER_INVENTORY.json")["legacy_workflows"]
    controlled = len(known) == 14 and all(catalog.get(row["id"]) in {"disabled_manually", "disabled_inactivity"} for row in known)
    stable = current == before["production_snapshot_after"] and active == baseline_active and not writers
    run = git.json(f"/actions/runs/{ctx.run}")
    require(run["head_sha"] == ctx.commit and run["id"] == ctx.run, "V26_RUN_COMMIT")
    stamp = datetime.fromisoformat(run["created_at"]).astimezone(SNAPSHOT_ZONE).isoformat()
    result = {"spec": {"file_id": file_id, "sha256": SPEC_SHA}, "sop": {"project": sop_id, "global": global_id, "sha256": SOP_SHA},
              "start_here": {"file_id": start_id, "sha256": digest(start)}, "as_of_taipei": stamp,
              "production": current, "latest_production_run": latest, "active_writers": writers, "production_matches_baseline": stable,
              "bronze_identity": bronze, "quota": usage, "backer_entitlement_verified": probe["verified_backer_entitlement_response"],
              "shared_quota_consumer_isolation": controlled, "known_legacy_consumers": len(known)}
    return ("PASS" if stable and controlled and not usage["active_writer"] else "HOLD"), {
        "LOCK_INPUT.json": result, "ACTIVE_BATCH_RECONCILIATION.json": active,
        "BASELINE_PROOFS.json": {"run": V25_RUN, "commit": V25_COMMIT, "attempt": 1, "report_sha256": V25_REPORT,
            "inherited_gates": inherited, "jobs": {name: row["id"] for name, row in sorted(prior_jobs.items())},
            "new_broker_identity": False, "data_api_calls_this_read": 0}}


def handover(ctx):
    prior = ctx.previous("HANDOVER_EVIDENCE.json")
    return "PASS", {"HANDOVER_EVIDENCE.json": {"prior_run": V25_RUN, "prior_sha256": digest(encode(prior)),
                                               "H_finmind_calls": 0, "previous_ack_preserved": True},
                    "V26_DECISION.json": {"decision": "D-006/R12-V2.6-EXECUTABLE-MANIFEST",
                        "full_history_completion_as_engineering_gate": False, "membership_expiry_as_gate": False,
                        "capacity_and_schedule_method": "ROUGH_ESTIMATE_AND_ACTUAL_SPACE_GUARD",
                        "full_download_release": "HOLD", "production_dispatchable": 0}}


def generator_inputs(ctx):
    axes = manifest.build_inputs(ctx)
    previous = ctx.previous("MANIFEST_INDEX.json")
    return "PASS", {manifest.INPUTS: axes,
        "COMPILER_BASELINE.json": {"previous_report_sha256": V25_REPORT,
            "previous_http_estimate": previous["planned_scope_http_requests"], "previous_certified_windows": len(axes["certified_windows"]),
            "issuer_episodes": len(axes["issuer_intervals"]), "input_sha256": digest(encode(axes))}}


def run_tests():
    suite = unittest.defaultTestLoader.discover("tests", pattern="test_r12_v26_closeout.py")
    count = suite.countTestCases()
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=1).run(suite)
    require(count >= 6 and result.testsRun == count and result.wasSuccessful() and not result.skipped, "V26_AFFECTED_TESTS")
    return {"tests": result.testsRun, "errors": 0, "failures": 0, "skipped": 0}


def execute(ctx, job):
    if job == "G06":
        return v26_regression.engine(ctx, run_tests())
    return {"LOCK": lock, "H": handover, "D1": v26_boundaries.run, "D2": generator_inputs, "D3": v26_capacity.prep,
            "G04_FINAL": manifest.compile_plans, "G05_FINAL": v26_capacity.estimate, "G07": v26_regression.resume}[job](ctx)


def label(error):
    result = {"category": type(error).__name__}
    text = str(error)
    if re.fullmatch(r"[A-Z][A-Z0-9_]{2,99}", text):
        result["code"] = text
    if isinstance(error, KeyError) and error.args and re.fullmatch(r"[a-zA-Z_0-9]{1,55}", str(error.args[0])):
        result["missing_field"] = str(error.args[0])
    return result


def record(ctx, job):
    files = {}
    try:
        status, output = execute(ctx, job)
        require(REQUIRED[job] <= output.keys(), "V26_REQUIRED_JOB_ARTIFACTS")
        for name, value in output.items():
            require(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*\.json", name), "V26_OUTPUT_NAME")
            files[name] = {"file_id": ctx.write(name, value), "sha256": digest(encode(value))}
        error = None
    except Exception as exc:
        status, error = "FAIL", label(exc)
    ctx.write(f"{job}.json", ctx.binding() | {"job": job, "status": status, "outputs": files, "error": error})
    print(f"{job} {status}" + (f" {error}" if error else ""))


def read_job(ctx, job, jobs):
    try:
        result = ctx.read(f"{job}.json")
        require(result["job"] == job and all(result.get(key) == value for key, value in ctx.binding().items()) and
                jobs[job]["conclusion"] == "success", "V26_JOB_IDENTITY")
        for name, ref in result["outputs"].items():
            fid, raw = original(ctx.access, ctx.root, name)
            require(fid == ref["file_id"] and digest(raw) == ref["sha256"], "V26_OUTPUT_SHA")
        return result
    except (OSError, KeyError, RuntimeError, ValueError, TypeError) as error:
        return {"job": job, "status": "NOT_RUN", "error": label(error)}


def safe(ctx, name):
    try:
        return ctx.read(name)
    except (OSError, ValueError, RuntimeError, KeyError):
        return {}


def conclude(ctx):
    git = GitHub()
    jobs = {j["name"]: {key: j[key] for key in ("id", "conclusion", "status", "html_url", "started_at", "completed_at")}
            for j in git.pages(f"/actions/runs/{ctx.run}/attempts/{ctx.attempt}/jobs", "jobs")}
    with ThreadPoolExecutor(max_workers=5) as pool:
        rows = dict(zip(REQUIRED, pool.map(lambda job: read_job(ctx, job, jobs), REQUIRED), strict=True))
    before = safe(ctx, "LOCK_INPUT.json")
    prior = safe(ctx, "BASELINE_PROOFS.json").get("inherited_gates", {})
    current = snapshot(ctx)
    bronze = metadata(ctx.previous("BRONZE_STORAGE_RULE.json")["folder_id"], ctx.access)
    _, running = production_runs(git)
    quota = v25_account.usage(ctx)
    untouched = bool(before.get("production_matches_baseline") and current == before.get("production") and
                     bronze == before.get("bronze_identity") and not running)
    agreement = before.get("quota") == quota
    accounted = all(row["status"] not in {"NOT_RUN", "FAIL"} for row in rows.values())
    states = {"G00": rows["LOCK"], "G01": rows["H"], "G02": prior.get("G02", {"status": "HOLD"}),
              "G03": prior.get("G03", {"status": "HOLD"}), "G04": rows["G04_FINAL"], "G05": rows["G05_FINAL"],
              "G06": rows["G06"], "G07": rows["G07"]}
    if any(rows[stage]["status"] != "PASS" for stage in ("D1", "D2")):
        states["G04"] = states["G04"] | {"status": "HOLD", "unsatisfied_prerequisite": "OFFICIAL_BOUNDARY_OR_COMPILER"}
    if rows["D3"]["status"] != "PASS":
        states["G05"] = states["G05"] | {"status": "HOLD", "unsatisfied_prerequisite": "PRACTICAL_CAPACITY_POLICY"}
    states["G08"] = {"status": "PASS" if untouched and agreement and accounted else "HOLD",
                     "production_and_bronze_identity_unchanged": untouched, "durable_quota_matches_start": agreement,
                     "all_report_evidence_accounted_for": accounted}
    ctx.write("G08.json", ctx.binding() | states["G08"])
    verdict = final_readiness_verdict({key: row["status"] for key, row in states.items()})
    index = safe(ctx, manifest.INDEX)
    budget = safe(ctx, "RESOURCE_ESTIMATE.json")
    coverage = safe(ctx, "COVERAGE_GAPS.json")
    engine = safe(ctx, "ENGINE_TESTS.json")
    subset = ("READY_FOR_PARTITION_RELEASE" if engine.get("loaded_certified") and untouched and agreement and
              states["G05"]["status"] == "PASS" and states["G07"]["status"] == "PASS" else "HOLD")
    result = ctx.binding() | verdict | {"revision": "V2.6", "previous_run": V25_RUN, "previous_report_sha256": V25_REPORT,
        "gates": states, "workflow_job_evidence": rows, "github_jobs": jobs, "quota": quota,
        "production_snapshot_before": before.get("production"), "production_snapshot_after": current,
        "bronze_root_metadata": bronze, "scope": index, "resource_estimate": bud_summary(budget),
        "undownloaded_historical_scope": coverage.get("current", []), "remaining_engineering_holds": coverage.get("engineering_holds", []),
        "partition_readiness": {"status": subset, "certified_windows": index.get("certified_native_windows", 0)},
        "full_history_denominator": index.get("full_history_denominator"), "historical_download_executed_in_run": False,
        "full_download_release": "HOLD", "production_dispatchable": 0}
    file_id = ctx.write("FINAL_READINESS.json", result)
    markdown(ctx, result)
    checked, raw = original(ctx.access, ctx.root, "FINAL_READINESS.json")
    require(checked == file_id and digest(raw) == digest(encode(result)), "V26_FINAL_REPORT_READBACK")
    event = ctx.binding() | verdict | {"revision": "V2.6", "recorded_at": jobs.get("G08", {}).get("started_at"),
        "decision": "D-006/R12-V2.6-EXECUTABLE-MANIFEST", "classification": "EVIDENCE", "writer": "GitHub Actions",
        "report_file_id": file_id, "report_sha256": digest(raw), "partition_status": subset,
        "full_download_release": "HOLD", "production_dispatchable": 0}
    for kind in ("HANDOVER", "TASK_EVENT", "DECISION_EVENT"):
        write_json(ctx.shared, f"{kind}_{ctx.task_id}_V2_6_{ctx.run}_{ctx.attempt}.json", event | {"event_type": kind}, ctx.access)
    print(f"{verdict['result']} gates={verdict['passed_gates']}/{verdict['expected_gates']} production_dispatchable=0")


def bud_summary(budget):
    keep = ("rough_new_capacity_bytes", "rough_job_days", "plan_storage_400_gib_fits", "drive",
            "configured_split_comparison_request_allowance", "small_batch_admission")
    return {name: budget.get(name) for name in keep}


def markdown(ctx, result):
    counts = result["scope"]
    budget = result["resource_estimate"]
    rows = [f"| {gate} | {proof['status']} |" for gate, proof in result["gates"].items()]
    body = ["# R12.1 V2.6 可執行清冊與資源驗收", "", f"**{result['result']}**", "",
        f"Run `{ctx.run}` · Attempt `{ctx.attempt}` · Commit `{ctx.commit}`", "",
        "| 關卡 | 核銷結果 |", "|---|---|", *rows, "",
        f"清冊涵蓋 {counts.get('dataset_count', '待核對')} 個資料集，計畫 {counts.get('planned_request_count', '待核對')} 次原件請求。",
        f"全歷史資料尚待實際下載與驗收。全歷史分母 {result['full_history_denominator'] or '尚待核證'}。",
        f"估算新增空間 {budget.get('rough_new_capacity_bytes') or '待核對'} bytes，工期 {budget.get('rough_job_days') or '待核對'} 天。",
        f"分區狀態 `{result['partition_readiness']['status']}`。正式放行 `HOLD`，正式派發 `0`。", ""]
    return write_markdown(ctx, "FINAL_READINESS.md", "\n".join(body))


def run_job(job):
    try:
        ctx = V26Context(job)
        conclude(ctx) if job == "G08" else record(ctx, job)
    except Exception as error:
        print(f"{job} INFRASTRUCTURE_ERROR {label(error)}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=(*REQUIRED, "G08"))
    run_job(parser.parse_args().job)
