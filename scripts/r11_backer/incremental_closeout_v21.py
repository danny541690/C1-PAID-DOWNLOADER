"""Execute and reconcile the private R12.1 V2.1 incremental closeout."""

import argparse
from importlib import import_module
import json
import os
import re
import subprocess

from .drive_pilot import ROOT, token
from .final_readiness_v2 import (
    Context, DRIVE_ROOT, EXPECTED as V2_OUTPUTS, GATES, SOP, SOP_SHA, SPEC_SHA as V2_SHA,
    TASK_ID, original, private_path, snapshot, write_markdown,
)
from .integration_pilot import digest, encode
from .live_recovery_canary import read_json_file, write_json
from .production_preflight import final_readiness_verdict
from .release_rehearsal import require
from .test_archive import GitHub, attempt_jobs

SPEC = "R12_1_G03_G05_INCREMENTAL_CLOSEOUT_V2_1_20261128.md"
SPEC_SHA = "c83f8916d9559cba7638193bd5e206498f085ba8648337a685aeb2a5548b4271"
PRIOR_RUN = 46030259242
PRIOR_COMMIT = "4df007ddbe6c3f25eddd6ed7f3ac1599a2ced25c"
PRIOR_FILES = {
    "FINAL_READINESS.json": "049a250350455e819c3159731447f2f6e40548d94f664b69b9499fc9b0806611",
    "SCOPE_HOLDS.json": "9d820af450df02d10d86b50b66cf5a31983b546c96756249fe1e59082c47348d",
    "COVERAGE_GAPS.json": "5875d27770f472388bdc5d403ddd11d559bfee4faea61e017b0a85372d4ad7d1",
    "RESOURCE_BUDGET.json": "d2d2820c0e5d45ae6369e258c9b45a96f636a3e0fd60ffbbf9a5af8727ace466",
}
REUSE = ("G00", "G01", "G02", "G06", "G07", "G08")
RUNNERS = {
    "H": ("v21_handover", "run"), "G03": ("v2_registry", "run_v21"),
    "G04": ("v2_coverage", "run_v21"), "G05_PROBE": ("v2_resources", "probe_v21"),
    "G05": ("v2_resources", "run_v21"), "G06": ("v21_regression", "run"),
    "G07_STAGE": ("v21_regression", "run"), "G07": ("v21_regression", "run"),
}
PHASES = {"G06": "engine", "G07_STAGE": "stage", "G07": "resume"}
OUTPUTS = V2_OUTPUTS | {
    "LOCK": ("INPUT_LOCK.json",), "H": ("PRIOR_EVIDENCE_LOCK.json", "H_ACK.json", "AGENTS_TEST.json"),
    "G03": V2_OUTPUTS["G03"] + ("SCOPE_DECISIONS.json",),
    "G05_PROBE": ("PROBE_PREFLIGHT.json",),
}
STATES = {"PASS", "HOLD", "FAIL", "NOT_RUN"}


class Closeout(Context):
    def __init__(self, gate):
        require(os.environ.get("R12_TASK_ID") == TASK_ID and os.environ.get("R12_SPEC_SHA") == SPEC_SHA,
                "WORKFLOW_TASK_BINDING")
        require(os.environ["GITHUB_JOB"] == gate, "WORKFLOW_JOB_BINDING")
        require(gate == "G05_PROBE" or "FINMIND_BACKER_TOKEN" not in os.environ,
                "UNEXPECTED_BACKER_CREDENTIAL")
        self.task_id, self.spec_sha = TASK_ID, SPEC_SHA
        self.run = int(os.environ["GITHUB_RUN_ID"])
        self.attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
        self.commit = os.environ["GITHUB_SHA"]
        self.access = token()
        self.shared = private_path(self.access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT")
        task = private_path(self.access, self.shared, DRIVE_ROOT, TASK_ID)
        self.prior_root = private_path(self.access, task, V2_SHA, f"{PRIOR_RUN}_1")
        # LOCK is the only initializer. Dependent jobs open existing private locations.
        initialize = gate == "LOCK"
        self.task_root = private_path(self.access, task, SPEC_SHA, create=initialize)
        self.root = private_path(self.access, self.task_root, f"{self.run}_{self.attempt}", create=initialize)
        self._production = None
        self._prior_files = {}

    def prior(self, name):
        if name not in self._prior_files:
            _, raw = original(self.access, self.prior_root, name)
            if name in PRIOR_FILES:
                require(digest(raw) == PRIOR_FILES[name], "PRIOR_ORIGINAL_DIGEST")
            self._prior_files[name] = json.loads(raw)
        return self._prior_files[name]


def production_runs(github):
    runs = github.json("/actions/workflows/finmind-backer-canary.yml/runs?per_page=100")["workflow_runs"]
    formal = [r for r in runs if r["display_title"].startswith("R12.1 Backer production (")]
    require(bool(formal), "PRODUCTION_RUN_REFERENCE")
    fields = ("id", "run_attempt", "head_sha", "status", "conclusion", "html_url")
    return ({key: formal[0][key] for key in fields},
            [{key: run[key] for key in fields} for run in formal if run["status"] != "completed"])


def reuse_evidence(ctx, previous, jobs):
    binding = {"task_id": TASK_ID, "spec_sha256": V2_SHA, "commit": PRIOR_COMMIT,
               "run_id": PRIOR_RUN, "attempt": 1}
    require(all(previous.get(k) == v for k, v in binding.items()), "PRIOR_RESULT_BINDING")
    require(previous["full_download_release"] == "HOLD" and previous["production_dispatchable"] == 0,
            "PRIOR_RELEASE_BINDING")
    preserved = {}
    for gate in (*REUSE, "G07_STAGE"):
        report = ctx.prior(f"{gate}.json")
        stated = previous["isolation_checkpoint"] if gate == "G07_STAGE" else previous["gates"][gate]
        require(all(report.get(k) == v for k, v in binding.items()) and
                report["status"] == stated["status"] == "PASS", "PRIOR_GATE_BINDING")
        job = jobs[gate]
        require(job["conclusion"] == "success" and job["id"] == previous["github_jobs"][gate]["id"],
                "PRIOR_JOB_BINDING")
        outputs = report.get("output_sha256", {})
        require(set(V2_OUTPUTS.get(gate, ())) <= outputs.keys(), "PRIOR_OUTPUT_MISSING")
        require(all(digest(encode(ctx.prior(name))) == sha for name, sha in outputs.items()),
                "PRIOR_OUTPUT_DIGEST")
        preserved[gate] = {"status": "PASS", "inherited_from": binding | {"job_id": job["id"],
                           "report_sha256": digest(encode(report)), "output_sha256": outputs}}
    return preserved


def lock_inputs(ctx):
    spec_id, spec = original(ctx.access, ctx.shared, SPEC)
    sop_id, sop = original(ctx.access, ctx.shared, SOP)
    global_root = private_path(ctx.access, ROOT, "00_COMMAND", "AI_SHARED_CONTEXT")
    global_id, global_sop = original(ctx.access, global_root, SOP)
    require(digest(spec) == SPEC_SHA and sop == global_sop and digest(sop) == SOP_SHA, "SPEC_SOP_DIGEST")
    inputs = {"spec": spec_id, "project_sop": sop_id, "global_sop": global_id}
    for name in ("START_HERE.md", "HANDOVER.md", "DECISION_LOG.md", "BUILD_LOG.md"):
        file_id, raw = original(ctx.access, ctx.shared, name)
        require(bool(raw), "EMPTY_SHARED_INPUT")
        inputs[name] = {"file_id": file_id, "sha256": digest(raw)}
    github = GitHub()
    prior_run = github.json(f"/actions/runs/{PRIOR_RUN}/attempts/1")
    require(prior_run["head_sha"] == PRIOR_COMMIT and prior_run["conclusion"] == "success" and
            prior_run["run_attempt"] == 1, "PRIOR_RUN_BINDING")
    jobs = {job["name"]: job for job in github.pages(f"/actions/runs/{PRIOR_RUN}/attempts/1/jobs", "jobs")}
    previous = ctx.prior("FINAL_READINESS.json")
    for name in PRIOR_FILES:
        ctx.prior(name)
    inherited = reuse_evidence(ctx, previous, jobs)
    ending = snapshot(ctx)
    stable = ending == previous["production_snapshot_after"] == ctx.prior("INPUT_LOCK.json")["production"]
    # Changes outside the incremental modules invalidate the inherited B10/receipt acceptance.
    recovery_files = (
        "scripts/r11_backer/production_download.py", "scripts/r11_backer/v2_recovery.py",
        "scripts/r11_backer/drive_pilot.py", "scripts/r11_backer/cross_run_ledger.py",
        "scripts/r11_backer/live_recovery_canary.py", "scripts/r11_backer/integration_pilot.py",
    )
    same_code = subprocess.run(["git", "diff", "--quiet", PRIOR_COMMIT, ctx.commit, "--", *recovery_files],
                               check=False).returncode == 0
    latest, active = production_runs(github)
    if not stable or not same_code or active:
        inherited["G02"].update(status="HOLD", reason="RECOVERY_DEPENDENCIES_CHANGED_OR_WRITER_ACTIVE")
    output = {"inputs": inputs, "production": ending, "prior_production_matches": stable,
              "recovery_code_matches": same_code, "latest_production_run": latest, "active_writers": active,
              "prior_original_sha256": PRIOR_FILES, "prior_finmind_usage": previous["finmind_usage"],
              "inherited": inherited}
    return {"status": "PASS", "outputs": {"INPUT_LOCK.json": output}}


def checked_gate(ctx, gate, *, folder=None, attempt=None):
    read = ctx.read if folder is None else lambda name: read_json_file(folder, name, ctx.access)
    binding = ctx.binding() if attempt is None else ctx.binding() | {"attempt": attempt}
    report = read(f"{gate}.json")
    require(all(report.get(k) == v for k, v in binding.items()) and
            report["gate"] == report["job"] == gate and report["status"] in STATES, "GATE_BINDING")
    outputs = report["output_sha256"]
    if report["status"] == "PASS":
        require(set(OUTPUTS[gate]) <= outputs.keys(), "GATE_OUTPUT_MISSING")
    for name, sha in outputs.items():
        output_name(name)
        require(digest(encode(read(name))) == sha, "GATE_OUTPUT_DIGEST")
    return report


def output_name(name):
    require(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,120}\.json", name) and
            name not in {f"{gate}.json" for gate in (*RUNNERS, "LOCK", "G08")} and
            name != "FINAL_READINESS.json", "GATE_OUTPUT_NAME")


def resume_completed(ctx, gate):
    # Reuse safe checkpoints only for the identical commit and production snapshot.
    if not 1 < ctx.attempt <= 4 or gate not in {"G03", "G04"}:
        return None
    try:
        attempt = ctx.attempt - 1
        folder = private_path(ctx.access, ctx.task_root, f"{ctx.run}_{attempt}")
        checked_gate(ctx, "LOCK", folder=folder, attempt=attempt)
        old_input = read_json_file(folder, "INPUT_LOCK.json", ctx.access)
        require(old_input["production"] == ctx.read("INPUT_LOCK.json")["production"], "CHECKPOINT_PRODUCTION")
        report = checked_gate(ctx, gate, folder=folder, attempt=attempt)
        require(report["status"] == "PASS", "CHECKPOINT_GATE_HOLD")
        names = report.pop("output_sha256")
        prior = ctx.binding() | {"attempt": attempt, "gate": gate}
        return report | {"checkpoint": prior, "outputs": {
            name: read_json_file(folder, name, ctx.access) for name in names
        }}
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        return None


def execute(ctx, gate):
    if gate == "LOCK":
        return lock_inputs(ctx)
    if gate in {"G03", "G04", "G05_PROBE"}:
        require(checked_gate(ctx, "LOCK")["status"] == "PASS", "INPUT_LOCK_NOT_PASSED")
    if gate in {"G03", "G04"} and (resumed := resume_completed(ctx, gate)):
        return resumed
    module, function = RUNNERS[gate]
    runner = getattr(import_module(f".{module}", __package__), function)
    return runner(ctx, PHASES[gate]) if gate in PHASES else runner(ctx)


def record(ctx, gate):
    try:
        result = execute(ctx, gate)
        outputs = result.pop("outputs", {})
        require(result["status"] in STATES, "GATE_STATUS")
        for name, body in outputs.items():
            output_name(name)
            ctx.write(name, body)
        result["output_sha256"] = {name: digest(encode(body)) for name, body in outputs.items()}
        if result["status"] == "PASS" and set(OUTPUTS[gate]) - outputs.keys():
            result.update(status="FAIL", reason="GATE_OUTPUT_MISSING")
    except Exception as error:
        result = {"status": "FAIL", "output_sha256": {}, "exception_class": type(error).__name__}
        if re.fullmatch(r"[A-Z][A-Z0-9_]{2,70}", str(error)):
            result["reason"] = str(error)
    ctx.write(f"{gate}.json", result | ctx.binding() | {"gate": gate, "job": gate})
    print(f"{gate} {result['status']}")


def reconciled(ctx, gate, jobs):
    try:
        report = checked_gate(ctx, gate)
        if report["status"] == "PASS" and jobs.get(gate, {}).get("conclusion") != "success":
            report.update(status="HOLD", reason="GITHUB_JOB_COMPLETION_UNVERIFIED")
        return report
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
        return {"status": "NOT_RUN", "reason": "MISSING_OR_UNVERIFIED_REPORT", "exception_class": type(error).__name__}


def read_if_available(ctx, name):
    try:
        return ctx.read(name)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError):
        return {}


def usage(ctx):
    # Filled by the shared broker from persisted reservations, including uncertain results.
    ledger = read_if_available(ctx, "PROBE_LEDGER.json")
    data, account = ledger.get("actual_data_requests"), ledger.get("actual_account_requests")
    reserved_data, reserved_account = ledger.get("reserved_data_requests"), ledger.get("reserved_account_requests")
    known = (all(type(number) is int for number in (data, account, reserved_data, reserved_account)) and
             0 <= data <= reserved_data <= 48 and 0 <= account <= reserved_account <= 4)
    return {"data": data, "account": account, "data_cap": 48, "account_cap": 4,
            "reserved_data": reserved_data, "reserved_account": reserved_account, "verified": known}


def final(ctx):
    platform = {}
    github = GitHub()
    try:
        endpoint = f"/actions/runs/{ctx.run}/attempts/{ctx.attempt}"
        rows = attempt_jobs(github, endpoint, current=False)
        platform = {row["name"]: {k: row[k] for k in ("id", "status", "conclusion", "html_url")} for row in rows}
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
        platform = {"error": type(error).__name__}
    auxiliary = {gate: reconciled(ctx, gate, platform) for gate in ("LOCK", "H", "G05_PROBE", "G07_STAGE")}
    reports = {gate: reconciled(ctx, gate, platform) for gate in ("G03", "G04", "G05", "G06", "G07")}
    initial = read_if_available(ctx, "INPUT_LOCK.json")
    inherited = initial.get("inherited", {}) if auxiliary["LOCK"]["status"] == "PASS" else {}
    missing = {"status": "NOT_RUN", "reason": "PRIOR_EVIDENCE_UNVERIFIED"}
    reports["G00"] = inherited.get("G00", missing) | {"new_input_lock": auxiliary["LOCK"]["status"]}
    reports["G01"] = auxiliary["H"] | {"prior_evidence": inherited.get("G01", missing)}
    reports["G02"] = inherited.get("G02", missing)
    for gate in ("G06", "G07"):
        reports[gate]["prior_evidence"] = inherited.get(gate, missing)
    if reports["G07"]["status"] == "PASS" and auxiliary["G07_STAGE"]["status"] != "PASS":
        reports["G07"].update(status="HOLD", reason="STAGING_JOB_EVIDENCE_UNVERIFIED")
    try:
        ending = snapshot(ctx)
        _, active = production_runs(github)
        stable = ending == initial["production"] and not initial["active_writers"] and not active
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        ending, stable = None, False
    counters = usage(ctx)
    complete = all(platform.get(gate, {}).get("conclusion") == "success" for gate in (*RUNNERS, "LOCK"))
    missing_reports = [gate for gate, report in (reports | auxiliary).items() if report["status"] == "NOT_RUN"]
    reports["G08"] = {"status": "PASS" if stable and complete and not missing_reports and counters["verified"] else "HOLD",
                      "production_snapshot_matches": stable, "job_evidence_bound": complete,
                      "missing_gate_reports": missing_reports, "prior_evidence": inherited.get("G08", missing)}
    verdict = final_readiness_verdict({gate: r["status"] for gate, r in reports.items()})
    manifest = read_if_available(ctx, "MANIFEST_INDEX.json")
    published = ctx.binding() | verdict | {
        "revision": "V2.1", "gates": reports, "execution": auxiliary, "github_jobs": platform,
        "production_snapshot_before": initial.get("production"), "production_snapshot_after": ending,
        "finmind_usage": counters,
        "full_history_denominator": manifest.get("full_history_denominator") if reports["G04"]["status"] == "PASS" else None,
        "prior_run": PRIOR_RUN,
    }
    ctx.write("G08.json", ctx.binding() | reports["G08"] | {"gate": "G08", "job": "G08"})
    ctx.write("FINAL_READINESS.json", published)
    rows = [f"| {gate} | {title} | {reports[gate]['status']} |" for gate, title in GATES.items()]
    details = []
    for gate in verdict["unresolved_gates"]:
        report = reports[gate]
        reasons = report.get("blockers") or report.get("findings") or [report.get("reason", report["status"])]
        details.append(f"- {gate} " + "、".join(map(str, reasons)))
    markdown = ["# R12.1 G03–G05 增量驗收 V2.1", "", f"**{verdict['result']}**", "",
                f"{TASK_ID} · `{SPEC_SHA}`", "",
                f"Commit `{ctx.commit}`，Run `{ctx.run}`，Attempt `{ctx.attempt}`。", "",
                "| Gate | 驗收項目 | 結果 |", "|---|---|---|", *rows, "",
                f"Backer 累計資料請求 {counters['data'] if counters['verified'] else '待核對'} 次，"
                f"帳號查詢 {counters['account'] if counters['verified'] else '待核對'} 次。", "",
                "正式下載維持 `HOLD`，可派發數為 `0`。", ""]
    if details:
        markdown.extend(["## 待核證項目", "", *details, ""])
    write_markdown(ctx, "FINAL_READINESS.md", "\n".join(markdown))
    result_id, raw = original(ctx.access, ctx.root, "FINAL_READINESS.json")
    event = ctx.binding() | verdict | {"writer": "GitHub Actions", "event_type": "READINESS_HANDOVER",
            "classification": "EVIDENCE", "report_id": result_id, "report_sha256": digest(raw),
            "gates": {k: v["status"] for k, v in reports.items()}, "inherited_run": PRIOR_RUN}
    suffix = f"{TASK_ID}_V2_1_{ctx.run}_{ctx.attempt}.json"
    write_json(ctx.shared, "HANDOVER_" + suffix, event, ctx.access)
    write_json(ctx.shared, "TASK_EVENT_" + suffix, event | {"event_type": "READINESS_TASK_RESULT"}, ctx.access)
    print(f"{verdict['result']} full_download_release=HOLD production_dispatchable=0")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("gate", choices=("LOCK", *RUNNERS, "G08"))
    gate = parser.parse_args().gate
    try:
        ctx = Closeout(gate)
        if gate == "G08":
            final(ctx)
        else:
            record(ctx, gate)
    except Exception as error:
        print(f"{gate} INFRASTRUCTURE_ERROR {type(error).__name__}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
