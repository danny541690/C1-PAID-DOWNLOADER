"""Run the private R12.1 V2 readiness gates without production dispatch credentials."""

import argparse
from importlib import import_module
import json
import os
import re

from .cross_run_ledger import DriveJournal
from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import read_json_file, write_json
from .production_download import PLAN, TPEX_PLAN, management, pending_group, segment, year_budget
from .production_preflight import final_readiness_verdict
from .release_probes import read_private_file
from .release_rehearsal import RehearsalError, require
from .test_archive import GitHub


TASK_ID = "R12-FINAL-READINESS-V2-01"
SPEC_SHA = "d80cb0525c8f95fcb05763077cb8359f7839ec864abf3ffd60cec2286b5b4779"
SOP_SHA = "3f0a61439dc85372305fc839aa9a51fa22e52e7a94da4a6eccc1c83876887237"
SPEC = "R12_1_FINAL_INTEGRATED_READINESS_GATE_V2_20261127.md"
SOP = "JARVIS_V098_PLAN_B_SOP_V1_20261127.md"
GATES = {"G00": "輸入與版本", "G01": "共同知識交接", "G02": "B10 與既有任務復原",
         "G03": "資料集契約", "G04": "歷史清冊與防重", "G05": "額度、容量與期限",
         "G06": "隔離控制器", "G07": "跨工作驗收", "G08": "最終對帳"}
EXPECTED = {
    "G00": ("INPUT_LOCK.json",), "G01": ("H_ACK.json", "AGENTS_TEST.json"),
    "G02": ("RECOVERY_REPORT.json", "B09_REPAIR_PLAN.json"),
    "G03": ("DATASET_REGISTRY.json", "CONTRACT_EVIDENCE.json", "SCOPE_HOLDS.json"),
    "G04": ("HISTORICAL_COVERAGE.json", "MANIFEST_INDEX.json", "REUSE_MAP.json", "COVERAGE_GAPS.json"),
    "G05": ("RESOURCE_BUDGET.json", "PROBE_LEDGER.json"),
    "G06": ("ENGINE_TESTS.json", "TEST_MATRIX.json"), "G07": ("INTEGRATION_REPORT.json",),
    "G07_STAGE": ("G07_STAGE_CHECKPOINT.json",),
}
MODULES = {"G01": "v2_handover", "G02": "v2_recovery", "G03": "v2_registry",
           "G04": "v2_coverage", "G05": "v2_resources", "G06": "v2_engine",
           "G07_STAGE": "v2_engine", "G07": "v2_engine"}
DRIVE_ROOT = "FINAL_READINESS"


def private_path(access, parent, *names, create=False):
    verify_private_folder(parent, access)
    for name in names:
        parent = ensure(parent, name, FOLDER, access, create=create)
        verify_private_folder(parent, access)
    return parent


def original(access, parent, name):
    meta = find(parent, name, access)
    require(meta is not None, "INPUT_MISSING")
    verify_private_folder(meta["id"], access)
    return meta["id"], read_private_file(meta["id"], access)


class Context:
    def __init__(self):
        require(os.environ.get("R12_TASK_ID") == TASK_ID and os.environ.get("R12_SPEC_SHA") == SPEC_SHA,
                "WORKFLOW_TASK_BINDING")
        require("FINMIND_BACKER_TOKEN" not in os.environ, "UNEXPECTED_PRODUCTION_CREDENTIAL")
        self.task_id, self.spec_sha = TASK_ID, SPEC_SHA
        self.run = int(os.environ["GITHUB_RUN_ID"])
        self.attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
        self.commit = os.environ["GITHUB_SHA"]
        self.access = token()
        self.shared = private_path(self.access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT")
        self.task_root = private_path(self.access, self.shared, DRIVE_ROOT, TASK_ID, SPEC_SHA)
        self.root = private_path(self.access, self.task_root, f"{self.run}_{self.attempt}", create=True)
        self._production = None

    def read(self, name):
        return read_json_file(self.root, name, self.access)

    def write(self, name, body):
        return write_json(self.root, name, json.loads(encode(body)), self.access)

    def production(self):
        if self._production is None:
            _, root = management(self.access, create=False)
            current = DriveJournal(root, self.access).head()
            require(current is not None, "MASTER_MISSING")
            plan = read_json_file(root, PLAN, self.access)
            require(current["state"]["plan_sha"] == digest(encode(plan)), "BASE_DIGEST")
            supplement = None
            if sha := current["state"].get("otc_plan_sha"):
                supplement = read_json_file(root, TPEX_PLAN, self.access)
                require(sha == digest(encode(supplement)) and
                        supplement["base_plan_sha"] == current["state"]["plan_sha"], "TPEX_DIGEST")
            self._production = root, current, plan, supplement
        return self._production

    campaign = property(lambda self: self.production()[0])
    master = property(lambda self: self.production()[1])
    plan = property(lambda self: self.production()[2])
    supplement = property(lambda self: self.production()[3])

    def binding(self):
        return {"task_id": self.task_id, "spec_sha256": self.spec_sha, "commit": self.commit,
                "run_id": self.run, "attempt": self.attempt}


def snapshot(ctx):
    master = ctx.master
    group = pending_group(ctx.plan, ctx.supplement, master["state"])
    batch = None
    if group:
        active = ctx.plan if group == "BASE" else ctx.supplement
        folder, manifest, _, record = segment(active, master, ctx.campaign, ctx.access, group, create=False)
        batch = {"group": group, "folder_id": folder, "manifest_sha256": digest(encode(manifest)),
                 "journal_sha256": digest(encode(record)), "tasks": len(manifest["items"])}
    return {"master_sequence": master["sequence"], "master_sha256": digest(encode(master)),
            "base_sha256": master["state"]["plan_sha"], "tpex_sha256": master["state"].get("otc_plan_sha"),
            "base_cursor": master["state"]["cursor"], "tpex_cursor": master["state"].get("otc_cursor", 0),
            "totals": master["state"]["totals"], "campaign_windows": year_budget(ctx.plan, ctx.supplement)["windows"],
            "active_batch": batch}


def input_lock(ctx):
    spec_id, spec = original(ctx.access, ctx.shared, SPEC)
    sop_id, sop = original(ctx.access, ctx.shared, SOP)
    global_root = private_path(ctx.access, ROOT, "00_COMMAND", "AI_SHARED_CONTEXT")
    global_id, global_sop = original(ctx.access, global_root, SOP)
    require(digest(spec) == SPEC_SHA and sop == global_sop and digest(sop) == SOP_SHA, "SPEC_SOP_DIGEST")
    source_ids = {"spec": spec_id, "project_sop": sop_id, "global_sop": global_id}
    for name in ("START_HERE.md", "HANDOVER.md", "DECISION_LOG.md", "BUILD_LOG.md"):
        file_id, raw = original(ctx.access, ctx.shared, name)
        require(bool(raw), "EMPTY_SHARED_INPUT")
        source_ids[name] = {"file_id": file_id, "sha256": digest(raw)}
    github = GitHub()
    runs = github.json("/actions/workflows/finmind-backer-canary.yml/runs?per_page=100")["workflow_runs"]
    formal = [r for r in runs if r["display_title"].startswith("R12.1 Backer production (")]
    require(bool(formal), "PRODUCTION_RUN_REFERENCE")
    fields = ("id", "run_attempt", "head_sha", "status", "conclusion", "html_url")
    book = {"inputs": source_ids, "sop_sha256": SOP_SHA, "production": snapshot(ctx),
            "latest_production_run": {key: formal[0][key] for key in fields},
            "active_writers": [{key: r[key] for key in fields} for r in formal if r["status"] != "completed"],
            "historical_indexes": {"test_runs": "00_COMMAND/R11_BACKER/TEST_RUNS",
                                   "codex_archive": "00_COMMAND/R12_BACKER/02_CODEX_EXECUTION_ARCHIVE",
                                   "prior_shared_run": 45973149519, "prior_sample_readback_run": 45832107974},
            "backer_data_requests": 0, "backer_account_requests": 0}
    return {"status": "PASS", "outputs": {"INPUT_LOCK.json": book}}


def resume_gate(ctx, gate):
    previous = os.environ.get("R12_RESUME_FROM", "")
    if gate not in ("G01", "G02") or not re.fullmatch(r"\d+_\d+", previous):
        return None
    try:
        folder = private_path(ctx.access, ctx.task_root, previous)
        record = read_json_file(folder, f"{gate}.json", ctx.access)
        run, attempt = map(int, previous.split("_"))
        require(record["status"] == "PASS" and record["gate"] == gate and record["task_id"] == TASK_ID and
                record["spec_sha256"] == SPEC_SHA and record["run_id"] == run and record["attempt"] == attempt,
                "RESUME_REPORT_BINDING")
        if gate == "G02":
            locked = read_json_file(folder, "INPUT_LOCK.json", ctx.access)["production"]
            require(locked == ctx.read("INPUT_LOCK.json")["production"], "RESUME_PRODUCTION_SNAPSHOT")
        files = {name: read_json_file(folder, name, ctx.access) for name in EXPECTED[gate]}
        require(all(digest(encode(body)) == record["output_sha256"][name] for name, body in files.items()),
                "RESUME_OUTPUT_DIGEST")
        return {"status": "PASS", "outputs": files,
                "verified_checkpoint": {"run_id": run, "attempt": attempt, "commit": record["commit"],
                                        "sha256": digest(encode(record))}}
    except (OSError, ValueError, RuntimeError, KeyError, TypeError):
        return None


def record_gate(ctx, gate):
    try:
        if (result := resume_gate(ctx, gate)) is not None:
            pass
        elif gate == "G00":
            result = input_lock(ctx)
        else:
            module = import_module(f".{MODULES[gate]}", __package__)
            if gate in ("G06", "G07_STAGE", "G07"):
                result = module.run(ctx, phase={"G06": "engine", "G07_STAGE": "stage", "G07": "resume"}[gate])
            else:
                result = module.run(ctx)
        outputs = result.pop("outputs", {})
        require(result["status"] in ("PASS", "HOLD", "FAIL", "NOT_RUN"), "GATE_STATUS")
        for name, body in outputs.items():
            require(name.endswith(".json") and "/" not in name, "GATE_OUTPUT_NAME")
            ctx.write(name, body)
        result["output_sha256"] = {name: digest(encode(body)) for name, body in outputs.items()}
        if result["status"] == "PASS" and set(EXPECTED.get(gate, ())) - outputs.keys():
            result.update(status="FAIL", reason="REQUIRED_OUTPUT_MISSING")
    except Exception as error:
        result = {"status": "FAIL", "exception_class": type(error).__name__, "output_sha256": {}}
        if isinstance(error, RehearsalError) or re.fullmatch(r"[A-Z][A-Z0-9_]{2,70}", str(error)):
            result["reason"] = str(error)
    ctx.write(f"{gate}.json", ctx.binding() | result | {"gate": gate, "job": os.environ["GITHUB_JOB"]})
    print(f"{gate} {result['status']}")


def reconcile_gate(ctx, gate):
    try:
        report = ctx.read(f"{gate}.json")
        require(all(report.get(k) == v for k, v in ctx.binding().items()) and report["gate"] == gate,
                "GATE_BINDING")
        if report["status"] == "PASS":
            require(set(EXPECTED[gate]) <= report["output_sha256"].keys(), "GATE_EXPECTED_OUTPUTS")
        for name, sha in report["output_sha256"].items():
            require(digest(encode(ctx.read(name))) == sha, "GATE_OUTPUT_DIGEST")
        return report
    except Exception as error:
        return {"status": "NOT_RUN", "reason": "MISSING_OR_UNVERIFIED_REPORT", "exception_class": type(error).__name__}


def write_markdown(ctx, name, text):
    raw = text.encode()
    file_id = ensure(ctx.root, name, "text/markdown", ctx.access, raw, create=True)
    verify_private_folder(file_id, ctx.access)
    require(read_private_file(file_id, ctx.access) == raw, "MARKDOWN_READBACK")


def final(ctx):
    reports = {gate: reconcile_gate(ctx, gate) for gate in list(GATES)[:-1]}
    staging = reconcile_gate(ctx, "G07_STAGE")
    platform = {}
    try:
        endpoint = f"/actions/runs/{ctx.run}/attempts/{ctx.attempt}/jobs"
        github = GitHub()
        jobs = list(github.pages(endpoint, "jobs"))
        platform = {job["name"]: {k: job[k] for k in ("id", "status", "conclusion", "html_url")}
                    for job in jobs}
    except Exception as error:
        platform = {"error": type(error).__name__}
    try:
        initial = ctx.read("INPUT_LOCK.json")
        ending = snapshot(ctx)
        stable = initial["production"] == ending and not initial["active_writers"]
    except Exception:
        initial, ending, stable = {}, None, False
    for gate, report in reports.items():
        if report["status"] == "PASS" and platform.get(gate, {}).get("conclusion") != "success":
            report.update(status="HOLD", reason="GITHUB_JOB_COMPLETION_UNVERIFIED")
    if reports["G07"]["status"] == "PASS" and (staging["status"] != "PASS" or
                                                platform.get("G07_STAGE", {}).get("conclusion") != "success"):
        reports["G07"].update(status="HOLD", reason="STAGING_JOB_EVIDENCE_UNVERIFIED")
    evidence_ok = stable and all(gate in platform for gate in (*GATES, "G07_STAGE"))
    reports["G08"] = {"status": "PASS" if evidence_ok else "HOLD", "production_snapshot_matches": stable,
                      "job_evidence_bound": bool(platform) and "error" not in platform,
                      "missing_gate_reports": [gate for gate, report in reports.items() if report["status"] == "NOT_RUN"]}
    # All workflow jobs omit Backer credentials. No live Backer probe is part of this version.
    verdict = final_readiness_verdict({gate: r["status"] for gate, r in reports.items()})
    published = ctx.binding() | verdict | {
        "gates": reports, "isolation_checkpoint": staging, "github_jobs": platform,
        "production_snapshot_before": initial.get("production"),
        "production_snapshot_after": ending, "finmind_usage": {"data": 0, "account": 0, "data_cap": 48, "account_cap": 4},
        "full_history_denominator": "UNVERIFIED" if reports["G04"]["status"] != "PASS" else "SEE_MANIFEST_INDEX",
    }
    ctx.write("G08.json", ctx.binding() | reports["G08"])
    ctx.write("FINAL_READINESS.json", published)
    rows = "\n".join(f"| {gate} | {title} | {reports[gate]['status']} |" for gate, title in GATES.items())
    hold_details = []
    for gate in verdict["unresolved_gates"]:
        item = reports[gate]
        labels = item.get("blockers") or item.get("findings") or [item.get("reason", item["status"])]
        hold_details.append(f"- {gate} " + "、".join(map(str, labels)))
    report_text = (f"# R12.1 最終前置驗收\n\n{TASK_ID} · V2.0 · `{SPEC_SHA}`\n\n"
                   f"**{verdict['result']}**\n\n"
                   f"Commit `{ctx.commit}`，Run `{ctx.run}`，Attempt `{ctx.attempt}`。\n\n"
                   "| Gate | 驗收項目 | 結果 |\n|---|---|---|\n" + rows + "\n\n"
                   "本輪 FinMind 資料請求 0 次，帳號查詢 0 次。\n\n"
                   "正式下載維持 `HOLD`，可派發數為 `0`。各 Gate 的輸出及其 SHA-256 由 "
                   "`FINAL_READINESS.json` 對照。\n" + ("\n## 待核證項目\n\n" + "\n".join(hold_details) + "\n" if hold_details else ""))
    write_markdown(ctx, "FINAL_READINESS.md", report_text)
    result_id, raw = original(ctx.access, ctx.root, "FINAL_READINESS.json")
    event = ctx.binding() | verdict | {"writer": "GitHub Actions", "event_type": "READINESS_HANDOVER",
             "report_id": result_id, "report_sha256": digest(raw), "gates": {k: v["status"] for k, v in reports.items()},
             "next": "使用者另行核准正式放行前，先查閱各 Gate 的待核證證據。"}
    suffix = f"{TASK_ID}_{ctx.run}_{ctx.attempt}.json"
    write_json(ctx.shared, "HANDOVER_" + suffix, event, ctx.access)
    write_json(ctx.shared, "TASK_EVENT_" + suffix, event | {"event_type": "READINESS_TASK_RESULT"}, ctx.access)
    print(f"{verdict['result']} full_download_release=HOLD production_dispatchable=0")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("gate", choices=(*GATES, "G07_STAGE"))
    gate = parser.parse_args().gate
    try:
        ctx = Context()
        if gate == "G08":
            final(ctx)
        else:
            record_gate(ctx, gate)
    except Exception as error:
        print(f"{gate} INFRASTRUCTURE_ERROR {type(error).__name__}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
