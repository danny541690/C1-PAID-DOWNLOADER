"""Production CLI rehearsal with real private Drive journals and an isolated FinMind fixture."""

import argparse
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path

from . import autonomous_quota as quota, executable_manifest_v26 as manifest
from .bulk_manifest import SNAPSHOT_ZONE
from .cross_run_ledger import DriveJournal
from .drive_pilot import ROOT, token
from .final_readiness_v2 import SOP, SOP_SHA, TASK_ID, original, private_path, snapshot
from .full_release_evidence import BASE_COMMIT, BASE_REPORT_SHA, BASE_RUN, BRANCH, CAMPAIGN, INDEX_FILE, INDEX_SHA, SPEC_SHA, WORKFLOW, Sealed
from .integration_pilot import digest, encode
from .live_recovery_canary import read_json_file, write_json
from .production_full_history import LiveContext, NAMESPACE, Runtime, SUMMARY, here, workflow_output
from .release_rehearsal import require
from .test_archive import GitHub
from .v2_recovery import metadata
from .v24_coverage import active_snapshot

ATTACH = "V2_6_R"
PREFIX = "PRODUCTION_RELEASE_INTEGRATION_REPORT"
MARKER = "[r12-v26r-integration]"


def origin_run(origin=None, *, allow_active=False):
    event = os.environ["GITHUB_EVENT_NAME"]
    run = int(os.environ["GITHUB_RUN_ID"])
    require(os.environ.get("R12R_EXERCISE") == "true" and os.environ.get("GITHUB_REPOSITORY") == "danny541690/C1-PAID-DOWNLOADER" and
            os.environ.get("GITHUB_REF_NAME") == BRANCH and "FINMIND_BACKER_TOKEN" not in os.environ,
            "V26R_ISOLATED_RUNTIME_BOUNDARY")
    origin = origin or run
    require(event == "push" and origin == run or event == "workflow_dispatch" and origin < run, "V26R_REHEARSAL_EVENT")
    github = GitHub()
    data = github.json(f"/actions/runs/{origin}")
    require(data["head_sha"] == os.environ["GITHUB_SHA"] and data["head_branch"] == BRANCH and data["event"] == "push" and
            MARKER in data["head_commit"]["message"] and data["path"].split("@", 1)[0].endswith(WORKFLOW) and
            (origin == run or data["conclusion"] == "success" or allow_active), "V26R_REHEARSAL_ORIGIN")
    return origin


def integration_root(access, sealed, origin, *, create=False):
    audit = private_path(access, sealed.version, ATTACH, create=create)
    return private_path(access, audit, f"{origin}_1", create=create)


def runtime(access, sealed, origin):
    issuer = origin_run(origin)
    root = integration_root(access, sealed, issuer)
    area = here("ISOLATED_RUNTIME", root, access, create=False)
    master = DriveJournal(here("MASTER", area, access, create=False), access)
    bronze = here("TEST_BRONZE", area, access, create=False)
    authority = read_json_file(area, "SIMULATED_APPROVAL.json", access)
    require(authority["simulated"] and authority["origin_run"] == issuer and authority["sandbox"] == area and
            authority["code_commit"] == os.environ["GITHUB_SHA"], "V26R_REHEARSAL_APPROVAL_BINDING")
    return Runtime(access, sealed, authority, area, master, bronze, isolation=True, origin=issuer)


def fixture_provider(items):
    def payload(_auth, params, limit):
        candidates = [item for item in items if item["dataset"] == params["dataset"] and
                      item["parameters"].get("data_id") == params.get("data_id") and
                      item["start"] <= params.get("start_date", item["start"]) <= item["end"] and
                      item["start"] <= params.get("end_date", params.get("start_date", item["end"])) <= item["end"]]
        require(len(candidates) == 1, "V26R_FIXTURE_SCOPE")
        item = candidates[0]
        names = item["native_contract"]["key_fields"]
        expected = item["completeness"].get("keys") or [[day if key == "date" else
            params.get("data_id", "isolated-key") if key in {"stock_id", "cb_id", "futures_id"} else "isolated-key"
            for key in names] for day in sorted({item["start"], item["end"]})]
        if not names:
            expected = [[day] for day in sorted({item["start"], item["end"]})]
            names = ["date"]
        rows = []
        first, last = params.get("start_date", item["start"]), params.get("end_date", params.get("start_date", item["end"]))
        for serial, key in enumerate(expected):
            obj = {name: serial + 1 for name in item["native_contract"]["fields"]}
            obj.update(dict(zip(names, key, strict=True)))
            if item["native_contract"]["date_field"] and "date" not in names:
                obj["date"] = item["start"]
            if entity := item["native_contract"]["identity_field"]:
                if "data_id" in params:
                    obj[entity] = params["data_id"]
            if first <= obj.get("date", first) <= last:
                rows.append(obj)
        raw = encode({"status": 200, "msg": "ISOLATED_WORKFLOW_FIXTURE", "data": rows})
        require(len(raw) <= limit, "V26R_FIXTURE_RESPONSE_BOUND")
        return raw, "identity"
    return payload


def immutable_state(sealed):
    ctx = LiveContext(sealed.access)
    _, q, _ = quota.open_book(DriveJournal(sealed.account_folder(), sealed.access))
    return {"production": snapshot(ctx), "bronze": metadata(sealed.report["bronze_root_metadata"]["id"], sealed.access),
            "shared_quota_sha256": digest(encode(q))}


def lock(access, sealed, issuer):
    root = integration_root(access, sealed, issuer, create=True)
    scope = here("ISOLATED_RUNTIME", root, access, create=True)
    master = DriveJournal(here("MASTER", scope, access, create=True), access)
    quota_book = DriveJournal(here("TEST_ACCOUNT", scope, access, create=True), access)
    here("TEST_BRONZE", scope, access, create=True)
    current = immutable_state(sealed)
    require(current["production"] == sealed.report["production_snapshot_after"] and
            current["bronze"] == sealed.report["bronze_root_metadata"], "V26R_REAL_BASELINE_CHANGED")
    global_root = private_path(access, ROOT, "00_COMMAND", "AI_SHARED_CONTEXT")
    _, sop = original(access, global_root, SOP)
    _, project_sop = original(access, sealed.shared, SOP)
    start_fid, start = original(access, sealed.shared, "START_HERE.md")
    require(sop == project_sop and digest(sop) == SOP_SHA and start, "V26R_D006_GOVERNANCE")
    g = GitHub()
    baseline = g.json(f"/actions/runs/{BASE_RUN}/attempts/1")
    jobs = list(g.pages(f"/actions/runs/{BASE_RUN}/attempts/1/jobs", "jobs"))
    require(baseline["head_sha"] == BASE_COMMIT and len(jobs) == 10 and all(row["conclusion"] == "success" for row in jobs),
            "V26R_BASE_GITHUB_EVIDENCE")
    controls = sealed.check_consumers()
    active = active_snapshot(LiveContext(access))
    require(active == sealed.read("ACTIVE_BATCH_RECONCILIATION.json"), "V26R_ACTIVE_ORIGINAL_READBACK")
    carry = coverage_for_rehearsal(sealed, active)
    _, rows, axes = manifest.read_inputs(sealed.index, sealed.read)
    split_offset = next(i for i, row in enumerate(rows["rows"]) if row["normal"] and
                        row["normal"][0]["verification"] == "PROVIDER_SPLIT" and
                        row["normal"][0]["mode"].endswith("_RANGE") and
                        next(manifest.parameters(row["normal"][0], axes), None) is not None)
    inherited = {"plan_sha": current["production"]["base_sha256"], "cursor": current["production"]["base_cursor"],
                 "totals": current["production"]["totals"], "sandbox": True}
    if master.head() is None:
        master.initialize(inherited)
    seed = master.version(0)
    if quota_book.head() is None:
        old_cap = {"reserved_data_requests": 48, "actual_data_requests": 48,
                   "reserved_account_requests": 4, "actual_account_requests": 4}
        quota_book.initialize(quota.checkpoint(quota.initial(quota.utc_ms(), policy_evidence=INDEX_SHA, prior=old_cap)))
    binding = {"simulated": True, "origin_run": issuer, "sandbox": scope, "code_commit": os.environ["GITHUB_SHA"],
               "manifest_sha256": INDEX_SHA, "file_id": f"isolated-{issuer}", "master_sha256": digest(encode(seed))}
    binding["sha256"] = digest(encode(binding))
    write_json(scope, "SIMULATED_APPROVAL.json", binding, access)
    write_json(scope, "ISOLATED_BASE.json", {"master_sha256": binding["master_sha256"], "coverage": carry,
               "normal_split_dataset_offset": split_offset,
               "positions": active["dispositions"], "waiting": [], "stored_evidence": True}, access)
    baseline_summary = {"origin_run": issuer, "manifest_sha256": INDEX_SHA, "predecessor_report_sha256": BASE_REPORT_SHA,
        "production": current, "code_commit": os.environ["GITHUB_SHA"], "sop_sha256": SOP_SHA,
        "start_here": {"file_id": start_fid, "sha256": digest(start)}, "known_consumer_isolation": controls,
        "actual_verified_legacy_refs": carry["unique_verified_source_requests"], "active": active["dispositions"],
        "baseline_jobs": {row["name"]: row["id"] for row in jobs}}
    write_json(root, "LOCK.json", baseline_summary, access)


def coverage_for_rehearsal(sealed, active):
    # The exact input receipt shards are verified even though the isolated run
    # only carries their metadata and never writes back to their parents.
    from .full_release_evidence import coverage_snapshot

    return coverage_snapshot(sealed, active)


def handover(access, sealed, issuer):
    root = integration_root(access, sealed, issuer, create=True)
    proof = sealed.read("BASELINE_PROOFS.json")
    write_json(root, "H_LINE.json", {"task_id": TASK_ID, "base_run": BASE_RUN, "prior_proofs_sha256": digest(encode(proof)),
        "original_manifest_immutable": True, "production_dispatch_authorized": False, "live_api_requests": 0,
        "origin_run": issuer}, access)


def first_checkpoint(access, sealed, issuer):
    target = runtime(access, sealed, issuer)
    root = integration_root(access, sealed, issuer)
    rec, control = target.state()
    _, book, _ = quota.open_book(target.account())
    active, store = target.active_batch(control)
    require(active["queue"] == "normal" and control["first_receipt_verified"] and control["batches_committed"] == 1,
            "V26R_FIRST_RECEIPT_AND_AUTO_NEXT")
    first = read_json_file(target.reserve_directory(0), SUMMARY, access)
    uncertain = store.head()
    rows = list(uncertain["state"]["tasks"].values())
    require(first["states"] == {"VERIFIED": 1} and len(rows) == 1 and
            rows[0]["status"] == "REQUESTING" and rows[0]["reason"] == "STAGING_RESULT_UNCERTAIN" and
            len([event for event in book["events"] if event.get("preflight") is False]) == 2 and
            book["preflight"]["reserved"] == {"data": 48, "account": 4}, "V26R_EXPECTED_INTERRUPT_AND_QUOTA")
    write_json(root, "CROSS_RUN_CHECKPOINT.json", {"master_sha256": digest(encode(rec)), "active": control["active"],
          "request_id": active["items"][0]["request_id"], "ordinal": control["batch_number"],
          "stage_id": rows[0]["pending"]["id"], "primary_attempts_before_restart": 2,
          "first_b10_receipt_sha256": digest(encode(first)), "first_b10_receipt_count": len(first["receipts"]),
          "origin_run": issuer, "code_commit": os.environ["GITHUB_SHA"]}, access)
    workflow_output("ready_for_resume", "true")


def resumed_proof(access, sealed, issuer):
    target = runtime(access, sealed, issuer)
    root = integration_root(access, sealed, issuer)
    first = read_json_file(root, "CROSS_RUN_CHECKPOINT.json", access)
    done, control = target.state()
    _, book, _ = quota.open_book(target.account())
    result = read_json_file(target.reserve_directory(first["ordinal"]), SUMMARY, access)
    previous = read_json_file(target.reserve_directory(0), SUMMARY, access)
    end = read_json_file(target.reserve_directory(2), SUMMARY, access)
    totals, source, aux = control["totals"], control["source_requests"], control["b09_requests"]
    events = [event for event in book["events"] if event.get("preflight") is False]
    require(issuer != target.run and control["slot"] == len(control["queues"]) and control["batches_committed"] == 3 and
            totals == {"VERIFIED": 2, "EXPLORATION_CAPTURED": 1} and source == 3 and aux == 2 and len(events) == source + aux,
            "V26R_CROSS_RUN_INTEGRATION_COUNTS")
    require(len(result["receipts"]) == 1 and first["request_id"] in result["receipts"] and
            result["states"] == {"VERIFIED": 1} and result["source_requests"] == 1 and result["b09_requests"] == 2 and
            previous["queue"] == "certified" and first["first_b10_receipt_sha256"] == digest(encode(previous)) and
            not end["receipts"] and end["states"] == {"EXPLORATION_CAPTURED": 1} and
            book["preflight"]["actual"] == {"data": 48, "account": 4}, "V26R_CROSS_RUN_NO_DUPLICATION")
    result_row = result["receipts"][first["request_id"]]
    require(result_row["proof"].get("independent_source_evidence_sha256") and not control["active"],
            "V26R_B09_TO_B10_PROOF")
    write_json(root, "ENGINE_TESTS.json", {"origin_run": issuer, "resume_run": target.run,
       "primary_requests": source, "source_comparison_requests": aux, "unique_verified_receipts": totals["VERIFIED"],
       "normal_batch_accepted": True, "broader_replay_first_b10_verified": True, "isolated_master_sequence": done["sequence"],
       "verified_raw_and_gzip_sha": True, "same_staged_request_adopted": True,
       "same_existing_receipt_no_duplicate": True, "exploration_saved_outside_bronze": True,
       "shared_book_rolling_limit": book["limit"], "shared_book_preflight_cap_untouched": True,
       "live_finmind_requests": 0}, access)


def report(access, sealed, issuer):
    root = integration_root(access, sealed, issuer)
    g = GitHub()
    run = int(os.environ["GITHUB_RUN_ID"])
    jobs = {}
    for ident in (issuer, run):
        rows = g.pages(f"/actions/runs/{ident}/attempts/1/jobs", "jobs")
        jobs[str(ident)] = {a["name"]: {k: a[k] for k in ("id", "status", "conclusion", "html_url")} for a in rows}
    lock = read_json_file(root, "LOCK.json", access)
    current = immutable_state(sealed)
    names = ("LOCK.json", "H_LINE.json", "CROSS_RUN_CHECKPOINT.json", "ENGINE_TESTS.json")
    refs = {}
    for name in names:
        try:
            fid, data = original(access, root, name)
            refs[name] = {"file_id": fid, "sha256": digest(data)}
        except (OSError, ValueError, RuntimeError):
            continue
    matches = current == lock["production"]
    complete = all(name in refs for name in names) and matches
    outcome = "READY_FOR_PRODUCTION_LAUNCH" if complete else "HOLD_WITH_EVIDENCE"
    check = read_json_file(root, "ENGINE_TESTS.json", access) if "ENGINE_TESTS.json" in refs else {}
    value = {"task_id": TASK_ID, "revision": "V2.6-R", "spec_sha256": SPEC_SHA, "result": outcome, "run_id": run,
        "attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]), "commit": os.environ["GITHUB_SHA"], "workflow": WORKFLOW,
        "baseline": {"run": BASE_RUN, "commit": BASE_COMMIT, "report_sha256": BASE_REPORT_SHA},
        "manifest": {"file_id": INDEX_FILE, "sha256": INDEX_SHA, "planned_requests": sealed.index["planned_request_count"]},
        "campaign": CAMPAIGN, "admission_requires_separate_user_record": True,
        "after_user_release": {"repository_variable": "R12R_ACTIVE_RELEASE_FILE_ID", "approval_folder": "00_SHARED_CONTEXT/PRODUCTION_RELEASES",
            "launch_command": f"gh workflow run {WORKFLOW} --repo danny541690/C1-PAID-DOWNLOADER --ref {BRANCH} -f mode=launch -f release_file=<APPROVED_RELEASE_FILE_ID>",
            "resume_command": f"gh workflow run {WORKFLOW} --repo danny541690/C1-PAID-DOWNLOADER --ref {BRANCH} -f mode=resume -f release_file=<APPROVED_RELEASE_FILE_ID>",
            "automatic_resume": "Real capacity/authorization and persisted B00/B10 conditions"},
        "origin_run": issuer, "jobs": jobs, "artifact_readbacks": refs, "formal_snapshot_match": matches,
        "production_snapshot": current["production"], "isolated_engine_evidence": check,
        "new_finmind_data_requests": 0, "new_finmind_account_requests": 0,
        "full_download_release": "HOLD", "production_dispatchable": 0}
    fid = write_json(root, PREFIX + ".json", value, access)
    from .final_readiness_v2 import write_markdown
    from types import SimpleNamespace

    write_markdown(SimpleNamespace(root=root, access=access), PREFIX + ".md", "\n".join((
        "# R12.1 V2.6-R 正式入口整合驗收", "", f"**{outcome}**", "",
        f"正式清冊 SHA `{INDEX_SHA}`。基準 Run `{BASE_RUN}`。",
        f"演練 Run `{issuer}`、接續 Run `{run}`，Commit `{value['commit']}`。",
        "正式放行維持 HOLD，正式派發為 0。", "")))
    link, raw = original(access, root, PREFIX + ".json")
    require(link == fid and digest(raw) == digest(encode(value)), "V26R_FINAL_READBACK")
    event = {"task_id": TASK_ID, "spec_sha256": SPEC_SHA, "revision": "V2.6-R", "run_id": run, "origin_run": issuer,
        "attempt": value["attempt"], "commit": value["commit"], "classification": "EVIDENCE",
        "report_file_id": fid, "report_sha256": digest(raw), "result": outcome, "full_download_release": "HOLD",
        "production_dispatchable": 0, "recorded_taipei": datetime.now(SNAPSHOT_ZONE).isoformat()}
    for event_type in ("HANDOVER", "TASK_EVENT", "DECISION_EVENT"):
        write_json(sealed.shared, f"{event_type}_{TASK_ID}_V2_6_R_{run}_1.json", event | {"event_type": event_type}, access)
    print(outcome, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=("lock", "handover", "first_checkpoint", "resumed_proof", "report"))
    parser.add_argument("--origin", type=int)
    args = parser.parse_args()
    access = token()
    sealed = Sealed(access)
    origin = origin_run(args.origin)
    globals()[args.step](access, sealed, origin)


if __name__ == "__main__":
    main()
