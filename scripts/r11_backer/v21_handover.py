"""Verify V2.1's inherited private H-line evidence without issuing a new ACK."""

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
from zipfile import BadZipFile, ZipFile

from . import v2_handover as handover
from .drive_pilot import FOLDER, ensure
from .integration_pilot import digest, verify_private_folder
from .release_rehearsal import RehearsalError, require


SPEC_SHA = "c83f8916d9559cba7638193bd5e206498f085ba8648337a685aeb2a5548b4271"
PRIOR_SPEC_SHA = "d80cb0525c8f95fcb05763077cb8359f7839ec864abf3ffd60cec2286b5b4779"
PRIOR_COMMIT = "4df007ddbe6c3f25eddd6ed7f3ac1599a2ced25c"
PRIOR_RUN, PRIOR_ATTEMPT = 46030259242, 1
PRIOR_ARCHIVE = f"GITHUB_ACTIONS_{PRIOR_RUN}_{PRIOR_ATTEMPT}.zip"
PRIOR_SHA = {
    "FINAL_READINESS.json": "049a250350455e819c3159731447f2f6e40548d94f664b69b9499fc9b0806611",
    "SCOPE_HOLDS.json": "9d820af450df02d10d86b50b66cf5a31983b546c96756249fe1e59082c47348d",
    "COVERAGE_GAPS.json": "5875d27770f472388bdc5d403ddd11d559bfee4faea61e017b0a85372d4ad7d1",
    "RESOURCE_BUDGET.json": "d2d2820c0e5d45ae6369e258c9b45a96f636a3e0fd60ffbbf9a5af8727ace466",
    "H_ACK.json": "51d89ca43c924cebef33c8381bcd3833a731130b3634be3320670a81dbcdb7e6",
    "G01.json": "4c8109a7837a007dbd0040cd54299e35de6158bb9bdec0147be4c3014d0d43ed",
    "AGENTS_TEST.json": "64ed8ae37f5f185a50d131ba47cfea9785e4303960a3327af998504fff6bb957",
    "INPUT_LOCK.json": "456169efe3573e88f116c56b7e84332869860bca48046a9a6c0a5f9ceccf5ecc",
    PRIOR_ARCHIVE: "3504c384093d3924cc89ec7e306546445080aea84ef768b5581fe97dd9dc475a",
}
BOUND_OUTPUTS = {
    "INPUT_LOCK.json": "G00", "H_ACK.json": "G01", "AGENTS_TEST.json": "G01",
    "SCOPE_HOLDS.json": "G03", "COVERAGE_GAPS.json": "G04", "RESOURCE_BUDGET.json": "G05",
}


def private_descend(access, root, *names):
    verify_private_folder(root, access)
    for name in names:
        root = ensure(root, name, FOLDER, access, create=False)
        verify_private_folder(root, access)
    return root


def verify_run_archive(raw, github_jobs):
    with ZipFile(BytesIO(raw)) as archive:
        members = archive.infolist()
        require(len(members) == 12 and all(member.file_size <= 1024**2 for member in members), "H21_ARCHIVE_BOUNDS")
        require(len({member.filename for member in members}) == len(members), "H21_ARCHIVE_DUPLICATE_PATH")
        run = json.loads(archive.read("run.json"))
        index = json.loads(archive.read("archive.json"))
        require(run["headSha"] == PRIOR_COMMIT and run["status"] == "completed" and
                run["conclusion"] == "success" and run["url"].endswith(f"/actions/runs/{PRIOR_RUN}"),
                "H21_ARCHIVED_RUN_BINDING")
        jobs = run["jobs"]
        require(len(jobs) == len(github_jobs) == 10 and not index["missing_job_logs"], "H21_ARCHIVED_JOBS_MISSING")
        require({job["name"] for job in jobs} == github_jobs.keys(), "H21_ARCHIVED_JOB_SET")
        logs = index["logs_sha256"]
        expected_logs = {f"{job['name']}_{job['databaseId']}.log" for job in jobs}
        require(set(logs) == expected_logs and set(archive.namelist()) == expected_logs | {"run.json", "archive.json"},
                "H21_ARCHIVED_LOG_SET")
        for job in jobs:
            require(job["databaseId"] == github_jobs[job["name"]]["id"] and job["status"] == "completed" and
                    job["conclusion"] == "success", "H21_ARCHIVED_JOB_BINDING")
        require(all(digest(archive.read(name)) == sha for name, sha in logs.items()), "H21_ARCHIVED_LOG_DIGEST")
    return len(jobs)


def verify_prior_payloads(files):
    hashes = {name: digest(files[name]) for name in PRIOR_SHA}
    require(hashes == PRIOR_SHA, "H21_PRIOR_EVIDENCE_DIGEST")
    report = json.loads(files["FINAL_READINESS.json"])
    expected = {"task_id": handover.TASK_ID, "spec_sha256": PRIOR_SPEC_SHA, "commit": PRIOR_COMMIT,
                "run_id": PRIOR_RUN, "attempt": PRIOR_ATTEMPT, "result": "HOLD_WITH_EVIDENCE",
                "full_download_release": "HOLD", "production_dispatchable": 0}
    require(all(report.get(name) == value for name, value in expected.items()), "H21_PRIOR_FINAL_BINDING")
    require(report["gates"]["G01"] == json.loads(files["G01.json"]) and
            report["gates"]["G01"]["status"] == "PASS", "H21_PRIOR_G01_BINDING")
    for name, gate in BOUND_OUTPUTS.items():
        require(report["gates"][gate]["output_sha256"].get(name) == hashes[name], "H21_PRIOR_OUTPUT_BINDING")
    jobs = verify_run_archive(files[PRIOR_ARCHIVE], report["github_jobs"])
    return hashes, jobs


def run(ctx):
    blockers = []
    lock = {"task_id": handover.TASK_ID, "spec_sha256": ctx.spec_sha, "status": "HOLD",
            "inherited_from": {"run_id": PRIOR_RUN, "attempt": PRIOR_ATTEMPT, "commit": PRIOR_COMMIT,
                               "spec_sha256": PRIOR_SPEC_SHA}}
    ack_report = {"status": "HOLD", "mode": "INHERITED_READ_ONLY", "new_ack_test_performed": False,
                  "finmind_requests": 0, "production_mutations": 0}
    agents_report = {"status": "NOT_RUN"}
    try:
        require(ctx.spec_sha == SPEC_SHA, "H21_SPEC_BINDING")
        access = ctx.access
        verify_private_folder(ctx.task_root, access)
        prior_spec = private_descend(access, ctx.shared, "FINAL_READINESS", handover.TASK_ID, PRIOR_SPEC_SHA)
        prior_root = private_descend(access, prior_spec, f"{PRIOR_RUN}_{PRIOR_ATTEMPT}")

        def read_prior(name):
            mime = "application/zip" if name == PRIOR_ARCHIVE else "application/json"
            return handover.read_private_bytes(prior_root, name, mime, access)

        with ThreadPoolExecutor(max_workers=4) as pool:
            files = dict(zip(PRIOR_SHA, pool.map(read_prior, PRIOR_SHA), strict=True))
        lock["sha256"], lock["archived_jobs"] = verify_prior_payloads(files)
        old_ack = json.loads(files["H_ACK.json"])
        old_agents = json.loads(files["AGENTS_TEST.json"])
        require(old_ack.get("status") == "PASS" and old_ack.get("codex_direct_ack") == "PASS_DIRECT_CODEX",
                "H21_PRIOR_ACK_STATUS")

        global_context = handover.read_only_path(access, "00_COMMAND", "AI_SHARED_CONTEXT")
        global_sop = handover.read_private_bytes(global_context, handover.SOP_NAME, "text/markdown", access)
        project_sop = handover.read_private_bytes(ctx.shared, handover.SOP_NAME, "text/markdown", access)
        require(global_sop == project_sop and digest(global_sop) == old_ack["sop_sha256"], "H21_SOP_CHANGED")
        raw_ack = handover.read_private_bytes(prior_spec, handover.DIRECT_ACK, "application/json", access)
        require(digest(raw_ack) == old_ack["ack_sha256"], "H21_ACK_CHANGED")
        expected = handover.ack_fields(PRIOR_SPEC_SHA, old_ack["input_sha256"], old_ack["runner_receipt_sha256"],
                                       old_ack["sop_sha256"])
        ack = handover.verify_ack(raw_ack, expected)
        require(ack["event_id"] == old_ack["event_id"], "H21_ACK_EVENT")
        current_agents = handover.agents_test()
        require(current_agents["status"] == "PASS" and current_agents == old_agents, "H21_AGENTS_CHANGED")
        agents_report = current_agents | {"mode": "INHERITED_UNCHANGED_HASH"}
        ack_report.update(status="PASS", inherited_ack_sha256=old_ack["ack_sha256"],
                          inherited_ack_spec_sha256=PRIOR_SPEC_SHA, sop_sha256=old_ack["sop_sha256"],
                          chatgpt_readback=old_ack["chatgpt_readback"], event_id=ack["event_id"])
    except RehearsalError as exc:
        blockers.append(str(exc))
    except (BadZipFile, KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        blockers.append(f"H21_EVIDENCE_READ_{type(exc).__name__}")
    lock.update(status="HOLD" if blockers else "PASS", blockers=blockers)
    return {"status": lock["status"], "blockers": blockers, "inherited_from": lock["inherited_from"],
            "outputs": {"PRIOR_EVIDENCE_LOCK.json": lock, "H_ACK.json": ack_report, "AGENTS_TEST.json": agents_report}}
