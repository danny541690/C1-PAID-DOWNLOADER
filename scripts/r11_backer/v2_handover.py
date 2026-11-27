"""Independently verify the Codex Drive handoff for the R12 V2 gate."""

import json
from pathlib import Path
import re

from .drive_pilot import FOLDER, ROOT, ensure, find
from .integration_pilot import digest, encode, verify_private_folder
from .release_probes import read_private_file
from .release_rehearsal import RehearsalError, require


TASK_ID = "R12-FINAL-READINESS-V2-01"
ROUNDTRIP_ID = "H-R12-ROUNDTRIP-20261127-01"
ROUNDTRIP = "H_R12_ROUNDTRIP_20261127_01.json"
RUNNER_RECEIPT = "H_R12_RUNNER_RECEIPT_45973149519_1.json"
DIRECT_ACK = "H_CODEX_DIRECT_ACK.json"
SOP_NAME = "JARVIS_V098_PLAN_B_SOP_V1_20261127.md"
ROUNDTRIP_SHA = "8ec7bb6605920241ef89b8f9c989c511a5ea0f773164ebf4a9c5c944be2e48b4"
DIRECT_ACCESS = "GOOGLE_DRIVE_CONNECTOR_DIRECT"
GOVERNANCE = "JARVIS-GOV-HANDOVER-B-001 V1.0"
RULE_CHECKS = {
    "governance": ("D-006", GOVERNANCE),
    "workspace_entrypoints": ("AI_SHARED_CONTEXT/", "00_SHARED_CONTEXT/START_HERE.md", "FINAL_READINESS/"),
    "historical_archives": ("R11_BACKER/TEST_RUNS/", "99_RECOVERY"),
    "handover_idempotency": ("task_id", "固定事件鍵", "append-only"),
    "evidence_attribution": ("Codex 直接 ACK", "GitHub bridge", "VERBATIM", "SUMMARY", "EVIDENCE"),
    "production_guard": ("full_download_release=HOLD", "production_dispatchable=0"),
}


def read_only_path(access, *names):
    parent = ROOT
    verify_private_folder(parent, access)
    for name in names:
        parent = ensure(parent, name, FOLDER, access, create=False)
        verify_private_folder(parent, access)
    return parent


def read_private_bytes(parent, name, mime, access):
    file_id = ensure(parent, name, mime, access, create=False)
    verify_private_folder(file_id, access)
    return read_private_file(file_id, access)


def check_prior_evidence(challenge, receipt):
    input_sha = digest(challenge)
    require(input_sha == ROUNDTRIP_SHA, "H_CHALLENGE_DIGEST")
    event = json.loads(challenge)
    require(event.get("task_id") == ROUNDTRIP_ID and event.get("origin") == "ChatGPT", "H_CHALLENGE_BINDING")
    runner = json.loads(receipt)
    expected = {
        "task_id": ROUNDTRIP_ID,
        "input_sha256": input_sha,
        "writer": "GitHub Actions",
        "run_id": "45973149519",
        "attempt": "1",
        "commit": "8d51bebdefb803599f2ec0fa125ea9aff66aa14f",
        "result": "PASS_RUNNER_READ_WRITE",
        "codex_direct_ack": "NOT_TESTED",
        "finmind_requests": 0,
    }
    require(all(runner.get(field) == value for field, value in expected.items()), "H_PRIOR_RUNNER_BINDING")
    return input_sha, digest(receipt)


def ack_fields(spec_sha, input_sha, receipt_sha, sop_sha):
    require(all(re.fullmatch(r"[a-f0-9]{64}", value) for value in (spec_sha, input_sha, receipt_sha, sop_sha)),
            "H_INVALID_HASH")
    identity = [TASK_ID, ROUNDTRIP_ID, DIRECT_ACCESS, spec_sha, input_sha]
    return {
        "event_id": digest(encode(identity)),
        "task_id": TASK_ID,
        "roundtrip_task_id": ROUNDTRIP_ID,
        "decision_id": "D-006",
        "governance": GOVERNANCE,
        "spec_sha256": spec_sha,
        "input_sha256": input_sha,
        "runner_receipt_sha256": receipt_sha,
        "sop_sha256": sop_sha,
        "writer": "Codex",
        "access_path": DIRECT_ACCESS,
        "state": "ACKNOWLEDGED",
        "finmind_requests": 0,
    }


def verify_ack(raw, expected):
    ack = json.loads(raw)
    require(all(ack.get(field) == value for field, value in expected.items()), "H_DIRECT_ACK_BINDING")
    require(re.fullmatch(r"[a-f0-9]{40}", ack.get("engineering_base_commit", "")), "H_DIRECT_ACK_BASE")
    return ack


def agents_test():
    raw = (Path(__file__).resolve().parents[2] / "AGENTS.md").read_bytes()
    instructions = raw.decode("utf-8")
    checks = {name: all(term in instructions for term in terms) for name, terms in RULE_CHECKS.items()}
    return {"status": "PASS" if all(checks.values()) else "HOLD", "agents_sha256": digest(raw), "checks": checks}


def run(ctx):
    access = ctx.access
    agents = agents_test()
    result = {"status": "HOLD", "task_id": TASK_ID, "roundtrip_task_id": ROUNDTRIP_ID,
              "codex_direct_ack": "NOT_VERIFIED", "chatgpt_readback": "FOLLOW_UP_NOT_PERFORMED",
              "finmind_requests": 0, "production_mutations": 0}
    blockers = []
    try:
        shared = read_only_path(access, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT")
        global_context = read_only_path(access, "00_COMMAND", "AI_SHARED_CONTEXT")
        challenge = read_private_bytes(shared, ROUNDTRIP, "application/json", access)
        receipt = read_private_bytes(shared, RUNNER_RECEIPT, "application/json", access)
        input_sha, receipt_sha = check_prior_evidence(challenge, receipt)
        global_sop = read_private_bytes(global_context, SOP_NAME, "text/markdown", access)
        project_sop = read_private_bytes(shared, SOP_NAME, "text/markdown", access)
        require(global_sop == project_sop and b"JARVIS-GOV-HANDOVER-B-001" in global_sop and b"V1.0" in global_sop,
                "H_SOP_VERSION")
        sop_sha = digest(global_sop)
        result.update(input_sha256=input_sha, runner_receipt_sha256=receipt_sha,
                      historical_runner_roundtrip="VERIFIED", sop_sha256=sop_sha)
        verify_private_folder(ctx.task_root, access)
        require(find(ctx.task_root, DIRECT_ACK, access) is not None, "H_DIRECT_ACK_UNAVAILABLE")
        raw = read_private_bytes(ctx.task_root, DIRECT_ACK, "application/json", access)
        expected = ack_fields(ctx.spec_sha, input_sha, receipt_sha, sop_sha)
        ack = verify_ack(raw, expected)
        result.update(codex_direct_ack="PASS_DIRECT_CODEX", independent_runner_readback="PASS",
                      ack_sha256=digest(raw), event_id=ack["event_id"],
                      engineering_base_commit=ack["engineering_base_commit"])
    except RehearsalError as exc:
        blockers.append(str(exc))
    except (KeyError, ValueError, OSError, RuntimeError, TypeError) as exc:
        blockers.append(f"H_EVIDENCE_READ_{type(exc).__name__}")
    if agents["status"] != "PASS":
        blockers.append("H_AGENTS_RULES_INCOMPLETE")
    result.update(status="HOLD" if blockers else "PASS", blockers=blockers)
    return {"status": result["status"], "blockers": blockers,
            "outputs": {"H_ACK.json": result, "AGENTS_TEST.json": agents}}
