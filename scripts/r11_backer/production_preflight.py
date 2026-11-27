"""Read-only R12.1 production prerequisites. This command never reserves FinMind quota."""

from collections import Counter
from datetime import datetime, timezone
import json
import math
import re

from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .integration_pilot import verify_private_folder
from .live_preflight import drive_health
from .manifest_pilot import manifest_windows, sample_data
from .quota import NORMAL_LIMIT
from .release_probes import RECOVERY_ROOT, read_private_file, recovery_location
from .release_readiness import BOOK, DOCS_MEMBER, PREFIX, scope_register, specs
from .tpex_history import verified_archive_files

READINESS_RUN = 45784712597
FOREIGN_EQUITIES = {
    f"{market}Stock{suffix}" for market in ("US", "UK", "Europe", "Japan") for suffix in ("Info", "Price")
}
R12_SPEC = "R12_1_BACKER_DOWNLOAD_ONLY_20261126.md"
FINAL_GATES = tuple(f"G{number:02d}" for number in range(9))


def final_readiness_verdict(gates):
    """Evaluate evidenced technical readiness independently of release authorization."""
    unresolved = [gate for gate in FINAL_GATES if gates.get(gate) != "PASS"]
    return {
        "result": "HOLD_WITH_EVIDENCE" if unresolved else "READY_FOR_USER_RELEASE",
        "unresolved_gates": unresolved,
        "passed_gates": len(FINAL_GATES) - len(unresolved),
        "expected_gates": len(FINAL_GATES),
        "full_download_release": "HOLD",
        "production_dispatchable": 0,
    }


def positive_int(value):
    return type(value) is int and value > 0


def reconciled_catalog(access, management):
    """Recompute the scope decision using the same verified evidence as R11."""
    name = recovery_location(access, prefix=PREFIX, run_id=READINESS_RUN, attempt=1)
    parent = ensure(RECOVERY_ROOT, name, FOLDER, access, create=False)
    verify_private_folder(parent, access)
    archived = verified_archive_files(access, parent)
    saved = json.loads(archived["reconciliation.json"])
    rows = scope_register(specs(access), archived[f"official/{DOCS_MEMBER}"].decode("utf-8"))
    reference = {row["dataset"]: row["state"] for row in saved["datasets"]}
    if reference != {row["dataset"]: row["state"] for row in rows}:
        raise ValueError("dataset scope archive disagrees with the current register")
    spec = ensure(management, R12_SPEC, "text/markdown", access, create=False)
    verify_private_folder(spec, access)
    archived_counts = dict(Counter(row["state"] for row in rows))
    return resolve_r12_scope(rows, read_private_file(spec, access).decode("utf-8-sig")), archived_counts


def resolve_r12_scope(rows, specification):
    """Apply the R12 exclusion to the eight foreign equity datasets in the R11 review set."""
    reviews = {row["dataset"] for row in rows if row["state"] == "HOLD_SCOPE_REVIEW"}
    if not re.search(r"排除[^。\n]*境外個股", specification) or not FOREIGN_EQUITIES <= reviews:
        raise ValueError("R12 foreign equity scope requires review")
    return [row | {"state": "EXCLUDED_BY_SCOPE"} if row["dataset"] in FOREIGN_EQUITIES else row for row in rows]


def evaluate(plan, available_bytes, now, *, catalog_rows=None, gate_results=None):
    """Compute capacity only from explicitly approved production input."""
    if gate_results is not None:
        return final_readiness_verdict(gate_results)
    catalog = {entry["dataset"]: entry for entry in (sample_data() if catalog_rows is None else catalog_rows)}
    result = {
        "states": dict(Counter(entry["state"] for entry in catalog.values())),
        "production": "HOLD", "production_dispatchable": 0,
        "finmind_requests": 0,
    }
    findings = ["PRODUCTION_WORKFLOW_AND_PERSISTENT_LEDGER_NOT_CONNECTED"]
    result["blockers"] = findings
    if plan is None:
        findings.extend(("APPROVED_PRODUCTION_MANIFEST_MISSING", "BACKER_EXPIRY_UNVERIFIED",
                         "FULL_VOLUME_REQUESTS_STORAGE_AND_DURATION_UNVERIFIED"))
        return result

    approvals = plan["approvals"]
    for entry in approvals:
        item = catalog.get(entry["dataset"])
        if (item is None or entry["dataset"] == BOOK or entry["dataset"] in FOREIGN_EQUITIES
                or item["state"].startswith("EXCLUDED")
                or item["routing"] not in (entry["lane"], "REVIEW")):
            raise ValueError("out-of-scope manifest row")
    roster = plan["dataset_dispositions"]
    approved = {row["dataset"] for row in approvals}
    if roster.keys() != catalog.keys() or approved != {
        dataset for dataset, disposition in roster.items() if disposition["status"] == "APPROVED"
    } or any(not entry.get("evidence") or entry.get("status") not in
             {"APPROVED", "EXCLUDED", "BLOCKED"} for entry in roster.values()):
        findings.append("DATASET_DISPOSITIONS_INCOMPLETE")
    windows = manifest_windows(approvals)
    result["planned_windows"] = len(windows)
    if {window.dataset for window in windows} != approved:
        findings.append("APPROVED_DATASET_WITHOUT_VERIFIED_WINDOWS")
    if not windows:
        findings.append("NO_VERIFIED_WINDOWS")
    estimates = plan.get("window_estimates", {})
    expiry_text = plan.get("backer_expires_at")
    try:
        expiry = datetime.fromisoformat(expiry_text) if expiry_text else None
    except (TypeError, ValueError):
        expiry = None
    if expiry is None or expiry.tzinfo is None or not plan.get("backer_expiry_evidence") or expiry <= now:
        expiry = None
        findings.append("BACKER_EXPIRY_UNVERIFIED_OR_PASSED")
    safe_rate = plan.get("safe_hourly_requests")
    if not positive_int(safe_rate) or safe_rate > NORMAL_LIMIT or not plan.get("sustained_rate_evidence"):
        findings.append("SAFE_THROUGHPUT_UNVERIFIED")
        safe_rate = None
    if {window.task_id for window in windows} != estimates.keys() or any(
        not positive_int(item.get("requests")) or not positive_int(item.get("storage_bytes"))
        or not item.get("evidence") for item in estimates.values()
    ):
        findings.append("FULL_VOLUME_REQUESTS_STORAGE_AND_DURATION_UNVERIFIED")
        return result

    requests = sum(item["requests"] for item in estimates.values())
    storage = sum(item["storage_bytes"] for item in estimates.values())
    result["estimated_request_count"] = requests
    if available_bytes is None or available_bytes < storage:
        findings.append("DRIVE_HEADROOM_INSUFFICIENT_OR_UNVERIFIED")
    if safe_rate is not None:
        result["estimated_hours_lower_bound"] = math.ceil(requests / safe_rate)
        if expiry is not None and requests / safe_rate > (expiry - now).total_seconds() / 3600:
            findings.append("SUBSCRIPTION_TIME_INSUFFICIENT")
    return result


def main():
    try:
        access = token()
        health = drive_health(access)
        verify_private_folder(ROOT, access)
        command = ensure(ROOT, "00_COMMAND", FOLDER, access, create=False)
        verify_private_folder(command, access)
        parent = ensure(command, "R12_BACKER", FOLDER, access, create=False)
        verify_private_folder(parent, access)
        name = "R12_1_APPROVED_PRODUCTION_INPUT.json"
        plan = None
        if find(parent, name, access):
            file_id = ensure(parent, name, "application/json", access, create=False)
            verify_private_folder(file_id, access)
            plan = json.loads(read_private_file(file_id, access))
        headroom = health["available_bytes"] if health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" else None
        catalog, archived_counts = reconciled_catalog(access, parent)
        report = evaluate(plan, headroom, datetime.now(timezone.utc), catalog_rows=catalog)
        report["scope_resolution"] = {
            "subdaily_excluded": BOOK, "evidence_run": READINESS_RUN,
            "archived_states": archived_counts, "r12_foreign_equity_exclusions": len(FOREIGN_EQUITIES),
        }
        report["drive_capacity_check"] = health["status"]
        print(json.dumps(report, sort_keys=True))
    except Exception as error:
        print(f"R12_PRODUCTION_PREFLIGHT_FAILED class={type(error).__name__}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
