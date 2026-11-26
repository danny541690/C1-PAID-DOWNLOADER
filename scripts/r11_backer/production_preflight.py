"""Read-only R12.1 production prerequisites. This command never reserves FinMind quota."""

from collections import Counter
from datetime import datetime, timezone
import json
import math

from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .integration_pilot import verify_private_folder
from .live_preflight import drive_health
from .manifest_pilot import manifest_windows, sample_data
from .release_probes import read_private_file


def positive_int(value):
    return type(value) is int and value > 0


def evaluate(plan, available_bytes, now):
    """Compute capacity only from explicitly approved production input."""
    catalog = {entry["dataset"]: entry for entry in sample_data()}
    result = {
        "states": dict(Counter(entry["state"] for entry in catalog.values())),
        "production": "HOLD", "production_dispatchable": 0,
        "finmind_requests": 0,
    }
    findings = ["PRODUCTION_WORKFLOW_AND_PERSISTENT_LEDGER_NOT_CONNECTED"]
    result["blockers"] = findings
    if not plan:
        findings.extend(("APPROVED_PRODUCTION_MANIFEST_MISSING", "BACKER_EXPIRY_UNVERIFIED",
                         "FULL_VOLUME_REQUESTS_STORAGE_AND_DURATION_UNVERIFIED"))
        return result

    approvals = plan["approvals"]
    for entry in approvals:
        item = catalog.get(entry["dataset"])
        if item is None or item["state"].startswith("EXCLUDED") or item["routing"] not in (entry["lane"], "REVIEW"):
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
    if not windows:
        findings.append("NO_VERIFIED_WINDOWS")
    estimates = plan["window_estimates"]
    if {window.task_id for window in windows} != estimates.keys() or any(
        not positive_int(item.get("requests")) or not positive_int(item.get("storage_bytes"))
        or not item.get("evidence") for item in estimates.values()
    ):
        findings.append("FULL_VOLUME_REQUESTS_STORAGE_AND_DURATION_UNVERIFIED")
        return result

    expiry = datetime.fromisoformat(plan["backer_expires_at"])
    if expiry.tzinfo is None or not plan.get("backer_expiry_evidence") or expiry <= now:
        findings.append("BACKER_EXPIRY_UNVERIFIED_OR_PASSED")
        return result
    safe_rate = plan["safe_hourly_requests"]
    if not positive_int(safe_rate) or not plan.get("sustained_rate_evidence"):
        findings.append("SAFE_THROUGHPUT_UNVERIFIED")
        return result
    requests = sum(item["requests"] for item in estimates.values())
    storage = sum(item["storage_bytes"] for item in estimates.values())
    result["estimated_request_count"] = requests
    result["estimated_hours_lower_bound"] = math.ceil(requests / safe_rate)
    if available_bytes is None or available_bytes < storage:
        findings.append("DRIVE_HEADROOM_INSUFFICIENT_OR_UNVERIFIED")
    if requests / safe_rate > (expiry - now).total_seconds() / 3600:
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
        report = evaluate(plan, headroom, datetime.now(timezone.utc))
        report["drive_capacity_check"] = health["status"]
        print(json.dumps(report, sort_keys=True))
    except Exception as error:
        print(f"R12_PRODUCTION_PREFLIGHT_FAILED class={type(error).__name__}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
