"""Run the Backer release checks together from verified private evidence."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
import json
import os
from pathlib import Path
from zipfile import ZipFile
from zoneinfo import ZoneInfo

from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .history_pilot import PREFIX as MONTHLY_PREFIX, verified_history_archive
from .live_preflight import drive_health, read_json
from .manifest_pilot import plan as manifest_plan, sample_data
from .market_lifetimes import digest, earlier_evidence
from .release_probes import RECOVERY_ROOT, read_private_file
from .test_archive import HISTORICAL_LAST, json_bytes, private_drive_folder
from .tpex_history import private_folder, save_private, verified_archive_files


INPUT_HISTORY_RUN = 45742569508
PREFIX = "R11_COMBINED_PREFLIGHT"
REPORT = Path("r11-combined-preflight.json")
REPORTS = {
    "acceptance": (45597304811, "r11-release-acceptance", "r11-release-acceptance.json", "pilot-verdict"),
    "integration": (45639721341, "r11-integration-verdict", "r11-integration-verdict.json",
                    "B10 independently reconcile G0-G5"),
    "manifest": (45650216439, "r11-manifest-readiness", "r11-manifest-readiness.json", "manifest-readback"),
    "capacity": (45651295300, "r11-history-capacity", "r11-history-capacity.json",
                 "Independently verify the historical archive and month index"),
    "pit": (45651767533, "r11-history-followup", "r11-history-capacity.json",
            "Reconcile the early TWSE calendar and monthly revenue"),
    "stock_gaps": (45649993338, "r11-release-remediation", "r11-release-remediation.json",
                   "Verify targeted Backer release improvements"),
}


def archived_report(archive, run_id, artifact, member, job_name):
    prefix = f"runs/{run_id}/"
    metadata = json.loads(archive.read(prefix + "evidence.json"))
    if metadata["run_id"] != run_id or metadata["outcome"] != "success" or metadata["unavailable_evidence"]:
        raise ValueError("required historic test evidence is incomplete")
    files = {item["path"]: item for item in metadata["files"]}

    def verified(name):
        receipt = files[name]
        raw = archive.read(prefix + name)
        if len(raw) != receipt["bytes"] or digest(raw) != receipt["sha256"]:
            raise ValueError("historic test file digest does not match evidence register")
        return raw

    attempt, = metadata["attempts"]
    jobs = json.loads(verified(f"attempt-{attempt}/jobs.json"))
    matching = [job for job in jobs if job["name"] == job_name and job["conclusion"] == "success"]
    job, = matching
    artifacts = json.loads(verified("artifacts.json"))
    selected, = [entry for entry in artifacts if entry["name"] == f"{artifact}-{attempt}"]
    with ZipFile(BytesIO(verified(f"artifacts/artifact-{selected['id']}.zip"))) as report:
        raw = report.read(member)
    return json.loads(raw), {"run_id": run_id, "attempt": attempt, "job_id": job["id"],
                             "artifact_id": selected["id"], "report_sha256": digest(raw)}


def archived_reports(access):
    folder = private_drive_folder(access)
    file_id = ensure(folder, f"R11_BACKER_History_Through_{HISTORICAL_LAST}.zip", "application/zip",
                     access, create=False)
    if read_json(f"{BASE}/files/{file_id}?fields=shared", access).get("shared") is not False:
        raise ValueError("historic test archive is shared")
    with ZipFile(BytesIO(read_private_file(file_id, access))) as archive:
        return {name: archived_report(archive, *lookup) for name, lookup in REPORTS.items()}


def monthly_original(access):
    run_id = REPORTS["capacity"][0]
    folder = ensure(RECOVERY_ROOT, f"{MONTHLY_PREFIX}_{run_id}_1", FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{folder}?fields=shared", access).get("shared") is not False:
        raise ValueError("historical Backer sample folder is shared")
    return verified_history_archive(access, folder)


def validated_pending_history(history):
    pending = history["pending_listing_ledger"]
    endings = {row["end_source"] for row in pending}
    counts = dict(Counter(row["market"] for row in pending))
    if (history["status"] != "PASS_DIAGNOSTIC" or history["production"] != "HOLD"
            or len(endings) != len(pending) or len(pending) != history["historic_intervals_pending_after"]
            or counts != history["pending_by_market"] or not set(counts) <= {"TPEx", "TWSE"}):
        raise ValueError("verified historical pending ledger does not reconcile")
    return {"pending": len(pending), "by_market": counts,
            "contradictory_years": history["historical_coverage_contradiction_years"],
            "reason_counts": dict(Counter(row["disposition"] for row in pending))}


def evidence_summary(receipts, history_bytes, monthly, drive, history_run):
    documents = {label: pair[0] for label, pair in receipts.items()}
    baseline, integration, stored_manifest, capacity, pit, gaps = (documents[label] for label in REPORTS)
    cases = baseline["cases"]
    crosswalk = dict(Counter(row["status"] for row in cases))
    if (baseline["summary"]["test_crosswalk"] != len(cases)
            or len({row["id"] for row in cases}) != len(cases)
            or baseline["summary"]["test_statuses"] != crosswalk or not baseline["summary"]["live_sweep"]):
        raise ValueError("archived release test crosswalk does not reconcile")
    if (integration["status"] != "PASS" or integration["production"] != "HOLD"
            or integration["gates_fixture"] != {f"G{i}": "PASS" for i in range(6)}
            or integration["workstreams"] != 11 or integration["recovered_tasks"] < 1
            or integration["held_tasks"] < 1 or integration["archives_read_back"] != integration["private_receipts"]):
        raise ValueError("archived cross-runner integration proof does not match the pilot contract")
    preview = manifest_plan()
    if preview["fixture"] != stored_manifest["fixture"] or preview["production_dispatchable"] != 0:
        raise ValueError("existing certified manifest fixture did not reproduce")
    if (monthly["indexed_months"] != capacity["storage"]["indexed_months"]
            or monthly["compressed_bytes"] != capacity["storage"]["monthly_original_gzip_bytes"]
            or monthly["indexed_months"] != capacity["historical_business"]["verified_months"]
            or capacity["historical_business"]["status"] != "PASS_PROVIDER_WINDOW"):
        raise ValueError("actual Backer original does not match the historic coverage report")
    if pit["revenue_pit"]["status"] != "PASS_PIT_PUBLICATION_DATE_SAMPLE":
        raise ValueError("archived monthly publication test has no verified evidence")
    history = validated_pending_history(json.loads(history_bytes))
    rows = sample_data()
    roster = dict(Counter(row["state"] for row in rows))
    case_holds = [row["id"] for row in cases if row["status"] in {"HOLD", "PARTIAL"}]
    scope_holds = sorted(row["dataset"] for row in rows if row["state"] == "HOLD_SCOPE_REVIEW")
    summarized = {
        "status": "PASS_DIAGNOSTIC", "production": "HOLD", "production_dispatchable": 0,
        "history_run": history_run, "new_finmind_requests": 0, "drive_check": drive["status"],
        "historical_pending": history["pending"], "historical_pending_by_market": history["by_market"],
        "historical_contradictory_years": history["contradictory_years"],
        "unresolved_backer_price_samples": gaps["historical_tpex"]["missing_from_bulk"],
        "dataset_states": roster, "monthly_original_indexed_months": monthly["indexed_months"],
        "scoped_release_check": "HOLD_PENDING_REQUIRED_PRODUCTION_EVIDENCE",
        "reused_acceptance_cases": len(cases), "baseline_acceptance_statuses": crosswalk,
        "verified_test_artifacts": len(receipts), "pilot_workstreams_verified": integration["workstreams"],
    }
    private = {
        "prior_test_sources": {label: pair[1] for label, pair in receipts.items()},
        "history_ledger_sha256": digest(history_bytes), "history_ledger": history,
        "monthly_archive_readback": monthly, "drive_measurement": drive,
        "scope_hold_datasets": scope_holds, "datasets": rows, "prior_release_case_ids_to_reconcile": case_holds,
        "prior_release_cases": cases, "manifest_test": preview["fixture"],
        "remaining": {
            "market_coverage": "HOLD_OFFICIAL_LISTING_BOUNDARIES_AND_BACKER_GAPS",
            "scope": "HOLD_RESEARCH_SCOPE_APPROVALS",
            "dataset_contracts": "HOLD_APPROVED_DATASET_ERA_CONTRACTS_AND_LIVE_COVERAGE",
            "subscription_expiry": capacity["expiration"],
            "capacity": capacity["storage"]["full_volume"],
            "production_manifest_and_workers": "HOLD_PRODUCTION_MANIFEST_AND_DEPLOYMENT",
            "production_acceptance": "HOLD_REAL_MANIFEST_G0_G5",
        },
    }
    return summarized, private


def run(history_run, *, verify_only):
    access = drive_token()
    expected = None
    if verify_only:
        archived = verified_archive_files(access, private_folder(access, create=False, prefix=PREFIX))
        expected = json.loads(archived["reconciliation.json"])
        if expected["history_run"] != history_run:
            raise ValueError("readback must use the same historical ledger")
    with ThreadPoolExecutor(max_workers=4) as executor:
        evidence = executor.submit(archived_reports, access)
        history = executor.submit(earlier_evidence, history_run)
        monthly = executor.submit(monthly_original, access)
        if expected is None:
            drive = drive_health(access) | {"measured_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat()}
        else:
            drive = expected["drive_measurement"]
        summary, private = evidence_summary(evidence.result(), history.result()["reconciliation.json"],
                                            monthly.result(), drive, history_run)
    result = summary | private
    if expected is not None:
        if json_bytes(result) != json_bytes({key: expected[key] for key in result}):
            raise ValueError("Drive readback did not reproduce the combined release checks")
        return summary
    save_private({}, {}, {}, result, prefix=PREFIX)
    return summary | {"private_archive": "STAGED"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-run", type=int, default=INPUT_HISTORY_RUN)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        report = run(args.history_run, verify_only=args.verify_only)
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
    if not args.verify_only:
        REPORT.write_bytes(json_bytes(report))
        if out := os.environ.get("GITHUB_OUTPUT"):
            with open(out, "a", encoding="utf-8") as handle:
                handle.write(f"archive={report.get('private_archive', 'HOLD')}\n")
    elif report["status"] == "PASS_DIAGNOSTIC":
        print("combined_preflight_drive_readback=PASS")
    print(json.dumps(report, sort_keys=True))
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
