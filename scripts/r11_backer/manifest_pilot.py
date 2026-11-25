"""Freeze and independently verify a newest-first release manifest preview."""

import argparse
from collections import Counter
from contextlib import closing
from dataclasses import asdict
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from .dispatch import LocalLedger, Window, assignments
from .preflight import read_catalog

EVIDENCE_PATH = Path(__file__).with_name("release_evidence.json")
REQUIRED_PROOF = (
    "entitlement", "coverage_bounds", "dataset_contract",
    "market_identity", "query_mode",
)


def monthly_windows(dataset, lane, scope, start, end, contract):
    if start > end:
        raise ValueError("start date is later than the approved end date")
    cursor = end
    while cursor >= start:
        first = max(start, cursor.replace(day=1))
        yield Window(dataset, scope, first, cursor, contract, lane)
        if first == start:
            break
        cursor = first - timedelta(days=1)


def manifest_windows(approvals):
    windows = []
    latest = {}
    for entry in approvals:
        evidence = entry["evidence"]
        if any(not evidence.get(name) for name in REQUIRED_PROOF):
            raise ValueError("missing release evidence")
        start, end = map(date.fromisoformat, (entry["start"], entry["end"]))
        if start > end:
            raise ValueError("invalid approved range")
        listing_start, listing_end = map(
            date.fromisoformat, (entry["listing_start"], entry["listing_end"])
        )
        if listing_start > listing_end:
            raise ValueError("invalid listing period")
        window_start, window_end = max(start, listing_start), min(end, listing_end)
        if window_start > window_end:
            continue
        latest[entry["dataset"]] = max(end, latest.get(entry["dataset"], end))
        windows.extend(monthly_windows(
            entry["dataset"], entry["lane"], entry["scope"],
            window_start, window_end, entry["contract"],
        ))
    return assignments(windows, latest)


def sample_data():
    catalog = read_catalog()
    evidence = json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))
    if set(evidence) != {row["dataset"] for row in catalog}:
        raise ValueError("source evidence does not match the roster")
    rows = []
    for item in catalog:
        result = evidence[item["dataset"]]
        if type(result["run"]) is not int or result["run"] <= 0:
            raise ValueError("source results require a run reference")
        status = result["status"]
        if status == "VALID_SAMPLE":
            status = "SAMPLE_ONLY"
        rows.append(item | {"state": status, "evidence_run": result["run"]})
    return rows


def fixture_approvals(start):
    return [
        {
            "dataset": f"SyntheticLane{lane}", "lane": f"B{lane:02}", "scope": f"fixture-{symbol}",
            "start": start, "end": "2026-11-24", "listing_start": "1989-03-15",
            "listing_end": "2026-11-24", "contract": "synthetic-contract-v1",
            "evidence": {field: f"synthetic-{field}" for field in REQUIRED_PROOF},
        }
        for lane in range(1, 9)
        for symbol in range(2)
    ]


def resume_test(windows, expanded):
    """Persist synthetic receipts and verify that a restart reuses them."""
    with TemporaryDirectory() as temp:
        ledger_path = Path(temp) / "fixture.db"
        with closing(LocalLedger(ledger_path)) as original:
            original.seed(windows)
            first = original.claim("B01", now=1, lease_seconds=5)
            interrupted = original.claim("B01", now=1, lease_seconds=5)
            if not first or not interrupted or not original.accept(*first, now=2, receipt="fixture-first"):
                raise ValueError("synthetic start failed")
        with closing(LocalLedger(ledger_path)) as reopened:
            reopened.seed(expanded)
            takeover = reopened.claim("B01", now=6, lease_seconds=5)
            if not takeover or takeover[0] != interrupted[0] or takeover[1] <= interrupted[1]:
                raise ValueError("interrupted task was not safely reclaimed")
            if reopened.accept(*interrupted, now=7, receipt="stale-fixture"):
                raise ValueError("stale completion was accepted")
            if not reopened.accept(*takeover, now=7, receipt="fixture-takeover"):
                raise ValueError("new generation could not record progress")
            completed = reopened.db.execute(
                "SELECT count(*) FROM assignments WHERE receipt IS NOT NULL"
            ).fetchone()[0]
            queued = reopened.db.execute("SELECT count(*) FROM assignments").fetchone()[0]
        return {"completed": completed, "unique_assignments": queued, "stale_write_rejected": True}


def serial_window(item):
    return asdict(item) | {
        "start": item.start.isoformat(), "end": item.end.isoformat(), "task_id": item.task_id,
    }


def plan():
    rows = sample_data()
    counts = dict(Counter(row["state"] for row in rows))
    pending = [row["dataset"] for row in rows if row["state"] == "HOLD_CONTRACT"]
    initial = manifest_windows(fixture_approvals("1994-01-01"))
    expanded = manifest_windows(fixture_approvals("1990-01-01"))
    old_ids = {w.task_id for w in initial}
    new_ids = {w.task_id for w in expanded}
    if not old_ids < new_ids or len(new_ids - old_ids) != 16 * 48:
        raise ValueError("historical extension changed previous assignments")
    task_rows = [serial_window(window) for window in expanded]
    manifest_sha = hashlib.sha256(json.dumps(task_rows, sort_keys=True).encode()).hexdigest()
    return {
        "readiness": counts, "contract_holds": pending,
        "remediation_runs": {"native_layouts_and_capacity": 45649612210, "dated_scope_and_granularity": 45649993338},
        "sample_evidence_is_not_approval": True, "production_dispatchable": 0,
        "production": "HOLD",
        "blockers": {
            "contracts": "Approve per-era contracts and publication rules for manifest release",
            "scope": "Resolve twelve scope reviews and obtain effective market identities before bulk ingestion",
            "coverage": "Approve historical bounds and immutable manifests; retain the unresolved historical TPEx coverage gap",
            "dispatch": "Deploy B00–B10, production state, remote lease/cooldown and G0–G5",
            "capacity": "Compare the full storage projection against verified Drive headroom and confirm exact Backer expiry",
        },
        "fixture": {
            "lanes": 8, "oldest": min(w.start for w in expanded).isoformat(),
            "latest": expanded[0].end.isoformat(),
            "preserved_previous_tasks": len(old_ids), "added_older_tasks": len(new_ids - old_ids),
            "manifest_sha256": manifest_sha,
            "resume": resume_test(initial, expanded),
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    report = plan()
    path = Path("r11-manifest-readiness.json")
    if args.verify_only:
        if json.loads(path.read_text(encoding="utf-8")) != report:
            raise SystemExit("manifest readback does not match the fixture and verified source evidence")
        print("independent_manifest_readback=PASS")
    else:
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
