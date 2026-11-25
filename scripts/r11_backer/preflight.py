"""Validate the historical candidate catalog and the R11.1 release inputs."""

import argparse
import csv
import json
from pathlib import Path

CATALOG = Path(__file__).with_name("catalog.csv")
SLOTS = (1, 3, 3, 3, 3, 2, 3, 2, 2, 2, 1)
ROUTES = {f"B{number:02}" for number in range(1, 9)} | {"REVIEW", "EXCLUDE"}
GATES = (
    "repository_and_runner_verified",
    "backer_entitlement_and_expiry_verified",
    "shared_api_quota_verified",
    "drive_capacity_verified",
    "cost_approved",
    "deployment_review_passed",
    "cross_runner_lock_verified",
    "historical_scope_verified",
    "required_tests_passed",
    "private_data_isolation_verified",
)


def read_catalog(path=CATALOG):
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    names = {row["dataset"] for row in rows}
    if len(rows) != 89 or len(names) != 89 or "" in names:
        raise ValueError("expected 89 distinct historical candidates")
    if any(row["routing"] not in ROUTES for row in rows):
        raise ValueError("unknown candidate route")
    return rows


def release_holds(evidence):
    return [
        gate for gate in GATES
        if not isinstance(evidence.get(gate), str) or not evidence[gate].strip()
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog-only", action="store_true")
    args = parser.parse_args()
    candidates = read_catalog()
    report = {
        "historical_candidates": len(candidates),
        "planned_workflows": len(SLOTS),
        "planned_slots": sum(SLOTS),
        "catalog_validation": "PASS",
        "production": "HOLD",
        "unverified_gates": release_holds({}),
    }
    print(json.dumps(report))
    return 0 if args.catalog_only else 2


if __name__ == "__main__":
    raise SystemExit(main())
