"""Roll up the source specifications without upgrading partial evidence to release."""

from collections import Counter
import json
import os
from pathlib import Path

GROUPS = (
    ("R10-A", (
        "Eight-source identity and scope | scope",
        "Free and Backer credentials separated | credential",
        "Native frequency and markets | frequency",
        "Historical dates | history",
        "Original bytes and hashes unchanged | hashes",
        "Physical format fingerprint | schema",
        "Column semantics | semantics",
        "Publication date and PIT | pit",
        "Primary keys and duplicate rows | keys",
        "Original source units | units",
        "Weekly, monthly and quarterly gaps | gaps",
        "HTTP 400/429/504 handling | http",
        "Oversized responses | size",
        "Checkpoint interruption | recovery",
        "Cross-run replay and competing claims | orchestration",
        "Replay without duplicate entries | recovery",
        "Failure isolation by source | isolation",
        "Master-to-lane reconciliation | production_ledger",
        "Acceptance receipts and readback | hashes",
        "Bidirectional lineage | lineage",
        "Gold443 and XQ dependencies | downstream",
        "No production data writes during pilot | bronze",
        "Secrets absent from pilot logs | logging",
        "Existing workflows preserved | legacy",
        "Source limits and elapsed duration | throughput",
        "Coverage denominator and gaps | gaps",
    )),
    ("R10.1-P", (
        "Competing Master claims | race",
        "Lease expiry | orchestration",
        "Late-worker fencing | recovery",
        "Conflicting partition writes | race",
        "Checkpoint write and readback | recovery",
        "Interruption after staging | recovery",
        "Successful partition not repeated | recovery",
        "Provider 429 does not stop official lines | isolation",
        "Global Drive throttling | drive_throttle",
        "Hourly capacity exhausted | quota",
        "One source fails while others continue | isolation",
        "W10 early verification trigger | orchestration",
        "Cross-source dependencies wait | downstream",
        "W11 campaign stops at terminal state | orchestration",
        "Prior workflows preserved | legacy",
        "Bronze before/after hashes unchanged | bronze_hash",
        "No secrets in logs | logging",
        "Additional R10.1 item counted by R11 but not enumerated in the source | source_missing",
    )),
    ("R11-T", (
        "Single-day all-market query | bulk",
        "One-stock cross-month query | cross_month_live",
        "Native lower-frequency queries | frequency",
        "Historical listing, delisting and market moves | identity",
        "Daily broker aggregation entitlement | broker",
        "Response size and truncation | size",
        "Normal empty intervals | gaps",
        "Download interruption and retry | orchestration",
        "Drive write interrupted before checkpoint | recovery",
        "Duplicate submission | recovery",
        "Lease expiration | orchestration",
        "HTTP 429 and Retry-After | http",
        "Exhausted rolling request capacity | quota",
        "Subscription expiry | expiry",
        "Paid data kept out of public repository and artifacts | private",
        "Six official sources untouched | bronze",
        "Run stops after terminal tasks | orchestration",
    )),
    ("R11.1-H", (
        "Valid pre-1994 Backer data | early_1994",
        "Valid pre-1990 Backer dates | early_1990",
        "Create missing year folder | synthetic_archive",
        "Create missing source folder | synthetic_archive",
        "Create missing month folder | synthetic_archive",
        "Reuse verified folder ID | recovery",
        "Concurrent requests with one folder writer | race",
        "Existing duplicate folder name is rejected | conflicting_folders",
        "Cross-month archive and logical index | synthetic_archive",
        "Cross-year archive and logical index | synthetic_archive",
        "Fiscal period and publication date separation | pit",
        "Missing archive dates remain quarantined | dates",
        "Reject malformed or implausible dates | dates",
        "Version the manifest when older data is found | versioning",
        "Original six source objects preserved | bronze",
    )),
)

# These findings describe coverage at pilot scope. Each release-critical item that
# still needs production evidence stays HOLD in the per-requirement report.
FINDINGS = {
    "scope": ("PARTIAL", "R11 Backer scope checked. Other sources retain their R10 audit requirements."),
    "credential": ("PASS", "Backer bearer token only. The public catalog job receives no API credentials."),
    "frequency": ("PARTIAL", "Daily, weekly, monthly, quarterly and event samples. Full frequency/market contracts pending."),
    "history": ("PARTIAL", "Separate observed early windows by dataset. Actual oldest/latest unproven."),
    "hashes": ("PRIVATE_READBACK", "Separate Runner compares raw and compressed SHA-256 of private pilot samples."),
    "schema": ("PARTIAL", "Documented field coverage and identity/date checks; full era-specific format and type rules pending."),
    "semantics": ("HOLD", "Full documented field semantics across eras require contract review."),
    "pit": ("HOLD", "PIT uses actual publication date; production mapping and published-date evidence pending."),
    "keys": ("HOLD", "Dataset and era-specific business keys and duplicate policy are incomplete."),
    "units": ("PARTIAL", "Original bytes preserved; per-field units and conversion contracts pending."),
    "gaps": ("PARTIAL", "Empty sample intervals are preserved; expected periods need dataset release calendars."),
    "http": ("PARTIAL", "HTTP classification and controlled Retry-After tested; complete production retry/recovery pending."),
    "size": ("PARTIAL", "Byte cap enforced. Production split/reconciliation and upstream truncation tests pending."),
    "recovery": ("RECOVERY", "Separate Runner reuses staged file, rejects stale and duplicate completion, then reads back ledger."),
    "orchestration": ("HOLD", "B00–B10 production workflow and durable campaign reconciliation are not deployed."),
    "isolation": ("PARTIAL", "Probe failures are reported per dataset. Production official and Backer lane fault isolation remains unproven."),
    "production_ledger": ("HOLD", "Production MASTER, lane ledgers and G5 reconciliation not deployed."),
    "lineage": ("PARTIAL", "Private sample receipt traces exact bytes; complete per-dataset lineage and layers remain pending."),
    "downstream": ("HOLD", "Keep Gold443/XQ and RAW/Silver/Gold/Application outside Bronze release pilot."),
    "bronze": ("PASS", "This pilot only writes to the verified 99_RECOVERY directory."),
    "logging": ("PASS", "Workflow exposes outcome labels, summaries and synthetic state; credential and response bodies are not output."),
    "legacy": ("PASS", "Changes limited to the new R11 pilot, its scripts and tests."),
    "throughput": ("SWEEP", "Measured sample throughput. Provider cap is an external service boundary."),
    "race": ("RECOVERY", "Independent Runners share one successful CAS; only its owner writes the synthetic sample."),
    "drive_throttle": ("HOLD", "Production Drive 403/429 backoff and lane behavior pending."),
    "quota": ("PARTIAL", "Multi-Runner CAS and rolling limits tested; production campaign quota state is not deployed."),
    "bronze_hash": ("HOLD", "No whole-Bronze before/after digest was collected in this pilot."),
    "source_missing": ("HOLD", "R10 A01–A26 and 17 additional R10.1 cases were enumerated. The 18th is not named in the source."),
    "bulk": ("BULK", "Compare the exact chosen-symbol rows against all-market results over up to five closed days."),
    "cross_month_live": ("CROSS_MONTH", "One real symbol response contains dates in both months, and recovery validated archive reuse."),
    "identity": ("PARTIAL", "Current master and delisting queried. Full point-in-time identity history remains unproven."),
    "broker": ("HOLD", "Public daily branch aggregation contract is Sponsor; bulk is Sponsor Pro. No upgrade authorized."),
    "expiry": ("HOLD", "Account endpoint confirms usable credential but not subscription expiry."),
    "private": ("PRIVATE_READBACK", "Real paid original bytes written and verified solely in non-shared Drive recovery."),
    "early_1994": ("PASS", "Earlier live sample found 1991 Q4 statements (Run 45595104533)."),
    "early_1990": ("PASS", "1982 January business indicator probe verified (Run 45587223818)."),
    "synthetic_archive": ("RECOVERY", "Cross-year fixture verifies both monthly index entries and one recovered archive."),
    "conflicting_folders": ("PARTIAL", "Lookup fails closed on more than one matching folder; actual duplicate-folder fault injection pending."),
    "dates": ("PARTIAL", "Date parsing and range guards run; dataset-specific historical default-date rules are still pending."),
    "versioning": ("HOLD", "Production signed historical manifest versioning has not been exercised."),
}


def evaluate(sweep, *, recovery, private):
    dynamic = {
        "RECOVERY": recovery,
        "PRIVATE_READBACK": private,
        "SWEEP": bool(sweep.get("dataset_sweep_complete")),
        "CROSS_MONTH": recovery and bool(sweep.get("cross_month_single_symbol_verified")),
        "BULK": any(row.get("recommendation") == "MARKET_DAY_CANDIDATE"
                    for row in sweep.get("all_market", {}).values()),
    }
    entries = []
    for prefix, cases in GROUPS:
        for index, case in enumerate(cases, start=1):
            title, _, evidence = case.partition(" | ")
            kind, reason = FINDINGS[evidence]
            if kind in dynamic:
                status = "PASS_PILOT" if dynamic[kind] else "HOLD"
            else:
                status = kind
            entries.append({"id": f"{prefix}{index:02}", "case": title, "status": status, "evidence": reason})
    return entries


def main():
    try:
        sweep = json.loads(Path("r11-release-probes.json").read_text())
    except FileNotFoundError:
        sweep = {}
    cases = evaluate(
        sweep, recovery=os.environ.get("DRIVE_ROUND_TRIP") == "success",
        private=os.environ.get("PRIVATE_READBACK") == "success",
    )
    summary = {
        "test_crosswalk": len(cases),
        "test_statuses": dict(Counter(case["status"] for case in cases)),
        "source_enumerated": sum(case["id"] != "R10.1-P18" for case in cases),
        "live_sweep": bool(sweep.get("dataset_sweep_complete")),
        "production": "HOLD",
        "release_blockers": [
            "B00–B10 production deployment and production state",
            "Subscription expiry and durable global quota coordinator",
            "Per-dataset market identities, earliest/latest boundaries, and versioned download manifest",
            "Production contract, coverage and acceptance evidence",
        ],
    }
    Path("r11-release-acceptance.json").write_text(
        json.dumps({"summary": summary, "cases": cases, "sweep": sweep}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary))
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as out:
            out.write("## Full-volume release acceptance\n\n")
            out.write(f"- Crosswalk statuses: `{json.dumps(summary['test_statuses'])}`\n")
            out.write(f"- Routed dataset statuses: `{json.dumps(sweep.get('dataset_statuses', {}))}`\n")
            out.write(f"- Live requests: {sweep.get('request_count', 'unavailable')} + account metadata check\n")
            for dataset, row in sweep.get("all_market", {}).items():
                out.write(f"- {dataset}: {row['matching_days']}/{row['tested_days']} exact reference matches; "
                          f"{row['recommendation']}\n")
            out.write("- Production release: **HOLD**\n")


if __name__ == "__main__":
    main()
