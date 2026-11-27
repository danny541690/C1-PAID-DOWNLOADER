"""Reconcile all recorded release checks and rehearse one real archive in private Drive."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import timedelta
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
from time import sleep, time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .acceptance_report import GROUPS
from .archive import compress_original
from .combined_preflight import REPORTS, archived_reports
from .drive_pilot import BASE, FOLDER, ensure, find, token
from .final_source_probes import PREFIX as SOURCES_PREFIX, previous_capture, split_window
from .history_pilot import (
    BUSINESS, NDC_PERIOD, NDC_PUBLICATION, PREFIX as HISTORY_PREFIX, RECEIPT_FILE, archive_name, archive_parent,
    business_rows, canonical_json, iso_date, read_verified_history_archive,
)
from .integration_pilot import verify_private_folder
from .manifest_pilot import monthly_windows, serial_window
from .market_lifetimes import digest
from .quota import can_reserve, retry_deadline
from .release_probes import RECOVERY_ROOT, Task, read_private_file
from .test_archive import json_bytes
from .tpex_history import private_folder, save_private, verified_archive_files


PREFIX = "R11_FINAL_ACCEPTANCE"
REPORT = Path("r11-final-preflight.json")
INTEGRATION = Path("r11-integration-verdict.json")
RETRY_REASONS = {"rateLimitExceeded", "userRateLimitExceeded"}


def retry_drive(operation, *, wait=sleep, now=time):
    for attempt in range(4):
        try:
            return operation()
        except HTTPError as error:
            retryable = error.code in (408, 429, 500, 502, 503, 504)
            if error.code == 403:
                try:
                    reasons = {row.get("reason") for row in json.loads(error.read(16384))["error"]["errors"]}
                    retryable = bool(reasons & RETRY_REASONS)
                except (TypeError, ValueError, KeyError):
                    retryable = False
            if not retryable or attempt == 3:
                raise
            stamp = int(now())
            header = error.headers.get("Retry-After") if error.headers else None
            delay = retry_deadline(header, stamp) - stamp if header else 2 ** attempt
            if delay > 30:
                raise
            wait(delay)


def test_retry():
    waits = []
    events = [
        HTTPError("fixture", 403, None, {}, BytesIO(b'{"error":{"errors":[{"reason":"userRateLimitExceeded"}]}}')),
        HTTPError("fixture", 429, None, {"Retry-After": "5"}, None),
        HTTPError("fixture", 503, None, {}, None),
    ]

    def transient():
        if events:
            raise events.pop(0)
        return "readback"

    assert retry_drive(transient, wait=waits.append, now=lambda: 100) == "readback" and waits == [1, 5, 4]
    for code, body, expected in ((403, b'{"error":{"errors":[{"reason":"insufficientPermissions"}]}}', 0),
                                 (503, b"", 3), (429, b"", 0)):
        attempt_waits = []

        def rejected():
            raise HTTPError("fixture", code, None, {"Retry-After": "45"} if code == 429 else {}, BytesIO(body))

        try:
            retry_drive(rejected, wait=attempt_waits.append, now=lambda: 100)
        except HTTPError:
            assert len(attempt_waits) == expected
        else:
            raise ValueError("retry failure fixture was accepted")
    return "PASS_SIMULATED_RETRY_AFTER_AND_THREE_RETRIES"


def test_capacity_and_split():
    now = 10_000
    used = [(now - 1700, "download")] * 1200 + [(now - 1000, "probe")] * 400
    assert not can_reserve(used, now, "download") and not can_reserve(used, now, "probe")
    assert can_reserve(used, now + 3600, "download")
    assert not can_reserve([], now, "probe", paused_until=now + 8)
    task = Task("fixture", "2000-12-29", "2001-01-03", "sample")
    newer, older = split_window(task)
    assert (newer.start, newer.end, older.start, older.end) == ("2001-01-01", task.end, task.start, "2000-12-31")
    assert not split_window(Task("fixture", "2000-01-01", "2000-01-01"))
    return "PASS_MOCKED_CAPACITY_AND_NEWEST_FIRST_SPLIT"


def duplicate_folder_test(root, access):
    target = ensure(root, "duplicate-folder-test", FOLDER, access, create=True)
    verify_private_folder(target, access)
    for _ in range(2):
        request = Request(f"{BASE}/files?fields=id,parents,mimeType", method="POST",
                          headers={"Authorization": "Bearer " + access, "Content-Type": "application/json"},
                          data=json_bytes({"name": "collision-fixture", "parents": [target], "mimeType": FOLDER}))
        with urlopen(request, timeout=20) as response:
            folder = json.load(response)
        if folder["parents"] != [target] or folder["mimeType"] != FOLDER:
            raise ValueError("unexpected duplicate fixture location")
        verify_private_folder(folder["id"], access)
    try:
        find(target, "collision-fixture", access)
    except RuntimeError as error:
        if str(error) == "ambiguous Drive child":
            return "PASS_REAL_DUPLICATE_FOLDER_REJECTED"
        raise
    raise ValueError("duplicate Drive folders were accepted")


def historical_versions(first, last, data):
    cutoff = sorted(data)[2]
    older, newer = (list(monthly_windows(BUSINESS, "B07", "TW", start, last, "monthly-native"))
                    for start in (cutoff, first))
    original = [serial_window(part) for part in older]
    updated = [serial_window(part) for part in newer]
    if updated[:len(original)] != original or len(updated) != len(original) + 2:
        raise ValueError("earlier month changed the identity of existing tasks")
    return {"status": "PASS_EARLIER_MONTHS_PRESERVE_PRIOR_TASK_IDS",
            "old_sha256": digest(canonical_json(original)), "expanded_sha256": digest(canonical_json(updated)),
            "old_task_count": len(original), "expanded_task_count": len(updated)}


def macro_rehearsal(root, access, reports):
    prior_run = REPORTS["capacity"][0]
    prior = ensure(RECOVERY_ROOT, f"{HISTORY_PREFIX}_{prior_run}_1", FOLDER, access, create=False)
    verify_private_folder(prior, access)
    original, raw, packed = read_verified_history_archive(access, prior)
    manifest = original["manifest"]
    data = business_rows(json.loads(raw)["data"])
    first, last = iso_date(manifest["first"]), iso_date(manifest["verified_through"])
    expected = reports["capacity"][0]["historical_business"]
    if (expected["verified_months"] != len(data) or expected["latest_observed"] != str(last)
            or first != min(data) or last != max(data) or last != NDC_PERIOD
            or expected["status"] != "PASS_PROVIDER_WINDOW"):
        raise ValueError("G0/G3 earlier complete month coverage drifted")
    shadow = ensure(root, "bronze-rehearsal", FOLDER, access, create=True)
    verify_private_folder(shadow, access)
    parent = archive_parent(shadow, manifest, access, create=True)
    verify_private_folder(parent, access)
    filename = archive_name(manifest)
    staged = ensure(parent, filename, "application/gzip", access, packed, create=True)
    recovered = ensure(parent, filename, "application/gzip", access, create=False)
    readback = retry_drive(lambda: read_private_file(recovered, access))
    if recovered != staged or readback != packed or recovered == original["file_id"]:
        raise ValueError("G1 real archive was not recovered as the same staged file")
    receipt = {
        **original, "file_id": recovered, "stage": "PRIVATE_BRONZE_REHEARSAL",
        "previous_archive_file_id": original["file_id"], "previous_archive_run": prior_run,
        "point_in_time_sample": {"period": str(NDC_PERIOD), "published": str(NDC_PUBLICATION),
                                 "earliest_use": str(NDC_PUBLICATION + timedelta(days=1))},
    }
    receipt_id = ensure(shadow, RECEIPT_FILE, "application/json", access, canonical_json(receipt), create=True)
    reread, replay_raw, replay_packed = read_verified_history_archive(access, shadow)
    if reread != receipt or replay_raw != raw or replay_packed != packed:
        raise ValueError("G5 real raw original and its receipt disagree after readback")
    return {
        "gates": {"G0": "PASS_SHADOW_MANIFEST", "G1": "PASS_RAW_AND_GZIP_SHA", "G2": "PASS_NATIVE_FIELD_STRUCTURE",
                  "G3": "PASS_537_MONTH_INDEX", "G4": "PASS_ONE_OFFICIAL_PIT_SAMPLE",
                  "G5": "PASS_SHADOW_FILE_AND_RECEIPT_RECONCILIATION"},
        "months": len(data), "source_archive": receipt, "private_receipt_id": receipt_id,
        "versioning": historical_versions(first, last, data), "reused_same_file_on_resume": True,
    }


def stored_samples(access):
    archive = verified_archive_files(access, private_folder(access, create=False, prefix=SOURCES_PREFIX))
    report = json.loads(archive["reconciliation.json"])
    prior_files, _ = previous_capture(access)
    if digest(prior_files["reconciliation.json"]) != report["prior_result_sha"]:
        raise ValueError("reused source evidence changed")

    def read_packet(prefix):
        if not prefix.startswith(SOURCES_PREFIX + "_"):
            raise ValueError("unexpected private packet prefix")
        files = verified_archive_files(access, private_folder(access, create=False, prefix=prefix))
        packet = json.loads(files["reconciliation.json"])
        if packet["archive_prefix"] != prefix:
            raise ValueError("sample packet identity mismatch")
        return [(json.dumps(row["request"], sort_keys=True), files[row["original"]], row["status"])
                for row in packet["backer_requests"]]

    packet_raw = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for entries in pool.map(read_packet, report["packets"]):
            for key, raw, state in entries:
                if key in packet_raw:
                    raise ValueError("one paid response was stored twice")
                packet_raw[key] = (raw, state)
    pending = set(packet_raw)
    passed = {"VALID_SAMPLE", "PASS_NATIVE_SAMPLE"}
    windows = Counter()
    positive = set()
    for row in report["rows"]:
        for item in (row, *row.get("shards", [])):
            if "raw_sha256" not in item:
                continue
            if prior := item.get("reused_evidence"):
                raw = prior_files[prior["original"]]
            else:
                key = json.dumps(item["request"], sort_keys=True)
                raw, state = packet_raw[key]
                if state != item["provider_result"]:
                    raise ValueError("paid archive provider status mismatch")
                pending.discard(key)
            packed, raw_sha, packed_sha = compress_original(raw)
            if (raw_sha != item["raw_sha256"] or packed_sha != item["gzip_sha256"]
                    or len(packed) != item["gzip_bytes"] or len(raw) != item["raw_bytes"]):
                raise ValueError("raw or deterministic gzip fingerprint mismatch")
            if item["result"] in passed:
                windows[row["sample_period"]] += 1
                positive.add(row["dataset"])
    if pending:
        raise ValueError("unexpected raw sample has no matching control record")
    return report, {"observed_valid_samples": dict(windows), "datasets_with_new_or_reused_valid_raw": len(positive),
                    "verified_paid_payloads": len(packet_raw), "verified_packets": len(report["packets"])}


def acceptance_matrix(cases, runtime, macro):
    upgrades = {
        "size": ("PARTIAL", "Sample byte cap and newest-first split exercised. Production batch sizing pending."),
        "conflicting_folders": ("PASS_PILOT", runtime["duplicate_folders"]),
        "drive_throttle": ("PASS_PILOT", runtime["drive_retry"]),
        "versioning": ("PASS_PILOT", macro["versioning"]["status"]),
        "quota": ("PASS_PILOT", runtime["quota_and_split"]),
        "production_ledger": ("PARTIAL", "Cross-runner and B07 shadow ledger reconciliation. Production deployment pending."),
        "pit": ("PARTIAL", "Exact NDC period and next-day PIT verified for one real period. Per-Dataset PIT pending."),
        "schema": ("PARTIAL", "Private raw field and type profiles archived for each successful sampled window."),
        "gaps": ("PARTIAL", "Empty, invalid and blocked queries recorded by window. Full expected calendars pending."),
        "frequency": ("PARTIAL", "All 89 candidates classified. Full era coverage pending."),
        "history": ("PARTIAL", "Three bounded era windows or a snapshot planned per eligible Dataset. Absolute starts pending."),
        "hashes": ("PASS_PILOT", "Independent raw/GZIP replay and source/shadow/month lineage."),
        "lineage": ("PARTIAL", "Verified B07 receipt links to both raw original and archived shadow file."),
    }
    previous = {row["id"]: row for row in cases}
    result = []
    for group, labels in GROUPS:
        for number, label in enumerate(labels, 1):
            _, _, category = label.partition(" | ")
            item = previous[f"{group}{number:02}"]
            if category in upgrades:
                status, reason = upgrades[category]
                result.append(item | {"status": status, "previous_status": item["status"], "evidence": reason})
            else:
                result.append(item | {"previous_status": item["status"]})
    if len(result) != 76 or len(previous) != len(result):
        raise ValueError("preflight matrix does not cover all recorded checks")
    rows = StringIO()
    writer = csv.DictWriter(rows, fieldnames=("id", "case", "previous_status", "status", "evidence"))
    writer.writeheader()
    writer.writerows(result)
    return result, rows.getvalue().encode("utf-8-sig")


def main():
    try:
        access = token()
        root = private_folder(access, create=True, prefix=PREFIX)
        with ThreadPoolExecutor(max_workers=2) as pool:
            reference = pool.submit(archived_reports, access)
            samples = pool.submit(stored_samples, access)
            runtime = {"duplicate_folders": duplicate_folder_test(root, access),
                       "drive_retry": test_retry(), "quota_and_split": test_capacity_and_split()}
            reports = reference.result()
            macro = macro_rehearsal(root, access, reports)
            sources, source_readback = samples.result()
        integration = json.loads(INTEGRATION.read_bytes())
        if (integration["status"] != "PASS" or integration["workstreams"] != 11
                or set(integration["gates_fixture"].values()) != {"PASS"}):
            raise ValueError("cross-runner integration suite did not pass")
        cases, table = acceptance_matrix(reports["acceptance"][0]["cases"], runtime, macro)
        counts = Counter(row["status"] for row in cases)
        public = {"status": "PASS_DIAGNOSTIC", "production": "HOLD", "production_dispatchable": 0,
                  "check_matrix": dict(counts), "check_rows": len(cases), "cross_runner_workstreams": 11,
                  "fixture_negative_cases": integration["negative_cases"], "actual_macro_replayed_months": macro["months"],
                  "actual_macro_gates": macro["gates"], "source_checks": source_readback,
                  "source_sample_decisions": sources["sample_decisions"], "new_finmind_requests": sources["finmind_requests"],
                  "runtime": runtime}
        details = {"cases": cases, "actual_macro": macro, "source_sample_statuses": sources["results"],
                   "candidate_statuses": sources["candidates"], "cross_runner_report": integration,
                   "remaining": {
                       "scope_and_field_contracts": "HOLD_FOR_RESEARCH_SCOPE_AND_ERA_CONTRACTS",
                       "boundaries_and_historical_identity": "HOLD_FOR_OFFICIAL_AND_PROVIDER_EVIDENCE",
                       "subscription_and_full_capacity": "HOLD_EXACT_EXPIRY_AND_FULL_MANIFEST",
                       "authorization_and_final_deployment": "HOLD_PRODUCTION_G0_G5",
                   }}
        save_private({"r11-preflight-matrix.csv": ("GENERATED", table)}, {}, {}, public | details, prefix=PREFIX)
        staged = verified_archive_files(access, root)
        if staged["official/r11-preflight-matrix.csv"] != table:
            raise ValueError("private 76-item matrix readback mismatch")
    except Exception as error:
        public = {"status": "HOLD_INFRASTRUCTURE", "production": "HOLD", "failure_class": type(error).__name__}
        if os.environ.get("R11_SHOW_LOCAL_ERRORS") == "1":
            raise
    REPORT.write_bytes(json_bytes(public))
    print(json.dumps(public, sort_keys=True))
    return int(public["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
