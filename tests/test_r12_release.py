from datetime import datetime, timezone
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from scripts.r11_backer import drive_pilot
from scripts.r11_backer.cross_run_ledger import accept, claim, initial_state
from scripts.r11_backer.manifest_pilot import REQUIRED_PROOF, manifest_windows, sample_data
from scripts.r11_backer.production_preflight import FOREIGN_EQUITIES, evaluate, resolve_r12_scope
from scripts.r11_backer.release_readiness import BOOK


class ProductionPreflightTests(unittest.TestCase):
    def test_scope_dispositions_do_not_promote_samples(self):
        rows = resolve_r12_scope(sample_data(), "排除境外個股")
        states = {row["dataset"]: row["state"] for row in rows}
        self.assertTrue(all(states[name] == "EXCLUDED_BY_SCOPE" for name in FOREIGN_EQUITIES))
        self.assertEqual(sum(value == "SAMPLE_ONLY" for value in states.values()), 69)

    def test_expiry_storage_and_roster_errors_are_reported_together(self):
        row = {
            "dataset": "TaiwanStockPrice", "lane": "B01", "scope": "fixture",
            "start": "2026-01-01", "end": "2026-01-31",
            "listing_start": "2026-01-01", "listing_end": "2026-01-31",
            "contract": "fixture-v1", "evidence": dict.fromkeys(REQUIRED_PROOF, "fixture"),
        }
        window, = manifest_windows([row])
        plan = {
            "approvals": [row], "dataset_dispositions": {},
            "window_estimates": {window.task_id: {"requests": 3, "storage_bytes": 100, "evidence": "fixture"}},
            "safe_hourly_requests": 1, "sustained_rate_evidence": "fixture",
        }
        now = datetime(2026, 11, 26, tzinfo=timezone.utc)
        result = evaluate(plan, 90, now)
        self.assertEqual(result["estimated_request_count"], 3)
        self.assertEqual(result["production_dispatchable"], 0)
        self.assertCountEqual(result["blockers"], [
            "PRODUCTION_WORKFLOW_AND_PERSISTENT_LEDGER_NOT_CONNECTED",
            "DATASET_DISPOSITIONS_INCOMPLETE",
            "BACKER_EXPIRY_UNVERIFIED_OR_PASSED",
            "DRIVE_HEADROOM_INSUFFICIENT_OR_UNVERIFIED",
        ])
        for excluded in ("USStockPrice", BOOK):
            with self.subTest(excluded=excluded), self.assertRaisesRegex(ValueError, "out-of-scope"):
                evaluate(plan | {"approvals": [row | {"dataset": excluded}]}, 90, now)


class ReservedDriveIDTests(unittest.TestCase):
    def test_create_conflict_recovers_same_id_only(self):
        metadata = {"id": "fixed", "name": "original.json.gz", "parents": ["parent"],
                    "mimeType": "application/gzip", "trashed": False}
        conflict = HTTPError("https://example.invalid", 409, "exists", {}, None)
        with (patch.object(drive_pilot, "find", return_value=None),
              patch.object(drive_pilot, "build_opener") as opener,
              patch.object(drive_pilot, "read_json", return_value=metadata) as get):
            opener.return_value.open.side_effect = conflict
            self.assertEqual(drive_pilot.ensure("parent", "original.json.gz", "application/gzip", "fixture",
                                              b"raw", create=True, file_id="fixed"), "fixed")
            url = get.call_args.args[0]
            self.assertIn("/files/fixed?", url)
            get.return_value = metadata | {"parents": ["other"]}
            with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
                drive_pilot.ensure("parent", "original.json.gz", "application/gzip", "fixture",
                                   b"raw", create=True, file_id="fixed")


class CrossRunAdmissionTests(unittest.TestCase):
    def test_takeover_reuses_evidence_without_charging_or_releasing_held_tasks(self):
        manifest = {"tasks": [{"task_id": "first", "admission": "PENDING"},
                              {"task_id": "held", "admission": "HOLD"}]}
        original = claim(initial_state(manifest), "first", 11, 100, charge=True)
        self.assertIsNone(claim(original, "first", 22, 129, charge=False))
        self.assertIsNone(claim(original, "held", 22, 3700, charge=True))
        resumed = claim(original, "first", 22, 130, charge=False)
        self.assertIsNone(accept(resumed, "first", original["tasks"]["first"], 131, "fixture"))
        completed = accept(resumed, "first", resumed["tasks"]["first"], 131, "fixture")
        self.assertEqual(completed["tasks"]["first"]["status"], "VERIFIED")
        self.assertEqual(completed["tasks"]["first"]["generation"], 2)
        self.assertEqual(completed["quota"], original["quota"])
        self.assertIsNone(claim(completed, "first", 33, 3700, charge=True))


if __name__ == "__main__":
    unittest.main()
