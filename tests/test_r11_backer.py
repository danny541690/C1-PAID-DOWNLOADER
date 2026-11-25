from datetime import date
import gzip
import hashlib
from pathlib import Path
import tempfile
import unittest

from scripts.r11_backer.archive import archive_plan, compress_original
from scripts.r11_backer.live_preflight import (
    REQUIRED_PILOT_RESULTS, pilot_status, validate_month_sample,
)
from scripts.r11_backer.preflight import GATES, SLOTS, read_catalog, release_holds


class BackerBootstrapTests(unittest.TestCase):
    def test_historical_catalog_and_slot_counts(self):
        self.assertEqual(len(read_catalog()), 89)
        self.assertEqual(len(SLOTS), 11)
        self.assertEqual(sum(SLOTS), 25)

    def test_zero_candidates_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.csv"
            catalog.write_text("dataset,routing\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                read_catalog(catalog)

    def test_unverified_release_remains_on_hold(self):
        self.assertEqual(release_holds({}), list(GATES))
        self.assertIn(GATES[0], release_holds({GATES[0]: True}))

    def test_validated_early_and_cross_year_dates_share_one_anchor(self):
        plan = archive_plan([date(1961, 12, 8), date(1962, 1, 2), date(1961, 12, 9)])
        self.assertEqual(plan.anchor, "1961/FinMind_Backer/1961-12")
        self.assertEqual(plan.months, ("1961-12", "1962-01"))

    def test_missing_archive_date_is_rejected(self):
        with self.assertRaises(ValueError):
            archive_plan([])

    def test_compression_round_trip_and_both_hashes(self):
        raw = b'{ "data": [1] }\n'
        compressed, original_sha, archived_sha = compress_original(raw)
        self.assertEqual(gzip.decompress(compressed), raw)
        self.assertEqual(original_sha, hashlib.sha256(raw).hexdigest())
        self.assertEqual(archived_sha, hashlib.sha256(compressed).hexdigest())
        self.assertEqual(compress_original(raw)[0], compressed)

    def test_backer_sample_requires_contract_fields_and_requested_dates(self):
        start, end = date(1982, 1, 1), date(1982, 1, 31)
        row = dict(date="1982-01-01", leading=1, coincident=2, lagging=3, monitoring=4)
        self.assertEqual(
            validate_month_sample({"status": 200, "data": [row]}, start, end),
            "VALID_MONTH_SAMPLE",
        )
        for patch, expected in (
            ({"date": "1900-01-01"}, "OUTSIDE_REQUESTED_WINDOW"),
            ({"date": None}, "DATE_UNVERIFIED"),
        ):
            self.assertEqual(
                validate_month_sample({"status": 200, "data": [row | patch]}, start, end),
                expected,
            )
        self.assertEqual(
            validate_month_sample({"status": 200, "data": []}, start, end),
            "EMPTY_WINDOW",
        )


class DispatchTests(unittest.TestCase):
    def setUp(self):
        from scripts.r11_backer.dispatch import Window

        self.latest = date(2026, 11, 25)
        self.recent = Window("Synthetic", "ALL", self.latest, self.latest, "v1", "B01")
        early = date(1982, 1, 1)
        self.early = Window("Synthetic", "ALL", early, early, "v1", "B01")

    def test_assignments_sort_newest_first_and_deduplicate(self):
        from scripts.r11_backer.dispatch import assignments

        plan = assignments(
            [self.early, self.recent, self.early], {"Synthetic": self.latest}
        )
        self.assertEqual(plan, [self.recent, self.early])

    def test_unpublished_window_does_not_dispatch(self):
        from scripts.r11_backer.dispatch import assignments

        with self.assertRaises(ValueError):
            assignments([self.recent], {"Synthetic": self.early.end})

    def test_expired_claim_recovery_and_completed_task_survive_restart(self):
        from contextlib import closing
        from scripts.r11_backer.dispatch import LocalLedger

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dispatch.db"
            with closing(LocalLedger(path)) as first, closing(LocalLedger(path)) as second:
                first.seed([self.early, self.recent])
                old = first.claim("B01", 100, 10)
                self.assertEqual(old, (self.recent.task_id, 1))
                self.assertEqual(second.claim("B01", 101, 30), (self.early.task_id, 1))
                self.assertIsNone(second.claim("B01", 109, 30))
                recovered = second.claim("B01", 110, 30)
                self.assertEqual(recovered, (self.recent.task_id, 2))
                self.assertFalse(first.accept(*old, 111, "old"))
                self.assertTrue(second.accept(*recovered, 111, "verified"))
                self.assertFalse(second.accept(*recovered, 111, "verified"))
            with closing(LocalLedger(path)) as restarted:
                restarted.seed([self.early, self.recent])
                self.assertEqual(restarted.claim("B01", 150, 30), (self.early.task_id, 2))
                self.assertIsNone(restarted.claim("B01", 150, 30))

    def test_pilot_requires_every_real_check(self):
        self.assertEqual(pilot_status(REQUIRED_PILOT_RESULTS), "PASS")
        self.assertEqual(pilot_status({}), "HOLD")
        self.assertEqual(
            pilot_status(REQUIRED_PILOT_RESULTS | {"backer_recent_probe": "EMPTY_WINDOW"}),
            "HOLD",
        )
