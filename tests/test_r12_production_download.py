import json
import unittest

from scripts.r11_backer.integration_pilot import encode
from scripts.r11_backer.production_download import candidate_windows, next_request_time, storage_bytes
from scripts.r11_backer.quota import TOTAL_LIMIT
from scripts.r11_backer.release_rehearsal import RehearsalError


class ProductionDownloadTests(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "contracts": {"Price": {"venues": ["TWSE", "TPEx"], "lane": "B01"},
                          "Holding": {"venues": ["TWSE"], "lane": "B03"}},
            "months": {"2026-10-01": {"TWSE": ["2026-10-05"], "TPEx": ["2026-10-05"]},
                       "2026-11-01": {"TWSE": ["2026-11-03", "2026-11-05"], "TPEx": ["2026-11-03"]}},
            "listings": [{"market": "TWSE", "code": "0001", "start": "2026-11-04", "end_exclusive": "2026-11-21"},
                         {"market": "TPEx", "code": "0002", "start": "2024-01-02", "end_exclusive": "2026-11-20"}],
            "reused": [["Price", "0002", "2026-10-01", "2026-10-31"]],
        }

    def test_cursor_order_and_prior_receipts_are_stable_after_drive_round_trip(self):
        original = list(candidate_windows(self.plan))
        self.assertEqual(original, list(candidate_windows(json.loads(encode(self.plan)))))
        self.assertEqual(original, [
            ("Price", "0001", "TWSE", "2026-11-04", "2026-11-20", ["2026-11-05"]),
            ("Holding", "0001", "TWSE", "2026-11-04", "2026-11-20", ["2026-11-05"]),
            ("Price", "0002", "TPEx", "2026-11-01", "2026-11-19", ["2026-11-03"]),
        ])

    def test_partial_prior_coverage_cannot_be_redownloaded_as_a_whole_month(self):
        self.plan["reused"].append(["Price", "0002", "2026-11-03", "2026-11-03"])
        with self.assertRaisesRegex(RehearsalError, "RECEIPT_WINDOW_OVERLAP"):
            list(candidate_windows(self.plan))

    def test_cross_batch_quota_and_retry_wait(self):
        state = {"pause": 4050, "events": [{"at": 500}] * TOTAL_LIMIT}
        self.assertEqual(next_request_time(state, 4000), 4100)
        state["tasks"] = {"a": {"status": "REQUESTING", "lease_until": 4150}}
        self.assertEqual(next_request_time(state, 4000), 4150)
        state["tasks"]["b"] = {"status": "READY"}
        self.assertEqual(next_request_time(state, 4000), 4100)

    def test_capacity_bound_increases_at_the_next_batch(self):
        self.assertEqual(storage_bytes(0), 0)
        self.assertGreater(storage_bytes(97) - storage_bytes(96), storage_bytes(96) - storage_bytes(95))
