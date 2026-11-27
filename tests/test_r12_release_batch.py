from datetime import date
import unittest
from unittest.mock import patch

from scripts.r11_backer.archive import compress_original
from scripts.r11_backer.integration_pilot import encode
from scripts.r11_backer.release_batch import RehearsalError, compile_daily, dispatch, proof_for, ready_wave, retry_state
from scripts.r11_backer.release_probes import Contract
from scripts.r11_backer.release_rollout import LANES, PER_RUN, budget_for


class ReleaseBatchTests(unittest.TestCase):
    def test_manifest_clips_to_verified_cutoff_and_keeps_exact_calendar(self):
        month = date(2026, 11, 1)
        roster = {"TaiwanStockPrice": {"state": "SAMPLE_ONLY", "routing": "B01"},
                  "TaiwanStockMarketValue": {"state": "SAMPLE_ONLY", "routing": "B04"}}
        contracts = dict.fromkeys(roster, Contract("Backer", "data_id", "`end_date`", ("date", "stock_id")))
        rows = compile_daily({month: {date(2026, 11, 23), date(2026, 11, 24)}}, contracts, roster)
        self.assertEqual({row["lane"] for row in rows}, {"B01", "B04"})
        self.assertTrue(all(row["end"] == "2026-11-23" and row["dates"] == ["2026-11-23"] for row in rows))

    def test_completeness_checks_calendar_keys_even_when_byte_hash_matches(self):
        item = {"dataset": "TaiwanStockPrice", "scope": "2330", "fields": ["date", "stock_id"],
                "dates": ["2026-11-20", "2026-11-23"]}
        rows = [{"date": day, "stock_id": "2330"} for day in item["dates"]]
        body = {"status": 200, "data": rows}
        packaged, _, _ = compress_original(encode(body))
        self.assertEqual(proof_for(item, packaged)["rows"], 2)
        for invalid in (body | {"data": rows[1:]}, body | {"data": rows + rows},
                        body | {"next_page": "cursor"}, body | {"data": [rows[0] | {"stock_id": "wrong"}, rows[1]]}):
            packaged, _, _ = compress_original(encode(invalid))
            with self.assertRaises(RehearsalError):
                proof_for(item, packaged)

    def test_wave_uses_confirmed_cap_and_stops_at_shared_remaining_budget(self):
        items = [{"task_id": "a", "lane": "B01"}, {"task_id": "b", "lane": "B01"},
                 {"task_id": "c", "lane": "B04"}]
        state = {"account": {"limit": 1600, "headroom": 2, "at": 100, "first_event": 1598}, "pause": 0,
                 "events": [{"at": 100, "kind": "download"}] * 1598,
                 "tasks": dict.fromkeys(("a", "b", "c"), {"status": "READY"})}
        self.assertEqual(ready_wave(items, state, 100), [items[0], items[2]])
        state["pause"] = 110
        self.assertEqual(ready_wave(items, state, 109), [])
        state["events"].append({"at": 110, "kind": "download"})
        self.assertEqual(ready_wave(items, state, 110), [items[0]])

    def test_fourth_failed_attempt_leaves_only_that_window_in_repair(self):
        self.assertEqual(retry_state({"attempts": 3}, "TEMPORARY", 1)["status"], "READY")
        result = retry_state({"attempts": 4}, "TEMPORARY", 1)
        self.assertEqual(result, {"attempts": 4, "status": "REPAIR", "reason": "TEMPORARY"})

    def test_rollout_plan_is_bounded_by_month_lane_and_retry_budget(self):
        month = date(2026, 7, 1)
        roster = {dataset: {"state": "SAMPLE_ONLY", "routing": lane} for dataset, lane in LANES.items()}
        contracts = dict.fromkeys(roster, Contract("Backer", "data_id", "`end_date`", ("date", "stock_id")))
        rows = compile_daily({month: {month}}, contracts, roster, dataset_lanes=LANES, cutoff=month)
        self.assertEqual(len(rows), PER_RUN)
        self.assertEqual({row["lane"] for row in rows}, set(LANES.values()))
        budget = budget_for(rows)
        self.assertEqual(budget["source_requests_including_retries"], 4 * PER_RUN)
        self.assertGreater(budget["storage_bytes"], 4 * PER_RUN * 262144)

    @patch("scripts.r11_backer.release_batch.verify_private_folder")
    @patch("scripts.r11_backer.release_batch.drive_health")
    @patch("scripts.r11_backer.release_batch.account_check")
    def test_replayed_runner_preserves_its_existing_request_ceiling(self, account, health, verify):
        manifest = {"capacity_bound": 1, "stage": "test-stage", "items": [{"task_id": "a"}]}
        state = {"tasks": {"a": {"status": "READY"}}, "events": [{"run": 7, "kind": "download"}] * PER_RUN}
        record = {"state": state}
        health.return_value = {"status": "POSITIVE_CAPACITY_AND_BRONZE_ACCESS", "available_bytes": 2}
        self.assertIs(dispatch(manifest, record, None, "test-access", 7, request_ceiling=PER_RUN), record)
        account.assert_not_called()
