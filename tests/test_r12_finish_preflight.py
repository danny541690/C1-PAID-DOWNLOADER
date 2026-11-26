from datetime import date
import unittest
from unittest.mock import patch

from scripts.r11_backer.finish_preflight import alternate, empty_confirmed, recover_wave, wave_keys
from scripts.r11_backer.release_probes import Contract, Task


class RemainingPreflightTests(unittest.TestCase):
    def test_empty_is_confirmed_only_against_complete_calendar_and_known_scope(self):
        row = {"dataset": "TaiwanStockPrice", "request": {
            "start_date": "2026-11-21", "end_date": "2026-11-22", "data_id": "2330",
        }}
        months = {date(2026, 11, 1): {date(2026, 11, 20), date(2026, 11, 23)}}
        self.assertTrue(empty_confirmed(row, months))
        self.assertFalse(empty_confirmed(row, {}))
        self.assertFalse(empty_confirmed(row | {"request": row["request"] | {"data_id": "unknown"}}, months))

    def test_large_industry_snapshot_is_retried_with_documented_symbol(self):
        row = {"result": "SIZE_LIMIT", "dataset": "TaiwanStockIndustryChain",
               "request": {"dataset": "TaiwanStockIndustryChain"}}
        doc = Contract("Backer", "`data_id`", "", ("stock_id", "industry"))
        self.assertEqual(alternate(row, doc, [], {}), Task(row["dataset"], None, None, "2330"))

    def test_global_wave_counts_prior_requests_and_429_cooldown(self):
        state = {"account": {"at": 10, "limit": 1600, "headroom": 33, "first_event": 0},
                 "pause": 20, "events": [], "tasks": {str(i): {"status": "READY"} for i in range(40)}}
        self.assertEqual(wave_keys(state, 19), [])
        self.assertEqual(len(wave_keys(state, 20)), 32)
        state["events"] = [{"at": 10, "kind": "download"}] * 32
        self.assertEqual(wave_keys(state, 20), ["0"])

    def test_recovery_keeps_archived_original_after_summary_interruption(self):
        packet = {"id": "private"}
        with (patch("scripts.r11_backer.finish_preflight.find", side_effect=[None, packet, packet, None]),
              patch("scripts.r11_backer.finish_preflight.ensure", return_value="private"),
              patch("scripts.r11_backer.finish_preflight.verify_private_folder"),
              patch("scripts.r11_backer.finish_preflight.packets", return_value={
                  "known": (b'{"status":200,"data":[]}', "EMPTY_WINDOW"),
              })):
            restored = recover_wave("root", 3, ["known", "missing"], "access")
        self.assertEqual(restored["outcomes"]["known"]["status"], "EMPTY_WINDOW")
        self.assertEqual(restored["outcomes"]["missing"]["status"], "INTERRUPTED")
        self.assertEqual(restored["packets"], ["wave-000003-part-01"])
