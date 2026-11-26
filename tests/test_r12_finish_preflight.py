from datetime import date
import unittest
from unittest.mock import patch

from scripts.r11_backer.archive import compress_original
from scripts.r11_backer.finish_preflight import accept, alternate, empty_confirmed, recover_wave, task_id, wave_keys
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

    def test_formal_bronze_readback_accepts_proved_stock_calendar(self):
        task = Task("TaiwanStockShareholding", "2026-11-23", "2026-11-23", "2330")
        key = task_id(task)
        raw = b'{"status":200,"data":[{"stock_id":"2330","date":"2026-11-23"}]}'
        packed, raw_sha, packed_sha = compress_original(raw)
        manifest = {
            "tests": {key: {"params": task.parameters(), "end": task.end, "lane": "B03",
                            "fields": ["date", "stock_id"], "expected_days": [task.start], "object_id": "original"}},
            "decisions": [], "previous_rows": 0, "previous_positive": [], "previous_lanes": [],
        }
        record = {"state": {"events": [], "packets": [], "inflight": [], "tasks": {
            key: {"status": "VALID_SAMPLE", "proof": {"raw_sha": raw_sha, "packed_sha": packed_sha, "rows": 1}},
        }}}
        base = "scripts.r11_backer.finish_preflight."
        with (patch(base + "packets", return_value={key: raw}),
              patch(base + "ensure", return_value="original"),
              patch(base + "verify_private_folder"),
              patch(base + "month_folder", return_value="month"),
              patch(base + "write_json"),
              patch(base + "read_private_file", return_value=packed) as read,
              patch.dict("os.environ", {"GITHUB_RUN_ATTEMPT": "1"})):
            result = accept(manifest, "test", record, "access", 10, 9)
        read.assert_called_once_with("original", "access")
        self.assertEqual(result["formal_bronze_verified"], 1)
        self.assertEqual(result["verification_finmind_requests"], 0)
