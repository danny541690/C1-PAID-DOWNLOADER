"""Changes to preparation, published periods, and capacity-driven continuation."""

from datetime import datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from scripts.r11_backer import bulk_batch, executable_manifest_v26 as manifest
from scripts.r11_backer.integration_pilot import digest, encode
from scripts.r11_backer.v26_acceptance import coverage_inventory, read_comparison
from scripts.r11_backer.v26_boundaries import quality_record, release_target
from scripts.r11_backer.v26_capacity import CapacityGuard, FLOOR, INTERVAL, LEDGER_BUFFER, WARNING
from scripts.r11_backer.v26_regression import comparison, fixture, FIRST, LAST, reply

STAMP = "2026-11-28T14:00:00+08:00"


class CloseoutTests(TestCase):
    def test_report_range_is_observational_evidence(self):
        data = ("<article class='box'>CrudeOilPrices 資料品質報告（2026-11） "
                "檢測日期 2026-11-14 資料期間 1990-01-02 ～ 2026-11-11 最新資料日 2026-11-11 "
                "查詢方式一致性 通過</article>").encode()
        self.assertEqual(quality_record("CrudeOilPrices", data)["observed_range"], ["1990-01-02", "2026-11-11"])
        self.assertIn("SAMPLED", quality_record("CrudeOilPrices", data)["proof_scope"])

    def test_native_publication_cutoffs(self):
        date = datetime.fromisoformat(STAMP)
        self.assertEqual(release_target("TaiwanStockFinancialStatements", "quarterly", date, [])[0], "2026-09-30")
        self.assertEqual(release_target("TaiwanStockMonthRevenue", "monthly", date, [])[0], "2026-11-01")
        self.assertEqual(release_target("TaiwanStockWeekPrice", "weekly", date, [])[0], "2026-11-22")
        self.assertEqual(list(manifest.period_days("quarterly", date.date().replace(month=1, day=1), date.date())),
                         ["2026-03-31", "2026-06-30", "2026-09-30"])

    def test_executable_cursor_uses_immutable_axes_and_blocks_reused_request(self):
        index, read = fixture(SimpleNamespace(read=lambda _: {"as_of_taipei": STAMP}))
        rows, cursor, exhausted, holds = manifest.render(index, read, manifest.origin(index))
        self.assertTrue(exhausted)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["coverage"]["requested_months"], ["1984-12", "1985-01"])
        after, _, _, _ = manifest.render(index, read, cursor)
        stale, _, _, _ = manifest.render(index, read, manifest.origin(index), known_requests=[rows[0]["request_id"]])
        self.assertEqual((after, stale, holds), ([], [], []))
        with self.assertRaisesRegex(ValueError, "FROZEN_GENERATOR_INPUT_SHA"):
            manifest.read_inputs(index, lambda name: read(name) | {"other": True})

    def test_split_verifies_completeness_with_distinct_bound_queries(self):
        index, read = fixture(SimpleNamespace(read=lambda _: {"as_of_taipei": STAMP}))
        item = manifest.render(index, read, manifest.origin(index))[0][0]
        checked = comparison(item)
        self.assertEqual(read_comparison(item, checked)["method"], "split_reconciliation")
        checked["replies"][1]["parameters"] = checked["replies"][0]["parameters"]
        with self.assertRaisesRegex(ValueError, "V26_SPLIT_INDEPENDENT_SOURCE"):
            read_comparison(item, checked)

    def test_missing_evidence_keeps_staged_original_out_of_destination(self):
        index, read = fixture(SimpleNamespace(read=lambda _: {"as_of_taipei": STAMP}))
        item = manifest.render(index, read, manifest.origin(index))[0][0]
        key = item["task_id"]
        batch = {"schema_version": 24, "items": [item], "stage": "isolated"}
        state = bulk_batch.initial(batch)
        state["tasks"][key]["status"] = "STAGED"
        destination = Mock()
        with patch.object(bulk_batch, "advance", side_effect=lambda _j, r, **s: r | {"state": r["state"] | s}):
            observed = bulk_batch.accept(batch, {"state": state}, None, None, destination=destination)
        self.assertEqual(observed["state"]["tasks"][key]["status"], "REPAIR")
        destination.assert_not_called()

    def test_capacity_warning_floor_and_periodic_recovery(self):
        real = [WARNING - 1]
        now = [100]
        service = Mock(side_effect=lambda: {"available_bytes": real[0]})
        guard = CapacityGuard(service, clock=lambda: now[0])
        self.assertEqual(guard.check(0, reserve=True)["state"], "WARN")
        self.assertEqual(guard.check(WARNING - FLOOR, 1, reserve=True)["state"], "PAUSED_CAPACITY_FLOOR")
        self.assertFalse(guard.check(WARNING - FLOOR, LEDGER_BUFFER)["can_admit"])
        service.assert_called_once()
        real[0] = WARNING * 10
        now[0] += INTERVAL
        self.assertTrue(guard.check(WARNING - FLOOR, 1, reserve=True)["can_admit"])
        self.assertEqual(service.call_count, 2)

    def test_native_coverage_and_raw_content_independent_of_query_identity(self):
        index, read = fixture(SimpleNamespace(read=lambda _: {"as_of_taipei": STAMP}))
        item = manifest.render(index, read, manifest.origin(index))[0][0]
        import json
        rows = json.loads(reply(FIRST, LAST))["data"]
        ids = coverage_inventory(item, rows)
        self.assertEqual(len(ids), 2)
        self.assertEqual(ids, coverage_inventory(item, list(reversed(rows))))
        self.assertNotEqual(digest(encode(rows)), digest(encode(list(reversed(rows)))))

    def test_source_unavailable_saturdays_are_not_trading_holidays(self):
        plan = {"mode": "MARKET_DAY", "dataset": "TaiwanStockConvertibleBondInstitutionalInvestors",
                "interval": ["2014-12-26", "2014-12-29"], "source_unavailable_days": [],
                "source_unavailable_saturdays_through": "2018-12-31"}
        axes = {"trading_dates": ["2014-12-26", "2014-12-27", "2014-12-29"], "exact_reuse_query_keys": {},
                "existing_market_query_days": {}}
        observed = list(manifest.parameters(plan, axes))
        self.assertEqual([r["start_date"] for r in observed], ["2014-12-26", "2014-12-29"])
        self.assertIn("2014-12-27", axes["trading_dates"])
