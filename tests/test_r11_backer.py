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


class RollingQuotaTests(unittest.TestCase):
    def test_download_cap_reserve_and_full_account_cap(self):
        from scripts.r11_backer.quota import can_reserve

        downloads = [(100, "download")] * 1200
        self.assertFalse(can_reserve(downloads, 3699, "download"))
        self.assertTrue(can_reserve(downloads, 3699, "retry"))
        full = downloads + [(100, "retry")] * 400
        self.assertFalse(can_reserve(full, 3699, "retry"))
        self.assertFalse(can_reserve(full, 3699, "probe"))
        self.assertTrue(can_reserve(full, 3700, "download"))

    def test_retry_after_blocks_all_request_types_until_deadline(self):
        from scripts.r11_backer.quota import can_reserve, retry_deadline

        deadline = retry_deadline("90", 100)
        self.assertEqual(deadline, 190)
        self.assertEqual(retry_deadline("Thu, 01 Jan 1970 00:03:10 GMT", 100), 190)
        self.assertEqual(retry_deadline("invalid", 100), 160)
        for purpose in ("download", "probe", "retry"):
            self.assertFalse(can_reserve([], 189, purpose, paused_until=deadline))
            self.assertTrue(can_reserve([], 190, purpose, paused_until=deadline))


class PublicCatalogTests(unittest.TestCase):
    def test_union_preserves_exclusions_catalog_drift_and_entitlement_holds(self):
        from scripts.r11_backer.reconcile_catalog import documented_tiers, reconcile

        catalog = documented_tiers(
            "### Allowed\n- **Tier:** Backer\n"
            "### Unwanted\n- **Tier:** Backer\n"
            "### Upgrade\n- **Tier:** Sponsor Pro\n"
            "### Discovered\n- **Tier:** Free(w/ data_id)\n"
        )
        candidates = [
            {"dataset": name, "routing": route}
            for name, route in [
                ("Allowed", "B01"), ("Unwanted", "EXCLUDE"),
                ("Upgrade", "B02"), ("Missing", "REVIEW"),
            ]
        ]
        result = {row["dataset"]: row for row in reconcile(candidates, catalog)}
        self.assertEqual(result["Allowed"]["verdict"], "HOLD_LIVE_ENTITLEMENT_AND_CONTRACT")
        self.assertEqual(result["Unwanted"]["verdict"], "EXCLUDED_BY_SCOPE")
        self.assertEqual(result["Upgrade"]["verdict"], "HOLD_DOCUMENTED_HIGHER_TIER")
        self.assertEqual(result["Missing"]["verdict"], "HOLD_MISSING_FROM_DOCUMENTATION")
        self.assertEqual(result["Discovered"]["verdict"], "HOLD_SCOPE_REVIEW")
        self.assertTrue(all(row["live_entitlement"] == "NOT_TESTED" for row in result.values()))

    def test_empty_and_duplicated_documents_fail(self):
        from scripts.r11_backer.reconcile_catalog import documented_tiers

        for value in ("empty", "### Repeat\n- **Tier:** Backer\n" * 2):
            with self.subTest(value=value), self.assertRaises(ValueError):
                documented_tiers(value)


class DatasetPilotTests(unittest.TestCase):
    def test_probe_distinguishes_empty_denied_and_rate_limited(self):
        from scripts.r11_backer.dataset_pilot import classify

        def status(payload):
            return classify(payload, "2026-11-02", "2026-11-04", {"stock_id", "open"})

        self.assertEqual(status({"status": 403}), "ACCESS_DENIED")
        self.assertEqual(status({"status": 429}), "RATE_LIMITED")
        self.assertEqual(status({"status": 200, "data": []}), "EMPTY_WINDOW")
        row = {"stock_id": "2330", "open": 1, "date": "2026-11-03"}
        self.assertEqual(status({"status": 200, "data": [row]}), "VALID_SAMPLE")
        self.assertEqual(
            status({"status": 200, "data": [row | {"date": "2026-11-05"}]}),
            "DATE_OUTSIDE_WINDOW",
        )

    def test_futures_identity_uses_futures_id(self):
        from scripts.r11_backer.dataset_pilot import classify

        row = {"futures_id": "TX", "close": 1, "date": "2026-11-03"}
        args = ("2026-11-02", "2026-11-04", {"close"})
        self.assertEqual(
            classify({"status": 200, "data": [row]}, *args, identity_field="futures_id", identity="TX"),
            "VALID_SAMPLE",
        )
        self.assertEqual(
            classify({"status": 200, "data": [row | {"futures_id": "MTX"}]}, *args,
                     identity_field="futures_id", identity="TX"),
            "SCHEMA_UNVERIFIED",
        )

    def test_exact_window_evidence_is_reused_but_changed_contract_is_pending(self):
        from scripts.r11_backer.dataset_pilot import PROBES, probe_key, split_probes

        spec = PROBES[0]
        changed = (*spec[:-1], spec[-1] | {"new_field"})
        record = {"dataset": spec[0], "status": "VALID_SAMPLE", "run_id": 1, "job_id": 2}
        self.assertEqual(split_probes([spec, changed], {probe_key(spec): record}), ([spec], [changed]))
        self.assertEqual(split_probes([spec], {probe_key(spec): record | {"job_id": None}}), ([], [spec]))

    def test_rate_limit_stops_pending_probe_submission(self):
        from unittest.mock import patch
        from scripts.r11_backer.dataset_pilot import PROBES, probe_key, run_pending

        with patch("scripts.r11_backer.dataset_pilot.probe", return_value="RATE_LIMITED") as request:
            results = run_pending(PROBES[:3], "test-token", workers=1)
        self.assertEqual(results, {probe_key(PROBES[0]): "RATE_LIMITED"})
        self.assertEqual(request.call_count, 1)

    def test_historical_windows_keep_dataset_specific_identity_and_distinct_keys(self):
        from scripts.r11_backer.dataset_pilot import PROBES, pilot_probes, probe_key

        combined = pilot_probes()
        self.assertEqual(combined[:len(PROBES)], PROBES)
        self.assertEqual(len({probe_key(spec) for spec in combined}), len(combined))
        early = next(spec for spec in combined if spec[3] == "1991-10-01")
        self.assertEqual(early[:3], ("TaiwanStockFinancialStatements", "2330", "stock_id"))
        self.assertIn("origin_name", early[-1])


class ReleaseSweepTests(unittest.TestCase):
    def test_documented_contract_read_and_broker_tier_guard(self):
        from scripts.r11_backer.release_probes import contracts

        document = (
            "### Daily\n- **Endpoint:** `/api/v4/data`\n- **Tier:** Backer\n"
            "- **Required:** `dataset=Daily`, `start_date`\n"
            "- **Optional:** `end_date`\n- **Key columns:** date, value\n"
            "### Broker\n- **Endpoint:** `/api/v4/separate`\n- **Tier:** Sponsor\n"
        )
        result = contracts(document)
        self.assertEqual(tuple(result), ("Daily",))
        self.assertEqual(result["Daily"].fields, ("date", "value"))
        self.assertEqual(result["Daily"].tier, "Backer")

    def test_closed_days_exclude_current_and_future(self):
        from scripts.r11_backer.release_probes import Sample, recent_trading_days

        calendar = Sample("VALID_SAMPLE", rows=(
            {"date": "2026-11-24"}, {"date": "2026-11-25"},
            {"date": "2026-11-26"}, {"date": "not a date"},
        ))
        self.assertEqual(recent_trading_days(calendar, date(2026, 11, 25)), ["2026-11-24"])

    def test_market_batch_must_preserve_reference_rows(self):
        from scripts.r11_backer.release_probes import Sample, compare_market

        row = {"stock_id": "2330", "close": 10}
        single = Sample("VALID_SAMPLE", rows=(row,))
        market = Sample("VALID_SAMPLE", rows=(row, {"stock_id": "2317", "close": 1}))
        self.assertTrue(compare_market(single, market))
        self.assertFalse(compare_market(single, Sample("VALID_SAMPLE", rows=(row,))))
        self.assertFalse(compare_market(
            single, Sample("VALID_SAMPLE", rows=(row | {"close": 11}, market.rows[1])),
        ))
        self.assertFalse(compare_market(
            single, Sample("VALID_SAMPLE", rows=(row, row, market.rows[1])),
        ))

    def test_bond_identity_requires_effective_dates(self):
        from scripts.r11_backer.release_probes import Sample, active_bond

        base = {"cb_id": "17172", "InitialDateOfConversion": "2025-10-01"}
        candidates = Sample("VALID_SAMPLE", rows=(
            base | {"DueDateOfConversion": "2026-10-31"},
            base | {"cb_id": "17173", "DueDateOfConversion": "2027-01-01"},
        ))
        self.assertEqual(active_bond(candidates, "2026-11-02", "2026-11-07"), "17173")
        self.assertIsNone(active_bond(candidates, "2027-03-01", "2027-03-01"))

    def test_release_report_does_not_promote_unproven_requirements(self):
        from scripts.r11_backer.acceptance_report import evaluate

        rows = evaluate({"dataset_sweep_complete": True}, recovery=True, private=True)
        self.assertEqual(len({row["id"] for row in rows}), 76)
        statuses = {row["id"]: row["status"] for row in rows}
        self.assertEqual(statuses["R10.1-P18"], "HOLD")
        self.assertEqual(statuses["R11-T14"], "HOLD")
        self.assertEqual(statuses["R11-T01"], "HOLD")
        self.assertEqual(statuses["R11-T15"], "PASS_PILOT")

    def test_gzip_sample_limited_by_decoded_bytes_and_date_shape(self):
        import gzip
        import io
        from types import SimpleNamespace
        from unittest.mock import patch
        from scripts.r11_backer.release_probes import Contract, Task, request_sample

        raw = b'{"status":200,"data":[{"date":"2026-11-24 09:30:00","value":1}]}'
        task = Task("PublicExample", "2026-11-24", "2026-11-24")
        doc = Contract("Backer", "", "", ("date", "value"))

        def open_response(*args, **kwargs):
            response = io.BytesIO(gzip.compress(raw))
            response.headers = {"Content-Encoding": "gzip"}
            return response

        with patch("scripts.r11_backer.release_probes.build_opener",
                   return_value=SimpleNamespace(open=open_response)):
            capped = request_sample(task, doc, "synthetic", max_bytes=len(raw) - 1)
            inspected = request_sample(task, doc, "synthetic", max_bytes=len(raw))
        self.assertEqual(capped.status, "SIZE_LIMIT")
        self.assertEqual(inspected.status, "DATE_UNVERIFIED")
        self.assertEqual(inspected.date_issue, "ISO_DATE_TIME")
        self.assertEqual(inspected.rows, ())
