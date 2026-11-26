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

    def test_market_subset_rejects_out_of_scope_even_with_reference_match(self):
        from scripts.r11_backer.market_scope import audit_market, market_universe
        from scripts.r11_backer.release_probes import Sample

        master = market_universe(({"stock_id": "2330", "type": "twse"},
                                  {"stock_id": "6488", "type": "tpex"},
                                  {"stock_id": "AB99", "type": "twse"}))
        reference = {"date": "2026-11-24", "stock_id": "2330"}
        market = Sample("VALID_SAMPLE", rows=(reference, {"date": "2026-11-24", "stock_id": "99999"}))
        check = audit_market(market, {"2330": Sample("VALID_SAMPLE", rows=(reference,))},
                             master, ("date", "stock_id"))
        self.assertEqual(check["matched_reference_queries"], 1)
        self.assertEqual(check["verdict"], "HOLD_OUT_OF_SCOPE_ROWS")

    def test_gold_day_marker_is_distinct_from_intraday_data(self):
        from scripts.r11_backer.release_probes import observation_date

        self.assertEqual(observation_date("GoldPrice", "2026-11-24 00:00:00"), date(2026, 11, 24))
        with self.assertRaises(ValueError):
            observation_date("GoldPrice", "2026-11-24 09:00:00")

    def test_dated_market_members_preserves_transfer_and_delisting(self):
        from scripts.r11_backer.contract_closeout import dated_market_members

        change = date(2015, 1, 1)
        stop = date(2020, 1, 1)
        history = (
            ("1234", "tpex", date(2010, 1, 1), change),
            ("1234", "twse", change, stop),
        )
        self.assertEqual(dated_market_members(history, date(2014, 1, 1)), {"1234": "tpex"})
        self.assertEqual(dated_market_members(history, change), {"1234": "twse"})
        self.assertEqual(dated_market_members(history, stop), {})
        with self.assertRaises(ValueError):
            dated_market_members((*history, ("1234", "twse", change, None)), change)

    def test_open_ended_limit_does_not_accept_future_or_missed_days(self):
        from scripts.r11_backer.contract_closeout import inspect_dates
        from scripts.r11_backer.release_probes import Sample, Task

        limit = Task("TaiwanStockPriceLimit", "2026-11-24", "2026-11-24", "2330", send_end=False)
        row = {"date": "2026-11-24"}
        next_day = {"date": "2026-11-25"}
        self.assertEqual(
            inspect_dates(limit, Sample("VALID_SAMPLE", rows=(row, next_day)), date(2026, 11, 25)),
            "OPEN_ENDED_RESPONSE_VERIFIED",
        )
        self.assertEqual(
            inspect_dates(limit, Sample("VALID_SAMPLE", rows=(row, next_day)), date(2026, 11, 24)),
            "HOLD_OUTSIDE_WINDOW",
        )
        self.assertEqual(
            inspect_dates(limit, Sample("VALID_SAMPLE", rows=(next_day,)), date(2026, 11, 25)),
            "HOLD_OUTSIDE_WINDOW",
        )

    def test_gold_daily_civil_date_disallows_multiple_same_day_observations(self):
        from scripts.r11_backer.contract_closeout import inspect_dates
        from scripts.r11_backer.release_probes import Sample, Task

        task = Task("GoldPrice", "2026-10-01", "2026-10-03")
        first = {"date": "2026-10-01 17:00:00+00:00"}
        second = {"date": "2026-10-02 17:00:00+00:00"}
        check = lambda rows: inspect_dates(task, Sample("VALID_SAMPLE", rows=rows), date(2026, 11, 25))
        self.assertEqual(check((first, second)), "VALID_SAMPLE")
        self.assertEqual(check((first, first)), "HOLD_MULTIPLE_RECORDS_PER_SOURCE_DAY")


class ManifestReadinessTests(unittest.TestCase):
    def test_manifest_requires_every_release_input(self):
        from scripts.r11_backer.manifest_pilot import fixture_approvals, manifest_windows

        candidate = fixture_approvals("1990-01-01")[0]
        candidate["evidence"].pop("coverage_bounds")
        with self.assertRaisesRegex(ValueError, "missing release evidence"):
            manifest_windows([candidate])

    def test_manifest_clips_to_effective_identity_and_is_newest_first(self):
        from scripts.r11_backer.manifest_pilot import fixture_approvals, manifest_windows

        candidate = fixture_approvals("1990-01-01")[0] | {
            "listing_start": "1992-11-13", "listing_end": "1993-01-15",
        }
        result = manifest_windows([candidate])
        self.assertEqual(
            [(x.start.isoformat(), x.end.isoformat()) for x in result],
            [("1993-01-01", "1993-01-15"), ("1992-12-01", "1992-12-31"),
             ("1992-11-13", "1992-11-30")],
        )

    def test_readiness_distinguishes_live_sample_from_release_approval(self):
        from scripts.r11_backer.manifest_pilot import sample_data

        rows = sample_data()
        self.assertEqual(len(rows), 89)
        self.assertEqual(sum(row["state"] == "SAMPLE_ONLY" for row in rows), 69)
        self.assertEqual(next(row["state"] for row in rows if row["dataset"] == "GoldPrice"), "EXCLUDED_SUBDAILY")
        self.assertTrue(all(row["state"] != "PRODUCTION_APPROVED" for row in rows))


class ReleaseRemediationTests(unittest.TestCase):
    def test_native_futures_identity_replaces_label_and_rejects_duplicates(self):
        from scripts.r11_backer.native_contracts import native_result
        from scripts.r11_backer.release_probes import Contract, Sample, Task

        task = Task("TaiwanFuturesInstitutionalInvestors", "2026-11-24", "2026-11-24", "TX")
        doc = Contract("Backer", "", "", ("name", "date", "institutional_investors", "long_deal_volume"))
        row = {"futures_id": "TX", "institutional_investors": "sample", "date": task.start, "long_deal_volume": 1}
        self.assertEqual(native_result(task, doc, Sample("FIELD_DIFFERENCES", rows=(row,))), "PASS_NATIVE_SAMPLE")
        self.assertEqual(native_result(task, doc, Sample("FIELD_DIFFERENCES", rows=(row, row))), "HOLD_DUPLICATE_KEY")
        self.assertEqual(native_result(task, doc, Sample("FIELD_DIFFERENCES", rows=(row | {"futures_id": "MTX"},))),
                         "HOLD_ENTITY_MISMATCH")

    def test_bond_investor_net_must_match_native_categories(self):
        from scripts.r11_backer.native_contracts import INVESTORS, bond_investor_balance

        row = {f"{owner}_{action}": value for owner in INVESTORS
               for action, value in (("Buy", 9), ("Sell", 6), ("Overbuy", 3))}
        self.assertTrue(bond_investor_balance(row | {"Total_Overbuy": 9}))
        self.assertFalse(bond_investor_balance(row | {"Total_Overbuy": 6}))
        self.assertFalse(bond_investor_balance(row | {"Total_Overbuy": 9, "Dealer_self_Sell": 7}))

    def test_start_only_replay_keeps_raw_positions_for_later_dates(self):
        from scripts.r11_backer.release_probes import Sample
        from scripts.r11_backer.release_remediation import PRICE_LIMIT_TASKS, suffix_index

        prior, replay = PRICE_LIMIT_TASKS
        closed = date(2026, 11, 24)
        rows = tuple({"stock_id": "2330", "date": day, "reference_price": 10, "limit_up": 11, "limit_down": 9}
                     for day in ("2000-01-04", "2026-11-24", "2026-11-27"))
        samples = {prior: Sample("VALID_SAMPLE", rows=rows), replay: Sample("VALID_SAMPLE", rows=rows[1:])}
        report, index = suffix_index(PRICE_LIMIT_TASKS, samples, closed, closed)
        self.assertEqual(report["status"], "PASS_START_ONLY_SNAPSHOT_SAMPLE")
        self.assertEqual(index["months"], {"2000-01": [0], "2026-11": [1]})
        self.assertEqual(index["after_verified_close"], [2])
        samples[replay] = Sample("VALID_SAMPLE", rows=rows[1:2])
        self.assertEqual(suffix_index(PRICE_LIMIT_TASKS, samples, closed, closed)[0]["status"],
                         "HOLD_START_ONLY_SUFFIX_MISMATCH")

    def test_official_close_rejects_wrong_day_and_reconciles_original_positions(self):
        from decimal import Decimal
        from scripts.r11_backer.exchange_scope import parse_official_close, reconcile_close
        from scripts.r11_backer.release_probes import Sample

        day = date(2026, 11, 24)
        table = {"date": "20261124", "stat": "ok", "tables": [
            {"fields": ["代號", "收盤 "], "data": [["1234", "1,085.20"], ["061234", "0.50"]]},
        ]}
        official = {"TPEx": parse_official_close(table, day, "TPEx"), "TWSE": {"2330": Decimal("90")}}
        with self.assertRaises(ValueError):
            parse_official_close(table, date(2026, 11, 23), "TPEx")
        raw = ({"stock_id": "061234", "close": 0.5}, {"stock_id": "1234", "close": 1085.2},
               {"stock_id": "2330", "close": 90})
        result, positions = reconcile_close(Sample("VALID_SAMPLE", rows=raw), official)
        self.assertEqual(result["status"], "PASS_DATED_CLOSE_SAMPLE")
        self.assertEqual(result["rows_outside_dated_reference"], 1)
        self.assertEqual(positions, [1, 2])
        failed, _ = reconcile_close(Sample("VALID_SAMPLE", rows=raw[:2]), official)
        self.assertEqual(failed["status"], "HOLD_DATED_CLOSE_RECONCILIATION")
        self.assertEqual(failed["markets"]["TWSE"]["missing"], 1)

    def test_direct_fallback_requires_exact_date_identity_and_official_close(self):
        from decimal import Decimal
        from scripts.r11_backer.release_probes import Sample, Task
        from scripts.r11_backer.release_remediation import direct_close_result

        task = Task("TaiwanStockPrice", "2010-11-24", "2010-11-24", "1234")
        row = {"stock_id": "1234", "date": task.start, "close": 10.3}
        self.assertEqual(direct_close_result(task, Sample("VALID_SAMPLE", rows=(row,)), Decimal("10.30")),
                         "PASS_DIRECT_CLOSE")
        self.assertEqual(direct_close_result(task, Sample("VALID_SAMPLE", rows=(row,)), Decimal("10.31")),
                         "HOLD_DIRECT_CLOSE_MISMATCH")
        self.assertEqual(direct_close_result(task, Sample("VALID_SAMPLE", rows=(row | {"stock_id": "1235"},)),
                                             Decimal("10.30")), "HOLD_DIRECT_IDENTITY_OR_DATE")

    def test_gold_distinguishes_repeated_rows_from_distinct_intraday_observations(self):
        from scripts.r11_backer.release_probes import Sample
        from scripts.r11_backer.release_remediation import gold_outcome

        same = {"date": "2026-11-24 09:00:00"}
        later = {"date": "2026-11-24 09:05:00"}
        repeated = gold_outcome(Sample("VALID_SAMPLE", rows=(same, same)))
        self.assertEqual(repeated["status"], "HOLD_DAILY_OBSERVATION_AND_PUBLICATION_CONTRACT")
        intraday = gold_outcome(Sample("VALID_SAMPLE", rows=(same, same, later)))
        self.assertEqual(intraday["status"], "EXCLUDE_SUBDAILY_SOURCE_SERIES")
        self.assertEqual(intraday["most_common_interval_seconds"], 300)


class HistoricalCapacityPilotTests(unittest.TestCase):
    def test_month_manifest_detects_gaps_and_keeps_raw_offsets_after_sorting(self):
        from scripts.r11_backer.history_pilot import (
            BUSINESS_START, NDC_PERIOD, classify_business, months, raw_month_manifest,
        )
        from scripts.r11_backer.release_probes import Sample

        rows = tuple({"date": day.isoformat(), "leading": 1, "coincident": 1, "lagging": 1,
                      "monitoring": 41, "monitoring_color": "sample"} for day in months(BUSINESS_START, NDC_PERIOD))
        trial = date(2026, 11, 26)
        suffix = Sample("VALID_SAMPLE", rows=rows[-5:])
        earlier = Sample("EMPTY_WINDOW")
        result, index = classify_business(Sample("VALID_SAMPLE", rows=rows), suffix, earlier, trial)
        self.assertEqual(result["status"], "PASS_PROVIDER_WINDOW")
        self.assertEqual(result["verified_months"], 537)
        self.assertEqual(raw_month_manifest(index)["newest_first_month_offsets"][:1], [["2026-09", 536]])
        incomplete = Sample("VALID_SAMPLE", rows=rows[:1] + rows[2:])
        blocked, _ = classify_business(incomplete, suffix, earlier, trial)
        self.assertEqual(blocked["status"], "HOLD_UNVERIFIED_MONTHLY_WINDOW")
        self.assertEqual(blocked["missing_months"], 1)

    def test_calendar_gap_counts_old_trade_as_known_without_releasing_history(self):
        from scripts.r11_backer.history_pilot import BENCHMARK_ARCHIVE_BYTES, calendar_evidence
        from scripts.r11_backer.release_probes import Sample

        calendar = Sample("VALID_SAMPLE", rows=({"date": "1999-01-05"}, {"date": "2026-11-24"}))
        prices = Sample("VALID_SAMPLE", rows=({"date": "1994-09-05", "stock_id": "2330"},))
        result = calendar_evidence(calendar, prices)
        self.assertEqual(result["status"], "HOLD_CALENDAR_ROSTER_GAP")
        self.assertEqual(result["early_trade_days_missing_from_calendar"], 1)
        self.assertEqual(result["illustrative_known_day_bytes"], 3 * BENCHMARK_ARCHIVE_BYTES)

    def test_month_revenue_uses_official_release_and_next_day(self):
        from scripts.r11_backer.history_pilot import revenue_evidence
        from scripts.r11_backer.release_probes import Sample

        rows = tuple({"date": f"2026-{month + 1:02}-01", "revenue_year": 2026, "revenue_month": month,
                      "stock_id": "2330", "create_time": "2026-11-26", "revenue": millions * 1_000_000}
                     for month, millions in ((9, 511857), (10, 586890)))
        report = revenue_evidence(Sample("VALID_SAMPLE", rows=rows))
        self.assertEqual(report["status"], "PASS_PIT_PUBLICATION_DATE_SAMPLE")
        self.assertEqual(report["periods"][1]["official_announced_on"], "2026-11-10")
        self.assertEqual(report["periods"][1]["pit_calendar_lower_bound"], "2026-11-11")
        self.assertEqual(report["periods"][1]["provider_query_date"], "2026-11-01")

    def test_twse_history_rejects_shifted_or_duplicate_sessions(self):
        from scripts.r11_backer.history_pilot import parse_twse_sessions

        month = date(1994, 9, 1)
        official = {"stat": "OK", "date": "19940901", "fields": ["日期", "成交股數"],
                    "data": [[" 83/09/13", "100,000"], [" 83/09/14", "200,000"]]}
        self.assertEqual(parse_twse_sessions(official, month), {date(1994, 9, 13), date(1994, 9, 14)})
        with self.assertRaises(ValueError):
            parse_twse_sessions(official, date(1994, 10, 1))
        with self.assertRaises(ValueError):
            parse_twse_sessions(official | {"data": official["data"] * 2}, month)

    def test_partial_exchange_artifact_cannot_silently_change_scope(self):
        from unittest.mock import patch
        from scripts.r11_backer.history_pilot import canonical_json, sha, verified_exchange_months

        start, end = date(1994, 9, 1), date(1994, 10, 1)
        prior = {"venue": "TWSE", "start_month": "1994-09-01", "end_month": "1994-10-01",
                 "newest_first_sessions": ["1994-09-13"]}
        with patch("scripts.r11_backer.history_pilot.PREVIOUS_INDEX_SHA256", sha(canonical_json(prior))):
            self.assertEqual(verified_exchange_months(prior, start, end), {start: ("PASS", {date(1994, 9, 13)})})
            changed = prior | {"newest_first_sessions": ["1994-09-14"]}
            with self.assertRaises(ValueError):
                verified_exchange_months(changed, start, end)


class TPExHistoricalEvidenceTests(unittest.TestCase):
    def test_calendar_with_adjacent_conflicting_dates_keeps_only_agreed_dates(self):
        from scripts.r11_backer.tpex_history import compare_calendar

        agreed, sunday, monday = (date(1999, 5, day) for day in (29, 30, 31))
        report, dates = compare_calendar(1999, {agreed, sunday}, {agreed, monday, date(2000, 1, 1)})
        self.assertEqual(report['status'], 'HOLD_CALENDAR_DATE_CONFLICT')
        self.assertEqual(report['tpex_yearbook_only'], ['1999-05-30'])
        self.assertEqual(report['finmind_only'], ['1999-05-31'])
        self.assertEqual(dates, {agreed})

    def test_transfer_window_requires_exclusive_membership_and_matching_prices(self):
        from decimal import Decimal
        from scripts.r11_backer.release_probes import Sample
        from scripts.r11_backer.tpex_history import transfer_proof

        before, switch = date(2024, 1, 24), date(2024, 1, 25)
        good = "VALID_OFFICIAL_DAY"
        quotes = {("TPEx", before): (good, {"6446": Decimal("1.5")}, {}), ("TWSE", before): (good, {}, {}),
                  ("TPEx", switch): (good, {}, {}), ("TWSE", switch): (good, {"6446": Decimal("2.5")}, {})}
        provider = Sample("VALID_SAMPLE", rows=({"date": str(before), "stock_id": "6446", "close": 1.5},
                                                 {"date": str(switch), "stock_id": "6446", "close": 2.5}))
        verdict, result = transfer_proof("6446", switch, quotes, provider)
        self.assertEqual(verdict, "PASS_TRANSFER_PRICE_AND_VENUE_SAMPLE")
        self.assertEqual(result[0]["end_exclusive"], str(switch))
        self.assertEqual(result[1]["start"], str(switch))
        quotes["TPEx", switch] = (good, {"6446": Decimal("2.5")}, {})
        self.assertEqual(transfer_proof("6446", switch, quotes, provider)[0], "HOLD_TRANSFER_MARKET_MEMBERSHIP")

    def test_official_endpoint_empty_and_delisting_year_remain_distinct(self):
        import json
        from scripts.r11_backer.tpex_history import close_rows, termination_rows

        empty = json.dumps({"stat": "ok", "date": "19961121", "tables": [{"data": []}]})
        self.assertEqual(close_rows(empty, date(1996, 11, 21), "TPEx"), ("EMPTY_OFFICIAL_ENDPOINT", {}, {}))
        termination = {"stat": "ok", "date": "2024", "tables": [
            {"fields": ["股票代號", "終止上櫃日期", "終止上櫃原因"], "data": [["6446", "113-01-25", "example"]]},
        ]}
        self.assertEqual(termination_rows(json.dumps(termination), 2024)[0]["end"], "2024-01-25")
        with self.assertRaises(ValueError):
            termination_rows(json.dumps(termination), 2023)

    def test_market_lifetimes_require_dated_issuer_identity_and_exclusive_intervals(self):
        from scripts.r11_backer.market_lifetimes import Event, identity_intervals, nonoverlapping

        begin, switch = date(1990, 6, 25), date(2000, 5, 31)
        start = Event("TPEx", "1234", "甲公司", begin, "start#1")
        end = Event("TPEx", "1234", "甲公司", switch, "end#1")
        other_issuer = Event("TPEx", "4321", "甲公司", begin, "start#2")
        second_exit = Event("TPEx", "4321", "乙公司", switch, "end#2")
        active = {"market": "TWSE", "code": "1234", "issuer": "甲公司", "start": str(switch),
                  "as_of": "2026-11-26", "source": "active#1"}
        intervals, held = identity_intervals([start, other_issuer], [end, second_exit], [active])
        self.assertEqual(len(intervals), 2)
        self.assertEqual(intervals[0]["end_exclusive"], intervals[1]["start"])
        self.assertEqual(held[0]["reason"], "HOLD_UNPROVEN_HISTORICAL_START_OR_IDENTITY")
        accepted, collisions = nonoverlapping([intervals[0], intervals[1] | {"start": "2000-05-30"}])
        self.assertEqual(accepted, [])
        self.assertEqual(len(collisions), 2)

    def test_official_fallback_reuses_original_row_without_reclassifying_backer_gap(self):
        from scripts.r11_backer.market_lifetimes import fallback_receipts
        from scripts.r11_backer.test_archive import json_bytes

        first = ["1234", "10.5", "5,000", "2"]
        zero_volume = ["5678", "20.5", "0", "0"]
        document = {"stat": "ok", "date": "20101124", "tables": [
            {"fields": ["代號", "收盤 ", "成交股數", "成交筆數"], "data": [first, zero_volume]},
        ]}
        raw = json_bytes(document)
        gaps = [{"code": code, "window_status": "HOLD_PROVIDER_WINDOW_EMPTY"} for code in ("5678", "1234", "1234")]
        receipts = fallback_receipts(raw, gaps)
        self.assertEqual(receipts, fallback_receipts(raw, reversed(gaps)))
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["source_row_sha256"], hashlib.sha256(json_bytes(first)).hexdigest())
        self.assertEqual(receipts[0]["original_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(receipts[0]["backer_status"], "HOLD_SOURCE_GAP")

    def test_early_tpex_rows_use_requested_month_symbol_and_actual_turnover(self):
        from scripts.r11_backer.test_archive import json_bytes
        from scripts.r11_backer.tpex_calendar import Source, monthly_rows

        month = date(1994, 1, 1)
        source = Source(month, "5301")
        document = {"stat": "ok", "date": "19940101", "code": "5301", "tables": [
            {"fields": ["日 期", "成交仟股", "收盤", "筆數"],
             "data": [["83/01/05", "0", "--", "0"], ["83/01/25＊", "0", "11.50", "2"]]},
        ]}
        result = monthly_rows(json_bytes(document), source)
        self.assertFalse(result[date(1994, 1, 5)]["traded"])
        self.assertTrue(result[date(1994, 1, 25)]["traded"])
        with self.assertRaises(ValueError):
            monthly_rows(json_bytes(document), Source(month, "5302"))
        document["tables"][0]["data"].append(["83/02/01", "2", "12", "1"])
        with self.assertRaises(ValueError):
            monthly_rows(json_bytes(document), source)

    def test_tpex_yearbook_correction_needs_matching_index_and_dated_trades(self):
        from decimal import Decimal
        from scripts.r11_backer.tpex_calendar import verified_date_correction

        old, expected = date(1999, 5, 30), date(1999, 5, 31)
        market = {expected: {"source": "market#23", "traded": True, "reported_value": 169.86}}
        stock = {expected: {"source": "stock#23", "traded": True, "positive_price": True}}
        self.assertEqual(verified_date_correction({old}, {expected}, market, stock, Decimal("169.86"))["status"],
                         "PASS_YEARBOOK_DATE_CORRECTION")
        self.assertEqual(verified_date_correction({old}, {expected}, market, stock, Decimal("169.85"))["status"],
                         "HOLD_DATE_CONFLICT")
        stock[expected]["traded"] = False
        self.assertEqual(verified_date_correction({old}, {expected}, market, stock, Decimal("169.86"))["status"],
                         "HOLD_DATE_CONFLICT")

    def test_tp_ex_annual_turnover_does_not_force_one_day_count_from_rounded_values(self):
        from decimal import Decimal
        from scripts.r11_backer.tpex_calendar import supported_session_counts

        self.assertEqual(supported_session_counts(Decimal("11981.58"), Decimal("44.21")), [271])
        self.assertGreater(len(supported_session_counts(Decimal("27.96"), Decimal("0.10"))), 1)
        self.assertEqual(supported_session_counts(Decimal("0.01"), None), [])

    def test_archive_checks_final_timestamp_and_steps_when_job_status_is_stale(self):
        from unittest.mock import Mock, patch
        from scripts.r11_backer.test_archive import ARCHIVE_JOB, attempt_jobs

        published = {"status": "in_progress", "completed_at": "2026-11-25T17:25:29Z", "conclusion": "success",
                     "steps": [{"status": "completed"}]}
        pending = published | {"completed_at": None, "conclusion": None}
        github = Mock()
        github.pages.side_effect = [[pending], [published, {"name": ARCHIVE_JOB, "status": "in_progress"}]]
        with patch("scripts.r11_backer.test_archive.time.sleep") as wait:
            self.assertEqual(attempt_jobs(github, "attempt", current=True), [published])
        wait.assert_called_once_with(2)

    def test_calendar_resume_only_downloads_missing_sources(self):
        from unittest.mock import patch
        from scripts.r11_backer.test_archive import json_bytes
        from scripts.r11_backer.tpex_calendar import EVIDENCE_RUN, resume_sources

        origins = {"a.json": "https://example.com/a", "b.json": "https://example.com/b"}
        report = {"official_sources": origins, "source_run": EVIDENCE_RUN,
                  "official_contract_statuses": {"a.json": "VALID_OFFICIAL_ROWS"}}
        archive = {"reconciliation.json": json_bytes(report), "official/a.json": b"original"}
        with patch("scripts.r11_backer.tpex_calendar.earlier_evidence", return_value=archive):
            self.assertEqual(resume_sources(1234, origins), {"a.json": ("REUSED_VERIFIED_SOURCE", b"original")})
            with self.assertRaises(ValueError):
                resume_sources(1234, origins | {"b.json": "https://example.com/other"})

    def test_managed_stock_turnover_matches_only_with_correct_session_scope(self):
        from scripts.r11_backer.test_archive import json_bytes
        from scripts.r11_backer.tpex_closeout import FULL_QUOTES, FIXED_QUOTES, managed_stock_reconciliation

        def daily(title, columns, values):
            return json_bytes({"stat": "ok", "date": "20101124", "tables": [
                {"title": title, "fields": ["代號", *columns], "data": [["1234", *values]]},
            ]})

        fields = ["開盤", "最高", "最低", "收盤", "成交股數", "成交金額(元)", "成交筆數"]
        ohlc = ["1.34", "1.39", "1.34", "1.39"]
        source = "official/TPEx-2010-11-24.json"
        paid = {"dataset": "TaiwanStockPrice", "start_date": "2010-11-24", "end_date": "2010-11-24"}
        summary = {"backer_requests": [{"request": paid, "status": "VALID_SAMPLE", "original": "original.json"}],
                   "historical_price_gaps": [{"code": "1234", "window_status": "HOLD_PROVIDER_WINDOW_EMPTY"}]}
        baseline = {"reconciliation.json": json_bytes(summary), "original.json": json_bytes({"data": []}),
                    source: daily("上櫃股票每日收盤行情(不含定價)", fields, [*ohlc, "80,000", "110,350", "10"])}
        monthly = {"stat": "ok", "date": "20101101", "code": "1234", "tables": [
            {"fields": ["日 期", "成交仟股", "成交仟元", "開盤", "最高", "最低", "收盤", "筆數"],
             "notes": ["以上資料不含上櫃股票鉅額交易"],
             "data": [["99/11/24", "82", "112", *ohlc, "16"]]},
        ]}
        responses = {FULL_QUOTES: daily("管理股票", fields, [*ohlc, "81,600", "112,411", "16"]),
                     FIXED_QUOTES: daily("盤後定價行情", ["成交筆數"], ["0"]),
                     "tpex-1234-2010-11.json": json_bytes(monthly)}
        public, private = managed_stock_reconciliation(baseline, responses)
        self.assertEqual(public["official_fallback_rows"], 1)
        self.assertEqual(private["managed_stock_receipts"][0]["differences_from_regular"], [1600, 2061, 6])
        self.assertEqual(private["managed_stock_receipts"][0]["backer_status"], "HOLD_PROVIDER_SCOPE_GAP")
        monthly["tables"][0]["data"][0][-1] = "10"
        public, _ = managed_stock_reconciliation(baseline, responses | {"tpex-1234-2010-11.json": json_bytes(monthly)})
        self.assertEqual(public["status"], "HOLD_OFFICIAL_SCOPE")

    def test_dated_transfer_requires_unique_otc_start_and_official_issuer_profile(self):
        from scripts.r11_backer.market_lifetimes import Event
        from scripts.r11_backer.tpex_closeout import verified_handover_intervals

        start = Event("TPEx", "1234", "甲公司", date(2000, 5, 1), "tpex-start#1")
        end = Event("TPEx", "1234", "甲公司股份有限公司", date(2010, 11, 24), "tpex-end#1")
        profile = {"code": "1234", "issuer": end.issuer, "alias": "甲股", "company_id": "12345678",
                   "start": str(end.day), "source": "official-profile#1"}
        summary = {"handover_events": [{"code": "1234", "effective": str(end.day),
                                        "proof": [end.source, "twse-listing#1"]}],
                   "listing_intervals": [{"market": "TWSE", "code": "1234", "start": str(end.day),
                                          "end_exclusive": "2026-11-26", "kind": "OFFICIAL_ACTIVE_THROUGH_PUBLICATION",
                                          "proof": [profile["source"]]}],
                   "held_intervals_detail": [{"market": "TPEx", "source": end.source,
                                              "reason": "HOLD_UNPROVEN_HISTORICAL_START_OR_IDENTITY"}]}
        rows = verified_handover_intervals({"1234": [start]}, {"1234": [end]}, [profile], summary)
        self.assertEqual([(row["start"], row["end_exclusive"]) for row in rows], [(str(start.day), str(end.day))])
        self.assertEqual(verified_handover_intervals({"1234": [start, start]}, {"1234": [end]}, [profile], summary), [])
        self.assertEqual(verified_handover_intervals({"1234": [start]}, {"1234": [end]},
                                                     [profile | {"issuer": "乙股份有限公司"}], summary), [])

    def test_legacy_listing_date_is_read_from_the_complete_table_cell(self):
        from scripts.r11_backer.market_chronology import parse_listing_book

        doc = """<book><para>表一、八十八年度新上櫃公司資料</para><informaltable><tbody>
          <row><entry>上櫃
          日期</entry><entry>編號</entry><entry>公司名稱</entry></row>
          <row><entry>88.12.01</entry><entry>1234</entry><entry>甲公司</entry></row>
          </tbody></informaltable></book>"""
        self.assertEqual(parse_listing_book(doc, 1999)["1234"]["start"], "1999-12-01")
        with self.assertRaises(ValueError):
            parse_listing_book(doc.replace("88.12.01", "88.12.0"), 1999)

    def test_listing_sources_conflict_or_missing_symbol_proof_keeps_interval_pending(self):
        from scripts.r11_backer.market_chronology import PENDING, reconcile_pending
        from scripts.r11_backer.market_lifetimes import Event

        otc = Event("TPEx", "1234", "甲公司股份有限公司", date(2011, 3, 7), "tp-end")
        main = Event("TWSE", "5678", "乙公司", date(2015, 2, 6), "tw-end")
        other = Event("TPEx", "1234", "甲公司", date(2000, 1, 6), "different-start")
        ledger = {"held_intervals_detail": [{"source": event.source, "reason": PENDING} for event in (otc, main)]}
        end_rows = {("TPEx", otc.code): [otc], ("TWSE", main.code): [main]}
        yearbook = {otc.code: [{"issuer": "甲公司", "start": "2000-01-07", "source": "yearbook#3"}]}
        dates = [(date(1983, 1, 26), "LISTED", "乙公司")]
        accepted, held, _ = reconcile_pending(ledger, [], {("TPEx", otc.code): [other]}, end_rows, yearbook, dates, [])
        self.assertEqual(accepted, [])
        self.assertEqual([row["disposition"] for row in held],
                         ["HOLD_OFFICIAL_START_DATE_CONFLICT", "HOLD_HISTORICAL_SYMBOL_LINK"])

    def test_annual_publication_code_and_effective_date_both_must_match(self):
        from scripts.r11_backer.twse_historical_codes import reviewed_case

        case = {"code": "1234", "issuer": "甲公司", "dated_name_only_start": "1998-06-29"}
        row = {"code": "1235", "docket_issuer": "甲公司", "date": "1998-06-29"}
        self.assertEqual(reviewed_case(case, [row])[0], "HOLD_HISTORICAL_CODE_CONFLICT")
        self.assertEqual(reviewed_case(case, [row | {"code": "1234", "date": "1998-06-30"}])[0],
                         "HOLD_OFFICIAL_START_DATE_CONFLICT")
        self.assertEqual(reviewed_case(case, [row | {"code": "1234"}])[0], "PASS")

    def test_official_stock_day_floor_is_separate_from_completed_date_coverage(self):
        from scripts.r11_backer.test_archive import json_bytes
        from scripts.r11_backer.twse_historical_codes import daily_probe_results

        name = "twse-day-1234-1998-06-29.json"
        floor = json_bytes({"stat": "查詢日期小於99年1月4日，請重新查詢!"})
        self.assertEqual(daily_probe_results({name: floor}, {name: "official"}), {
            name: "HOLD_OFFICIAL_DATE_FLOOR_2010_01_04",
        })
        self.assertEqual(daily_probe_results({}, {name: "official"}), {name: "HOLD_SOURCE_UNAVAILABLE"})
