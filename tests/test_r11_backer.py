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
