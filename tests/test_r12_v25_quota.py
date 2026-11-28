"""Decisive quota and manifest regression cases for the V2.5 adapter."""

from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from unittest import TestCase
from unittest.mock import patch

from scripts.r11_backer import autonomous_quota as quota, bulk_batch, release_batch
from scripts.r11_backer.integration_pilot import digest, encode
from scripts.r11_backer.quota import HOUR_MS, REBALANCE_MS, available_slots, paced_slot
from scripts.r11_backer.v24_regression import fixture, fetch_fixture
from scripts.r11_backer.v25_account import FinMindTransport
from scripts.r11_backer.v25_contracts import TRADING_DATE, observed_contract
from scripts.r11_backer.v25_manifest import build_partition, history_denominator, planned_request_identity, query_parameters


class Journal:
    def __init__(self, state):
        self.lock = Lock()
        self.rows = [self.record(0, state, None)]
        self.ids = 0

    def record(self, seq, state, previous):
        return {"sequence": seq, "state": state, "previous": previous, "next_id": f"row-{seq+1}"}

    def head(self):
        return self.rows[-1]

    def load(self, file_id, sequence):
        return self.rows[sequence] if file_id == f"row-{sequence}" and len(self.rows) > sequence else None

    def new_ids(self, count):
        self.ids += count
        return [f"file-{i}" for i in range(self.ids - count, self.ids)]

    def append(self, old, state):
        with self.lock:
            if self.head() != old:
                return False
            self.rows.append(self.record(len(self.rows), state, digest(encode(old))))
            return True


class Cases(TestCase):
    def book(self, now=10_000, *, limit=1600, prior=None):
        state = quota.initial(now, policy_evidence="test", prior=prior)
        state["limit"] = limit
        j = Journal(quota.checkpoint(state))
        clock = [now]
        owner = quota.AccountBroker(j, "run-1:job", clock=lambda: clock[0])
        owner.acquire()
        return j, clock, owner

    def test_real_rolling_model_and_no_burst_at_hour_boundary(self):
        marks, groups = [], []
        for begin in range(0, 3 * HOUR_MS, REBALANCE_MS):
            add = available_slots(marks, begin, last_start=marks[-1] if marks else None)
            groups.append(len(add))
            marks.extend(add)
        self.assertEqual(groups[:6], [267, 267, 266, 267, 267, 266])
        self.assertEqual(len(marks), 4800)
        self.assertTrue(all(bisect_right(marks, t) - bisect_right(marks, t-HOUR_MS) <= 1600 for t in marks))
        self.assertEqual(paced_slot([1000], 100_000, last_start=1000), 100_000)
        self.assertEqual(len(available_slots([], 0, 600)), 100)

    def test_concurrent_workers_compete_for_one_slot(self):
        j, clock, owner = self.book(limit=1)

        def reserve(i):
            try:
                return owner.reserve(str(i), "data", "B01", "fixture", preflight=True)
            except quota.Deferred:
                return None

        with ThreadPoolExecutor(max_workers=4) as workers:
            grants = list(workers.map(reserve, range(4)))
        self.assertEqual(sum(g is not None for g in grants), 1)
        other = quota.AccountBroker(j, "competing-run", clock=lambda: clock[0])
        with self.assertRaisesRegex(quota.Deferred, "B00_WRITER_ACTIVE"):
            other.acquire()

    def test_expired_grant_stays_charged_and_clock_cannot_rewind(self):
        j, clock, owner = self.book()
        grant = owner.reserve("older", "data", "B01", "fixture", preflight=True)
        clock[0] = grant.deadline + 1
        with self.assertRaisesRegex(quota.Deferred, "EXPIRED_OR_USED"):
            owner.begin(grant)
        owner.result(grant, None, status="NOT_SENT")
        self.assertEqual(owner.book["preflight"]["reserved"]["data"], 1)
        clock[0] = 9_999
        with self.assertRaisesRegex(quota.Deferred, "WRITER_OR_CLOCK_HOLD"):
            owner.reserve("late", "data", "B01", "fixture", preflight=True)

    def test_crash_checkpoint_and_new_run_fence_keep_unknown_charge(self):
        j, clock, owner = self.book()
        grant = owner.reserve("lost", "data", "B01", "fixture", preflight=True)
        clock[0] += quota.LEASE_MS + 1
        new = quota.AccountBroker(j, "run-2:job", clock=lambda: clock[0])
        new.acquire()
        with self.assertRaisesRegex(quota.Deferred, "ALREADY_CHARGED"):
            new.reserve("lost", "data", "B03", "fixture", preflight=True)
        with self.assertRaisesRegex(quota.Deferred, "EXPIRED_OR_USED"):
            new.begin(grant)
        self.assertEqual(new.book["preflight"]["reserved"]["data"], 1)
        self.assertEqual(new.book["preflight"]["actual"]["data"], 0)

    def test_dynamic_quota_reallocation_removes_empty_lane(self):
        _, _, account = self.book()
        a = quota.rebalance(account.book, 10_000, {"B01": {"count": 500}, "B03": {"count": 500}, "B10": {"count": 900}})
        b = quota.rebalance(account.book, 10_000, {"B01": {"count": 0}, "B03": {"count": 500}})
        self.assertEqual(sum(a["lane_allocations"].values()), 267)
        self.assertEqual(b["lane_allocations"], {"B03": 267})
        self.assertNotIn("B10", a["eligible"])

    def test_rate_drop_and_all_account_cooldown(self):
        j, clock, owner = self.book()
        grant = owner.reserve("first", "data", "B02", "fixture", preflight=True)
        start = owner.begin(grant)
        owner.result(grant, start, status="HTTP_402", code=402)
        self.assertEqual(owner.book["paused"], 10_000 + HOUR_MS)
        self.assertEqual(owner.book["limit"], 1200)
        with self.assertRaisesRegex(quota.Deferred, "NEXT_PACED_SLOT"):
            owner.reserve("other-lane", "data", "B08", "other", preflight=True)
        retry = quota.response_policy(owner.book, 10_000, kind="data", scope="x", code=429, retry_after="7200", transient=False)
        self.assertEqual(retry["paused"], 7_210_000)

    def test_auth_scope_5xx_breaker_and_provider_floor(self):
        _, _, owner = self.book()
        denied = quota.response_policy(owner.book, 10_000, kind="data", scope="A", code=403, retry_after=None, transient=False)
        self.assertEqual(denied, {"denied": ["A"]})
        global_hold = quota.response_policy(owner.book, 10_000, kind="data", scope="A", code=401, retry_after=None, transient=False)
        self.assertTrue(global_hold["auth_hold"])
        paused = quota.response_policy(owner.book | {"errors": 2}, 10_000, kind="data", scope="A", code=503,
                                       retry_after=None, transient=True)
        self.assertGreater(paused["paused"], 10_000)
        cap = quota.response_policy(owner.book, 10_000, kind="account", scope="A", code=200, retry_after=None,
                                    transient=False, observed={"api_request_limit": 600, "user_count": 0})
        self.assertEqual(cap["limit"], 600)

    def test_no_user_info_needed_and_task_caps_are_inherited(self):
        p = {"reserved_data_requests": 47, "actual_data_requests": 46, "reserved_account_requests": 4, "actual_account_requests": 4}
        _, _, owner = self.book(prior=p)
        transport = FinMindTransport(owner, "fixture", preflight=True,
            payload=lambda *_: (b'{"status":200,"data":[]}', "identity"), account=lambda *_: self.fail("user_info should be optional"))
        evidence, reply = transport.fetch({"dataset": "fixture"}, 1000, identifier="increment", lane="B01")
        self.assertEqual(evidence["status"], "HTTP_RESPONSE_VERIFIED")
        self.assertIsNotNone(reply)
        with self.assertRaisesRegex(quota.Deferred, "TASK_CAP_EXHAUSTED"):
            owner.reserve("other", "data", "B09", "fixture", preflight=True)
        with self.assertRaisesRegex(quota.Deferred, "TASK_CAP_EXHAUSTED"):
            owner.reserve("extra", "account", "ACCOUNT", "account", preflight=True)

    def test_account_and_data_share_rolling_book(self):
        _, clock, owner = self.book()
        transport = FinMindTransport(owner, "fixture", preflight=True,
            payload=lambda *_: (b'{"status":200,"data":[]}', "identity"),
            account=lambda *_: {"user_count": 1, "api_request_limit": 1600})
        evidence, _ = transport.calibrate("account-query")
        self.assertEqual(evidence["status"], "HTTP_RESPONSE_VERIFIED")
        clock[0] += 2250
        transport.fetch({"dataset": "fixture"}, 512, identifier="data-page", lane="B02")
        self.assertEqual([p["kind"] for p in owner.book["events"]], ["account", "data"])

    def test_native_and_legacy_use_same_source_ingress(self):
        _, clock, owner = self.book()
        _, shard, source = fixture()
        transport = FinMindTransport(owner, "fixture", preflight=True,
            payload=lambda _, params, limit: fetch_fixture(source, allow_market=True)(params, limit))
        first = shard["rows"][0]
        legacy = {"task_id": "old-id", "dataset": "TaiwanStockPrice", "scope": "1001", "lane": "B01",
                  "start": source["first"], "end": source["last"]}
        with patch.object(release_batch, "proof_for", return_value={"test": True}), patch.object(release_batch, "stage_original"):
            status, _, _ = release_batch.receive({}, legacy, {"stage_id": "file-id", "attempts": 1}, "fixture", None, transport=transport)
        self.assertEqual(status, "STAGED")
        clock[0] += 2250
        native = {"schema_version": 24, "stage": "test", "items": [first], "index_sha256": "native-index"}
        j = Journal(bulk_batch.initial(native))
        with patch.object(bulk_batch, "stage_blob"):
            result = bulk_batch.receive_one(native, j.head(), j, first, "test", 123, None, None, 123, transport=transport)
        self.assertEqual(result["state"]["tasks"][first["task_id"]]["status"], "STAGED")
        self.assertEqual(owner.book["preflight"]["reserved"]["data"], 2)
        self.assertEqual({p["lane"] for p in owner.book["events"]}, {"B01", "B03"})

    def test_selector_yields_native_days_and_nonoverlapping_intervals(self):
        axes = {"trading_dates": ["1999-12-31", "2000-01-03", "2000-01-04"], "issuer_intervals": [],
                "existing_market_query_days": {"Price": ["2000-01-03"]},
                "legacy_receipt_intervals": {"Yield": {"x": [["2000-01-02", "2000-01-03"]]}}}
        day = {"dataset": "Price", "mode": "MARKET_DAY", "source_interval": ["1999-12-31", "2000-01-04"]}
        result = list(query_parameters(day, axes))
        self.assertEqual([r["start_date"] for r in result], ["1999-12-31", "2000-01-04"])
        a, b = planned_request_identity(day, result[0]); self.assertNotEqual(a, b)
        long = {"dataset": "Yield", "mode": "SERIES_RANGE", "source_interval": ["2000-01-01", "2000-01-04"],
                "selector": "BOUNDED_NATIVE_RANGE", "observed_data_ids": ["x"]}
        self.assertEqual([(r["start_date"], r["end_date"]) for r in query_parameters(long, axes)],
                         [("2000-01-01", "2000-01-01"), ("2000-01-04", "2000-01-04")])
        reg = [{"dataset": "Yield", "state": "CONTRACT_READY"}]
        self.assertIsNone(history_denominator(reg, [long], ["unknown_broker_coverage"]))
        self.assertEqual(history_denominator(reg, [long | {"full_universe_verified": True, "complete_native_window_count": 2}], []), 2)

    def test_archived_trading_dates_admit_snapshot_with_unique_dates(self):
        row = {"dataset": TRADING_DATE}
        evidence = {"evidence": {"anchor": "taiwanstocktradingdate"}, "parameters": row}
        raw = b'{"status":200,"data":[{"date":"2002-01-03"},{"date":"1999-12-31"}]}'
        rule, mode, dates, proof = observed_contract(row, evidence, raw)
        self.assertEqual((mode, rule.key_fields, dates), ("SNAPSHOT", ("date",), ["1999-12-31", "2002-01-03"]))
        self.assertFalse(proof["complete_market_history"])

    def test_missing_official_start_keeps_observed_entity_as_bounded_scope(self):
        name = "TaiwanStockWeekPrice"
        sample = {"parameters": {"dataset": name, "data_id": "2330", "start_date": "2005-01-03", "end_date": "2005-01-10"},
                  "observations": ["2005-01-03", "2005-01-10"]}
        row = {"dataset": name, "grain": "weekly", "schema": {"identity_field": "stock_id"},
               "source_dates_documented_from": [], "evidence": [], "completeness_methods": ["expected_keys"],
               "mode_contracts": {"ENTITY_RANGE": {"required_parameters": ["dataset", "data_id", "start_date"]}}}
        axes = {"source_witnesses": {name: [sample]}, "issuer_intervals": [
                    {"id": "5555", "start": "1990-01-01", "end_exclusive": "2025-01-01"}],
                "legacy_receipt_intervals": {}, "trading_dates": [], "existing_market_query_days": {}}
        part, _ = build_partition(row, axes, {})
        self.assertEqual(part["status"], "ARCHIVE_WITNESS_ONLY")
        self.assertEqual(part["first_boundary_type"], "OBSERVED_SAMPLE_BOUND_ONLY")
        self.assertIn("OFFICIAL_START_NOT_WITNESSED", part["unknown_boundaries"])
        self.assertEqual(part["scheduled_requests"], 0)
        self.assertIsNone(history_denominator([row | {"state": "CONTRACT_READY"}], [part], part["unknown_boundaries"]))
