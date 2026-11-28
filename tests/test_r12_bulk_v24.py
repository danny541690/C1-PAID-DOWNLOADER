"""Required regressions for the native controller, evidence gates, and account bootstrap."""

from dataclasses import replace
from datetime import datetime
import gzip
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch

from scripts.r11_backer import bulk_batch, v21_broker as broker
from scripts.r11_backer.archive import compress_original
from scripts.r11_backer.bulk_manifest import (
    compile_window, native_proof, optimize, read_manifest, window_months,
)
from scripts.r11_backer.integration_pilot import encode
from scripts.r11_backer.live_bronze_admission import fetch_native_payload
from scripts.r11_backer.v24_regression import fixture
from scripts.r11_backer.v24_sources import topics
from scripts.r11_backer.v2_registry import NativeContract, validate_pages


class MemoryJournal:
    def __init__(self, state):
        self.record = {"state": state, "sequence": 0, "next_id": "version-1"}

    def head(self):
        return self.record

    def append(self, old, state):
        if old != self.record:
            return False
        sequence = old["sequence"] + 1
        self.record = {"state": state, "sequence": sequence, "next_id": f"version-{sequence + 1}"}
        return True

    def load(self, file_id, sequence):
        return self.record if self.record["sequence"] == sequence and file_id == f"version-{sequence}" else None


class Response(BytesIO):
    def __init__(self, raw, headers=None):
        super().__init__(raw)
        self.status = 200
        self.headers = headers or {}


class Cases(TestCase):
    def test_market_original_is_full_and_large(self):
        _, shard, source = fixture()
        item = shard["rows"][0]
        raw = encode(source["responses"][item["dataset"]]) + b"\n"
        packed, raw_sha, _ = compress_original(raw)
        self.assertGreater(len(raw), 256 * 1024)
        self.assertEqual(gzip.decompress(packed), raw)
        self.assertEqual(native_proof(item, packed)["raw_sha"], raw_sha)
        self.assertEqual(native_proof(item, packed)["rows"], 4)
        self.assertEqual(len(item["coverage"]["coverage_ids"]), 3)
        partial = encode({"status": 200, "data": source["responses"][item["dataset"]]["data"][:-1]})
        with self.assertRaisesRegex(ValueError, "INCOMPLETE_EXPECTED_KEYS"):
            native_proof(item, compress_original(partial)[0])

    def test_cross_year_and_snapshot_use_start_or_taipei_capture(self):
        _, shard, _ = fixture()
        self.assertEqual(shard["rows"][1]["anchor_month"], "1984-12")
        self.assertEqual(window_months("1984-12-31", "1985-01-03"), ["1984-12", "1985-01"])
        rule = NativeContract("TaiwanStockActiveETFInfo", "snapshot", "snapshot", ("stock_id",),
                              ("stock_id",), date_field=None)
        item = compile_window(rule=rule, mode="SNAPSHOT", params={"dataset": rule.dataset}, lane="B08", venue="TW",
                              version="fixed", capture="2025-12-31T18:00:00Z", raw_limit=4096,
                              completeness={"method": "expected_keys", "keys": [["1001"]]},
                              coverage=[], evidence={"test": "approved snapshot fixture"})
        self.assertEqual(item["anchor_month"], "2026-01")
        self.assertEqual(datetime.fromisoformat(item["capture_at"]).isoformat(), "2025-12-31T18:00:00+00:00")

    def test_optimizer_avoids_overlap_and_reuses_equal_request(self):
        _, shard, _ = fixture()
        bulk = shard["rows"][0]
        partial = bulk["coverage"]["coverage_ids"][0]
        alternatives = {"MARKET_DAY": [bulk]}
        mode, windows, notes = optimize(alternatives, bulk["coverage"]["coverage_ids"], known_coverage=[partial])
        self.assertIsNone(mode)
        self.assertEqual(windows, [])
        self.assertEqual(notes[0]["findings"][0]["reason"], "HOLD_PARTIAL_COVERAGE_OVERLAP")
        index, shard, _ = fixture()
        with self.assertRaisesRegex(ValueError, "NATIVE_MANIFEST_WINDOW_OVERLAP"):
            read_manifest(index, lambda _: shard, known_requests=[bulk["request_id"]])

    def test_transport_refuses_oversize_and_truncated_framing(self):
        for raw, headers, expected in ((b"abc", {"Content-Length": "5"}, "TRUNCATED_CONTENT_LENGTH"),
                                        (b"abcdef", {}, "NATIVE_RESPONSE_LIMIT"),
                                        (b"abc", {"Content-Range": "bytes 0-2/5"}, "PARTIAL_HTTP_RESPONSE"),
                                        (gzip.compress(b"x" * 100), {"Content-Encoding": "gzip"}, "NATIVE_RESPONSE_LIMIT")):
            with self.subTest(expected=expected), patch(
                "scripts.r11_backer.live_bronze_admission.build_opener"
            ) as opener:
                opener.return_value.open.return_value = Response(raw, headers)
                with self.assertRaisesRegex(ValueError, expected):
                    fetch_native_payload("fixture", {"dataset": "TaiwanStockPrice"}, 5)

    def test_pagination_binds_framing_and_multiset(self):
        _, shard, source = fixture()
        item = shard["rows"][3]
        rule = NativeContract(**item["native_contract"])
        p1 = encode({"status": 200, "data": [source["future_row"]], "next": "b"})
        p2 = encode({"status": 200, "data": [source["future_row"]], "next": None})
        self.assertEqual(validate_pages(rule, [(item["parameters"], p1), (item["parameters"] | {"page": "b"}, p2)],
                                        completeness=item["completeness"])["rows"], 2)
        with self.assertRaisesRegex(ValueError, "INCOMPLETE_PAGINATION"):
            validate_pages(rule, [(item["parameters"], p1)], completeness=item["completeness"])
        with self.assertRaisesRegex(ValueError, "PAGINATION_NOT_DOCUMENTED"):
            validate_pages(replace(rule, page_field=None), [(item["parameters"], p1)], completeness=item["completeness"])

    def test_confirmed_empty_event_receipt(self):
        _, shard, _ = fixture()
        item = shard["rows"][2]
        proof = native_proof(item, compress_original(encode({"status": 200, "data": []}))[0])
        self.assertEqual(proof["status"], "EMPTY_CONFIRMED")
        self.assertEqual(proof["rows"], 0)
        self.assertIsNone(proof["first_sha"])
        self.assertIsNone(proof["last_sha"])

    def test_first_account_read_does_not_require_provider_snapshot(self):
        now = 10_000
        baseline = {"mode": broker.MODE, "task_id": broker.TASK_ID, "spec_sha": "0" * 64,
                    "events": [], "results": {}, "auth_hold": False, "paused_until": 0}
        journal = MemoryJournal(baseline)
        lease = broker.acquire_lease(journal, journal.head(), run=7, attempt=1, now=now, expires_at=now + 300,
                                     spec_sha="a" * 64, evidence_sha="b" * 64)
        self.assertIsNone(broker.acquire_lease(journal, journal.head(), run=8, attempt=1, now=now, expires_at=now + 300,
                                               spec_sha="a" * 64, evidence_sha="b" * 64))
        proof = {"task_id": broker.TASK_ID, "spec_sha": "a" * 64, "account_ownership_evidence_sha256": "b" * 64,
                 "lease_id": lease["lease_id"], "all_consumers_in_broker": True, "exclusive_account_lease_read_back": True,
                 "bootstrap_basis": {"kind": "VERIFIED_ROLLING_WINDOW_BOUND", "observed_at": now, "exclusive_since": 1,
                                     "api_request_limit": 50, "user_count": 0, "evidence_sha256": "c" * 64},
                 "shared_events": []}
        self.assertEqual(broker.reserve(journal, journal.head(), kind="download", request_id="data", now=now, run=7,
                                        admission=proof), "HOLD_PROVIDER_HEADROOM_UNVERIFIED")
        with patch.object(broker, "read_json", return_value={"user_count": 1, "api_request_limit": 50}) as read, \
             patch.object(broker, "time", return_value=now):
            outcome = broker.account_probe(journal, journal.head(), request_id="account", now=now, run=7,
                                           admission=proof, token="fixture", bootstrap=True)
            self.assertEqual(outcome["status"], "PROVIDER_LIMIT_OBSERVED")
            self.assertEqual(read.call_count, 1)
        self.assertEqual(len(journal.head()["state"]["events"]), 1)
        self.assertEqual(broker.reserve(journal, journal.head(), kind="probe", request_id="account", now=now,
                                        run=7, admission=proof, bootstrap=True), "ALREADY_RESERVED")

    def test_b09_does_not_discard_provider_cooldown_or_auth_hold(self):
        state = {"tasks": {"x": {"status": "REQUESTING", "attempts": 4, "pending": {}}}, "pause": 0, "auth_hold": False}
        journal = MemoryJournal(state)
        blocked = bulk_batch.provider_result(journal, journal.head(), "x", state["tasks"]["x"], 402, "120", 100)
        self.assertEqual(blocked["state"]["pause"], 220)
        self.assertEqual(bulk_batch.reconcile(blocked)["B09"], {"x": "HTTP_402"})
        denied = bulk_batch.provider_result(journal, blocked, "x", state["tasks"]["x"], 403, None, 101)
        self.assertTrue(denied["state"]["auth_hold"])
        self.assertEqual(denied["state"]["pause"], 220)

    def test_official_market_variant_is_selected_for_exact_dataset_only(self):
        html = b'''<h4 id="taiwanstockprice">TaiwanStockPrice</h4><p>Example</p>'''
        html += '''<h4 id="backersponsor">一次拿特定日期，所有資料 backer sponsor</h4><pre>dataset=TaiwanStockPrice</pre>
        <h4 id="taiwanstockpricetick">TaiwanStockPriceTick</h4><p>Example</p>
        <h4 id="sponsorpro">一次拿特定日期，所有資料 sponsorpro</h4><pre>dataset=TaiwanStockPriceTick</pre>'''.encode()
        docs = topics({"Technical": html})
        self.assertEqual(docs["TaiwanStockPrice"][0]["market_day_tier"], "Backer")
        self.assertEqual(docs["TaiwanStockPriceTick"][0]["market_day_tier"], "SponsorPro")
