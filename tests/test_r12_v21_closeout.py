"""Focused regressions for the V2.1 historical and shared-quota fixes."""

from datetime import date, timedelta
from unittest import TestCase
from unittest.mock import patch
from urllib.error import HTTPError

from scripts.r11_backer.integration_pilot import encode
from scripts.r11_backer.v2_coverage import PRICE_DATASET, transfer_observations
from scripts.r11_backer.v21_broker import MODE, TASK_ID, account_probe


class HistoryCloseoutTests(TestCase):
    def test_transfer_original_retains_rows_and_uses_no_source_request(self):
        code, transfer = "5306", date(2022, 3, 8)
        previous = transfer - timedelta(days=1)
        params = {"dataset": PRICE_DATASET, "data_id": code,
                  "start_date": str(previous), "end_date": str(transfer)}
        raw = encode({"status": 200, "data": [{"stock_id": code, "date": str(day), "close": price}
                                             for day, price in ((previous, 8), (transfer, 9))]})
        packet = {f"official/{market}-{day}.json": b"fixture" for market in ("TWSE", "TPEx")
                  for day in (previous, transfer)} | {"backer/response-1.json": raw}
        detail = {"backer_requests": [{"request": params, "status": "VALID_SAMPLE", "original": "backer/response-1.json"}]}

        def close_rows(_, day, market):
            prices = {(previous, "TPEx"): {code: 8}, (transfer, "TWSE"): {code: 9}}
            return "VALID_OFFICIAL_DAY", prices.get((day, market), {}), {}

        with patch("scripts.r11_backer.v2_coverage.TRANSFERS", {code: transfer}), patch(
            "scripts.r11_backer.v2_coverage.close_rows", side_effect=close_rows
        ):
            rows, holds = transfer_observations(packet, detail, {"run": 5})
        self.assertEqual(holds, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["expected_dates"], [str(previous), str(transfer)])
        self.assertEqual(rows[0]["new_source_requests"], 0)


class RecordingJournal:
    def __init__(self, state):
        self.record = {"state": state, "sequence": 0}

    def append(self, previous, state):
        self.record = {"state": state, "sequence": previous["sequence"] + 1}
        return True

    def head(self):
        return self.record


class AccountCloseoutTests(TestCase):
    def test_http_and_json_quota_blocks_charge_one_attempt_and_persist_cooldown(self):
        now, digest = 100_000, "a" * 64
        admission = {"task_id": TASK_ID, "spec_sha": digest, "account_ownership_evidence_sha256": digest,
                     "all_consumers_in_broker": True, "exclusive_account_lease_read_back": True,
                     "shared_events": [], "provider_snapshot": {"observed_at": now, "api_request_limit": 100,
                                                                "user_count": 1, "evidence_sha256": digest}}
        for response in (HTTPError("https://example.invalid", 402, "quota", {"Retry-After": "120"}, None),
                         {"status": "429", "retry_after": 120}):
            state = {"mode": MODE, "task_id": TASK_ID, "spec_sha": digest, "events": [], "results": {},
                     "auth_hold": False, "paused_until": 0}
            journal = RecordingJournal(state)
            with patch("scripts.r11_backer.v21_broker.time", return_value=now), patch(
                "scripts.r11_backer.v21_broker.read_json",
                side_effect=response if isinstance(response, Exception) else None, return_value=response,
            ) as request:
                first = account_probe(journal, journal.head(), request_id="first", now=now, run=1,
                                      admission=admission, token="synthetic")
                second = account_probe(journal, journal.head(), request_id="second", now=now + 1, run=1,
                                       admission=admission, token="synthetic")
            self.assertEqual(first["actual_requests"], 1)
            self.assertEqual(first["paused_until"], now + 120)
            self.assertEqual(second["status"], "ACCOUNT_COOLDOWN")
            self.assertEqual(journal.head()["state"]["paused_until"], now + 120)
            self.assertEqual(request.call_count, 1)
