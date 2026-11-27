"""Reproducible B10 and Drive fault injection with no network calls."""

import io
import os
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from scripts.r11_backer import drive_pilot, drive_retry, live_preflight, production_download, release_batch


def http(code, headers=None):
    return HTTPError("https://example.invalid", code, "fixture", headers or {}, None)


class DriveReadTests(unittest.TestCase):
    def test_retry_after_429_transport_5xx_and_fourth_success(self):
        operation = Mock(side_effect=[http(429, {"Retry-After": "7"}), URLError(TimeoutError()), http(503), b"raw"])
        with patch.object(drive_retry, "sleep") as pause, patch.object(drive_retry, "uniform", return_value=0):
            self.assertEqual(drive_retry.retry_read(operation, "TEST"), b"raw")
        self.assertEqual(operation.call_count, 4)
        self.assertEqual([call.args[0] for call in pause.call_args_list], [7, 2, 4])

    def test_permission_and_provider_cooldown_stop_without_unapproved_wait(self):
        for failure in (http(401), http(403), http(429, {"Retry-After": "120"})):
            operation = Mock(side_effect=failure)
            with patch.object(drive_retry, "sleep") as pause, self.assertRaises(HTTPError):
                drive_retry.retry_read(operation, "TEST")
            operation.assert_called_once()
            pause.assert_not_called()

    def test_only_drive_get_is_retried(self):
        with patch.object(live_preflight, "build_opener") as opener, patch.object(drive_retry, "sleep"):
            request = opener.return_value.open
            request.side_effect = [URLError(TimeoutError()), io.BytesIO(b'{"ok":true}')]
            self.assertEqual(live_preflight.read_json(drive_pilot.BASE + "/about"), {"ok": True})
            self.assertEqual(request.call_count, 2)
            request.reset_mock(side_effect=True)
            request.side_effect = URLError(TimeoutError())
            with self.assertRaises(URLError):
                live_preflight.read_json("https://api.finmindtrade.com/api/v4/data", "fixture")
            request.assert_called_once()


class DriveWriteTests(unittest.TestCase):
    expected = {"id": "fixed", "name": "receipt.json", "parents": ["sandbox"],
                "mimeType": "application/json", "trashed": False}

    def test_timeout_after_commit_recovers_reserved_id_after_delayed_visibility(self):
        with (patch.object(drive_pilot, "find", return_value=None),
              patch.object(drive_pilot, "build_opener") as opener,
              patch.object(drive_pilot, "read_json", side_effect=[http(404), self.expected]) as get,
              patch.object(drive_pilot, "read_drive_bytes", return_value=b"{}") as read,
              patch.object(drive_pilot, "sleep") as pause):
            opener.return_value.open.side_effect = URLError(TimeoutError())
            self.assertEqual(drive_pilot.ensure("sandbox", "receipt.json", "application/json", "fixture", b"{}",
                                                create=True, file_id="fixed"), "fixed")
            opener.return_value.open.assert_called_once()
            self.assertEqual(get.call_count, 2)
            read.assert_called_once_with("fixed", "fixture", 2)
            pause.assert_called_once_with(1)

    def test_unproven_post_never_reissues_post_and_mismatch_is_hold(self):
        with (patch.object(drive_pilot, "find", return_value=None),
              patch.object(drive_pilot, "build_opener") as opener,
              patch.object(drive_pilot, "read_json", side_effect=http(404)) as get,
              patch.object(drive_pilot, "sleep"),
              patch.object(drive_pilot, "read_drive_bytes", return_value=b"[]")):
            opener.return_value.open.side_effect = URLError(TimeoutError())
            with self.assertRaises(URLError):
                drive_pilot.ensure("sandbox", "receipt.json", "application/json", "fixture", b"{}",
                                   create=True, file_id="fixed")
            opener.return_value.open.assert_called_once()
            self.assertEqual(get.call_count, 4)
            get.side_effect = None
            get.return_value = self.expected
            with self.assertRaisesRegex(RuntimeError, "content mismatch"):
                drive_pilot.ensure("sandbox", "receipt.json", "application/json", "fixture", b"{}",
                                   create=True, file_id="fixed")
            self.assertEqual(opener.return_value.open.call_count, 2)


class B10RecoveryTests(unittest.TestCase):
    def test_read_timeout_keeps_checkpoint_until_fourth_attempt_then_repair(self):
        state = {"status": "STAGED", "attempts": 1, "stage_id": "fixed"}
        with patch.object(release_batch, "stage_read", side_effect=URLError(TimeoutError())) as read:
            for attempt in range(1, 5):
                state = release_batch.accept_item({}, {"task_id": "abc"}, state, "sandbox", "fixture")
                self.assertEqual(state["accept_attempts"], attempt)
                self.assertEqual(state["status"], "STAGED" if attempt < 4 else "REPAIR")
                self.assertEqual(state["stage_id"], "fixed")
            self.assertEqual(read.call_count, 4)

    def test_permission_hold_does_not_record_a_retry_or_advance(self):
        state = {"status": "STAGED", "attempts": 1, "stage_id": "fixed"}
        with patch.object(release_batch, "stage_read", side_effect=http(403)), self.assertRaises(HTTPError):
            release_batch.accept_item({}, {"task_id": "abc"}, state, "sandbox", "fixture")
        self.assertEqual(state, {"status": "STAGED", "attempts": 1, "stage_id": "fixed"})

    def test_report_failure_preserves_acceptance_and_master_cursor(self):
        proof = {"rows": 1, "bytes": 99}
        receipt = {"file_id": "verified", "proof": proof}
        state = {"tasks": {"abc": {"status": "VERIFIED", "receipt": receipt}},
                 "events": [], "pause": 0, "inherited_events": 0}
        record = {"state": state}
        manifest = {"items": [{"task_id": "abc", "lane": "B01"}], "end": 1}
        master = {"state": {"totals": {}, "rows": 0, "bytes": 0, "source_requests": 0, "account_requests": 0}}

        def write(_, name, value, access):
            if name.startswith("B10_"):
                raise URLError(TimeoutError())

        with (patch.dict(os.environ, {"GITHUB_RUN_ATTEMPT": "1"}),
              patch.object(production_download.release_batch, "accept", return_value=record) as accept,
              patch.object(production_download, "write_json", side_effect=write) as report,
              patch.object(production_download, "advance", return_value=master) as advance):
            with self.assertRaises(URLError):
                production_download.accept_segment("sandbox", manifest, None, record, None, master, "fixture", 1, "BASE")
            advance.assert_not_called()
            report.side_effect = None
            production_download.accept_segment("sandbox", manifest, None, record, None, master, "fixture", 1, "BASE")
            advance.assert_called_once()
            self.assertEqual(advance.call_args.kwargs["rows"], 1)
            self.assertEqual(advance.call_args.kwargs["source_requests"], 0)
            self.assertTrue(all(call.kwargs.get("reread_verified") is False for call in accept.call_args_list))
