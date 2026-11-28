from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.r11_backer import v2_resources
from scripts.r11_backer.quota import TOTAL_LIMIT, next_reservation_at


SPEC = "c" * 64


class Journal:
    """CAS fixture that can be reopened from a persisted JSON-like record."""

    def __init__(self, record):
        self.record = deepcopy(record)

    def append(self, previous, state):
        if previous != self.record:
            return False
        self.record = {"sequence": previous["sequence"] + 1, "state": deepcopy(state)}
        return True


class V2ResourceTests(unittest.TestCase):
    def reserve(self, journal, purpose, identity, now=1, **opts):
        options = {"spec_sha": SPEC, "shared_events": [], "shared_ledger_proven": True, "run": 1}
        return v2_resources.reserve_probe(journal, journal.record, kind=purpose, request_id=identity,
                                          now=now, **(options | opts))

    def test_v2_task_budget_survives_hour_rollover_and_new_runner(self):
        journal = Journal({"sequence": 0, "state": v2_resources.probe_state(SPEC)})
        for kind, limit in v2_resources.TASK_LIMITS.items():
            for i in range(limit):
                self.assertEqual(self.reserve(journal, kind, f"{kind}-{i}"), "RESERVED")
        resumed = Journal(journal.record)
        for kind, limit in v2_resources.TASK_LIMITS.items():
            self.assertEqual(self.reserve(resumed, kind, f"{kind}-0", 7200, run=2), "ALREADY_RESERVED")
            self.assertEqual(self.reserve(resumed, kind, f"{kind}-{limit}", 7200, run=2), "TASK_LIMIT")
        self.assertEqual(len(resumed.record["state"]["events"]), 52)

    def test_v2_task_reservation_needs_verified_sharing_and_cas(self):
        genesis = {"sequence": 0, "state": v2_resources.probe_state(SPEC)}
        journal = Journal(genesis)
        self.assertEqual(self.reserve(journal, "probe", "new", shared_ledger_proven=False),
                         "HOLD_SHARED_LEDGER_UNVERIFIED")
        self.assertEqual(self.reserve(journal, "probe", "winner"), "RESERVED")
        self.assertEqual(v2_resources.reserve_probe(
            journal, genesis, kind="probe", request_id="winner", now=1, run=2, spec_sha=SPEC,
            shared_events=[], shared_ledger_proven=True), "CAS_CONFLICT")
        with self.assertRaisesRegex(ValueError, "UNSCOPED"):
            self.reserve(journal, "probe", "other-spec", spec_sha="d" * 64)

    def test_saved_external_usage_and_429_pause_apply_to_entire_account(self):
        journal = Journal({"sequence": 0, "state": v2_resources.probe_state(SPEC)})
        external = [{"kind": "download", "at": 10, "request_id": str(i)} for i in range(TOTAL_LIMIT)]
        self.assertEqual(self.reserve(journal, "download", "new", 11, shared_events=external),
                         "ACCOUNT_LIMIT_OR_COOLDOWN")
        self.assertEqual(self.reserve(journal, "probe", "account-full", 11, shared_events=external),
                         "ACCOUNT_LIMIT_OR_COOLDOWN")
        self.assertEqual(self.reserve(journal, "probe", "account", 11, shared_events=external[:-1]), "RESERVED")
        self.assertTrue(v2_resources.record_probe_block(journal, journal.record, 429, 11,
                                                        spec_sha=SPEC, retry_after="180"))
        self.assertEqual(self.reserve(journal, "probe", "too-soon", 190), "ACCOUNT_LIMIT_OR_COOLDOWN")
        self.assertEqual(self.reserve(journal, "download", "too-soon", 190), "ACCOUNT_LIMIT_OR_COOLDOWN")
        self.assertTrue(v2_resources.record_probe_block(journal, journal.record, 403, 200, spec_sha=SPEC))
        self.assertEqual(self.reserve(journal, "probe", "forbidden", 7200), "HOLD_AUTHORIZATION")

    def test_next_slot_uses_shared_rolling_boundary_for_data_and_account(self):
        events = [(200, "download")] * (TOTAL_LIMIT - 1) + [(100, "probe")]
        self.assertEqual(next_reservation_at(events, 300, "download"), 3700)
        self.assertEqual(next_reservation_at(events, 300, "probe"), 3700)
        self.assertEqual(next_reservation_at(events, 3800, "download"), 3800)

    def test_report_preserves_unproven_full_history_and_subscription_expiry(self):
        master = {"sequence": 51, "state": {"cursor": 2, "totals": {"VERIFIED": 1, "BLOCKED": 1},
                   "bytes": 214, "source_requests": 2, "account_requests": 1, "events": [], "pause": 0}}
        ctx = SimpleNamespace(access="fixture", plan={"budget": {"windows": 11}}, supplement=None,
                              master=master, spec_sha=SPEC, read=lambda _: {"full_history_denominator": None})
        with (patch.object(v2_resources, "read_json", return_value={"storageQuota": {"limit": "1000", "usage": "10"}}),
              patch.object(v2_resources, "task_checkpoint", return_value=(v2_resources.probe_state(SPEC), {}))):
            result = v2_resources.run(ctx)
        report = result["outputs"]["RESOURCE_BUDGET.json"]
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(report["drive_storage"]["available_bytes"], 990)
        self.assertEqual(report["limited_campaign"]["remaining_plan_windows"], 9)
        self.assertIsNone(report["full_history"]["denominator"])
        self.assertIsNone(report["provider_entitlement"]["subscription_expiry"])
        self.assertEqual(result["outputs"]["PROBE_LEDGER.json"]["events"], [])
