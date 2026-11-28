"""Production integration regressions for scope, atomicity, and safe scheduling."""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from scripts.r11_backer import autonomous_quota as quota, bulk_batch, production_download
from scripts.r11_backer.full_release_evidence import INDEX_SHA, approval, legacy_item
from scripts.r11_backer.integration_pilot import digest, encode
from scripts.r11_backer.production_full_history import CAMPAIGN, NAMESPACE, Runtime, initial_control
from scripts.r11_backer.v25_account import FinMindTransport
from scripts.r11_backer.v26_capacity import CapacityGuard, FLOOR


class Memory:
    def __init__(self, record=None, state=None):
        self.record = record or {"sequence": 0, "state": state, "previous": None, "next_id": "1"}
        self.rows = {str(self.record["sequence"]): self.record}

    def head(self):
        return self.record

    def append(self, before, state):
        if self.record != before:
            return False
        sequence = before["sequence"] + 1
        self.record = {"sequence": sequence, "state": state, "previous": digest(encode(before)), "next_id": str(sequence + 1)}
        self.rows[str(sequence)] = self.record
        return True

    def load(self, file_id, sequence):
        return self.rows.get(file_id)


class ReleaseTests(TestCase):
    def test_public_cli_routes_operational_phases_only_to_new_campaign(self):
        for phase in ("admission", "launch", "resume", "continue", "accept", "next"):
            with (patch("sys.argv", ["production_download", phase, "--release-file", "release-input"]),
                  patch("scripts.r11_backer.production_full_history.run_cli") as run):
                production_download.main()
                self.assertEqual(run.call_args.args[0].phase, phase)

    def test_activation_is_a_separate_record_and_rejects_mismatched_file(self):
        with self.assertRaisesRegex(ValueError, "RELEASE_APPROVAL_REQUIRED"):
            approval(None, "plausible-release-file", "another-release-file")
        with self.assertRaisesRegex(ValueError, "WRONG_CAMPAIGN_MANIFEST"):
            initial_control(None, {"manifest_sha256": "0" * 64}, ("certified",))

    def test_legacy_ready_window_keeps_its_native_expected_keys(self):
        dates = ["2026-01-02","2026-01-05"]
        item = legacy_item({"dataset":"TaiwanStockPrice","fields":["date","stock_id"],"market":"TPEx","lane":"B01",
                            "scope":"2388","start":dates[0],"end":dates[-1],"dates":dates,"task_id":"legacy"},
                           {"manifest_sha256":"old-calendar"})
        self.assertEqual(item["completeness"]["keys"], [[day,"2388"] for day in dates])
        self.assertEqual(item["legacy_task_id"], "legacy")
        self.assertEqual(item["mode"], "ENTITY_RANGE")

    def test_committing_b10_and_the_cursor_is_atomic_and_a_retry_does_not_count_again(self):
        initial = {"sequence": 51, "previous": None, "next_id": "52", "state":{"totals":{"VERIFIED":4749,"BLOCKED":51}}}
        master = Memory(record=initial)
        checkpoint = {"first_receipt_verified":False,"batch_number":0,"batches_committed":0,"totals":{},
                      "distinct_verified_rows":0,"stored_new_bytes":0,"source_requests":0,"b09_requests":0,
                      "active":{"ordinal":0},"cursors":{"certified":{"window_offset":0}},"slot":0}
        batch = {"queue":"certified","v26_next_cursor":{"window_offset":1},"v26_exhausted":True}
        summary = {"states":{"VERIFIED":1},"new_native_rows":3,"receipts":{"key":{"proof":{"bytes":187}}},
                   "source_requests":1,"b09_requests":0,"b09_records":0}
        runner = SimpleNamespace(master=master, run=101)
        result = Runtime.commit_master(runner, initial, checkpoint, batch, summary)
        self.assertTrue(result["first_batch_verified"])
        state = master.head()["state"]
        self.assertEqual(state["totals"], initial["state"]["totals"])
        self.assertEqual(state[NAMESPACE]["distinct_verified_rows"],3)
        self.assertEqual(state[NAMESPACE]["slot"],1)
        with self.assertRaisesRegex(ValueError, "CONCURRENT_COMMIT"):
            Runtime.commit_master(runner, initial, checkpoint, batch, summary)
        self.assertEqual(master.head()["sequence"], 52)

    def test_production_transport_uses_the_account_when_preflight_is_out_of_requests(self):
        book = quota.initial(1000, policy_evidence=INDEX_SHA, prior={"reserved_data_requests":48,"actual_data_requests":48,
                                                                   "reserved_account_requests":4,"actual_account_requests":4})
        broker = quota.AccountBroker(Memory(state=quota.checkpoint(book)), CAMPAIGN, clock=lambda:1000)
        broker.acquire()
        http = FinMindTransport(broker, "fixture", preflight=False, payload=lambda *_: (b'{"status":200,"data":[]}',"identity"))
        self.assertIsNotNone(http.fetch({"dataset":"fixture"},1000,identifier="new-production",lane="B09")[1])
        self.assertEqual(broker.book["preflight"]["reserved"],{"data":48,"account":4})
        self.assertFalse(broker.book["events"][0]["preflight"])

    def test_unknown_request_after_expired_lease_is_quarantined(self):
        from urllib.error import HTTPError

        batch = {"schema_version":24,"index_sha256":INDEX_SHA,"items":[{"task_id":"0"}]}
        state = bulk_batch.initial(batch)
        state["tasks"]["0"] |= {"status":"REQUESTING","pending":{"lease_until":1,"grant_id":"old"}}
        store = Memory(state=state)
        error = HTTPError("fixture",404,"missing",None,None)
        with patch.object(bulk_batch,"read_stage",side_effect=error):
            recovered = bulk_batch.recover(batch,store.head(),store,None,now=2)
        self.assertEqual(recovered["state"]["tasks"]["0"]["status"],"REPAIR")
        self.assertEqual(recovered["state"]["tasks"]["0"]["reason"],"UNKNOWN_REQUEST_OR_STAGE")

    def test_wait_for_real_capacity_before_marking_runnable(self):
        base = {"active":None,"slot":0,"queues":["normal"],"first_receipt_verified":True}
        book = quota.initial(quota.utc_ms(),policy_evidence=INDEX_SHA)
        runner = SimpleNamespace(state=lambda: ({},base),account=lambda:Memory(state=quota.checkpoint(book)),
                                 active_batch=lambda _: (None,None),available=lambda:{"available_bytes":FLOOR})
        self.assertEqual(Runtime.next_action(runner)["state"],"WAIT_CAPACITY")
        runner.available = lambda:{"available_bytes":FLOOR * 2}
        self.assertTrue(Runtime.next_action(runner)["continue_now"])
