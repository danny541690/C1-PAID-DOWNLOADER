"""The single production controller for the sealed V2.6 full-history campaign."""

from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime
from math import ceil
import json
import os
from time import time

from . import autonomous_quota as quota, bulk_batch, executable_manifest_v26 as manifest, release_batch
from .archive import compress_original
from .bulk_manifest import SNAPSHOT_ZONE, packed_limit
from .cross_run_ledger import DriveJournal
from .drive_pilot import ROOT, ensure, find, read_drive_bytes, token
from .drive_retry import error_class
from .final_readiness_v2 import Context, original, private_path, snapshot
from .full_release_evidence import (
    BRANCH, CAMPAIGN, INDEX_SHA, WORKFLOW, Sealed, EvidenceStore, actual_receipt_readback, approval,
    checkout_sha, coverage_snapshot, legacy_item,
)
from .integration_pilot import digest, encode, verify_private_folder
from .live_bronze_admission import advance
from .live_recovery_canary import read_json_file, write_json
from .production_download import management, pending_group, prepare_native_segment, segment
from .release_rehearsal import RehearsalError, require
from .test_archive import GitHub
from .v24_coverage import active_snapshot
from .v25_account import FinMindTransport
from .v26_acceptance import coverage_inventory
from .v26_capacity import CapacityGuard, FLOOR, LEDGER_BUFFER, flight_bytes, observed_drive

NAMESPACE = "full_history_v26"
QUEUE_ORDER = ("certified", "inherited", "normal", "exploration")
BATCH_NAME = "NATIVE_BATCH.json"
SUMMARY = "B10_MASTER_RECEIPTS.json"
TERMINAL = {"VERIFIED", "REPAIR", "BLOCKED", "EXPLORATION_CAPTURED", "REUSED"}
WORK_PER_RUN = 12


def count_status(state):
    return dict(Counter(row["status"] for row in state["tasks"].values()))


def initial_control(sealed, authorization, order):
    require(authorization["manifest_sha256"] == INDEX_SHA, "V26R_WRONG_CAMPAIGN_MANIFEST")
    return {"campaign": CAMPAIGN, "release": {"id": authorization["file_id"], "sha256": authorization["sha256"]},
            "manifest_sha256": INDEX_SHA, "slot": 0, "batch_number": 0, "first_receipt_verified": False,
            "queues": list(order), "cursors": {name: manifest.origin(sealed.index, name) for name in order if name != "inherited"},
            "inherited_offset": 0, "active": None, "totals": {}, "source_requests": 0, "b09_requests": 0,
            "stored_new_bytes": 0, "distinct_verified_rows": 0, "batches_committed": 0, "last_batch": None,
            "wait": None, "baseline_master_sha256": authorization["master_sha256"]}


def workflow_output(name, value):
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as target:
            print(f"{name}={value}", file=target)


def here(name, parent, access, *, create):
    from .v24_regression import private_child

    return private_child(parent, name, access, create=create)


class LiveContext(Context):
    def __init__(self, access):
        self.access, self._production = access, None


class Runtime:
    def __init__(self, access, sealed, authorization, parent, master, bronze, *, isolation=False, origin=None):
        self.access, self.sealed, self.auth = access, sealed, authorization
        self.parent, self.master, self.bronze = parent, master, bronze
        self.isolation, self.origin = isolation, origin
        self.run = int(os.environ["GITHUB_RUN_ID"])
        self.attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
        self.order = ("certified", "normal", "exploration") if isolation else QUEUE_ORDER
        self.step_limit = 1 if isolation else WORK_PER_RUN
        self._baseline = None

    def baseline(self):
        if self._baseline is not None:
            return self._baseline
        if self.isolation:
            self._baseline = read_json_file(self.parent, "ISOLATED_BASE.json", self.access)
            return self._baseline
        path = "CARRY_FORWARD.json"
        if find(self.parent, path, self.access):
            self._baseline = read_json_file(self.parent, path, self.access)
            return self._baseline
        context = LiveContext(self.access)
        current = self.master.head()
        require(digest(encode(current)) == self.auth["master_sha256"], "V26R_LATEST_MASTER_REQUIRES_RECONCILIATION")
        active = active_snapshot(context)
        require(not active["dispositions"].get("HOLD") and not active["dispositions"].get("REUSABLE_STAGED"),
                "V26R_LEGACY_BATCH_NEEDS_HANDOFF")
        group = pending_group(context.plan, context.supplement, current["state"])
        waiting = []
        if group:
            plan = context.plan if group == "BASE" else context.supplement
            _, ledger, _, _ = segment(plan, current, context.campaign, self.access, group, create=False)
            rows = {row["task_id"]: row for row in ledger["items"]}
            waiting = [legacy_item(rows[record["task_id"]], {"manifest_sha256": active["manifest_sha"],
                       "journal_sequence": active["journal_sequence"]})
                       for record in active["tasks"] if record["disposition"] == "DOWNLOAD_REQUIRED"]
        carry = coverage_snapshot(self.sealed, active)
        b09 = self.sealed.read("B09_QUEUE.json")
        pending = {key: b09[key] for key in ("legacy", "historical_source", "historical_calendar", "boundary_and_source")}
        data = {"master_sha256": self.auth["master_sha256"], "positions": active["dispositions"],
                "waiting": waiting, "coverage": carry, "b09": pending,
                "receipt_source_count": carry["unique_verified_source_requests"],
                "legacy_verified_active": [{"task_id": row["task_id"], "receipt": row["receipt"]}
                    for row in active["tasks"] if row["disposition"] == "VERIFIED_RECEIPT"]}
        write_json(self.parent, path, json.loads(encode(data)), self.access)
        self._baseline = read_json_file(self.parent, path, self.access)
        return self._baseline

    def state(self, allow_launch=False):
        rec = self.master.head()
        require(rec is not None, "V26R_MASTER_MISSING")
        value = rec["state"].get(NAMESPACE)
        if value is None:
            require(allow_launch and digest(encode(rec)) == self.auth["master_sha256"], "V26R_LAUNCH_APPROVAL_OR_ANCHOR")
            carry = self.baseline()
            require(carry["master_sha256"] == self.auth["master_sha256"], "V26R_CARRY_FORWARD_ANCHOR")
            value = initial_control(self.sealed, self.auth, self.order)
            if self.isolation:
                value["cursors"]["normal"]["dataset_offset"] = carry["normal_split_dataset_offset"]
            rec = advance(self.master, rec, **{NAMESPACE: value})
        require(value["campaign"] == CAMPAIGN and value["manifest_sha256"] == INDEX_SHA and
                value["release"] == {"id": self.auth["file_id"], "sha256": self.auth["sha256"]}, "V26R_ACTIVE_RELEASE_MISMATCH")
        return rec, value

    def reserve_directory(self, batch_number, *, create=False):
        return here(f"BATCH_{batch_number:08d}", self.parent, self.access, create=create)

    def resolve_destination(self, item, access, *, create):
        from .live_recovery_canary import month_folder
        from .release_probes import Task

        return month_folder(self.bronze, Task(item["dataset"], item["start"], item["end"]), access, create=create)

    def stage(self, parent, create):
        return here("STAGING", parent, self.access, create=create)

    def account(self):
        folder = here("TEST_ACCOUNT", self.parent, self.access, create=False) if self.isolation else self.sealed.account_folder()
        return DriveJournal(folder, self.access)

    def available(self):
        if self.isolation:
            mode = os.environ.get("R12R_TEST_CAPACITY")
            if mode == "LOW":
                return {"available_bytes": FLOOR - 1}
        return observed_drive(self.access)

    @contextmanager
    def transport(self, batch):
        broker = quota.AccountBroker(self.account(), f"{self.run}:{self.attempt}:{CAMPAIGN}")
        broker.acquire()
        try:
            if self.isolation:
                from .full_history_rehearsal import fixture_provider

                yield FinMindTransport(broker, "isolated-fixture", preflight=False, payload=fixture_provider(batch["items"]))
            else:
                yield FinMindTransport(broker, os.environ.get("FINMIND_BACKER_TOKEN"), preflight=False)
        finally:
            broker.close()

    def new_batch(self, rec, control, capacity):
        if control["slot"] >= len(control["queues"]):
            return rec, control, None, None, None
        if not capacity.check(0, 2 * 1024 * 1024, new_batch=True)["can_admit"]:
            return rec, control, None, None, "WAIT_CAPACITY"
        index, baseline = self.sealed.index, self.baseline()
        queue = control["queues"][control["slot"]]
        parent = self.reserve_directory(control["batch_number"], create=True)
        batch_store = DriveJournal(here("EXECUTION", parent, self.access, create=True), self.access)
        staging = self.stage(parent, True)
        persisted = find(parent, BATCH_NAME, self.access)
        if persisted:
            fid, raw = original(self.access, parent, BATCH_NAME)
            batch = json.loads(raw)
        else:
            if queue == "inherited":
                from .bulk_batch import prepare_windows

                start = control["inherited_offset"]
                selected = baseline["waiting"][start:start + self.step_limit]
                batch = prepare_windows(index, selected, batch_store, self.access, staging, self.resolve_destination) | {
                    "v26_cursor": {"inherited_offset": start}, "v26_next_cursor": {"inherited_offset": start + len(selected)},
                    "v26_exhausted": start + len(selected) >= len(baseline["waiting"]), "v26_overlap_holds": []}
            else:
                limit = 1 if queue == "certified" and not control["first_receipt_verified"] else self.step_limit
                capture = datetime.now(SNAPSHOT_ZONE).isoformat()
                batch = prepare_native_segment(index, self.sealed.read, batch_store, self.access, staging,
                       resolve_destination=self.resolve_destination, cursor=control["cursors"][queue], limit=limit,
                       known_requests=baseline["coverage"]["exact"], capture_at=capture)
                if self.isolation:
                    batch["v26_exhausted"] = True
            batch |= {"campaign": CAMPAIGN, "release_sha256": self.auth["sha256"], "queue": queue,
                      "batch_number": control["batch_number"]}
            fid = write_json(parent, BATCH_NAME, json.loads(encode(batch)), self.access)
        expected_cursor = ({"inherited_offset": control["inherited_offset"]} if queue == "inherited"
                           else control["cursors"][queue])
        require(batch["queue"] == queue and batch["campaign"] == CAMPAIGN and batch["batch_number"] == control["batch_number"]
                and batch["stage"] == staging and batch["v26_cursor"] == expected_cursor and
                batch["index_sha256"] == INDEX_SHA and batch["release_sha256"] == self.auth["sha256"],
                "V26R_BATCH_PLAN_MISMATCH")
        if not batch_store.head():
            batch_store.initialize(bulk_batch.initial(batch))
        control = control | {"active": {"folder_id": parent, "file_id": fid, "sha256": digest(encode(batch)),
                                        "queue": queue, "ordinal": control["batch_number"]}, "wait": None}
        rec = advance(self.master, rec, **{NAMESPACE: control})
        return rec, control, batch, batch_store, None

    def active_batch(self, control):
        active = control["active"]
        if not active:
            return None, None
        parent = self.reserve_directory(active["ordinal"])
        fid, raw = original(self.access, parent, BATCH_NAME)
        require(parent == active["folder_id"] and fid == active["file_id"] and digest(raw) == active["sha256"],
                "V26R_BATCH_READBACK")
        batch = json.loads(raw)
        store = DriveJournal(here("EXECUTION", parent, self.access, create=False), self.access)
        bulk_batch.check_batch(batch, store.head())
        return batch, store

    def evidence(self, control, *, create):
        corpus = here("B09_ORIGINALS", self.parent, self.access, create=create)
        approved = self.auth.get("approved_source_proofs") or {}
        parent = control["active"]["folder_id"]
        journal = DriveJournal(here("B09_EXECUTION", parent, self.access, create=create), self.access)
        return EvidenceStore(corpus, self.access, approved, journal=journal,
                             known_sources=self.baseline()["coverage"].get("sources"))

    def dispatch(self, phase, interrupt=False):
        rec, current = self.state(allow_launch=phase == "launch")
        if not current["first_receipt_verified"] and current["slot"] > 0:
            raise RehearsalError("V26R_FIRST_VERIFIED_RECEIPT_REQUIRED")
        capacity = CapacityGuard(self.available)
        batch, store = self.active_batch(current)
        if batch is None:
            rec, current, batch, store, deferred = self.new_batch(rec, current, capacity)
            if batch is None:
                return {"state": deferred or "COMPLETE", "stage": phase}
        row = store.head()
        evidence = self.evidence(current, create=True)
        ready = row["state"]["tasks"]
        blocked = {}
        for item in batch["items"]:
            key = item["task_id"]
            if ready[key]["status"] == "READY" and (reason := evidence.reason_to_defer_primary(item)):
                blocked[key] = ready[key] | {"status": "REPAIR", "reason": reason}
        if blocked:
            row = advance(store, row, tasks=ready | blocked)
        with self.transport(batch) as transport:
            if transport.broker.book["auth_hold"]:
                return {"state": "STOP_AUTH"}
            before = len(row["state"]["events"])
            expired_now = int(time()) + 91 if self.isolation and phase == "resume" else None
            if expired_now:
                row = bulk_batch.recover(batch, row, store, self.access, now=expired_now)
            if interrupt:
                from unittest.mock import patch

                upload = bulk_batch.stage_blob
                def simulate(*args):
                    upload(*args)
                    raise OSError("isolated interruption after durable upload")
                with patch.object(bulk_batch, "stage_blob", side_effect=simulate):
                    row = release_batch.dispatch(batch, row, store, self.access, self.run, transport=transport,
                                                 capacity=capacity, request_ceiling=self.step_limit)
            else:
                row = release_batch.dispatch(batch, row, store, self.access, self.run, transport=transport,
                                             capacity=capacity, request_ceiling=self.step_limit)
            for item in batch["items"]:
                key = item["task_id"]
                task = row["state"]["tasks"][key]
                if (task["status"] != "STAGED" or item["completeness"]["method"] != "deferred_source_reconciliation" or
                        item["completeness"].get("planned_class") == "EXPLORATION"):
                    continue
                if task.get("comparison", {}).get("retry_at", 0) > int(time()):
                    continue
                outcome = evidence.collect(item, transport, capacity, int(time()))
                if task.get("comparison") != outcome:
                    row = bulk_batch.update(store, row, key, task | {"comparison": outcome})
            return {"state": "BATCH_CHECKPOINTED", "slot": current["slot"], "queue": batch["queue"],
                    "batch_number": current["batch_number"], "states": count_status(row["state"]),
                    "physical_primary_attempt_events": len(row["state"]["events"]) - before,
                    "capacity": row["state"].get("capacity_guard")}

    def accept(self):
        rec, current = self.state()
        batch, store = self.active_batch(current)
        if batch is None:
            return {"state": "NO_ACTIVE_BATCH"}
        evidence = self.evidence(current, create=True)
        guard = CapacityGuard(self.available)

        def read_comparison(item):
            return evidence.comparison(item)

        row = bulk_batch.accept(batch, store.head(), store, self.access, destination=self.resolve_destination,
                                comparison_reader=read_comparison, reread_verified=False, capacity=guard)
        if any(task["status"] not in TERMINAL for task in row["state"]["tasks"].values()):
            return {"state": "BATCH_REMAINS_OPEN", "states": count_status(row["state"])}
        archived = self.commit_receipts(current, batch, row, evidence)
        return self.commit_master(rec, current, batch, archived)

    def commit_receipts(self, control, batch, record, evidence):
        active = control["active"]
        folder = active["folder_id"]
        if find(folder, SUMMARY, self.access):
            result = read_json_file(folder, SUMMARY, self.access)
            require(result["manifest_sha256"] == active["sha256"] and
                    result["batch_journal_sha256"] == digest(encode(record)), "V26R_COMMIT_RECOVERY_WITNESS")
            return result
        receipts, repair, lanes, seen = {}, {}, defaultdict(dict), set()
        carry = self.baseline()["coverage"]["intervals"]
        for item in batch["items"]:
            key = item["task_id"]
            task = record["state"]["tasks"][key]
            lanes[item["lane"]][key] = {"status": task["status"], "attempts": task["attempts"]}
            if task["status"] in {"REPAIR", "BLOCKED", "EXPLORATION_CAPTURED"}:
                repair[key] = {"dataset": item["dataset"], "parameters": item["parameters"], "status": task["status"],
                               "reason": task.get("reason"), "comparison": task.get("comparison"), "pages": task.get("pages", []),
                               "unknown_reservation": task.get("pending", {}).get("grant_id")}
            if task["status"] != "VERIFIED":
                continue
            rows = actual_receipt_readback(item, task["receipt"], self.access)
            evidence.remember(item, task["receipt"])
            scoped = carry.get(item["dataset"], {})
            # Existing validated originals overlap some unavoidable full-venue
            # replies. Keep the provider bytes and credit only new keys.
            novel = [row for row in rows if not any(lo <= row.get("date", "") <= hi for lo, hi in
                                                    [*scoped.get(str(row.get("stock_id", item["parameters"].get("data_id", ""))), []),
                                                     *scoped.get("", [])])]
            keys = coverage_inventory(item, novel)
            unique = [key for key in keys if key not in seen]
            seen.update(unique)
            receipts[key] = task["receipt"] | {"logical_credit": {"rows_new_after_verified_reuse": len(unique),
                 "native_key_sha256": digest(encode(unique)), "rows_reused_or_duplicate": len(rows) - len(unique)}}
        for lane, tasks in sorted(lanes.items()):
            write_json(folder, f"{lane}_EXECUTION.json", {"tasks": tasks}, self.access)
        write_json(folder, "B09_RESULTS.json", {"records": repair, "source_evidence_missing_stays_out_of_bronze": True}, self.access)
        aux = evidence.journal.head()
        result = {"manifest_sha256": active["sha256"], "batch_journal_sha256": digest(encode(record)),
                  "batch_number": active["ordinal"], "queue": batch["queue"], "states": count_status(record["state"]),
                  "receipts": receipts, "b09_records": len(repair), "new_native_rows": len(seen),
                  "source_requests": len(record["state"]["events"]),
                  "b09_requests": sum(item.get("attempts", 0) for item in aux["state"]["comparisons"].values()) if aux else 0}
        write_json(folder, SUMMARY, result, self.access)
        return result

    def commit_master(self, rec, control, batch, summary):
        # One atomic transition commits both the credited receipts and the next
        # cursor. An interrupted retry cannot advance it or count it twice.
        verified = summary["states"].get("VERIFIED", 0)
        ready = control["first_receipt_verified"] or verified > 0
        next_control = control | {
            "active": None, "first_receipt_verified": ready, "batch_number": control["batch_number"] + 1,
            "batches_committed": control["batches_committed"] + 1,
            "totals": dict(Counter(control["totals"]) + Counter(summary["states"])),
            "distinct_verified_rows": control["distinct_verified_rows"] + summary["new_native_rows"],
            "stored_new_bytes": control["stored_new_bytes"] + sum(row["proof"]["bytes"] for row in summary["receipts"].values()),
            "source_requests": control["source_requests"] + summary["source_requests"],
            "b09_requests": control["b09_requests"] + summary["b09_requests"],
            "last_batch": control["active"] | {"summary_sha256": digest(encode(summary)), "accept_run": self.run}}
        if batch["queue"] == "inherited":
            next_control["inherited_offset"] = batch["v26_next_cursor"]["inherited_offset"]
        else:
            next_control["cursors"] = control["cursors"] | {batch["queue"]: batch["v26_next_cursor"]}
        if batch["v26_exhausted"]:
            next_control["slot"] = control["slot"] + 1
        updated = advance(self.master, rec, **{NAMESPACE: next_control})
        return {"state": "MASTER_COMMITTED", "master_sequence": updated["sequence"], "verified": verified,
                "b09_records": summary["b09_records"], "first_batch_verified": ready, "queue": batch["queue"]}

    def next_action(self, *, check_capacity=True):
        _, current = self.state()
        if current["slot"] >= len(current["queues"]) and not current["active"]:
            return {"state": "COMPLETE", "continue_now": False}
        if not current["first_receipt_verified"] and current["slot"] > 0:
            return {"state": "FIRST_BATCH_UNVERIFIED", "continue_now": False}
        _, book, _ = quota.open_book(self.account())
        now = int(time())
        if book["auth_hold"]:
            return {"state": "STOP_AUTH", "continue_now": False}
        if book["lease"] and ceil(book["lease"]["expires"] / 1000) > now:
            return {"state": "WAIT_ACCOUNT_LEASE", "retry_at": ceil(book["lease"]["expires"] / 1000), "continue_now": False}
        if ceil(book["paused"] / 1000) > now:
            return {"state": "WAIT_PROVIDER", "retry_at": ceil(book["paused"] / 1000), "continue_now": False}
        batch, store = self.active_batch(current)
        execution = store.head() if batch else None
        if check_capacity:
            candidates = [2 * packed_limit(item) * item["max_pages"] for item in batch["items"]
                          if execution["state"]["tasks"][item["task_id"]]["status"] == "READY"] if batch else []
            potential = CapacityGuard(self.available).check(
                flight_bytes(batch, execution, native=True) if execution else 0, min(candidates, default=0), new_batch=True)
            if not potential["can_admit"]:
                return {"state": "WAIT_CAPACITY", "continue_now": False, "capacity": potential}
        if batch:
            tasks = execution["state"]["tasks"]
            deadlines = [max(task.get("retry_after", 0), task.get("pending", {}).get("lease_until", 0),
                             task.get("comparison", {}).get("retry_at", 0)) for task in tasks.values()
                         if task["status"] not in TERMINAL]
            if deadlines and min(deadlines) > now:
                return {"state": "WAIT_CHECKPOINT", "retry_at": min(deadlines), "continue_now": False}
        return {"state": "RUNNABLE", "continue_now": True, "next_phase": "continue"}


def live_runtime(access, sealed, release_file, phase):
    authorized = approval(sealed, release_file, os.environ.get("R12R_ACTIVE_RELEASE_FILE_ID"))
    require(os.environ.get("GITHUB_EVENT_NAME") in {"workflow_dispatch", "schedule"} and
            os.environ.get("GITHUB_REPOSITORY") == "danny541690/C1-PAID-DOWNLOADER" and
            os.environ.get("R12R_WORKFLOW_INVOKE") == WORKFLOW, "V26R_OFFICIAL_WORKFLOW_REQUIRED")
    if phase in {"admission", "launch"}:
        sealed.check_consumers()
    prior_parent, master_root = management(access, create=False)
    master = DriveJournal(master_root, access)
    current = master.head()
    require(current and (current["state"].get(NAMESPACE) or digest(encode(current)) == authorized["master_sha256"]),
            "V26R_APPROVED_MASTER_DRIFT")
    parent = here(CAMPAIGN, prior_parent, access, create=phase == "launch") if phase != "admission" else prior_parent
    if phase != "admission":
        parent = here(f"RELEASE_{release_file}", parent, access, create=phase == "launch")
    bronze = private_path(access, ROOT, "20_BRONZE")
    return Runtime(access, sealed, authorized, parent, master, bronze)


def run_cli(args):
    access = token()
    sealed = Sealed(access)
    if args.profile == "integration":
        from .full_history_rehearsal import runtime

        target = runtime(access, sealed, args.integration_origin)
    else:
        require(not args.interrupt_after_stage, "V26R_SIMULATION_FLAG_FORBIDDEN")
        target = live_runtime(access, sealed, args.release_file, args.phase)
    if args.phase == "admission":
        result = {"state": "ADMITTED", "campaign": CAMPAIGN}
        workflow_output("admitted", "true")
    elif args.phase == "accept":
        result = target.accept()
    elif args.phase == "next":
        result = target.next_action()
        workflow_output("continue_now", "true" if result["continue_now"] else "false")
        workflow_output("control_state", result["state"])
    else:
        result = target.dispatch(args.phase, interrupt=args.interrupt_after_stage)
    print(json.dumps(result | {"campaign": CAMPAIGN, "execution_profile": args.profile}, sort_keys=True))
    return result
