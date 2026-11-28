"""Sealed inputs, production authorization, and durable B09 source evidence."""

from collections import defaultdict
from datetime import datetime
import os
from io import BytesIO
import gzip
import json
import re
import subprocess

from . import autonomous_quota as quota, executable_manifest_v26 as manifest, v26_acceptance
from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .bulk_manifest import SNAPSHOT_ZONE, compile_window, request_id
from .drive_pilot import ROOT, ensure, find, read_drive_bytes
from .final_readiness_v2 import SPEC_SHA as V20_SPEC, TASK_ID, original, private_path
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import read_json_file, write_json
from .live_bronze_admission import advance
from .release_probes import read_private_file
from .release_rehearsal import require
from .v2_registry import NativeContract, read_rows
from .v2_recovery import metadata
from .v25_account import EARLY_JOURNAL_SPEC
from .v26_capacity import FLOOR
from .test_archive import GitHub

CAMPAIGN = "R12_1_PRODUCTION_FULL_HISTORY_V26"
BRANCH = "codex/r12-final-readiness-v2"
WORKFLOW = "r12-v26r-production-full-history.yml"
BASE_RUN = 46115231184
BASE_COMMIT = "b8236a97239b9263c8d4c2cacfbdd7adbea5eb0f"
SPEC_SHA = "861ddcd825818f2bdd36d22d5a45a7428b6ad787fbdc1ba7179bf85a4071a327"
BASE_REPORT_FILE = "1C4tAUYCFsQrhQZ7deNQ1kGhDvVXpAPb4"
BASE_REPORT_SHA = "989c1eba05fc56ec921386bef3deb4f9e5e20c8090d90155accef3ab1913d000"
INDEX_FILE = "1X2VwwFtsyo1mubAWt7J0kprzpo78mzH4"
INDEX_SHA = "1b5eb70876a6fe6a76d44ec1c42555625580896e55354b3476d9bd43993cc624"
V25_RUN = 46099758774
V25_SPEC = "1ebae4aa6c81edb3ac45ff65181065195ad3e2d99666f63f8588ae4dda2defa2"
V25_CONSUMERS = "04b21b52113e128d20a1945a390473a740f5537a49c1a8d1749cc20ded26211e"
ID = re.compile(r"[A-Za-z0-9_-]{12,120}")
SHA = re.compile(r"[a-f0-9]{64}")
GZIP = "application/gzip"


def ref_root(access, *tail, create=False):
    return private_path(access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT", *tail, create=create)


class Sealed:
    def __init__(self, access):
        self.access = access
        self.shared = ref_root(access)
        self.task = private_path(access, self.shared, "FINAL_READINESS", TASK_ID)
        self.version = private_path(access, self.task, SPEC_SHA)
        self.root = private_path(access, self.version, f"{BASE_RUN}_1")
        self.prior = private_path(access, self.task, V25_SPEC, f"{V25_RUN}_1")
        self._values = {}
        fid, raw = original(access, self.root, "FINAL_READINESS.json")
        require(fid == BASE_REPORT_FILE and digest(raw) == BASE_REPORT_SHA, "V26R_BASE_REPORT_DIGEST")
        self.report = json.loads(raw)
        require(self.report["result"] == "READY_FOR_USER_RELEASE" and self.report["run_id"] == BASE_RUN and
                self.report["commit"] == BASE_COMMIT and self.report["passed_gates"] == 9 and
                self.report["full_download_release"] == "HOLD" and self.report["production_dispatchable"] == 0,
                "V26R_BASELINE_READINESS")
        self.index = self.read(manifest.INDEX)
        require(self.report["scope"] == self.index and self.index["task_id"] == TASK_ID and
                self.index["spec_sha256"] == SPEC_SHA and self.index["planned_request_count"] == 135065 and
                self.index["dataset_count"] == 70, "V26R_V26_INDEX_REQUIRED")

    def reference(self, name):
        matches = [stage["outputs"][name] for stage in self.report["workflow_job_evidence"].values()
                   if name in stage.get("outputs", {})]
        require(len(matches) == 1, "V26R_FILE_NOT_IN_BASELINE")
        return matches[0]

    def read(self, name):
        if name not in self._values:
            expected = self.reference(name)
            fid, raw = original(self.access, self.root, name)
            require(fid == expected["file_id"] and digest(raw) == expected["sha256"], "V26R_BASELINE_ARTIFACT_DIGEST")
            if name == manifest.INDEX:
                require(fid == INDEX_FILE and digest(raw) == INDEX_SHA, "V26R_EXACT_MANIFEST_ID")
            self._values[name] = json.loads(raw)
        return self._values[name]

    def account_folder(self):
        return private_path(self.access, self.task, EARLY_JOURNAL_SPEC, "ISOLATED_ACCOUNT_QUOTA_BROKER")

    def check_consumers(self):
        _, raw = original(self.access, self.prior, "CONSUMER_INVENTORY.json")
        require(digest(raw) == V25_CONSUMERS, "V26R_CONSUMER_EVIDENCE")
        previous = json.loads(raw)["legacy_workflows"]
        # Check all current repo refs for actual use of the same Actions secret,
        # so a new workflow cannot silently escape the legacy inventory.
        from .v24_account import inventory

        github = GitHub()
        catalog = {row["id"]: row for row in github.pages("/actions/workflows", "workflows")}
        old = {row["workflow"] for row in previous}
        remote = inventory(type("Run", (), {"run": int(os.environ["GITHUB_RUN_ID"])})(),
                           broker_workflow=WORKFLOW, entry_module="production_full_history")
        known = set(remote["same_repository_secret_workflows"])
        require(known <= old | {WORKFLOW} and len(previous) == 14 and not remote["active_peer_runs"] and
                all(catalog[o["id"]]["state"] in {"disabled_manually", "disabled_inactivity"} for o in previous),
                "V26R_WORKFLOWS_NOT_ISOLATED")
        return {"disabled_legacy": sorted(old), "controlled_current_routes": sorted(known), "active_peer_runs": []}


def checkout_sha():
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, timeout=10).strip()


def approval(sealed, file_id, active_id):
    require(ID.fullmatch(file_id or "") and file_id == active_id, "V26R_RELEASE_APPROVAL_REQUIRED")
    access = sealed.access
    folder = ref_root(access, "PRODUCTION_RELEASES")
    info = metadata(file_id, access)
    require(info and info["parents"] == [folder] and info["shared"] is False and
            info["mimeType"] == "application/json" and not info["trashed"], "V26R_RELEASE_RECORD_PARENT")
    raw = read_private_file(file_id, access)
    value = json.loads(raw)
    require(value["kind"] == "R12_1_FULL_HISTORY_RELEASE" and value["campaign"] == CAMPAIGN and
            value["task_id"] == TASK_ID and value["decision"] == "USER_RELEASE_APPROVED" and
            value["manifest_file_id"] == INDEX_FILE and value["manifest_sha256"] == INDEX_SHA and
            value["readiness_report_file_id"] == BASE_REPORT_FILE and value["readiness_report_sha256"] == BASE_REPORT_SHA and
            value["baseline_run"] == BASE_RUN and value["code_commit"] == checkout_sha(), "V26R_RELEASE_RECORD_BINDING")
    when = datetime.fromisoformat(value["approved_at"])
    require(when.tzinfo and when <= datetime.now(SNAPSHOT_ZONE) and SHA.fullmatch(value["master_sha256"]),
            "V26R_RELEASE_TIMESTAMP_OR_MASTER_ANCHOR")
    return value | {"file_id": file_id, "sha256": digest(raw)}


def legacy_item(row, evidence):
    rule = NativeContract(row["dataset"], "daily", "entity_range", tuple(row["fields"]), ("date", "stock_id"),
                          identity_field="stock_id", required_parameters=("dataset", "data_id", "start_date", "end_date"),
                          allowed_parameters=("dataset", "data_id", "start_date", "end_date"))
    params = {"dataset": row["dataset"], "data_id": row["scope"], "start_date": row["start"], "end_date": row["end"]}
    keys = [[day, row["scope"]] for day in row["dates"]]
    base = compile_window(rule=rule, mode="ENTITY_RANGE", params=params, venue=row["market"], version=manifest.VERSION,
                          lane=row["lane"], raw_limit=512 * 1024, coverage=[],
                          completeness={"method": "expected_keys", "keys": keys, "evidence": evidence},
                          evidence={"legacy_task_id": row["task_id"], "native_calendar_witness": evidence},
                          legacy_query=manifest.exact_query_key(params))
    return base | {"legacy_task_id": row["task_id"]}


def inherited_coverage(active, reuse):
    """Keep actual accepted intervals separate from the prior reservations."""
    intervals, exact = defaultdict(lambda: defaultdict(list)), {}
    entries = list((row.get("evidence") or {}).get("receipt") for row in reuse.get("archive_intervals", ()))
    entries += [row["receipt"] | {"params": {"dataset": row["dataset"], "data_id": row["scope"],
                                            "start_date": row["start"], "end_date": row["end"]}}
                for row in active["tasks"] if row["disposition"] == "VERIFIED_RECEIPT"]
    for entry in filter(None, entries):
        params = entry["params"]
        key = manifest.exact_query_key(params)
        previous = exact.setdefault(key, {"file_id": entry["file_id"], "proof": entry["proof"], "params": params})
        require(previous["proof"]["raw_sha"] == entry["proof"]["raw_sha"], "V26R_EXISTING_QUERY_CONFLICT")
        intervals[params["dataset"]][params.get("data_id", "")].append([params["start_date"], params["end_date"]])
    return {"exact": exact, "intervals": intervals}


def actual_receipt_readback(item, receipt, access):
    from .bulk_batch import read_blob
    from .bulk_manifest import page_label, receipt_id

    fields = [receipt["request_id"], [p["file_id"] for p in receipt["files"]],
              [p["raw_sha"] for p in receipt["files"]], [p["packed_sha"] for p in receipt["files"]]]
    require(receipt["request_id"] == item["task_id"] and receipt["receipt_id"] == receipt_id(*fields) and
            receipt["contract_version"] == item["contract_version"] and receipt["folder_id"] == item["destination"] and
            receipt["anchor_month"] == item["anchor_month"], "V26R_RECEIPT_BINDING")
    pages = []
    for p in receipt["files"]:
        info = metadata(p["file_id"], access)
        require(info and info["parents"] == [receipt["folder_id"]] and info["name"] == page_label(item, p["position"]) and
                info["shared"] is False, "V26R_RECEIPT_BRONZE_PARENT")
        raw, packed = read_blob(p["file_id"], item, access)
        require(digest(raw) == p["raw_sha"] and digest(packed) == p["packed_sha"], "V26R_RECEIPT_ORIGINAL_SHA")
        pages.extend(read_rows(NativeContract(**item["native_contract"]), p["params"], raw)[0])
    return pages


class EvidenceStore:
    def __init__(self, folder, access, approved_proofs=None, journal=None, known_sources=None):
        self.folder, self.access = folder, access
        self.approved = approved_proofs or {}
        self.journal = journal
        self.known_sources = known_sources or {}

    def _read(self, name):
        return read_json_file(self.folder, name, self.access) if find(self.folder, name, self.access) else None

    def _write(self, name, value):
        return write_json(self.folder, name, value, self.access)

    def raw(self, params, key, limit):
        name = f"RAW_{key}.json.gz"
        found = find(self.folder, name, self.access)
        known = None if found else self.known_sources.get(key) or self._read(f"KNOWN_{key}.json")
        if not found and not known:
            return None
        file_id = found["id"] if found else known["file_id"]
        verify_private_folder(file_id, self.access)
        packed = read_drive_bytes(file_id, self.access, limit + limit // 256 + 1024)
        with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
            raw = stream.read(limit + 1)
        require(0 < len(raw) <= limit and (not known or (
            digest(raw) == known["raw_sha"] and digest(packed) == known["packed_sha"])), "V26R_B09_ORIGINAL_READBACK")
        return {"parameters": params, "request_id": key, "raw": raw, "raw_sha256": digest(raw)}

    def remember(self, item, receipt):
        if item["mode"] == "SNAPSHOT" or len(receipt["files"]) != 1:
            return
        page, = receipt["files"]
        key = manifest.exact_query_key(page["params"])
        value = {"file_id": page["file_id"], "raw_sha": page["raw_sha"], "packed_sha": page["packed_sha"],
                 "receipt_id": receipt["receipt_id"], "parameters": page["params"]}
        self._write(f"KNOWN_{key}.json", value)

    def official(self, item):
        approved = self.approved.get(item["request_id"])
        if not approved:
            return None
        require(ID.fullmatch(approved["file_id"]) and SHA.fullmatch(approved["sha256"]), "V26R_OFFICIAL_PROOF_REFERENCE")
        fid = approved["file_id"]
        verify_private_folder(fid, self.access)
        raw = read_private_file(fid, self.access)
        require(digest(raw) == approved["sha256"], "V26R_OFFICIAL_PROOF_DIGEST")
        return json.loads(raw)

    def reason_to_defer_primary(self, item):
        completeness = item["completeness"]
        if (completeness["method"] != "deferred_source_reconciliation" or completeness.get("planned_class") == "EXPLORATION"
                or completeness.get("verifier") == "PROVIDER_SPLIT" and item["start"] < item["end"]):
            return None
        try:
            witness = self.official(item)
            if witness is None:
                return "V26R_B09_OFFICIAL_SOURCE_REQUIRED"
            v26_acceptance.read_comparison(item, witness)
        except (KeyError, TypeError, ValueError):
            return "V26R_B09_OFFICIAL_SOURCE_INVALID"
        return None

    def collect(self, item, transport, capacity, now):
        if item["completeness"].get("verifier") != "PROVIDER_SPLIT" or item["start"] == item["end"]:
            return {"state": "AVAILABLE" if self.official(item) else "INDEPENDENT_SOURCE_PENDING"}
        require(self.journal is not None, "V26R_B09_EXECUTION_LEDGER")
        if self.journal.head() is None:
            self.journal.initialize({"comparisons": {}})
        record = self.journal.head()
        for params in v26_acceptance.split_queries(item):
            key, limit = manifest.exact_query_key(params), item["raw_limit"]
            if self.raw(params, key, limit):
                continue
            executions = record["state"]["comparisons"]
            state = executions.get(key, {"attempts": 0, "state": "READY", "retry_at": 0})
            if state["state"] == "REQUESTING":
                return {"state": "B09_COMPARISON_UNKNOWN"}
            if state["state"] == "REPAIR" or state["attempts"] >= 4:
                return {"state": "B09_COMPARISON_REPAIR"}
            if state["retry_at"] > now:
                return {"state": "WAIT", "retry_at": state["retry_at"]}
            if not capacity.check(0, limit * 2, reserve=True)["can_admit"]:
                return {"state": "WAIT_CAPACITY"}
            attempt = digest(encode([CAMPAIGN, "B09", key, record["sequence"] + 1]))
            record = advance(self.journal, record, comparisons=executions | {key: state | {
                "state": "REQUESTING", "parameters": params, "attempt_id": attempt, "planned_at": now}})
            try:
                outcome, response = transport.fetch(params, limit, identifier=attempt, lane="B09")
            except quota.Deferred as error:
                if str(error) in {"B00_NEXT_PACED_SLOT", "B00_PROVIDER_AUTHORIZATION_HOLD"}:
                    due = ((error.due or 0) + 999) // 1000
                    found = state | {"state": "READY" if due else "REPAIR", "retry_at": due,
                                     "last_outcome": str(error)}
                    advance(self.journal, record, comparisons=executions | {key: found})
                    return {"state": "WAIT", "retry_at": due} if due else {"state": "B09_COMPARISON_REPAIR"}
                return {"state": "B09_COMPARISON_UNKNOWN"}
            attempts = state["attempts"] + 1
            if response:
                raw, _ = response
                read_rows(NativeContract(**item["native_contract"]), params, raw)
                packed, raw_sha, packed_sha = compress_original(raw)
                fid = ensure(self.folder, f"RAW_{key}.json.gz", GZIP, self.access, packed, create=True)
                verify_private_folder(fid, self.access)
                require(self.raw(params, key, limit)["raw_sha256"] == raw_sha, "V26R_B09_RAW_SHA")
                record = advance(self.journal, record, comparisons=executions | {key: {
                    "state": "AVAILABLE", "attempts": attempts, "parameters": params, "attempt_id": attempt, "file_id": fid,
                    "raw_sha256": raw_sha, "packed_sha256": packed_sha, "retry_at": 0}})
                continue
            code = outcome.get("provider_code")
            retry = (code in {402, 429} or outcome.get("retryable")) and attempts < 4
            deadline = ((transport.broker.book.get("paused", 0) + 999) // 1000) if code in {402, 429} else now + 2**attempts
            advance(self.journal, record, comparisons=executions | {key: {
                "state": "READY" if retry else "REPAIR", "attempts": attempts, "retry_at": deadline, "attempt_id": attempt,
                "provider_code": code, "parameters": params}})
            return {"state": "WAIT", "retry_at": deadline} if retry else {"state": "B09_COMPARISON_REPAIR"}
        return {"state": "AVAILABLE"}

    def comparison(self, item):
        if official := self.official(item):
            return official
        if item["completeness"].get("verifier") != "PROVIDER_SPLIT" or item["start"] == item["end"]:
            return None
        replies = [self.raw(params, manifest.exact_query_key(params), item["raw_limit"])
                   for params in v26_acceptance.split_queries(item)]
        if any(value is None for value in replies):
            return None
        return {"request_id": item["request_id"], "contract_version": item["contract_version"],
                "kind": "PROVIDER_SPLIT", "replies": replies}


def checked_legacy_refs(sealed):
    from concurrent.futures import ThreadPoolExecutor

    reuse = sealed.read("REUSE_MAP.json")
    previous = private_path(sealed.access, sealed.task, V20_SPEC, "46030259242_1")

    def read(ref):
        _, raw = original(sealed.access, previous, ref["name"])
        rows = json.loads(raw)["receipts"]
        require(digest(raw) == ref["sha256"] and len(rows) == ref["receipts"], "V26R_PAST_RECEIPT_SHARD_HASH")
        return rows

    with ThreadPoolExecutor(max_workers=3) as pool:
        rows = [row for page in pool.map(read, reuse["legacy"]["shards"]) for row in page]
    require(len(rows) == reuse["legacy_verified_receipts"] == len({row["query_key"] for row in rows}), "V26R_PAST_RECEIPTS")
    return rows


def coverage_snapshot(sealed, active):
    reuse = sealed.read("REUSE_MAP.json")
    covered = inherited_coverage(active, reuse)
    exact, spans = covered["exact"], covered["intervals"]
    for row in checked_legacy_refs(sealed):
        params = row["params"]
        key = manifest.exact_query_key(params)
        saved = exact.setdefault(key, {"file_id": row["file_id"], "proof": row["proof"], "params": params})
        require(saved["proof"]["raw_sha"] == row["proof"]["raw_sha"], "V26R_ARCHIVED_REQUEST_HASH_CONFLICT")
        spans[params["dataset"]][params.get("data_id", "")].append([params["start_date"], params["end_date"]])
    intervals = {
        dataset: {code: [list(interval) for interval in sorted({tuple(value) for value in ranges})]
                  for code, ranges in scopes.items()}
        for dataset, scopes in spans.items()
    }
    return {"intervals": intervals,
            "exact": sorted(exact), "unique_verified_source_requests": len(exact),
            "sources": {key: {"file_id": row["file_id"], "raw_sha": row["proof"]["raw_sha"],
                              "packed_sha": row["proof"]["packed_sha"]} for key, row in exact.items()}}
