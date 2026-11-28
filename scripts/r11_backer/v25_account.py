"""Admit bounded Backer calls through the autonomous B00 account journal."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.client import IncompleteRead
import json
import os
from pathlib import PurePosixPath
from time import monotonic, sleep
from urllib.error import HTTPError

from . import autonomous_quota as quota, v21_broker, v24_account, v25_contracts
from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .drive_retry import transient
from .final_readiness_v2 import ROOT, original, private_path
from .integration_pilot import digest, encode
from .live_bronze_admission import fetch_native_payload
from .live_preflight import read_json
from .production_download import pending_group, segment
from .quota import HOUR_MS, REBALANCE_MS, TOTAL_LIMIT, provider_block_code
from .release_readiness import ACCOUNT
from .release_rehearsal import RehearsalError, require
from .test_archive import GitHub
from .v2_recovery import list_children

WORKFLOW = "r12-v25-autonomous-closeout.yml"
EARLY_JOURNAL_SPEC = "c83f8916d9559cba7638193bd5e206498f085ba8648337a685aeb2a5548b4271"
HORIZON_DAYS = 21
PLAN_LIMIT = 4
PROBE_RAW_LIMIT = 4 * 1024 * 1024


def cutover(ctx):
    """Verify the temporary repository cutover and recent known-project activity."""
    routes = v24_account.inventory(ctx, broker_workflow=WORKFLOW, entry_module="autonomous_closeout_v25")
    github = GitHub()
    discovered = set(routes["same_repository_secret_workflows"])
    require(WORKFLOW in discovered, "B00_NEW_WORKFLOW_NOT_DISCOVERED")
    names = discovered - {WORKFLOW}
    catalog = {PurePosixPath(w["path"]).name: w for w in github.pages("/actions/workflows", "workflows")}
    # Each older, potentially runnable repository route is disabled at the workflow API.
    # Branches containing the historical source remain immutable and can be restored.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

    def verify(name):
        flow = catalog.get(name, {})
        if not flow or flow["state"] not in {"disabled_manually", "disabled_inactivity"}:
            return {"workflow": name, "id": flow.get("id"), "state": flow.get("state"), "result": "DIRECT_ENTRY_NOT_ISOLATED"}
        runs = github.json(f"/actions/workflows/{flow['id']}/runs?per_page=10")["workflow_runs"]
        recent = [r for r in runs if r["status"] != "completed" or datetime.fromisoformat(r["updated_at"]) > cutoff]
        return {"workflow": name, "id": flow["id"], "state": flow["state"],
                "result": "ROLLING_HISTORY_OR_INFLIGHT_UNRESOLVED" if recent else "ISOLATED_PAST_FULL_HOUR",
                "latest_completed_run": runs[0]["id"] if runs else None,
                "latest_completed_at": runs[0]["updated_at"] if runs else None,
                "unreconciled_recent_run_ids": [r["id"] for r in recent]}

    with ThreadPoolExecutor(max_workers=5) as pool:
        confirmed = list(pool.map(verify, sorted(names)))
    return {"branches_checked": routes["branches_checked"], "known_workflows": sorted(discovered),
            "legacy_workflows": confirmed, "old_active_candidate_runs": routes["active_peer_runs"],
            "new_workflow": WORKFLOW, "unmanaged_external_traffic_requires_no_evidence": True,
            "mode": "SHARED_B00_FOR_NEW_ENTRANCE_LEGACY_GITHUB_QUARANTINE_REVERSIBLE",
            "status": "PASS" if all(r["result"] == "ISOLATED_PAST_FULL_HOUR" for r in confirmed) and
            not routes["active_peer_runs"] else "HOLD"}


def account_journal(ctx):
    parent = private_path(ctx.access, ctx.task_base, EARLY_JOURNAL_SPEC, "ISOLATED_ACCOUNT_QUOTA_BROKER")
    return DriveJournal(parent, ctx.access)


@contextmanager
def production_transport(access, run, attempt):
    """The formal controller opens the same journal when separately dispatched."""
    parent = private_path(access, ROOT, "00_COMMAND", "R12_BACKER", "00_SHARED_CONTEXT", "FINAL_READINESS",
                          quota.TASK, EARLY_JOURNAL_SPEC, "ISOLATED_ACCOUNT_QUOTA_BROKER")
    broker = quota.AccountBroker(DriveJournal(parent, access), f"{run}:{attempt}:PRODUCTION")
    broker.acquire()
    try:
        yield FinMindTransport(broker, os.environ.get("FINMIND_BACKER_TOKEN"), preflight=False)
    finally:
        broker.close()


def project_events(ctx, now):
    master = ctx.master["state"]
    group = pending_group(ctx.plan, ctx.supplement, master)
    events = list(master["events"])
    in_flight = []
    if group is not None:
        plan = ctx.plan if group == "BASE" else ctx.supplement
        _, _, _, current = segment(plan, ctx.master, ctx.campaign, ctx.access, group, create=False)
        events.extend(current["state"]["events"])
        in_flight = [key for key, item in current["state"]["tasks"].items() if item["status"] == "REQUESTING"
                     and item.get("lease_until", now // 1000) >= now // 1000]
    require(not in_flight, "B00_FORMAL_REQUESTS_STILL_IN_FLIGHT")
    require(all(type(event.get("at")) is int and event["at"] * 1000 <= now for event in events), "B00_PROJECT_CLOCK_OR_EVENT")
    return [event for event in events if event["at"] * 1000 > now - HOUR_MS]


def policy(ctx):
    return {"task_id": ctx.task_id, "account_scope_sha256": quota.ACCOUNT, "total_requests_per_rolling_hour": TOTAL_LIMIT,
            "local_target_from_user_decision": True, "rolling_window_ms": HOUR_MS, "pacing_target_ms": 2250,
            "rebalance_every_ms": REBALANCE_MS, "allocated_reference": [267, 267, 266, 267, 267, 266],
            "grant_ms": quota.GRANT_MS, "lease_ms": quota.LEASE_MS,
            "task_limits_across_runs_and_versions": quota.TASK_CAPS, "horizon_days_approximate": HORIZON_DAYS,
            "require_user_info": False, "require_subscription_expiry": False, "require_exclusive_account_evidence": False,
            "count_each_physical_http_attempt": True, "unknown_and_unused_reservations_charged": True,
            "no_direct_legacy_worker_calls_in_v25": True, "H_and_Drive_are_unmetered_by_finmind": True,
            "no_provider_snapshot_synthesized": True, "full_download_release": "HOLD", "production_dispatchable": 0}


def setup(ctx):
    consumers = cutover(ctx)
    rules = policy(ctx)
    if consumers["status"] != "PASS":
        return "HOLD", {"QUOTA_POLICY.json": rules, "CONSUMER_INVENTORY.json": consumers,
                        "QUOTA_MIGRATION.json": {"state": "KNOWN_REPOSITORY_CUTOVER_INCOMPLETE", "live_admission": False}}
    journal = account_journal(ctx)
    prior = ctx.prior("PROBE_LEDGER.json")
    require(prior["checkpoint"]["folder_id"] == journal.parent, "B00_EXISTING_JOURNAL_ID")
    previous = journal.head()
    if previous["state"].get("format") != quota.FORMAT:
        now = quota.utc_ms()
        old_events = v21_broker.verify_migration_source(previous, prior, ctx.task_id)
        imported = [*old_events, *project_events(ctx, now)]
        ancestor = {"sequence": previous["sequence"], "sha256": digest(encode(previous)), "next_file_id": previous["next_id"]}
        book = quota.initial(now, policy_evidence=digest(encode(rules)), prior=prior, previous_record=ancestor, imported=imported)
        new = quota.checkpoint(book)
        require(journal.append(previous, new), "B00_MIGRATION_CAS")
        readback = journal.load(previous["next_id"], previous["sequence"] + 1)
        require(readback and readback["state"] == new, "B00_MIGRATION_READBACK")
    record, book, _ = quota.open_book(journal)
    require(book["policy_evidence"] == digest(encode(rules)), "B00_POLICY_CHECKPOINT_BINDING")
    return "PASS", {"QUOTA_POLICY.json": rules, "CONSUMER_INVENTORY.json": consumers,
                    "QUOTA_MIGRATION.json": {"state": "EXISTING_ACCOUNT_JOURNAL_EXTENDED", "live_admission": True,
                                            "folder_id": journal.parent, "sequence": record["sequence"],
                                            "sha256": digest(encode(record)), "prior": book["prior_checkpoint"],
                                            "active_imported_requests": len(book["events"])}}


class FinMindTransport:
    """One governor for native and compatibility callers. No redirects or hidden retries."""

    def __init__(self, broker, token, *, preflight, payload=fetch_native_payload, account=read_json):
        require(bool(token), "BACKER_SECRET_UNAVAILABLE")
        self.broker, self._token, self.preflight = broker, token, preflight
        self.payload, self.account = payload, account

    def fetch(self, params, raw_limit, *, identifier, lane):
        return self._call(kind="data", scope=params["dataset"], identifier=identifier, lane=lane,
                          operation=lambda: self.payload(self._token, params, raw_limit))

    def calibrate(self, identifier):
        return self._call(kind="account", scope="user_info", identifier=identifier, lane="ACCOUNT",
                          operation=lambda: self.account(ACCOUNT, self._token))

    def _call(self, *, kind, scope, identifier, lane, operation):
        grant, started = self.broker.wait_reserve(identifier, kind, lane, scope, preflight=self.preflight)
        t0 = monotonic()
        try:
            answer = operation()
            raw = answer[0] if kind == "data" else None
            body = json.loads(raw) if kind == "data" else answer
            code = provider_block_code(body.get("status")) if isinstance(body, dict) else None
            detail = {"http_success": True, "provider_code": code, "elapsed_seconds": round(monotonic() - t0, 4)}
            retry_after = body.get("retry_after") if isinstance(body, dict) else None
        except HTTPError as error:
            code = error.code
            retry_after = error.headers.get("Retry-After") if error.headers else None
            retry = code == 408 or 500 <= code < 600
            self.broker.result(grant, started, status=f"HTTP_{code}", code=code, retry_after=retry_after, transient=retry)
            return {"status": "HTTP_ERROR", "provider_code": code, "retryable": retry,
                    "elapsed_seconds": round(monotonic() - t0, 4)}, None
        except (OSError, ValueError, RuntimeError, IncompleteRead, EOFError) as error:
            retry = transient(error)
            self.broker.result(grant, started, status="NETWORK_OR_INVALID_RESPONSE", transient=retry, code=None if retry else 500)
            return {"status": "NETWORK_OR_INVALID_RESPONSE", "classification": type(error).__name__, "retryable": retry,
                    "elapsed_seconds": round(monotonic() - t0, 4)}, None
        if code:
            self.broker.result(grant, started, status=f"PAYLOAD_{code}", code=code, retry_after=retry_after)
            return detail | {"status": "PROVIDER_BLOCK", "retryable": False}, None
        # Actual API entitlement depends on the response body. A malformed success never increments the clean streak.
        usable = isinstance(body, dict) and ((body.get("status") == 200 and isinstance(body.get("data"), list))
                  if kind == "data" else (type(body.get("api_request_limit")) is int and type(body.get("user_count")) is int))
        self.broker.result(grant, started, status="HTTP_RESPONSE_VERIFIED" if usable else "SOURCE_STATUS_UNVERIFIED",
                           code=200 if usable else 422, observed=body if usable and kind == "account" else None)
        return detail | {"status": "HTTP_RESPONSE_VERIFIED" if usable else "SOURCE_STATUS_UNVERIFIED",
                         "retryable": False}, answer if usable else None


def stable_attempt(task_id, params, number):
    return digest(encode([task_id, "V25_WITNESS", params, number]))


def recover_witness(ctx, identity):
    """Find the prior immutable witness for a charged successful request."""
    name = f"WITNESS_{identity}.json"
    scopes = list_children(ctx.task_root, ctx.access)
    for label, scope in sorted(scopes.items(), reverse=True):
        if label == f"{ctx.run}_{ctx.attempt}" or not all(part.isdecimal() for part in label.split("_")):
            continue
        children = list_children(scope["id"], ctx.access)
        if name not in children:
            continue
        _, raw = original(ctx.access, scope["id"], name)
        witness = json.loads(raw)
        row = witness["result"]
        require(witness["task_id"] == ctx.task_id and witness["spec_sha256"] == ctx.spec_sha and
                label == f"{witness['run_id']}_{witness['attempt']}" and row["request_id"] == identity and
                row["source_folder_id"] == scope["id"] and row["status"] == "HTTP_RESPONSE_VERIFIED",
                "V25_WITNESS_CHECKPOINT_BINDING")
        v25_contracts.original_probe(ctx, row)
        return row | {"reused_from_run": witness["run_id"], "reused_from_attempt": witness["attempt"]}
    return None


def probe(ctx):
    migration = ctx.read("QUOTA_MIGRATION.json")
    plan = ctx.read("PROBE_PLAN.json")["selected"]
    if not migration["live_admission"]:
        return "HOLD", {"PROBE_RESULTS.json": {"rows": [], "hold": migration["state"]},
                        "PROBE_LEDGER.json": usage(ctx), "10MIN_REBALANCE.json": {"workloads": [], "actual_runs": 0}}
    before = monotonic()
    journal = account_journal(ctx)
    broker = quota.AccountBroker(journal, f"{ctx.run}:{ctx.attempt}:G05_B")
    broker.acquire()
    completed, reports = [], []
    try:
        transport = FinMindTransport(broker, os.environ.get("FINMIND_BACKER_TOKEN"), preflight=True)
        todo = [{**row, "attempt_number": 0, "enqueued_at": quota.utc_ms()} for row in plan[:PLAN_LIMIT]]
        previous = ctx.legacy("CONTRACT_EVIDENCE.json")["stored_samples"]
        previous_requests = {digest(encode(row["request"])) for row in previous if row.get("request") and
                             row["result"] in {"VALID_SAMPLE", "PASS_NATIVE_SAMPLE"}}
        while todo:
            lanes = {}
            now = quota.utc_ms()
            for row in todo:
                lane = row["lane"]
                entry = lanes.setdefault(lane, {"count": 0, "priority": row["priority"], "wait_ms": now - row["enqueued_at"],
                                                "recent_completed": sum(item["lane"] == lane for item in completed)})
                entry["count"] += 1
            report = broker.plan(lanes)
            reports.append(report)
            if not report["lane_allocations"]:
                break
            eligible = [item for item in todo if report["lane_allocations"].get(item["lane"], 0)]
            selected = max(eligible, key=lambda item: (item["priority"], -item["enqueued_at"]))
            todo.remove(selected)
            params = selected["parameters"]
            item = {"dataset": params["dataset"], "parameters": params, "lane": selected["lane"],
                    "evidence": selected["evidence"], "access_tier": selected["tier"]}
            if digest(encode(params)) in previous_requests:
                completed.append(item | {"status": "EXISTING_EVIDENCE_REUSED"})
                continue
            identity = stable_attempt(ctx.task_id, params, selected["attempt_number"])
            previous_attempt = broker.book["preflight"]["attempts"].get(identity)
            if previous_attempt:
                previous_status = previous_attempt["status"]
                if previous_status == "HTTP_RESPONSE_VERIFIED":
                    recovered = recover_witness(ctx, identity)
                    completed.append(recovered or item | {"status": "CHARGED_RESPONSE_WITNESS_MISSING", "request_id": identity})
                elif selected["attempt_number"] < 3 and (
                    previous_status in {"HTTP_408", "NETWORK_OR_INVALID_RESPONSE"} or
                    previous_status.startswith("HTTP_5")
                ):
                    selected["attempt_number"] += 1
                    todo.append(selected)
                else:
                    completed.append(item | {"status": "PREVIOUS_ATTEMPT_CHARGED", "prior_status": previous_status,
                                             "request_id": identity})
                continue
            try:
                detail, answer = transport.fetch(params, PROBE_RAW_LIMIT, identifier=identity, lane=selected["lane"])
            except quota.Deferred as error:
                completed.append(item | {"status": str(error), "request_id": identity, "next_ms": error.due})
                continue
            item |= detail | {"request_id": identity}
            if answer is not None:
                raw, encoding = answer
                packed, raw_sha, packed_sha = compress_original(raw)
                started = monotonic()
                file_id = ctx.binary(f"PROBE_{identity}.json.gz", packed, "application/gzip")
                item |= {"file_id": file_id, "filename": f"PROBE_{identity}.json.gz", "raw_sha256": raw_sha,
                         "source_folder_id": ctx.root, "gzip_sha256": packed_sha, "raw_bytes": len(raw),
                         "packed_bytes": len(packed), "encoding": encoding,
                         "drive_write_readback_seconds": round(monotonic() - started, 4),
                         "captured_at": datetime.now(timezone.utc).isoformat(), "rows": len(json.loads(raw)["data"])}
                ctx.write(f"WITNESS_{identity}.json", ctx.binding() | {"result": item})
            completed.append(item)
            if item["retryable"] and selected["attempt_number"] < 3:
                selected["attempt_number"] += 1
                sleep(2 ** (selected["attempt_number"] - 1))
                todo.append(selected)
        reports.append(broker.plan({}))
    finally:
        broker.close()
    has_backer = any(row["access_tier"] == "Backer" and row["status"] == "HTTP_RESPONSE_VERIFIED" and row.get("file_id")
                     for row in completed)
    output = {"rows": completed, "verified_backer_entitlement_response": has_backer, "real_finmind_response_count": sum(
        row["status"] == "HTTP_RESPONSE_VERIFIED" and not row.get("reused_from_run") for row in completed),
        "elapsed_seconds": round(monotonic() - before, 4),
        "used_user_info": False, "partial_remaining": [item["parameters"]["dataset"] for item in todo]}
    return ("PASS" if has_backer else "HOLD"), {"PROBE_RESULTS.json": output, "PROBE_LEDGER.json": usage(ctx),
                                                "10MIN_REBALANCE.json": {"workloads": reports, "actual_runs": len(reports),
                                                                          "rule": "ON_QUEUE_CHANGE_OR_10_MINUTE_EXPIRY"}}


def usage(ctx):
    journal = account_journal(ctx)
    current = journal.head()
    if current["state"].get("format") != quota.FORMAT:
        prior = ctx.prior("PROBE_LEDGER.json")
        v21_broker.verify_migration_source(current, prior, ctx.task_id)
        return prior | {"journal_mode": "PRE_V25_UNCHANGED"}
    record, book, _ = quota.open_book(journal)
    budget = book["preflight"]
    totals = {key: budget["reserved"][key] for key in quota.TASK_CAPS}
    actual = {key: budget["actual"][key] for key in quota.TASK_CAPS}
    return {"journal_mode": quota.FORMAT, "checkpoint": {"folder_id": journal.parent, "sequence": record["sequence"],
            "sha256": digest(encode(record))}, "account_identity_sha256": quota.ACCOUNT,
            "actual_data_requests": actual["data"], "actual_account_requests": actual["account"],
            "reserved_data_requests": totals["data"], "reserved_account_requests": totals["account"],
            "unknown_or_unsent_charged": {key: totals[key] - actual[key] for key in totals},
            "known_unsent_charged": budget["unsent"], "task_limits": quota.TASK_CAPS, "attempts": budget["attempts"],
            "current_effective_hourly_limit": book["limit"], "provider_cap_observed": book["provider_cap"],
            "all_known_calls_use_same_journal": True, "cooldown_until_ms": book["paused"], "auth_hold": book["auth_hold"],
            "dataset_permission_blocks": book["denied"], "recent_reservations": len(book["events"]),
            "active_writer": book["lease"], "last_rebalance": book["rebalance"]}
