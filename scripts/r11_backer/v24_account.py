"""Reopen the account broker checkpoint and admit only bounded, evidenced requests."""

from collections import Counter
from datetime import date, datetime
from http.client import IncompleteRead
import json
import os
from pathlib import PurePosixPath
import re
import subprocess
from time import sleep, time
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

from . import v21_broker as broker, v2_resources
from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .drive_retry import transient as retryable_network
from .final_readiness_v2 import original, private_path
from .integration_pilot import digest, encode
from .live_bronze_admission import fetch_native_payload
from .quota import NORMAL_LIMIT, ROLLING_SECONDS, TOTAL_LIMIT, provider_block_code
from .release_rehearsal import require
from .test_archive import GitHub
from .v2_registry import certify_shape, NativeContract

CONSUMER = "secrets.FINMIND_BACKER_TOKEN"
WORKFLOW = "r12-v24-official-bulk.yml"
GROUP = "r12-1-bounded-preflight"
MINIMUM = ["SAME_BACKER_ACCOUNT_PLAN_AND_EXPIRY", "ACCOUNT_WIDE_CONSUMER_CONTROL_AND_RECENT_USAGE_EVIDENCE"]


def git(*args):
    return subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL, text=True, timeout=20)


def inventory(ctx, *, broker_workflow=WORKFLOW, entry_module="official_bulk_v24"):
    """Trace actual repository refs, including manually callable legacy workflows."""
    refs = sorted(ref for ref in git("for-each-ref", "--format=%(refname)", "refs/remotes/origin").splitlines()
                  if ref != "refs/remotes/origin/HEAD")
    if "refs/remotes/origin/main" not in refs:
        raise ValueError("ACCOUNT_DEFAULT_REF_UNAVAILABLE")
    by_blob = {}
    reviewed = []
    for ref in refs:
        for row in git("ls-tree", "-r", ref, "--", ".github/workflows/").splitlines():
            head, path = row.split("\t", 1)
            blob = head.split()[2]
            if not path.endswith((".yml", ".yaml")):
                continue
            text = by_blob.get(blob)
            if text is None:
                text = by_blob[blob] = git("cat-file", "-p", blob)
            if CONSUMER not in text:
                continue
            name = PurePosixPath(path).name
            same_group = bool(re.search(r"(?m)^  group: ['\"]?r12-1-bounded-preflight['\"]?$", text))
            reviewed.append({"ref": ref.removeprefix("refs/remotes/origin/"), "workflow": name,
                             "workflow_git_blob": blob, "same_concurrency_group": same_group,
                             "uses_persistent_broker": same_group and name == broker_workflow and
                             entry_module in text, "workflow_dispatch": "workflow_dispatch:" in text})
    github = GitHub()
    active = []
    workflow_paths = {r["workflow"] for r in reviewed}
    for state in ("in_progress", "queued", "waiting", "pending", "requested"):
        for row in github.pages("/actions/runs", "workflow_runs", status=state):
            if row["id"] != ctx.run and PurePosixPath(row.get("path", "")).name in workflow_paths:
                active.append({"id": row["id"], "head_branch": row["head_branch"], "path": row["path"],
                               "status": row["status"]})
    base = {"branches_checked": len(refs), "same_repository_secret_workflows": sorted(workflow_paths),
            "consumer_paths": reviewed, "unmigrated_refs": [row for row in reviewed if not row["uses_persistent_broker"]],
            "active_peer_runs": sorted(active, key=lambda row: row["id"])}
    return base | {"snapshot_sha256": digest(encode(base)), "external_consumers_observed": False,
                   "concurrency_only_does_not_prove_account_exclusion": True}


def checkpoint(ctx):
    previous = ctx.prior("PROBE_LEDGER.json")
    folder = private_path(ctx.access, ctx.prior_version, "ISOLATED_ACCOUNT_QUOTA_BROKER")
    journal = DriveJournal(folder, ctx.access)
    record = journal.head()
    require(record is not None, "QUOTA_CHECKPOINT_MISSING")
    state = record["state"]
    events = state["events"]
    inherited = previous["events"]
    tracked = broker._counts(events)
    require(state["mode"] == broker.MODE and state["task_id"] == ctx.task_id and state["spec_sha"] == ctx.prior_spec and
            state["prior_actual"] == previous["prior_actual"] and
            state["prior_evidence_sha256"] == previous["prior_evidence_sha256"] and
            len(tracked) == len(events) and events[:len(inherited)] == inherited and
            all(record["state"]["results"].get(key) == value for key, value in previous["results"].items()) and
            set(state["results"]) <= tracked.keys(), "V24_QUOTA_CHECKPOINT_BINDING")
    totals = Counter(kind for _, kind in tracked.values())
    require(all(totals[kind] <= bound for kind, bound in broker.LIMITS.items()), "V24_QUOTA_CHECKPOINT_CAP")
    return journal, record


def usage(ctx):
    journal, record = checkpoint(ctx)
    state = record["state"]
    known, unknown, reserved = Counter(), Counter(), Counter()
    old = ctx.prior("PROBE_LEDGER.json")
    old_requests = {event["request_id"] for event in old["events"]}
    for event in state["events"]:
        reserved[event["kind"]] += 1
        if event["request_id"] in old_requests:
            continue
        result = state["results"].get(event["request_id"])
        if result:
            require(result["actual_requests"] == 1, "INVALID_SOURCE_ATTEMPT_COUNT")
            known[event["kind"]] += 1
        else:
            unknown[event["kind"]] += 1
    total_actual = {label: old[f"actual_{label}_requests"] + known[kind]
                    for label, kind in (("data", "download"), ("account", "probe"))}
    total_reserved = {"data": reserved["download"], "account": reserved["probe"]}
    require(all(0 <= actual <= total_reserved[label] for label, actual in total_actual.items()), "SOURCE_COUNT_RECONCILIATION")
    return {"checkpoint": {"folder_id": journal.parent, "sequence": record["sequence"],
                            "sha256": digest(encode(record))}, "ledger": "REUSED_V2.1_ACCOUNT_JOURNAL",
            "events_sha256": digest(encode(state["events"])), "actual_data_requests": total_actual["data"],
            "actual_account_requests": total_actual["account"], "reserved_data_requests": total_reserved["data"],
            "reserved_account_requests": total_reserved["account"],
            "uncertain_requests_included_in_caps": {label: total_reserved[label] - total_actual[label]
                                                    for label in total_actual},
            "data_cap": broker.LIMITS["download"], "account_cap": broker.LIMITS["probe"],
            "pause_until": state["paused_until"], "auth_hold": state["auth_hold"],
            "new_known_requests": dict(known), "new_uncertain_requests": dict(unknown)}


def read_attestation(ctx, consumers, now):
    """Verify private referenced evidence and the current exclusion interval before binding a lease."""
    try:
        fid, raw = original(ctx.access, ctx.shared, "BACKER_ACCOUNT_ADMISSION.json")
    except (OSError, RuntimeError, ValueError):
        return {}, ["ACCOUNT_CONTROL_AND_HEADROOM_EVIDENCE_MISSING"]
    signed = json.loads(raw)
    blockers = []
    if signed.get("task_id") != ctx.task_id or signed.get("spec_sha256") != ctx.spec_sha:
        return {}, ["ACCOUNT_EVIDENCE_SPEC_BINDING"]
    references = {}
    for key in ("account_control", "external_consumers", "entitlement", "rolling_window"):
        ref = signed.get(key, {})
        name = ref.get("file")
        if not isinstance(name, str) or not re.fullmatch(r"BACKER_ACCOUNT_[A-Z0-9_]+\.json", name) or name == "BACKER_ACCOUNT_ADMISSION.json":
            blockers.append("ACCOUNT_EVIDENCE_REFERENCE_" + key.upper())
            continue
        try:
            _, body = original(ctx.access, ctx.shared, name)
            require(digest(body) == ref.get("sha256"), "ACCOUNT_EVIDENCE_DIGEST")
            references[key] = json.loads(body)
        except (OSError, RuntimeError, ValueError, KeyError):
            blockers.append("ACCOUNT_EVIDENCE_UNREADABLE_" + key.upper())
    if blockers:
        return {}, blockers
    owner, external, entitlement, rolling = (references[k] for k in
                                              ("account_control", "external_consumers", "entitlement", "rolling_window"))
    owner_scope = owner.get("account_scope_sha256")
    required = {(row["ref"], row["workflow"], row["workflow_git_blob"]) for row in consumers["unmigrated_refs"]}
    verified = {tuple(x) for x in external.get("excluded_repository_consumers", []) if isinstance(x, list) and len(x) == 3}
    end, exclusive = external.get("exclusive_until"), external.get("exclusive_since")
    floor, upper = entitlement.get("minimum_hourly_limit"), rolling.get("maximum_requests_in_current_hour")
    try:
        expiry = date.fromisoformat(entitlement["subscription_expiry"])
        sufficiently_current = expiry >= datetime.now(ZoneInfo("Asia/Taipei")).date()
    except (TypeError, ValueError, KeyError):
        sufficiently_current = False
    if (not broker.sha(owner_scope) or any(x.get("account_scope_sha256") != owner_scope for x in references.values()) or
            owner.get("owner_confirmed") is not True or not sufficiently_current):
        blockers.append("SAME_ACCOUNT_ENTITLEMENT_UNVERIFIED")
    if (external.get("account_wide_exclusive") is not True or not required <= verified or consumers["active_peer_runs"] or
            type(end) is not int or end <= now + 60 or type(exclusive) is not int or exclusive > now - ROLLING_SECONDS):
        blockers.append("ACCOUNT_WIDE_EXCLUSION_NOT_ESTABLISHED")
    observed = rolling.get("observed_at")
    shared_events = rolling.get("shared_events")
    if (type(floor) is not int or type(upper) is not int or not 0 <= upper < floor - broker.HEADROOM_RESERVE or
            type(observed) is not int or not 0 <= now - observed <= broker.SNAPSHOT_VALID_SECONDS or not isinstance(shared_events, list)):
        blockers.append("SAFE_FIRST_USER_INFO_HEADROOM_MISSING")
    if blockers:
        return {}, blockers
    broker._counts(shared_events)
    return {"task_id": ctx.task_id, "spec_sha": ctx.spec_sha, "account_ownership_evidence_sha256": digest(raw),
            "account_evidence_file_id": fid, "all_consumers_in_broker": True, "exclusive_account_lease_read_back": True,
            "expires_at": end, "shared_events": shared_events, "subscription_expiry": str(expiry),
            "bootstrap_basis": {"kind": "VERIFIED_ROLLING_WINDOW_BOUND", "exclusive_since": exclusive,
                                "api_request_limit": floor, "user_count": upper, "observed_at": observed,
                                "evidence_sha256": signed["rolling_window"]["sha256"]}}, []


def account_phase(ctx):
    consumers = inventory(ctx)
    journal, record = checkpoint(ctx)
    admission, blockers = read_attestation(ctx, consumers, int(time()))
    if consumers["unmigrated_refs"] and not admission:
        blockers.append("LEGACY_AND_EXTERNAL_ACCOUNT_EXCLUSION_UNVERIFIED")
    lease, probe = None, {"status": "NOT_SENT", "actual_requests": 0}
    credential = os.environ.get("FINMIND_BACKER_TOKEN")
    if admission and not credential:
        blockers.append("ACCOUNT_CREDENTIAL_UNAVAILABLE")
    if not blockers:
        lease = broker.acquire_lease(journal, record, run=ctx.run, attempt=ctx.attempt, now=int(time()),
                                     expires_at=admission["expires_at"], spec_sha=ctx.spec_sha,
                                     evidence_sha=admission["account_ownership_evidence_sha256"])
        if not lease:
            blockers.append("DURABLE_LEASE_NOT_ACQUIRED")
    if lease:
        admission |= {"lease_id": lease["lease_id"]}
        for number in range(broker.LIMITS["probe"]):
            head = journal.head()
            identifier = digest(encode([ctx.task_id, ctx.run, ctx.attempt, "USER_INFO", number]))
            prior = head["state"]["results"].get(identifier)
            probe = prior or broker.account_probe(journal, head, request_id=identifier, now=int(time()), run=ctx.run,
                                                  admission=admission, token=credential, bootstrap=True)
            if probe["status"] == "PROVIDER_LIMIT_OBSERVED":
                break
            code = probe.get("status_code", 0)
            transient = (probe["status"] == "HOLD_PROVIDER_HTTP" and (code == 408 or 500 <= code < 600) or
                         probe["status"] == "HOLD_PROVIDER_RESPONSE" and probe.get("transient"))
            if not transient:
                break
            if number < broker.LIMITS["probe"] - 1:
                sleep(2 ** number)
        if probe["status"] != "PROVIDER_LIMIT_OBSERVED":
            blockers.append(probe["status"])
    return ("HOLD" if blockers else "PASS"), {"ACCOUNT_CONSUMER_INVENTORY.json": consumers,
        "LEASE_EVIDENCE.json": {"lease": lease, "blockers": blockers, "proof": admission,
                                "minimal_missing_inputs": MINIMUM if not admission else [],
                                "isolated_task_budget": usage(ctx)},
        "ACCOUNT_BOOTSTRAP_RECEIPT.json": probe | {"endpoint": "https://api.web.finmindtrade.com/v2/user_info",
                                                   "requires_prior_live_snapshot": False}}


class SharedPermit:
    """Use the same durable account lease for preflight and a native batch caller."""

    def __init__(self, journal, run, admission):
        self.journal, self.run, self.admission = journal, run, admission

    def __call__(self, identity, parameters, item):
        previous = self.journal.head()
        verdict = broker.reserve(self.journal, previous, kind="download", request_id=identity, now=int(time()),
                                 run=self.run, admission=self.admission)
        if verdict != "RESERVED":
            return verdict
        head = self.journal.head()
        return ("RESERVED" if head and head["sequence"] == previous["sequence"] + 1 and
                head["state"]["events"][-1]["request_id"] == identity else "RESERVATION_UNCERTAIN")

    def complete(self, identity, status, *, code=None, retry_after=None):
        head = self.journal.head()
        require(any(e["request_id"] == identity and e.get("lease_id") == self.admission["lease_id"]
                    for e in head["state"]["events"]), "PROVIDER_RESULT_WITHOUT_PERMIT")
        if prior := head["state"]["results"].get(identity):
            return prior
        result = {"status": status, "actual_requests": 1, "observed_at": int(time())}
        return broker.commit_result(self.journal, head, identity, result, code=code, retry_after=retry_after)


def shape_sample(ctx, permit, auth, rule, mode, params):
    attempts = []
    for number in range(4):
        identity = digest(encode([ctx.task_id, ctx.spec_sha, "INCREMENTAL_SHAPE_PROBE", params, number]))
        granted = permit(identity, params, None)
        evidence = {"dataset": rule.dataset, "mode": mode, "params": params, "status": granted,
                    "request_id": identity, "attempt": number + 1, "window_verified": False}
        attempts.append(evidence)
        if granted != "RESERVED":
            break
        code, delay, again, received = None, None, False, False
        try:
            raw, encoding = fetch_native_payload(auth, params, rule.byte_limit)
            received = True
            payload = json.loads(raw)
            code = provider_block_code(payload.get("status")) if isinstance(payload, dict) else None
            delay = payload.get("retry_after") if isinstance(payload, dict) else None
            if code:
                evidence["status"] = f"HTTP_{code}"
            else:
                packed, raw_sha, packed_sha = compress_original(raw)
                evidence |= {"status": "ARCHIVED_SAMPLE", "raw_sha256": raw_sha, "packed_sha256": packed_sha,
                             "content_encoding": encoding, "file_id": ctx.binary(
                                 f"PROBE_{identity}.json.gz", packed, "application/gzip")}
                evidence["native_shape"] = certify_shape(rule, params, raw)
        except HTTPError as error:
            code = error.code
            delay = error.headers.get("Retry-After") if error.headers else None
            evidence["status"] = f"HTTP_{code}"
            again = code == 408 or 500 <= code < 600
        except (OSError, RuntimeError, ValueError, IncompleteRead, EOFError) as error:
            evidence["status"] = type(error).__name__
            # Once the payload has arrived, an archive failure is not a source retry.
            again = retryable_network(error) and not received
        saved = permit.complete(identity, evidence["status"], code=code, retry_after=delay)
        if not again or number == 3 or saved.get("status") == "HOLD_PROBE_RESULT_WRITE_UNCERTAIN":
            break
        sleep(2 ** number)
    return attempts


def samples_phase(ctx):
    lease = ctx.read("LEASE_EVIDENCE.json")
    account = ctx.read("ACCOUNT_BOOTSTRAP_RECEIPT.json")
    blockers = []
    probes = []
    if lease["blockers"] or account["status"] != "PROVIDER_LIMIT_OBSERVED":
        blockers.append("SAFE_ACCOUNT_BOOTSTRAP_UNAVAILABLE")
    if not blockers:
        observed = account["observed_at"]
        if not 0 <= int(time()) - observed <= broker.SNAPSHOT_VALID_SECONDS:
            blockers.append("PROVIDER_HEADROOM_SNAPSHOT_EXPIRED")
    if not blockers:
        permit = SharedPermit(checkpoint(ctx)[0], ctx.run, lease["proof"] | {"provider_snapshot": {
            "observed_at": observed, "api_request_limit": account["api_request_limit"],
            "user_count": account["user_count"], "evidence_sha256": digest(encode(account)),
        }})
        previous = ctx.prior("CONTRACT_EVIDENCE.json")["stored_samples"]
        rules = {r["dataset"]: r for r in ctx.read("OFFICIAL_DATASET_REVIEW.json")["rows"]}
        authorized = [("TaiwanStockPriceLimit", "MARKET_DAY", {"start_date": "2026-11-25"}),
                      ("TaiwanStockActiveETFInfo", "SNAPSHOT", {})]
        auth = os.environ.get("FINMIND_BACKER_TOKEN")
        require(auth, "PROBE_CREDENTIAL_UNAVAILABLE")
        for dataset, mode, extra in authorized:
            contract = rules.get(dataset, {}).get("mode_contracts", {}).get(mode)
            params = {"dataset": dataset, **extra}
            if not contract or any(entry.get("request") == params for entry in previous):
                continue
            probes.extend(shape_sample(ctx, permit, auth, NativeContract(**contract), mode, params))
    return ("HOLD" if blockers else "PASS"), {"PROBE_RESULTS.json": {"probes": probes, "blockers": blockers,
                                                                        "readiness_only": True},
                                               "PROBE_LEDGER.json": usage(ctx)}


def resource_phase(ctx):
    ledger = usage(ctx)
    lease, account = ctx.read("LEASE_EVIDENCE.json"), ctx.read("ACCOUNT_BOOTSTRAP_RECEIPT.json")
    index = ctx.read("MANIFEST_INDEX.json")
    drive = v2_resources.observed_drive(ctx.access)
    limited = v2_resources.campaign_forecast(ctx.plan, ctx.supplement, ctx.master, drive["available_bytes"], {})
    forecast = index["forecast"]
    per_hour = min(NORMAL_LIMIT, account["api_request_limit"], TOTAL_LIMIT) if account["status"] == "PROVIDER_LIMIT_OBSERVED" else None
    horizon = lease["proof"].get("subscription_expiry")
    requests = forecast["requests"]
    scenarios = {"low": {"http_requests": requests, "provider_hours_envelope": requests / per_hour if per_hour else None},
                 "baseline": {"http_requests": requests, "raw_bytes_ceiling": forecast["decoded_bytes_bound"],
                              "packed_bytes_ceiling": forecast["stored_packed_bound"]},
                 "conservative": {"http_requests": requests * 4, "stage_and_final_bytes_ceiling":
                                  forecast["stored_packed_bound"] + forecast["stage_peak_serial_bound"] * 4,
                                  "provider_hours_envelope": requests * 4 / per_hour if per_hour else None}}
    blockers = lease["blockers"][:]
    if not horizon:
        blockers.append("MEMBER_EXPIRY_UNVERIFIED")
    if not per_hour:
        blockers.append("OBSERVED_ACCOUNT_QUOTA_UNAVAILABLE")
    if not index.get("full_history_denominator"):
        blockers.append("FULL_HISTORY_REQUEST_STORAGE_AND_END_TO_END_ESTIMATES_UNAVAILABLE")
    resource = {"drive": drive, "account": {"observed_hourly_limit": account.get("api_request_limit"),
                "observed_used": account.get("user_count"), "effective_source_hourly_cap": per_hour,
                "subscription_expiry": horizon}, "backer_preflight_usage": ledger,
                "limited_2026_campaign": limited, "bounded_native_partition": scenarios,
                "bounded_requests_per_mode": forecast["requests_by_mode"], "github_runner": {
                    "platform": "ubuntu-latest", "paid_resource_increase": 0,
                    "end_to_end_minutes": None, "measure_in_stage_and_b10_job_telemetry": True},
                "blockers": blockers, "minimum_external_evidence": MINIMUM if not horizon or not per_hour else [],
                "full_history_denominator": index["full_history_denominator"]}
    fits = drive["available_bytes"] is not None and scenarios["conservative"]["stage_and_final_bytes_ceiling"] <= drive["available_bytes"]
    part_ready = requests > 0 and bool(per_hour and horizon and fits and not lease["blockers"])
    partition = {"dataset_scope": index["scope"], "known_prepared_requests": requests, "storage_bound_fits": fits,
                 "download_data_complete": False, "actual_entitlement_and_period_required": True,
                 "base_state": "RESOURCES_EVIDENCED_AWAITING_ENGINE" if part_ready else "PARTITION_HOLD",
                 "account_blockers": blockers, "prior_archives_need_no_requests": index["reused_historical_archives"],
                 "full_download_release": "HOLD", "production_dispatchable": 0}
    return ("HOLD" if blockers or not fits else "PASS"), {"RESOURCE_BUDGET.json": resource,
                                                           "PARTITION_RESOURCE_CHECK.json": partition}
