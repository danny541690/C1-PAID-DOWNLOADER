"""R12 readiness capacity and quota evidence with no FinMind network calls."""

from collections import Counter
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
import re
from uuid import uuid4

from .cross_run_ledger import DriveJournal
from .drive_pilot import BASE, FOLDER, ensure
from .integration_pilot import verify_private_folder
from .live_preflight import read_json
from .production_download import TERMINAL, storage_bytes, year_budget
from .quota import NORMAL_LIMIT, ROLLING_SECONDS, TOTAL_LIMIT, can_reserve, next_reservation_at, retry_deadline
from .release_batch import MAX_ATTEMPTS, WINDOW_LIMIT


TASK_ID = "R12-FINAL-READINESS-V2-01"
PROBE_MODE = "ISOLATED_READINESS_BUDGET"
TASK_LIMITS = {"download": 48, "probe": 4}
WORKFLOW_GROUP = "r12-1-bounded-preflight"


def probe_state(spec_sha):
    """Initialize an isolated ledger keyed to the task and the approved specification."""
    return {"mode": PROBE_MODE, "task_id": TASK_ID, "spec_sha": spec_sha, "events": [],
            "auth_hold": False, "paused_until": 0}


def reserve_probe(journal, previous, *, kind, request_id, now, run, spec_sha, shared_events, shared_ledger_proven):
    """Return a one-use permit after durable CAS. Charge reservations even on interruption."""
    state = previous["state"]
    if (state.get("mode") != PROBE_MODE or state.get("task_id") != TASK_ID or state.get("spec_sha") != spec_sha or
            not isinstance(spec_sha, str) or not re.fullmatch(r"[a-f0-9]{64}", spec_sha) or
            kind not in TASK_LIMITS or not request_id or not run):
        raise ValueError("UNSCOPED_PROBE_REQUEST")
    own = state["events"]
    existing = next((event for event in own if event["request_id"] == request_id), None)
    if existing is not None:
        if existing["kind"] != kind:
            raise ValueError("PROBE_IDENTITY_CONFLICT")
        return "ALREADY_RESERVED"
    if not shared_ledger_proven:
        return "HOLD_SHARED_LEDGER_UNVERIFIED"
    if state["auth_hold"]:
        return "HOLD_AUTHORIZATION"
    if sum(event["kind"] == kind for event in own) >= TASK_LIMITS[kind]:
        return "TASK_LIMIT"
    # External reservations are deduplicated by request ID, including recovered copies.
    events = {}
    for event in (*shared_events, *own):
        key, payload = event["request_id"], (event["at"], event["kind"])
        if not key or payload[1] not in TASK_LIMITS or events.setdefault(key, payload) != payload:
            raise ValueError("SHARED_QUOTA_EVENT_IDENTITY")
    if not can_reserve(events.values(), now, kind, paused_until=state["paused_until"]):
        return "ACCOUNT_LIMIT_OR_COOLDOWN"
    reserved = {"request_id": request_id, "kind": kind, "at": now, "run": run, "nonce": uuid4().hex}
    return "RESERVED" if journal.append(previous, state | {"events": [*own, reserved]}) else "CAS_CONFLICT"


def record_probe_block(journal, previous, code, now, *, spec_sha, retry_after=None):
    """Record an account-wide pause or a credential hold in the isolated ledger."""
    state = previous["state"]
    if (state.get("mode") != PROBE_MODE or state.get("task_id") != TASK_ID or state.get("spec_sha") != spec_sha or
            not isinstance(spec_sha, str) or not re.fullmatch(r"[a-f0-9]{64}", spec_sha)):
        raise ValueError("UNSCOPED_PROBE_RESPONSE")
    if code == 429:
        updated = state | {"paused_until": max(state["paused_until"], retry_deadline(retry_after, now))}
    elif code in (401, 403):
        updated = state | {"auth_hold": True}
    else:
        raise ValueError("UNEXPECTED_PROBE_STATUS")
    return journal.append(previous, updated)


def task_checkpoint(ctx):
    """Reopen the isolated task-wide ledger across rollout attempts."""
    folder = ensure(ctx.task_root, "ISOLATED_PROBE_CHECKPOINT", FOLDER, ctx.access, create=True)
    verify_private_folder(folder, ctx.access)
    journal = DriveJournal(folder, ctx.access)
    head = journal.head()
    if head is None:
        journal.initialize(probe_state(ctx.spec_sha))
        head = journal.version(0)
    if head is None:
        raise ValueError("PROBE_CHECKPOINT_NOT_VISIBLE")
    state = head["state"]
    counts = Counter(event["kind"] for event in state["events"])
    if (state["mode"] != PROBE_MODE or state["task_id"] != TASK_ID or state["spec_sha"] != ctx.spec_sha
            or any(kind not in TASK_LIMITS or count > TASK_LIMITS[kind] for kind, count in counts.items())
            or len({event["request_id"] for event in state["events"]}) != len(state["events"])):
        raise ValueError("PROBE_CHECKPOINT_IDENTITY")
    return state, {"folder_id": folder, "sequence": head["sequence"], "status": "READ_BACK"}


def observed_drive(access):
    """Report account storage metadata, with no storage or permission mutation."""
    try:
        quota = read_json(f"{BASE}/about?fields=storageQuota(limit%2Cusage%2CusageInDrive%2CusageInDriveTrash)",
                          access)["storageQuota"]
        fields = {name: int(quota[name]) for name in ("limit", "usage", "usageInDrive", "usageInDriveTrash")
                  if name in quota}
        limit, usage = fields["limit"], fields["usage"]
        if limit <= 0 or any(value < 0 for value in fields.values()):
            raise ValueError("DRIVE_QUOTA_FIELDS")
    except (OSError, ValueError, TypeError, KeyError) as error:
        return {"status": "HOLD_DRIVE_QUOTA_UNVERIFIED", "error": type(error).__name__, "available_bytes": None}
    return {"status": "VERIFIED" if usage < limit else "HOLD_DRIVE_CAPACITY_EXHAUSTED",
            "limit_bytes": limit, "used_bytes": usage,
            "used_in_drive_bytes": fields.get("usageInDrive"),
            "used_in_trash_bytes": fields.get("usageInDriveTrash"),
            "available_bytes": max(0, limit - usage)}


def token_workflows(directory=Path(".github/workflows")):
    """Find workflow-level sharing gaps without reading any credential values."""
    registered, gaps = [], []
    for path in sorted(directory.glob("*.y*ml")):
        content = path.read_text(encoding="utf-8")
        if "secrets.FINMIND_BACKER_TOKEN" not in content:
            continue
        registered.append(path.name)
        if not re.search(rf"(?m)^concurrency:\s*\n[ \t]+group:[ \t]+{WORKFLOW_GROUP}[ \t]*$", content):
            gaps.append(path.name)
    return {"same_repository_secret_workflows": registered, "unproven_shared_group": gaps,
            "cross_campaign_global_usage_proven": False,
            "external_token_consumers_proven": False}


def campaign_forecast(plan, supplement, record, available_bytes, coverage):
    budget = year_budget(plan, supplement)
    snapshot = record["state"]
    terminal = sum(snapshot["totals"].get(name, 0) for name in TERMINAL)
    total = budget["windows"]
    cursor = snapshot["cursor"] + snapshot.get("otc_cursor", 0)
    if not 0 <= terminal == cursor <= total:
        raise ValueError("CAMPAIGN_COUNTER_MISMATCH")
    remaining = total - terminal
    # Preserve the campaign's existing byte-bound model. Current pending/recovered receipts may reduce the demand.
    conservative_bytes = storage_bytes(remaining)
    if supplement:
        remaining_base = plan["budget"]["windows"] - snapshot["cursor"]
        remaining_otc = supplement["budget"]["windows"] - snapshot.get("otc_cursor", 0)
        if min(remaining_base, remaining_otc) < 0:
            raise ValueError("CAMPAIGN_CURSOR_OUTSIDE_PLAN")
        conservative_bytes = storage_bytes(remaining_base) + storage_bytes(remaining_otc)
    verified = snapshot["totals"].get("VERIFIED", 0)
    current_bytes = snapshot["bytes"]
    packed_limit = WINDOW_LIMIT + 1024
    final_bound = remaining * packed_limit
    staging_bound = final_bound * MAX_ATTEMPTS
    packed_totals = coverage.get("reused_packed_bytes_by_dataset") or {}
    receipt_counts = coverage.get("reused_receipt_count_by_dataset") or {}
    sample_stats = {}
    for dataset, count in (budget.get("windows_by_dataset") or {}).items():
        observed, packed = receipt_counts.get(dataset, 0), packed_totals.get(dataset)
        if observed > 0 and isinstance(packed, int) and packed > 0:
            projected = ceil(packed * count / observed)
            sample_stats[dataset] = {"sample_receipts": observed, "sample_packed_bytes": packed,
                                     "mean_packed_bytes_per_receipt": packed / observed,
                                     "full_limited_campaign_windows": count,
                                     "illustrative_final_packed_bytes_at_observed_mean": projected}
    return {"scope": "EXISTING_LIMITED_CAMPAIGN_ONLY", "master_sequence": record["sequence"],
            "planned_windows": total, "windows_by_dataset": budget.get("windows_by_dataset"),
            "closed_windows": terminal, "verified_windows": verified, "remaining_plan_windows": remaining,
            "blocked_or_repair_windows": terminal - verified, "closed_source_requests": snapshot["source_requests"],
            "closed_account_requests": snapshot["account_requests"], "verified_packed_bytes": current_bytes,
            "mean_packed_bytes_per_verified_window": current_bytes / verified if verified else None,
            "if_all_remaining_need_one_fetch": remaining,
            "if_all_remaining_use_maximum_source_attempts": remaining * MAX_ATTEMPTS,
            "source_only_hours_at_normal_cap_under_one_fetch_scenario": ceil(remaining / NORMAL_LIMIT),
            "source_only_hours_at_normal_cap_under_max_attempts_scenario": ceil(remaining * MAX_ATTEMPTS / NORMAL_LIMIT),
            "remaining_storage_bound_bytes": conservative_bytes,
            "remaining_capacity_components": {"staging_bytes": staging_bound, "final_bytes": final_bound,
                                               "journal_and_receipt_reserve_bytes":
                                               conservative_bytes - staging_bound - final_bound},
            "packed_samples_by_dataset": sample_stats,
            "remaining_storage_bound_fits_observed_headroom": conservative_bytes <= available_bytes
            if available_bytes is not None else None,
            "throughput_and_runner_time_forecast": None}


def run(ctx):
    """G05 inspects capacity, bounded inventory and safety blockers with zero paid API traffic."""
    now = int(datetime.now(timezone.utc).timestamp())
    drive = observed_drive(ctx.access)
    blockers = ["PROVIDER_SUBSCRIPTION_EXPIRY_EVIDENCE_MISSING", "ACCOUNT_WIDE_ROLLING_LEDGER_UNPROVEN",
                "CURRENT_PROVIDER_ACCOUNT_HEADROOM_UNCONFIRMED", "FULL_HISTORY_COST_DURATION_AND_STORAGE_UNVERIFIED"]
    try:
        coverage = ctx.read("MANIFEST_INDEX.json") or {}
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        coverage = {}
        blockers.append("MANIFEST_INDEX_UNAVAILABLE")
    coverage = coverage.get("payload", coverage)
    full_denom = coverage.get("full_history_denominator")
    try:
        campaign = campaign_forecast(ctx.plan, ctx.supplement, ctx.master, drive["available_bytes"], coverage)
        state = ctx.master["state"]
        recent = [(event["at"], event["kind"]) for event in state["events"] if event["at"] > now - ROLLING_SECONDS]
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
        campaign, state, recent = {"error": type(error).__name__}, {}, []
        blockers.append("LIMITED_CAMPAIGN_INPUT_UNAVAILABLE")
    sharing = token_workflows()
    if state.get("auth_hold"):
        blockers.append("CAMPAIGN_AUTHORIZATION_HOLD")
    if full_denom is None:
        blockers.append("FULL_HISTORY_DENOMINATOR_UNPROVEN")
    if campaign.get("remaining_storage_bound_fits_observed_headroom") is not True:
        blockers.append("LIMITED_CAMPAIGN_REMAINING_STORAGE_UNPROVEN_OR_INSUFFICIENT")
    try:
        saved, checkpoint = task_checkpoint(ctx)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
        saved = probe_state(ctx.spec_sha)
        checkpoint = {"status": "HOLD", "error": type(error).__name__}
        blockers.append("TASK_QUOTA_CHECKPOINT_UNVERIFIED")
    reservations = Counter(event["kind"] for event in saved["events"])
    evidence = saved | {"checkpoint": checkpoint,
                        "reserved_data_requests": reservations["download"],
                        "reserved_account_requests": reservations["probe"],
                        "data_request_limit_including_retries": TASK_LIMITS["download"],
                        "account_request_limit": TASK_LIMITS["probe"],
                        "credential_injected": False,
                        "collection_mode": "DRIVE_AND_EXISTING_EVIDENCE_ONLY",
                        "live_probe_admission": "HOLD_SHARED_LEDGER_UNVERIFIED"}
    report = {"drive_storage": drive, "limited_campaign": campaign, "blocked_on": blockers,
              "provider_entitlement": {"live_account_status": "NOT_PROBED",
                                       "subscription_expiry": None, "proof": "NO_VERIFIED_PROVIDER_EXPIRY"},
              "shared_quota": sharing | {"download_per_rolling_hour": NORMAL_LIMIT,
                                         "total_per_rolling_hour": TOTAL_LIMIT,
                                         "source_retries_count_in_both_limits": True,
                                         "observed_master_snapshot_recent_reservations": dict(Counter(k for _, k in recent)),
                                         "snapshot_does_not_include_other_consumers": True,
                                         "earliest_download_slot_by_saved_master_only":
                                         next_reservation_at(recent, now, "download", paused_until=state.get("pause", 0))
                                         if state else None},
              "full_history": {"denominator": full_denom, "known_candidate_windows": coverage.get("candidate_windows"),
                               "known_reusable_windows": coverage.get("reusable_windows"),
                               "observed_reused_packed_bytes": coverage.get("reused_packed_bytes"),
                               "source_requests_forecast": None, "storage_forecast_bytes": None,
                               "runner_forecast_minutes": None, "forecast_cost": None, "eta": None},
              "actual_data_requests": 0, "actual_account_requests": 0}
    return {"status": "HOLD", "reason": "RESOURCE_EVIDENCE_INCOMPLETE", "blockers": blockers,
            "outputs": {"RESOURCE_BUDGET.json": report, "PROBE_LEDGER.json": evidence}}
