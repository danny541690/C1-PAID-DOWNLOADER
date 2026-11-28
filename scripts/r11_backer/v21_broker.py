"""One persistent quota broker for the V2.1 readiness task."""

from collections import Counter
import re
from http.client import IncompleteRead
from time import time
from urllib.error import HTTPError
from uuid import uuid4

from .integration_pilot import digest, encode
from .drive_retry import transient
from .live_preflight import read_json
from .quota import (
    AUTH_BLOCK_CODES, NORMAL_LIMIT, QUOTA_BLOCK_CODES, TOTAL_LIMIT, can_reserve,
    provider_block_code, retry_deadline,
)
from .release_readiness import ACCOUNT


MODE = "ISOLATED_V21_ACCOUNT_BROKER"
TASK_ID = "R12-FINAL-READINESS-V2-01"
LIMITS = {"download": 48, "probe": 4}
HEADROOM_RESERVE = 1
SNAPSHOT_VALID_SECONDS = 150


def sha(value):
    return isinstance(value, str) and bool(re.fullmatch("[0-9a-f]{64}", value))


def _counts(events):
    tracked = {}
    for event in events:
        key, value = event["request_id"], (event["at"], event["kind"])
        if (not isinstance(key, str) or not key or type(value[0]) is not int or value[0] < 0 or
                value[1] not in LIMITS or tracked.setdefault(key, value) != value):
            raise ValueError("BROKER_EVENT_IDENTITY")
    return tracked


def new_state(spec_sha, previous_ledger, previous_final):
    """Verify inherited usage against the earlier reported digest before carrying it forward."""
    previous_hash = digest(encode(previous_ledger))
    prior_spec_sha = previous_final["spec_sha256"]
    if (not sha(spec_sha) or not sha(prior_spec_sha) or spec_sha == prior_spec_sha or
            previous_final["task_id"] != TASK_ID or previous_ledger["task_id"] != TASK_ID or
            previous_ledger["spec_sha"] != prior_spec_sha or
            previous_final["gates"]["G05"]["output_sha256"]["PROBE_LEDGER.json"] != previous_hash):
        raise ValueError("PRIOR_BROKER_EVIDENCE")
    events = previous_ledger["events"]
    tracked = _counts(events)
    totals = Counter(kind for _, kind in tracked.values())
    old_usage = previous_final["finmind_usage"]
    if len(tracked) != len(events):
        raise ValueError("PRIOR_BROKER_DUPLICATE_RESERVATION")
    for kind, label in (("download", "data"), ("probe", "account")):
        actual, reserved = old_usage[label], previous_ledger[f"reserved_{label}_requests"]
        if (type(actual) is not int or type(reserved) is not int or
                not 0 <= actual <= reserved == totals[kind] <= LIMITS[kind]):
            raise ValueError("PRIOR_BROKER_USAGE_UNVERIFIED")
    return {"mode": MODE, "task_id": TASK_ID, "spec_sha": spec_sha, "prior_spec_sha": prior_spec_sha,
            "prior_evidence_sha256": previous_hash, "prior_actual": {k: old_usage[k] for k in ("data", "account")},
            "events": events, "results": {}, "paused_until": previous_ledger["paused_until"],
            "auth_hold": previous_ledger["auth_hold"]}


def validate_checkpoint(state, genesis):
    fields = ("mode", "task_id", "spec_sha", "prior_spec_sha", "prior_evidence_sha256", "prior_actual")
    if any(state[name] != genesis[name] for name in fields):
        raise ValueError("BROKER_CHECKPOINT_BINDING")
    events = state["events"]
    tracked = _counts(events)
    counts = Counter(event["kind"] for event in events)
    if (len(tracked) != len(events) or events[:len(genesis["events"])] != genesis["events"] or
            not state["results"].keys() <= (set(tracked) - _counts(genesis["events"]).keys()) or
            any(counts[kind] > limit for kind, limit in LIMITS.items())):
        raise ValueError("BROKER_CHECKPOINT_USAGE")


def acquire_lease(journal, previous, *, run, attempt, now, expires_at, spec_sha, evidence_sha):
    """Acquire a time-bounded permit in the task's existing durable quota ledger."""
    state = previous["state"]
    if (state.get("mode") != MODE or not sha(spec_sha) or not sha(evidence_sha) or
            type(expires_at) is not int or expires_at < now + 30):
        raise ValueError("INVALID_ACCOUNT_LEASE")
    owner = {"run_id": run, "attempt": attempt, "spec_sha": spec_sha, "evidence_sha256": evidence_sha}
    current = state.get("active_lease")
    if current and current["expires_at"] > now:
        return current if all(current.get(k) == v for k, v in owner.items()) else None
    identifier = digest(encode(owner | {"began_at": now, "expires_at": expires_at}))
    granted = owner | {"lease_id": identifier, "began_at": now, "expires_at": expires_at}
    if not journal.append(previous, state | {"active_lease": granted}):
        return None
    observed = journal.head()
    return granted if observed and observed["state"].get("active_lease") == granted else None


def reserve(journal, previous, *, kind, request_id, now, run, admission, bootstrap=False):
    """Charge one one-use request before sending it under a verified account-wide lease."""
    state = previous["state"]
    if state.get("mode") != MODE or state.get("task_id") != TASK_ID or kind not in LIMITS or not request_id:
        raise ValueError("BROKER_REQUEST_BINDING")
    owned = state["events"]
    own = _counts(owned)
    if request_id in own:
        if own[request_id][1] != kind:
            raise ValueError("BROKER_REQUEST_IDENTITY_CONFLICT")
        return "ALREADY_RESERVED"
    if state["auth_hold"]:
        return "HOLD_AUTHORIZATION"
    if sum(event["kind"] == kind for event in owned) >= LIMITS[kind]:
        return "TASK_LIMIT"
    if now < state["paused_until"]:
        return "ACCOUNT_COOLDOWN"
    lease_id = admission.get("lease_id")
    lease = state.get("active_lease", {})
    if lease_id is not None or bootstrap:
        if (not lease_id or lease.get("lease_id") != lease_id or lease.get("run_id") != run or
                lease.get("expires_at", 0) < now + 25 or
                lease.get("evidence_sha256") != admission.get("account_ownership_evidence_sha256")):
            return "HOLD_DURABLE_ACCOUNT_LEASE_REQUIRED"
        active_spec = lease["spec_sha"]
    else:
        active_spec = state["spec_sha"]
    if (admission.get("task_id") != TASK_ID or admission.get("spec_sha") != active_spec or
            not sha(admission.get("account_ownership_evidence_sha256")) or
            admission.get("all_consumers_in_broker") is not True or
            admission.get("exclusive_account_lease_read_back") is not True):
        return "HOLD_ACCOUNT_WIDE_OWNERSHIP_UNVERIFIED"
    provider = admission.get("bootstrap_basis" if bootstrap else "provider_snapshot", {})
    at, limit, used = provider.get("observed_at"), provider.get("api_request_limit"), provider.get("user_count")
    # For the first user_info call the numbers are independently evidenced bounds.
    # They never become the provider snapshot used by data permits.
    if bootstrap and (kind != "probe" or provider.get("kind") != "VERIFIED_ROLLING_WINDOW_BOUND" or
                      provider.get("exclusive_since", now) > now - 3600):
        return "HOLD_FIRST_PROBE_BASIS_UNVERIFIED"
    if (type(at) is not int or not 0 <= now - at <= SNAPSHOT_VALID_SECONDS or
            type(limit) is not int or type(used) is not int or not 0 <= used <= limit or
            not sha(provider.get("evidence_sha256"))):
        return "HOLD_PROVIDER_HEADROOM_UNVERIFIED"
    events = _counts([*admission["shared_events"], *owned])
    if sum(stamp >= at for stamp, _ in events.values()) + HEADROOM_RESERVE >= limit - used:
        return "PROVIDER_HEADROOM"
    total = min(TOTAL_LIMIT, limit - HEADROOM_RESERVE)
    normal = min(NORMAL_LIMIT, total)
    if total <= 0 or not can_reserve(events.values(), now, kind, normal_limit=normal, total_limit=total,
                                     paused_until=state["paused_until"]):
        return "ACCOUNT_LIMIT"
    event = {"request_id": request_id, "kind": kind, "at": now, "run": run, "nonce": uuid4().hex,
             **({"lease_id": lease_id, "bootstrap": bootstrap} if lease_id else {})}
    return "RESERVED" if journal.append(previous, state | {"events": [*owned, event]}) else "CAS_CONFLICT"


def provider_block(state, code, now, retry_after=None):
    code = provider_block_code(code)
    if code in QUOTA_BLOCK_CODES:
        return state | {"paused_until": max(state["paused_until"], retry_deadline(retry_after, now))}
    if code in AUTH_BLOCK_CODES:
        return state | {"auth_hold": True}
    raise ValueError("UNEXPECTED_PROVIDER_BLOCK")


def commit_result(journal, confirmed, request_id, result, *, code=None, retry_after=None):
    """Record one provider outcome and share any cooled-down or denied status across callers."""
    state = confirmed["state"]
    if code in QUOTA_BLOCK_CODES | AUTH_BLOCK_CODES:
        state = provider_block(state, code, result["observed_at"], retry_after)
        result = result | {"paused_until": state["paused_until"]}
    if not journal.append(confirmed, state | {"results": state["results"] | {request_id: result}}):
        return result | {"status": "HOLD_PROBE_RESULT_WRITE_UNCERTAIN"}
    return result


def account_probe(journal, previous, *, request_id, now, run, admission, token, bootstrap=False):
    """Send only user_info after durable reservation. A future bounded attempt needs a new ID."""
    if not token:
        return {"status": "HOLD_ACCOUNT_CREDENTIAL_UNAVAILABLE", "actual_requests": 0}
    decision = reserve(journal, previous, kind="probe", request_id=request_id, now=now, run=run, admission=admission,
                       bootstrap=bootstrap)
    if decision != "RESERVED":
        return {"status": decision, "actual_requests": 0}
    confirmed = journal.head()
    if (confirmed is None or confirmed["sequence"] != previous["sequence"] + 1 or
            confirmed["state"]["events"][-1]["request_id"] != request_id):
        return {"status": "HOLD_PROBE_RESERVATION_READBACK", "actual_requests": 0}
    code, retry_after = None, None
    try:
        payload = read_json(ACCOUNT, token)
        code = provider_block_code(payload.get("status")) if isinstance(payload, dict) else None
        retry_after = payload.get("retry_after") if isinstance(payload, dict) else None
        limit = payload.get("api_request_limit") if isinstance(payload, dict) else None
        used = payload.get("user_count") if isinstance(payload, dict) else None
        if code:
            report = {"status": "PROVIDER_BLOCK", "status_code": code}
        elif type(limit) is int and type(used) is int and limit > 0 and 0 <= used <= limit:
            report = {"status": "PROVIDER_LIMIT_OBSERVED", "api_request_limit": limit, "user_count": used}
        else:
            report = {"status": "HOLD_PROVIDER_PAYLOAD"}
    except HTTPError as error:
        code = provider_block_code(error.code)
        retry_after = error.headers.get("Retry-After") if error.headers else None
        report = {"status": "PROVIDER_BLOCK" if code else "HOLD_PROVIDER_HTTP", "status_code": error.code}
    except (OSError, ValueError, IncompleteRead) as error:
        report = {"status": "HOLD_PROVIDER_RESPONSE", "error_class": type(error).__name__,
                  "transient": transient(error)}
    received_at = max(now, int(time()))
    result = report | {"actual_requests": 1, "observed_at": received_at}
    return commit_result(journal, confirmed, request_id, result, code=code, retry_after=retry_after)
