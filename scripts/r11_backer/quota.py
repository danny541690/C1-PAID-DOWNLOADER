"""Rolling quota decision for a single serialized account ledger."""


from email.utils import parsedate_to_datetime

TOTAL_LIMIT = 1600
NORMAL_LIMIT = TOTAL_LIMIT
ROLLING_SECONDS = 3600
HOUR_MS = ROLLING_SECONDS * 1000
REBALANCE_MS = 600_000
QUOTA_BLOCK_CODES = frozenset((402, 429))
AUTH_BLOCK_CODES = frozenset((401, 403))


def provider_block_code(value):
    """FinMind can signal quota or credential errors in either the HTTP or JSON status."""
    if isinstance(value, str) and len(value) == 3 and value.isascii() and value.isdecimal():
        value = int(value)
    return value if type(value) is int and value in QUOTA_BLOCK_CODES | AUTH_BLOCK_CODES else None


def can_reserve(events, now, kind, *, normal_limit=NORMAL_LIMIT, total_limit=TOTAL_LIMIT, paused_until=0):
    """Return whether one more request fits both rolling limits."""
    if now < paused_until:
        return False
    active = [(stamp, purpose) for stamp, purpose in events if stamp > now - ROLLING_SECONDS]
    if len(active) >= total_limit:
        return False
    return kind != "download" or sum(purpose == "download" for _, purpose in active) < normal_limit


def next_reservation_at(events, now, kind, *, normal_limit=NORMAL_LIMIT, total_limit=TOTAL_LIMIT, paused_until=0):
    """Return the earliest slot implied by the saved events and cooldown."""
    active = sorted((stamp, purpose) for stamp, purpose in events if stamp > now - ROLLING_SECONDS)
    due = max(now, paused_until)
    if len(active) >= total_limit:
        due = max(due, active[-total_limit][0] + ROLLING_SECONDS)
    if kind == "download":
        normal = [stamp for stamp, purpose in active if purpose == "download"]
        if len(normal) >= normal_limit:
            due = max(due, normal[-normal_limit] + ROLLING_SECONDS)
    return due


def retry_deadline(retry_after, now, *, fallback_seconds=60):
    """Interpret Retry-After seconds or HTTP-date, with a conservative fallback."""
    value = (str(retry_after) if type(retry_after) is int else retry_after or "")
    value = value.strip() if isinstance(value, str) else ""
    if value.isascii() and value.isdecimal() and len(value) <= 12:
        return now + int(value)
    try:
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is not None:
            return max(now, int(deadline.timestamp()))
    except (ValueError, TypeError, OverflowError):
        pass
    return now + fallback_seconds


def paced_slot(charges, now, limit=TOTAL_LIMIT, *, pause=0, last_start=None):
    """Use the conservative effective send time for each unacknowledged grant."""
    if type(limit) is not int or not 1 <= limit <= TOTAL_LIMIT:
        raise ValueError("INVALID_ACCOUNT_CAP")
    t = max(now, pause)
    if last_start is not None:
        t = max(t, last_start + (HOUR_MS + limit - 1) // limit)
    active = sorted(stamp for stamp in charges if stamp > t - HOUR_MS)
    if len(active) >= limit:
        t = max(t, active[-limit] + HOUR_MS)
    return t


def available_slots(charges, now, limit=TOTAL_LIMIT, *, pause=0, last_start=None, horizon=REBALANCE_MS):
    """Forecast a pacing window without crediting unused capacity from earlier windows."""
    targets, charged = [], list(charges)
    due = now
    while (due := paced_slot(charged, due, limit, pause=pause, last_start=last_start)) < now + horizon:
        targets.append(due)
        charged.append(due)
        last_start = due
    return targets
