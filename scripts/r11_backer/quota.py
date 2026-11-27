"""Rolling quota decision for a single serialized account ledger."""


from email.utils import parsedate_to_datetime

NORMAL_LIMIT = 1200
TOTAL_LIMIT = 1600
ROLLING_SECONDS = 3600


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


def retry_deadline(retry_after, now):
    """Interpret Retry-After seconds or HTTP-date, with a conservative fallback."""
    value = (retry_after or "").strip()
    if value.isascii() and value.isdecimal() and len(value) <= 12:
        return now + int(value)
    try:
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is not None:
            return max(now, int(deadline.timestamp()))
    except (ValueError, TypeError, OverflowError):
        pass
    return now + 60
