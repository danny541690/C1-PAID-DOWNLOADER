"""Rolling quota decision for a single serialized account ledger."""


from email.utils import parsedate_to_datetime


def can_reserve(events, now, kind, *, normal_limit=1200, total_limit=1600, paused_until=0):
    """Return whether one more request fits both rolling limits."""
    if now < paused_until:
        return False
    active = [(stamp, purpose) for stamp, purpose in events if stamp > now - 3600]
    if len(active) >= total_limit:
        return False
    return kind != "download" or sum(purpose == "download" for _, purpose in active) < normal_limit

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
