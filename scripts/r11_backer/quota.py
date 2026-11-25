"""Rolling quota decision for a single serialized account ledger."""


def can_reserve(events, now, kind, *, normal_limit=1200, total_limit=1600):
    """Return whether one more request fits both rolling limits."""
    active = [(stamp, purpose) for stamp, purpose in events if stamp > now - 3600]
    if len(active) >= total_limit:
        return False
    return kind != "download" or sum(purpose == "download" for _, purpose in active) < normal_limit
