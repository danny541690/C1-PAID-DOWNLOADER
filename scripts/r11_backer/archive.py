"""Plan archival locations and fingerprint unchanged API response bytes."""

from dataclasses import dataclass
from datetime import date
import gzip
import hashlib


@dataclass(frozen=True)
class ArchivePlan:
    anchor: str
    months: tuple[str, ...]


def archive_plan(dates: list[date]) -> ArchivePlan:
    if not dates or any(type(value) is not date for value in dates):
        raise ValueError("archive planning requires validated contract dates")
    months = tuple(sorted({value.isoformat()[:7] for value in dates}))
    month = months[0]
    return ArchivePlan(f"{month[:4]}/FinMind_Backer/{month}", months)


def compress_original(body: bytes) -> tuple[bytes, str, str]:
    compressed = gzip.compress(body, mtime=0)
    return (
        compressed,
        hashlib.sha256(body).hexdigest(),
        hashlib.sha256(compressed).hexdigest(),
    )
