"""Build deterministic, newest-first assignments from verified coverage windows."""

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import sqlite3


@dataclass(frozen=True)
class Window:
    dataset: str
    scope: str
    start: date
    end: date
    contract: str
    lane: str

    @property
    def task_id(self):
        identity = (
            self.dataset, self.scope, self.start.isoformat(),
            self.end.isoformat(), self.contract,
        )
        encoded = json.dumps(identity, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def assignments(windows, latest_published):
    unique = {}
    for window in windows:
        if (
            window.start > window.end
            or window.end > latest_published[window.dataset]
            or window.lane not in {f"B{number:02}" for number in range(1, 9)}
            or not all((window.dataset, window.scope, window.contract))
        ):
            raise ValueError("invalid or unpublished coverage window")
        previous = unique.setdefault(window.task_id, window)
        if previous != window:
            raise ValueError("one partition is assigned to multiple lanes")
    return sorted(
        unique.values(),
        key=lambda item: (-item.end.toordinal(), -item.start.toordinal(), item.task_id),
    )


class LocalLedger:
    """Single-host reference ledger for testing replay and lease fencing."""

    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS assignments (
                id TEXT PRIMARY KEY,
                lane TEXT NOT NULL,
                first_day TEXT NOT NULL,
                last_day TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                deadline INTEGER,
                receipt TEXT
            )"""
        )

    def close(self):
        self.db.close()

    def seed(self, windows):
        with self.db:
            for window in windows:
                self.db.execute(
                    "INSERT INTO assignments(id, lane, first_day, last_day) "
                    "VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING",
                    (window.task_id, window.lane, window.start.isoformat(),
                     window.end.isoformat()),
                )
                stored = self.db.execute(
                    "SELECT lane FROM assignments WHERE id = ?", (window.task_id,)
                ).fetchone()
                if stored != (window.lane,):
                    raise ValueError("existing partition belongs to another lane")

    def claim(self, lane, now, lease_seconds):
        if lease_seconds <= 0:
            raise ValueError("lease duration must be positive")
        with self.db:
            return self.db.execute(
                """UPDATE assignments SET generation = generation + 1, deadline = ?
                   WHERE id = (
                     SELECT id FROM assignments
                     WHERE lane = ? AND receipt IS NULL
                       AND (deadline IS NULL OR deadline <= ?)
                     ORDER BY last_day DESC, first_day DESC, id LIMIT 1
                   )
                   RETURNING id, generation""",
                (now + lease_seconds, lane, now),
            ).fetchone()

    def accept(self, task_id, generation, now, receipt):
        if not isinstance(receipt, str) or not receipt.strip():
            raise ValueError("a verified receipt reference is required")
        with self.db:
            return self.db.execute(
                """UPDATE assignments SET receipt = ?
                   WHERE id = ? AND generation = ? AND deadline > ?
                     AND receipt IS NULL""",
                (receipt, task_id, generation, now),
            ).rowcount == 1
