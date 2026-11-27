"""Exercise bounded dispatch and recovery against a durable test ledger."""

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

MAX_ATTEMPTS = 4  # Initial download plus three retries.


class Interrupted(Exception):
    """Stop the runner after writing a staged or published file."""


def record(db, task, event, detail=None):
    with db:
        db.execute("INSERT INTO events(task, event, detail) VALUES (?, ?, ?)",
                   (task, event, detail))


def events(db, task, event):
    return [row[0] for row in db.execute(
        "SELECT detail FROM events WHERE task=? AND event=? ORDER BY id", (task, event)
    )]


def matches(path, digest):
    return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == digest


def schedule(db, root, tasks, fetch, checkpoint=lambda *_: None):
    """Run one isolated publisher, preserving evidence across a process restart."""
    with db:
        db.execute("""CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY, task TEXT NOT NULL, event TEXT NOT NULL, detail TEXT)
        """)
    stage, bronze, recovery = (root / name for name in ("stage", "bronze", "recovery"))
    for directory in (stage, bronze, recovery):
        directory.mkdir(exist_ok=True)
    for task, lane, month in tasks:
        if events(db, task, "VERIFIED") or events(db, task, "REPAIR_REQUIRED"):
            continue
        if not events(db, task, "DISPATCH"):
            record(db, task, "DISPATCH", lane)
        target = bronze / month[:4] / "FinMind_Backer" / month / (task + ".raw")
        attempts = events(db, task, "ATTEMPT")
        evidence = dict(json.loads(item) for item in events(db, task, "SOURCE"))
        errors = events(db, task, "ERROR")
        last_attempt = len(attempts)
        if last_attempt and last_attempt not in evidence and str(last_attempt) not in errors:
            record(db, task, "ERROR_DETAIL", json.dumps([last_attempt, "interrupted request"]))
            record(db, task, "ERROR", str(last_attempt))
            errors.append(str(last_attempt))
        for number in range(max(1, last_attempt), MAX_ATTEMPTS + 1):
            if str(number) in errors:
                continue
            if number > last_attempt:
                record(db, task, "ATTEMPT", str(number))
            source = stage / f"{task}.{number}.raw"
            digest = evidence.get(number)
            try:
                if digest is None:
                    if source.exists() or target.exists():
                        raise ValueError("untracked file")
                    original = fetch(task)
                    digest = hashlib.sha256(original).hexdigest()
                    record(db, task, "SOURCE", json.dumps([number, digest]))
                    with source.open("xb") as file:
                        file.write(original)
                        file.flush()
                        os.fsync(file.fileno())
                    checkpoint(task, "staged")
                if not matches(target, digest):
                    if target.exists() or not matches(source, digest):
                        raise ValueError("readback mismatch or incomplete staged upload")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    record(db, task, "PUBLISH_INTENT", str(target.relative_to(root)))
                    os.link(source, target)  # Atomic, refuses to overwrite.
                    checkpoint(task, "published")
                record(db, task, "PUBLISHED", str(target.relative_to(root)))
                if not matches(target, digest):
                    raise ValueError("published readback mismatch")
                if source.exists():
                    record(db, task, "CLEANUP_INTENT", source.name)
                    source.unlink()
                    record(db, task, "CLEANUP_DONE", source.name)
                elif source.name in events(db, task, "CLEANUP_INTENT") and source.name not in events(db, task, "CLEANUP_DONE"):
                    record(db, task, "CLEANUP_DONE", source.name)
                record(db, task, "VERIFIED", digest)
                break
            except (OSError, ValueError) as error:
                record(db, task, "ERROR_DETAIL", json.dumps([number, type(error).__name__]))
                for candidate in (target, source):
                    tracked_target = str(target.relative_to(root)) in events(db, task, "PUBLISH_INTENT")
                    if candidate.exists() and (candidate == source or tracked_target):
                        destination = recovery / f"{task}.{number}.{candidate.parent.name}.{candidate.name}"
                        record(db, task, "QUARANTINE_INTENT", destination.name)
                        candidate.rename(destination)
                        record(db, task, "QUARANTINE_DONE", destination.name)
                if target.exists():
                    record(db, task, "REPAIR_REQUIRED", "untracked target")
                    break
                record(db, task, "ERROR", str(number))
        else:
            record(db, task, "REPAIR_REQUIRED", "retry limit")
    return [task for task, _, _ in tasks if events(db, task, "REPAIR_REQUIRED")]


def self_test():
    tasks = [("old", "B01", "1984-03"), ("bad", "B02", "2026-09"),
             ("corrupt", "B04", "2026-09"), ("later", "B07", "2026-09")]
    calls = []

    def check(condition, message):
        if not condition:
            raise AssertionError(message)

    def fetch(task):
        calls.append(task)
        if task == "bad":
            raise OSError("simulated upstream error")
        return task.encode()

    with TemporaryDirectory() as directory:
        root = Path(directory)
        ledger = root / "master.sqlite"
        def stop(task, state):
            if task == "old" and state == "published":
                raise Interrupted
        try:
            with closing(sqlite3.connect(ledger)) as db:
                schedule(db, root, tasks, fetch, stop)
        except Interrupted:
            pass
        else:
            raise AssertionError("interruption was not exercised")

        def damage(task, state):
            if task == "corrupt" and state == "staged" and calls.count(task) == 1:
                (root / "stage" / "corrupt.1.raw").write_bytes(b"damaged")

        with closing(sqlite3.connect(ledger)) as db:
            repair = schedule(db, root, tasks, fetch, damage)
            check(calls == ["old", "bad", "bad", "bad", "bad", "corrupt", "corrupt", "later"],
                  "unexpected source requests or retry count")
            check(repair == ["bad"], "second-round list includes successful work")
            check((root / "bronze/1984/FinMind_Backer/1984-03/old.raw").read_bytes() == b"old",
                  "old month was not recovered")
            check(len(events(db, "old", "VERIFIED")) == 1, "duplicate success")
            check(bool(events(db, "old", "CLEANUP_DONE")), "cleanup missing")
            check(bool(events(db, "corrupt", "QUARANTINE_DONE")), "damage was not quarantined")
            previous = calls.copy()
            check(schedule(db, root, tasks, fetch) == repair and calls == previous,
                  "terminal jobs were fetched again")
    print("PASS bounded schedule: restart, old month, quarantine, four attempts, continuation, repair-only list")


if __name__ == "__main__":
    self_test()
