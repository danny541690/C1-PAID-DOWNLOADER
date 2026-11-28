"""B00 account quota on the existing private Drive CAS journal.

Records between checkpoints carry compact deltas. A reservation stays charged even if
its worker never sends or its result is unknown. A single fenced ingress opens HTTP.
"""

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from threading import RLock
from time import sleep, time_ns
from uuid import uuid4

from .integration_pilot import digest, encode
from .quota import HOUR_MS, REBALANCE_MS, TOTAL_LIMIT, available_slots, paced_slot, retry_deadline
from .release_rehearsal import RehearsalError, require

FORMAT = "B00_FINMIND_ACCOUNT_QUOTA_V25"
ACCOUNT = digest(b"C1_0.98:FinMind:Backer:FINMIND_BACKER_TOKEN")
TASK = "R12-FINAL-READINESS-V2-01"
TASK_CAPS = {"data": 48, "account": 4}
LANES = frozenset({f"B{n:02}" for n in range(1, 10)} | {"ACCOUNT"})
GRANT_MS = 10_000
LEASE_MS = 15 * 60 * 1000
CHECKPOINT_REVISIONS = 96


class Deferred(RehearsalError):
    def __init__(self, reason, due=None):
        super().__init__(reason)
        self.due = due


def utc_ms():
    return time_ns() // 1_000_000


def initial(now, *, policy_evidence, prior=None, previous_record=None, imported=()):
    baseline = prior or {}
    reserved = {kind: baseline.get(f"reserved_{kind}_requests", 0) for kind in TASK_CAPS}
    actual = {kind: baseline.get(f"actual_{kind}_requests", 0) for kind in TASK_CAPS}
    require(all(type(actual[k]) is int and type(reserved[k]) is int and 0 <= actual[k] <= reserved[k] <= TASK_CAPS[k]
                for k in TASK_CAPS), "B00_PRIOR_TASK_COUNTERS")
    entries = {}
    for entry in imported:
        identity = entry["request_id"]
        recorded = entry["at"] * 1000
        expiry = entry.get("conservative_charge_until_ms", recorded)
        require(recorded <= now and type(expiry) is int and recorded <= expiry, "B00_PRIOR_REQUEST_TIME")
        if expiry > now - HOUR_MS:
            kind = "account" if entry["kind"] == "probe" else "data"
            previous = entries.setdefault(identity, {"id": identity, "planned": recorded, "charge": expiry,
                                                       "kind": kind, "lane": "ACCOUNT" if kind == "account" else "B09",
                                                       "fence": "historical", "known": False})
            require(previous["charge"] == expiry and previous["kind"] == kind, "B00_IMPORTED_ATTEMPT_CONFLICT")
    return {"policy_evidence": policy_evidence, "prior_checkpoint": previous_record, "time_ms": now,
            "generation": 0, "lease": None, "limit": TOTAL_LIMIT, "provider_cap": None,
            "paused": baseline.get("pause_until", 0) * 1000, "auth_hold": baseline.get("auth_hold", False),
            "denied": [], "events": sorted(entries.values(), key=lambda e: e["charge"]),
            "last_start": max((e["charge"] for e in entries.values()), default=None), "recovery": False,
            "errors": 0, "clean": 0, "last_rate_change": now, "rebalance": None,
            "preflight": {"id": TASK, "reserved": reserved, "actual": actual, "unsent": {k: 0 for k in TASK_CAPS},
                          "attempts": {}}}


def checkpoint(state):
    return {"format": FORMAT, "account": ACCOUNT, "type": "checkpoint", "book": state}


def expire_active(state, now):
    state["events"] = [event for event in state["events"] if event["charge"] > now - HOUR_MS]


def apply(state, change):
    """Replay the same mutation for a fresh process and for the originating writer."""
    book = deepcopy(state)
    now, action = change["now"], change["action"]
    require(type(now) is int and now >= state["time_ms"], "B00_CLOCK_REGRESSION")
    book["time_ms"] = now
    expire_active(book, now)
    if action == "lease":
        book["lease"] = change["lease"]
        book["generation"] = change["generation"]
    elif action == "reserve":
        event = change["event"]
        book["events"].append(event)
        book["last_start"] = event["charge"]
        if event["preflight"]:
            budget = book["preflight"]
            budget["reserved"][event["kind"]] += 1
            budget["attempts"][event["id"]] = {"kind": event["kind"], "status": "UNKNOWN"}
    elif action == "result":
        event = next(entry for entry in book["events"] if entry["id"] == change["id"])
        started = change["started"]
        if started is not None:
            # The http ingress captures its start. Retaining the later bound is safer
            # than inferring exactly when the operating system sent the packet.
            require(event["planned"] <= started <= event["charge"], "B00_RESULT_START_OUTSIDE_GRANT")
            event["known"] = True
            event["started"] = started
            if event is book["events"][-1]:
                book["last_start"] = started
        event["status"] = change["status"]
        if event["preflight"]:
            budget = book["preflight"]
            budget["attempts"][event["id"]]["status"] = change["status"]
            if started is not None:
                budget["actual"][event["kind"]] += 1
            elif change["status"] == "NOT_SENT":
                budget["unsent"][event["kind"]] += 1
        for key, value in change["policy"].items():
            book[key] = value
    elif action == "rebalance":
        book["rebalance"] = change["report"]
    else:
        raise ValueError("B00_DELTA_TYPE")
    return book


def open_book(journal):
    latest = journal.head()
    require(latest is not None, "B00_MISSING_CHECKPOINT")
    state = latest["state"]
    require(state.get("format") == FORMAT and state.get("account") == ACCOUNT, "B00_ACCOUNT_BINDING")
    if state["type"] == "checkpoint":
        return latest, state["book"], None
    root = state["base"]
    current = journal.load(root["id"], root["sequence"])
    require(current and digest(encode(current)) == root["sha256"] and current["state"]["type"] == "checkpoint" and
            current["state"]["account"] == ACCOUNT, "B00_DELTA_BASE_BINDING")
    book = current["state"]["book"]
    require(0 < latest["sequence"] - current["sequence"] <= CHECKPOINT_REVISIONS, "B00_REPLAY_LIMIT")
    while current["sequence"] < latest["sequence"]:
        following = journal.load(current["next_id"], current["sequence"] + 1)
        require(following and following["previous"] == digest(encode(current)) and
                following["state"].get("base") == root and following["state"].get("account") == ACCOUNT,
                "B00_DELTA_CHAIN")
        book = apply(book, following["state"]["change"])
        current = following
    require(current == latest, "B00_READBACK_CHANGED")
    return latest, book, root


def lease_change(book, owner, now, *, release=False):
    lease = book["lease"]
    if release:
        require(lease and lease["owner"] == owner, "B00_STALE_WRITER")
        return {"action": "lease", "now": now, "lease": None, "generation": book["generation"]}
    if lease and lease["expires"] > now and lease["owner"] != owner:
        raise Deferred("B00_WRITER_ACTIVE", lease["expires"])
    generation = book["generation"] + 1
    replacement = {"owner": owner, "generation": generation,
                   "epoch": uuid4().hex, "expires": now + LEASE_MS}
    return {"action": "lease", "now": now, "lease": replacement, "generation": generation}


def response_policy(book, now, *, kind, scope, code, retry_after, transient, observed=None):
    if code in (402, 429):
        # FinMind quota replies without Retry-After get a full rolling hour.
        until = retry_deadline(retry_after, (now + 999) // 1000, fallback_seconds=3600) * 1000
        return {"paused": max(book["paused"], until, now + 2250), "limit": max(1, book["limit"] * 3 // 4),
                "last_rate_change": now, "clean": 0, "recovery": True, "errors": 0}
    if code == 401 or code == 403 and kind == "account":
        return {"auth_hold": True, "clean": 0}
    if code == 403:
        return {"denied": sorted(set(book["denied"]) | {scope})}
    if transient:
        count = book["errors"] + 1
        cool = {"paused": max(book["paused"], now + 60_000), "errors": 0} if count >= 3 else {"errors": count}
        return cool | {"clean": 0}
    if code is not None and code != 200:
        return {}
    clean = book["clean"] + 1
    cap = book["provider_cap"] or TOTAL_LIMIT
    result = {"clean": clean, "errors": 0, "recovery": False}
    if observed and type(observed.get("api_request_limit")) is int and observed["api_request_limit"] > 0:
        cap = min(TOTAL_LIMIT, observed["api_request_limit"])
        result |= {"provider_cap": cap, "limit": min(cap, book["limit"])}
        used = observed.get("user_count")
        if type(used) is int and used >= cap:
            result["paused"] = max(book["paused"], now + HOUR_MS)
    if clean >= 8 and now - book["last_rate_change"] >= REBALANCE_MS and book["limit"] < cap:
        result |= {"limit": min(cap, book["limit"] + 100), "last_rate_change": now, "clean": 0}
    return result


def rebalance(book, now, pending):
    active = [event for event in book["events"] if event["charge"] > now - HOUR_MS]
    available = available_slots([e["charge"] for e in active], now, book["limit"], pause=book["paused"],
                                last_start=book["last_start"]) if not book["auth_hold"] else []
    waiting = {lane: dict(row) for lane, row in pending.items() if lane in LANES and row["count"] > 0}
    allocation = Counter()
    for _ in available:
        ready = [lane for lane in waiting if allocation[lane] < waiting[lane]["count"]]
        if not ready:
            break
        def score(lane):
            row = waiting[lane]
            weighted = (max(1, row.get("priority", 1)) + min(10, row.get("wait_ms", 0) / REBALANCE_MS)) * (
                1 + min(row["count"], 32) / 32)
            return weighted / (1 + allocation[lane] + row.get("recent_completed", 0) / 8)
        best = max(sorted(ready), key=score)
        allocation[best] += 1
    claimed = Counter(e["lane"] for e in active)
    return {"as_of_ms": now, "next_rebalance_by_ms": now + REBALANCE_MS, "eligible": waiting,
            "rolling_reserved": len(active), "unknown_or_unsent": sum(not e["known"] for e in active),
            "in_flight_or_uncertain": sum(e.get("status") is None for e in active),
            "rolling_remaining": max(0, book["limit"] - len(active)), "remaining_slots_10m": len(available),
            "next_slot_ms": available[0] if available else None, "cooldown_until_ms": book["paused"],
            "limit": book["limit"], "lane_allocations": dict(allocation), "lane_reservations": dict(claimed)}


@dataclass(frozen=True)
class Grant:
    identifier: str
    planned: int
    deadline: int
    epoch: str


class AccountBroker:
    def __init__(self, journal, owner, clock=utc_ms):
        self.journal, self.owner, self.clock = journal, owner, clock
        self.record, self.book, self.base = open_book(journal)
        self.lock = RLock()
        self.local_start = None
        self.used_grants = set()
        self.closed = False

    def _append(self, change):
        previous = self.record
        book = apply(self.book, change)
        use_checkpoint = self.base is None or previous["sequence"] - self.base["sequence"] + 1 >= CHECKPOINT_REVISIONS
        state = checkpoint(book) if use_checkpoint else {"format": FORMAT, "account": ACCOUNT, "type": "delta",
                                                         "base": self.base, "change": change}
        # The previous reserved successor ID is the CAS key, including across processes.
        if not self.journal.append(previous, state):
            self.closed = True
            raise Deferred("B00_LOST_CAS")
        actual = self.journal.load(previous["next_id"], previous["sequence"] + 1)
        if not actual or actual["state"] != state or actual["previous"] != digest(encode(previous)):
            self.closed = True
            raise Deferred("B00_DURABLE_READBACK_UNKNOWN")
        self.record, self.book = actual, book
        if use_checkpoint:
            self.base = {"id": previous["next_id"], "sequence": actual["sequence"], "sha256": digest(encode(actual))}

    def acquire(self):
        with self.lock:
            self._append(lease_change(self.book, self.owner, self.clock()))
            return self.book["lease"]

    def assert_writer(self, now, margin=0):
        lease = self.book["lease"]
        if self.closed or now < self.book["time_ms"]:
            raise Deferred("B00_WRITER_OR_CLOCK_HOLD")
        if not lease or lease["owner"] != self.owner or now + margin >= lease["expires"]:
            raise Deferred("B00_LEASE_EXPIRED_OR_REPLACED")
        return lease

    def close(self):
        with self.lock:
            if not self.closed:
                now = self.clock()
                self.assert_writer(now)
                self._append(lease_change(self.book, self.owner, now, release=True))
                self.closed = True

    def reserve(self, identifier, kind, lane, scope, *, preflight):
        require(kind in TASK_CAPS and lane in LANES and bool(identifier) and bool(scope), "B00_ATTEMPT_BINDING")
        with self.lock:
            now = self.clock()
            lease = self.assert_writer(now, GRANT_MS)
            if self.book["auth_hold"] or scope in self.book["denied"]:
                raise Deferred("B00_PROVIDER_AUTHORIZATION_HOLD")
            budget = self.book["preflight"]
            if identifier in budget["attempts"] or any(e["id"] == identifier for e in self.book["events"]):
                raise Deferred("B00_ATTEMPT_ALREADY_CHARGED")
            if preflight and budget["reserved"][kind] >= TASK_CAPS[kind]:
                raise Deferred("B00_TASK_CAP_EXHAUSTED")
            marks = [e["charge"] for e in self.book["events"]]
            prior = self.local_start if self.local_start is not None else self.book["last_start"]
            due = paced_slot(marks, now, self.book["limit"], pause=self.book["paused"], last_start=prior)
            if due != now:
                raise Deferred("B00_NEXT_PACED_SLOT", due)
            event = {"id": identifier, "planned": now, "charge": now + GRANT_MS, "kind": kind, "lane": lane,
                     "fence": lease["epoch"], "scope": scope, "preflight": preflight, "known": False}
            self._append({"action": "reserve", "now": now, "event": event})
            return Grant(identifier, now, event["charge"], lease["epoch"])

    def begin(self, grant):
        """Only the writer that persisted the grant can start its one HTTP attempt."""
        with self.lock:
            now = self.clock()
            lease = self.assert_writer(now)
            if grant.identifier in self.used_grants or grant.epoch != lease["epoch"] or now > grant.deadline:
                raise Deferred("B00_GRANT_EXPIRED_OR_USED")
            if self.book["paused"] > now or self.book["auth_hold"]:
                raise Deferred("B00_ACCOUNT_PAUSE_AFTER_RESERVE", self.book["paused"])
            require(any(e["id"] == grant.identifier and e["fence"] == grant.epoch
                        for e in self.book["events"]), "B00_RESERVATION_READBACK")
            self.used_grants.add(grant.identifier)
            self.local_start = max(self.local_start or 0, now)
            return now

    def result(self, grant, started, *, status, code=None, retry_after=None, transient=False, observed=None):
        with self.lock:
            now = self.clock()
            lease = self.assert_writer(now)
            event = next((e for e in self.book["events"] if e["id"] == grant.identifier), None)
            require(event and event["fence"] == lease["epoch"] and "status" not in event, "B00_RESULT_IDENTITY")
            policy = response_policy(self.book, now, kind=event["kind"], scope=event["scope"], code=code,
                                     retry_after=retry_after, transient=transient, observed=observed) if started is not None else {}
            self._append({"action": "result", "now": now, "id": grant.identifier, "started": started,
                          "status": status, "policy": policy})

    def plan(self, pending):
        with self.lock:
            now = self.clock()
            self.assert_writer(now)
            report = rebalance(self.book, now, pending)
            self._append({"action": "rebalance", "now": now, "report": report})
            return report

    def wait_reserve(self, identifier, kind, lane, scope, *, preflight, wait_limit=15_000):
        cutoff = self.clock() + wait_limit
        while True:
            try:
                # Hold the same local lock until the transport has consumed the grant;
                # subsequent workers may then run HTTP concurrently under the pace guard.
                with self.lock:
                    grant = self.reserve(identifier, kind, lane, scope, preflight=preflight)
                    try:
                        return grant, self.begin(grant)
                    except Deferred:
                        self.result(grant, None, status="NOT_SENT")
                        raise
            except Deferred as exc:
                if str(exc) != "B00_NEXT_PACED_SLOT" or exc.due > cutoff:
                    raise
                sleep(max(0.005, (exc.due - self.clock()) / 1000))
