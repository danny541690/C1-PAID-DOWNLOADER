"""Exercise atomic private Drive ledger commits across independent Actions runs."""

import argparse
from copy import deepcopy
from itertools import count
import json
import os
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.parse import urlencode

from .drive_pilot import BASE, ensure, find, token
from .integration_pilot import digest, encode
from .live_preflight import read_json
from .live_recovery_canary import TASKS, read_json_file, write_json
from .quota import can_reserve, retry_deadline
from .release_probes import RECOVERY_ROOT, read_private_file, recovery_location
from .release_rehearsal import RehearsalError, connected_manifest, read_original, require, root_folder
from .test_archive import BRANCH, GitHub

PREFIX = "R12_CROSS_RUN_LEDGER"
WORKFLOW = "r12-1-production-preflight.yml"
MANIFEST = "TASK_MANIFEST.json"
LEASE_SECONDS = 30


def until(operation, failure):
    cutoff = monotonic() + 150
    while True:
        result = operation()
        if result is not None:
            return result
        require(monotonic() < cutoff, failure)
        sleep(3)


class DriveJournal:
    """Each committed version reserves the single Drive file ID of its successor."""

    def __init__(self, parent, access):
        self.parent = parent
        self.access = access

    @staticmethod
    def name(sequence):
        return f"master-{sequence:06d}.json"

    def new_id(self):
        return self.new_ids(1)[0]

    def new_ids(self, count):
        require(0 < count <= 1000, "DRIVE_ID_COUNT")
        result = read_json(f"{BASE}/files/generateIds?count={count}&space=drive", self.access)
        ids = result["ids"]
        require(len(ids) == count and len(set(ids)) == count, "DRIVE_IDS")
        return ids

    def load(self, file_id, sequence):
        fields = "id,name,parents,shared,trashed,mimeType"
        try:
            meta = read_json(f"{BASE}/files/{file_id}?fields={fields}", self.access)
        except HTTPError as error:
            if error.code == 404:
                return None
            raise
        require(meta == {"id": file_id, "name": self.name(sequence), "parents": [self.parent],
                         "shared": False, "trashed": False, "mimeType": "application/json"},
                "JOURNAL_FILE_IDENTITY")
        value = json.loads(read_private_file(file_id, self.access))
        require(value["sequence"] == sequence, "JOURNAL_SEQUENCE")
        return value

    def initialize(self, state):
        name = self.name(0)
        existing = find(self.parent, name, self.access)
        if existing is None:
            write_json(self.parent, name, {"sequence": 0, "previous": None, "next_id": self.new_id(),
                                           "state": state}, self.access)
        require(self.version(0)["state"] == state, "GENESIS_STATE_MISMATCH")

    def history(self):
        """Yield each committed revision after validating its predecessor hash."""
        entry = find(self.parent, self.name(0), self.access)
        if entry is None:
            return
        current = self.load(entry["id"], 0)
        require(current is not None and current["previous"] is None, "JOURNAL_GENESIS")
        yield current
        for number in count(1):
            following = self.load(current["next_id"], number)
            if following is None:
                return
            require(following["previous"] == digest(encode(current)), "JOURNAL_PREDECESSOR")
            current = following
            yield current

    def version(self, sequence=None):
        """Read a specific revision, or the latest committed revision."""
        current = None
        for current in self.history():
            if sequence == current["sequence"]:
                return current
        return current if sequence is None else None

    def append(self, previous, state):
        sequence = previous["sequence"] + 1
        fixed = previous["next_id"]
        body = {"sequence": sequence, "previous": digest(encode(previous)), "next_id": self.new_id(),
                "state": state}
        # Both contenders POST to the same reserved ID. An existing object is read after a 409.
        ensure(self.parent, self.name(sequence), "application/json", self.access, encode(body),
               create=True, file_id=fixed, lookup=False, read_winner=True)
        observed = until(lambda: self.load(fixed, sequence), "JOURNAL_WRITE_READBACK")
        require(observed["previous"] == body["previous"], "JOURNAL_WRITE_PREDECESSOR")
        return observed["state"] == state

    def head(self):
        """Open the latest private journal version without replaying old batches."""
        query = urlencode({"q": f"'{self.parent}' in parents and trashed=false and name contains 'master-'",
                           "orderBy": "name desc", "pageSize": 2, "fields": "incompleteSearch,files(id,name)"})
        listing = read_json(f"{BASE}/files?{query}", self.access)
        require(not listing.get("incompleteSearch"), "JOURNAL_LIST_INCOMPLETE")
        if not listing["files"]:
            return None
        current, *predecessors = listing["files"]
        number = int(current["name"].removeprefix("master-").removesuffix(".json"))
        record = self.load(current["id"], number)
        require(record is not None, "JOURNAL_HEAD_MISSING")
        if number:
            require(len(predecessors) == 1, "JOURNAL_PREDECESSOR_MISSING")
            previous = self.load(predecessors[0]["id"], number - 1)
            require(previous and previous["next_id"] == current["id"] and
                    record["previous"] == digest(encode(previous)), "JOURNAL_PREDECESSOR")
        else:
            require(not predecessors and record["previous"] is None, "JOURNAL_GENESIS")
        # A newly committed successor can be visible by ID before it appears in a list result.
        while following := self.load(record["next_id"], record["sequence"] + 1):
            require(following["previous"] == digest(encode(record)), "JOURNAL_PREDECESSOR")
            record = following
        return record


def initial_state(manifest):
    return {
        "manifest_sha": digest(encode(manifest)),
        "quota": {"events": [], "paused_until": 0},
        "tasks": {row["task_id"]: {"status": row["admission"], "generation": 0} for row in manifest["tasks"]},
    }


def claim(state, task_id, owner, now, *, charge):
    current = state["tasks"][task_id]
    if not (current["status"] == "PENDING" or
            (current["status"] == "LEASED" and current["deadline"] <= now)):
        return None
    quota = state["quota"]
    # Use a one-slot limit to exercise rolling-window admission without a provider request.
    if charge and not can_reserve(quota["events"], now, "download", normal_limit=1, total_limit=1,
                                  paused_until=quota["paused_until"]):
        return None
    updated = deepcopy(state)
    updated["tasks"][task_id] = {"status": "LEASED", "owner": owner,
                                 "generation": current["generation"] + 1, "deadline": now + LEASE_SECONDS}
    if charge:
        updated["quota"]["events"].append([now, "download"])
    return updated


def accept(state, task_id, lease, now, receipt):
    current = state["tasks"][task_id]
    if current["status"] != "LEASED" or current != lease or current["deadline"] <= now:
        return None
    updated = deepcopy(state)
    updated["tasks"][task_id] = current | {"status": "VERIFIED", "receipt": receipt}
    return updated


def select(manifest):
    by_dataset = {row["dataset"]: row for row in manifest["tasks"]}
    require(set(by_dataset) == {task.dataset for task in TASKS}, "MANIFEST_TASKS")
    return by_dataset["TaiwanStockPrice"], by_dataset["TaiwanBusinessIndicator"], by_dataset["TaiwanOptionDaily"]


def origin_run_id():
    sha = os.environ["GITHUB_SHA"]
    query = urlencode({"head_sha": sha, "event": "push", "per_page": 10})
    github = GitHub()

    def lookup():
        rows = github.json(f"/actions/workflows/{WORKFLOW}/runs?{query}")["workflow_runs"]
        matches = [row["id"] for row in rows if row["head_sha"] == sha and row["head_branch"] == BRANCH
                   and row["event"] == "push"]
        require(len(matches) <= 1, "ORIGIN_RUN_AMBIGUOUS")
        return matches[0] if matches else None

    return until(lookup, "ORIGIN_RUN_NOT_FOUND")


def prepare(root, journal, access, run):
    windows, originals = connected_manifest(access)
    rows = []
    for window in windows:
        source = originals[window["dataset"]]
        admission = "HOLD" if window["dataset"] == "TaiwanOptionDaily" else "PENDING"
        rows.append(window | {"admission": admission, "file_id": source["source_id"], "proof": source["proof"]})
    manifest = {"origin_run": run, "commit": os.environ["GITHUB_SHA"], "mode": "ARCHIVED_PREFLIGHT",
                "tasks": rows}
    write_json(root, MANIFEST, manifest, access)
    journal.initialize(initial_state(manifest))

    stock, _, _ = select(manifest)
    first = journal.version(0)
    leased = claim(first["state"], stock["task_id"], run, 100, charge=True)
    require(leased is not None and journal.append(first, leased), "INITIAL_LEASE")
    first_lease = journal.version(1)
    paused = deepcopy(first_lease["state"])
    paused["quota"]["paused_until"] = retry_deadline("180", 100)
    require(journal.append(first_lease, paused), "PERSIST_429")
    return manifest, journal.version(2)


def check_admission(manifest, base, run):
    state = base["state"]
    stock, historical, held = (row["task_id"] for row in select(manifest))
    require(state["manifest_sha"] == digest(encode(manifest)), "MANIFEST_DIGEST")
    require(state["quota"] == {"events": [[100, "download"]], "paused_until": 280}, "PRIOR_QUOTA_MISSING")
    require(claim(state, historical, run, 101, charge=True) is None, "COLD_CLAIM")
    require(claim(state, historical, run, 280, charge=True) is None, "ROLLING_CAP")
    require(claim(state, stock, run, 101, charge=False) is None, "ACTIVE_DUPLICATE")
    require(claim(state, held, run, 3700, charge=True) is None, "UNAPPROVED_TASK_CLAIMED")


def compete(root, journal, access, manifest, base, role, run):
    check_admission(manifest, base, run)
    peer = "resume" if role == "origin" else "origin"
    write_json(root, f"ready-{role}.json", {"run": run}, access)
    until(lambda: find(root, f"ready-{peer}.json", access), "CONTENDER_NOT_READY")
    other = read_json_file(root, f"ready-{peer}.json", access)["run"]
    require(run != other, "SAME_RUN")
    key = select(manifest)[1]["task_id"]
    proposed = claim(base["state"], key, run, 3700, charge=True)
    require(proposed is not None, "EXPECTED_QUOTA_RELEASE")
    won = journal.append(base, proposed)
    write_json(root, f"claim-{role}.json", {"run": run, "committed": won}, access)


def reconcile(root, journal, access, manifest, run, *, commit):
    racers = []
    for role in ("origin", "resume"):
        name = f"claim-{role}.json"
        until(lambda: find(root, name, access), "CONTENDERS_NOT_FINISHED")
        racers.append(read_json_file(root, name, access))
    winners = {entry["run"] for entry in racers if entry["committed"]}
    require(len(winners) == 1 and racers[0]["run"] != racers[1]["run"], "CLAIM_COUNT")
    require(racers[0]["run"] == manifest["origin_run"], "ORIGIN_MISMATCH")
    base = journal.version(3)
    state = base["state"]
    stock, history, held = select(manifest)
    require(state["tasks"][history["task_id"]]["owner"] in winners, "CLAIM_OWNER")
    require(state["quota"]["events"] == [[100, "download"], [3700, "download"]], "DUPLICATE_CHARGE")
    recovered = state
    for row in (stock, history):
        key = row["task_id"]
        task = next(task for task in TASKS if task.dataset == row["dataset"])
        read_original(task, row["file_id"], row["proof"], access)
        receipt = {"file_id": row["file_id"], "proof": row["proof"]}
        stale = state["tasks"][key]
        taken = claim(recovered, key, racers[1]["run"], 3730, charge=False)
        require(taken is not None and taken["tasks"][key]["generation"] == 2, "TAKEOVER_GENERATION")
        require(accept(taken, key, stale, 3731, receipt) is None, "EXPIRED_LEASE_ACCEPTED")
        recovered = accept(taken, key, taken["tasks"][key], 3731, receipt)
        require(recovered is not None, "VERIFIED_RECEIPT_REJECTED")
    require(recovered["quota"] == state["quota"] and recovered["tasks"][held["task_id"]]["status"] == "HOLD",
            "REPLAY_CHARGED_OR_HOLD_REMOVED")
    if commit:
        require(run == racers[1]["run"] and journal.append(base, recovered), "RESUME_COMMIT")
    terminal = until(lambda: journal.version(4), "RESUME_NOT_DURABLE")
    require(terminal["state"] == recovered and journal.version(5) is None, "FINAL_RECONCILIATION")
    print(json.dumps({"origin_run": racers[0]["run"], "resume_run": racers[1]["run"],
                      "contenders": len(racers), "committed_contested_leases": len(winners),
                      "shadow_quota_reservations": len(recovered["quota"]["events"]), "verified_reused": 2,
                      "held": 1, "finmind_requests": 0, "production": "HOLD"}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("origin", "resume", "audit"))
    phase = parser.parse_args().phase
    try:
        access = token()
        run = int(os.environ["GITHUB_RUN_ID"])
        first_run = origin_run_id() if phase == "resume" else run
        if phase == "origin":
            root = root_folder(access, prefix=PREFIX, run_id=first_run, attempt=1, create=True)
        else:
            name = recovery_location(access, prefix=PREFIX, run_id=first_run, attempt=1)
            until(lambda: find(RECOVERY_ROOT, name, access), "ORIGIN_FOLDER_MISSING")
            root = root_folder(access, prefix=PREFIX, run_id=first_run, attempt=1, create=False)
        journal = DriveJournal(root, access)
        if phase == "origin":
            manifest, base = prepare(root, journal, access, run)
        else:
            base = until(lambda: journal.version(2), "ORIGIN_CHECKPOINT_MISSING")
            manifest = read_json_file(root, MANIFEST, access)
        require(manifest["origin_run"] == first_run and manifest["commit"] == os.environ["GITHUB_SHA"]
                and manifest["mode"] == "ARCHIVED_PREFLIGHT", "REHEARSAL_IDENTITY")
        if phase == "origin":
            compete(root, journal, access, manifest, base, phase, run)
        elif phase == "resume":
            compete(root, journal, access, manifest, base, phase, run)
            reconcile(root, journal, access, manifest, run, commit=True)
        else:
            reconcile(root, journal, access, manifest, run, commit=False)
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_CROSS_RUN_LEDGER_FAILED phase={phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
