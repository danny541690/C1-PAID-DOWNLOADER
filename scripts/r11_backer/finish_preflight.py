"""Complete the remaining bounded R12 checks with one private evidence ledger."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime, timedelta
import json
import os
from time import monotonic, sleep, time
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .drive_pilot import FOLDER, ROOT, ensure, find, token
from .final_source_probes import BOND_SEED, DATE_SEED, PREFIX as PRIOR, pack_samples, previous_capture
from .history_pilot import next_month, twse_month
from .integration_pilot import digest, encode, verify_private_folder
from .live_bronze_admission import advance, fetch_original
from .live_preflight import drive_health
from .live_recovery_canary import month_folder, read_json_file, write_json
from .manifest_pilot import sample_data
from .quota import can_reserve, retry_deadline
from .reconcile_catalog import BACKER_COMPATIBLE
from .release_batch import account_check, stored_event, proof_for
from .release_probes import MAX_BYTES, SCOPE_HOLDS, Sample, Task, contracts, observation_date, read_private_file
from .release_rehearsal import RehearsalError, require, root_folder
from .tpex_history import ARCHIVE, save_private, verified_archive_files

RUN_HISTORY = 45759092912
PREFIX = "R12_FINISH_PREFLIGHT"
MANIFEST = "TEST_MANIFEST.json"
WORKERS = 32
LATEST = date(2026, 11, 23)
RECENT = date(2026, 11, 1)
PASSED = {"VALID_SAMPLE", "PASS_NATIVE_SAMPLE"}
DAILY = {
    "TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanStockMarketValue",
    "TaiwanStockInstitutionalInvestorsBuySellWide", "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockDayTrading", "TaiwanStockShareholding", "TaiwanStockPER",
}
PUBLISH = {"TaiwanStockInstitutionalInvestorsBuySellWide": "B02", "TaiwanStockShareholding": "B03"}
UNRESOLVED = {"EMPTY_WINDOW", "SIZE_LIMIT", "HOLD_BOND_ID_UNVERIFIED", "HOLD_UNBOUNDED_START_ONLY_QUERY"}


def task_id(task):
    return digest(encode(task.parameters()))


def restore_task(value):
    params = value["params"]
    return Task(params["dataset"], params.get("start_date"), value["end"],
                params.get("data_id"), "end_date" in params)


def packets(access, archive, prefixes, *, include_status=False):
    raw_by_key = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        for entries in pool.map(lambda name: verified_archive_files(access, archive(name)), prefixes):
            info = json.loads(entries["reconciliation.json"])
            for row in info["backer_requests"]:
                if not row["original"]:
                    continue
                key = digest(encode(row["request"]))
                require(key not in raw_by_key, "DUPLICATE_PACKET_ORIGINAL")
                raw = entries[row["original"]]
                raw_by_key[key] = (raw, row["status"]) if include_status else raw
    return raw_by_key


def original_evidence(access):
    def locate(prefix):
        return root_folder(access, prefix=prefix, run_id=RUN_HISTORY, attempt=1, create=False)

    archived = verified_archive_files(access, locate(PRIOR))
    old = json.loads(archived["reconciliation.json"])
    prior, _ = previous_capture(access)
    require(digest(prior["reconciliation.json"]) == old["prior_result_sha"], "PRIOR_EVIDENCE_SHA")
    require(all(name.startswith(PRIOR + "_") for name in old["packets"]), "PRIOR_PACKET_NAME")
    originals = packets(access, locate, old["packets"])
    validated = set()
    for row in old["rows"]:
        for sample in (row, *row.get("shards", [])):
            if "raw_sha256" not in sample:
                continue
            key = digest(encode(sample["request"]))
            if "reused_evidence" in sample:
                raw = prior[sample["reused_evidence"]["original"]]
            else:
                raw = originals[key]
                validated.add(key)
            packed, raw_sha, packed_sha = compress_original(raw)
            require(raw_sha == sample["raw_sha256"] and packed_sha == sample["gzip_sha256"]
                    and len(packed) == sample["gzip_bytes"] and len(raw) == sample["raw_bytes"],
                    "PRIOR_SAMPLE_SHA")
    seeds = {task_id(DATE_SEED), task_id(BOND_SEED)}
    require(originals.keys() <= validated | seeds, "PRIOR_SAMPLE_INDEX_MISMATCH")
    return old, originals, contracts(archived["official/finmind-datasets.md"].decode("utf-8"))


def live_bond(originals, start, end):
    raw = originals.get(task_id(BOND_SEED))
    if not raw:
        return None
    for row in json.loads(raw)["data"]:
        key = row.get("cb_id")
        if isinstance(key, str) and key.isascii() and key.isdecimal() and isinstance(
            row.get("InitialDateOfConversion"), str
        ) and isinstance(row.get("DueDateOfConversion"), str) and (
            row["InitialDateOfConversion"] <= start.isoformat() <= end.isoformat() <= row["DueDateOfConversion"]
        ):
            return key
    return None


def month_starts(start, end):
    month = start.replace(day=1)
    while month <= end:
        yield month
        month = next_month(month)


def read_calendars(rows):
    months = {RECENT}
    for row in rows:
        req = row.get("request", {})
        if row["result"] == "EMPTY_WINDOW" and row["dataset"] in DAILY and req.get("end_date"):
            months.update(month_starts(date.fromisoformat(req["start_date"]), date.fromisoformat(req["end_date"])))
    months = sorted(months)
    with ThreadPoolExecutor(max_workers=12) as pool:
        return {month: days for month, (status, days) in zip(months, pool.map(twse_month, months), strict=True)
                if status == "PASS" and days}


def empty_confirmed(item, calendars):
    req = item.get("request", {})
    if item["dataset"] not in DAILY or not req.get("end_date") or req.get("data_id") != "2330":
        return False
    start, end = map(date.fromisoformat, (req["start_date"], req["end_date"]))
    months = set(month_starts(start, end))
    # The recent calendar has never certified dates after the verified cutoff.
    if end > LATEST or not months <= calendars.keys():
        return False
    return not any(start <= day <= end for month in months for day in calendars[month])


def alternate(row, doc, positive, originals):
    name = row["dataset"]
    req = row.get("request", {})
    if row["result"] == "SIZE_LIMIT" and name == "TaiwanStockIndustryChain":
        return Task(name, None, None, "2330")
    if row["result"] == "SIZE_LIMIT" and req:
        return restore_task({"params": req, "end": req.get("end_date", req.get("start_date"))})
    if not req.get("start_date") or not req.get("end_date") or "`end_date`" not in (doc.optional + doc.required):
        return None
    first, last = map(date.fromisoformat, (req["start_date"], req["end_date"]))
    if row["sample_period"] == "recent":
        start, end = date(2026, 10, 1), date(2026, 10, 31)
    else:
        matching = [date.fromisoformat(item["observed_dates"][0]) for item in positive
                    if item.get("request", {}).get("data_id") == req.get("data_id")
                    and item.get("observed_dates") and item["observed_dates"][0] > str(last)]
        if matching:
            end = last + (min(matching) - last) // 2
            start = max(last + timedelta(days=1), end - (last - first))
        else:
            start = last + timedelta(days=1)
            end = start + (last - first)
    if first == last and name in {"TaiwanOptionDaily", "TaiwanFuturesDealerTradingVolumeDaily",
                                  "TaiwanAssetSwapFixedIncomeDaily", "TaiwanAssetSwapOptionDaily"}:
        start = end
    if end > LATEST or start > end:
        return None
    symbol = req.get("data_id")
    if "可轉債" in doc.required and name != BOND_SEED.dataset:
        symbol = live_bond(originals, start, end)
        if not symbol:
            return None
    return Task(name, str(start), str(end), symbol)


def make_plan(old, originals, registry, calendars):
    roster = {item["dataset"]: item for item in sample_data()}
    rows = old["rows"]
    by_dataset = defaultdict(list)
    known = set()
    for row in rows:
        if row["result"] in PASSED:
            by_dataset[row["dataset"]].append(row)
        if row.get("request") and row["result"] != "SIZE_LIMIT":
            known.add(digest(encode(row["request"])))
    tests, decisions = {}, []

    def add(task, purpose):
        contract = registry.get(task.dataset)
        item = roster.get(task.dataset)
        if not (contract and contract.tier in BACKER_COMPATIBLE and item and
                item["state"] == "SAMPLE_ONLY" and task.dataset not in SCOPE_HOLDS):
            return None
        key = task_id(task)
        if key in known:
            return None
        tests.setdefault(key, {"params": task.parameters(), "end": task.end, "lane": item["routing"],
                               "fields": list(contract.fields), "purpose": purpose})
        return key

    for index, row in enumerate(rows):
        if row["result"] not in UNRESOLVED:
            continue
        name = row["dataset"]
        if row["result"] == "EMPTY_WINDOW" and empty_confirmed(row, calendars):
            decisions.append({"index": index, "dataset": name, "status": "CONFIRMED_NON_SESSION"})
            continue
        proposed = alternate(row, registry[name], by_dataset[name], originals) if name in registry else None
        key = add(proposed, f"{row['sample_period']}_followup") if proposed else None
        decisions.append({"index": index, "dataset": name, "prior": row["result"],
                          "status": "FOLLOWUP" if key else "HOLD_EVIDENCE", "key": key})

    close = LATEST.isoformat()
    start = RECENT.isoformat()
    representatives = [
        Task("TaiwanStockInstitutionalInvestorsBuySellWide", start, close, "2330"),
        Task("TaiwanStockShareholding", start, close, "2330"),
        Task("TaiwanStockDividend", "2026-06-01", "2026-09-30", "2330"),
        Task("TaiwanFuturesDaily", close, close, "TX"),
        Task("TaiwanOptionDaily", close, close, "TXO"),
    ]
    local = datetime.now(ZoneInfo("Asia/Taipei"))
    last_published = local.date() if local.hour >= 18 else local.date() - timedelta(days=1)
    if final_day := max((day for day in calendars.get(RECENT, set()) if day <= last_published), default=None):
        representatives.append(Task("TaiwanStockPriceLimit", str(final_day), str(final_day), "2330", send_end=False))
    if bond := live_bond(originals, LATEST - timedelta(days=7), LATEST):
        representatives.extend(Task(name, str(LATEST - timedelta(days=7)), close, bond)
                               for name in ("TaiwanStockConvertibleBondDaily", "TaiwanStockConvertibleBondDailyOverview"))
    current_days = sorted(str(day) for day in calendars.get(RECENT, set()) if day <= LATEST)
    for task in representatives:
        key = add(task, "lane_preflight")
        if key and current_days and current_days[-1] == close and task.dataset in PUBLISH:
            tests[key]["expected_days"] = current_days
    return tests, decisions


def prepare(root, journal, access):
    if find(root, MANIFEST, access) is not None:
        manifest = read_json_file(root, MANIFEST, access)
    else:
        old, originals, registry = original_evidence(access)
        tests, decisions = make_plan(old, originals, registry, read_calendars(old["rows"]))
        manifest = {"tests": tests, "decisions": decisions, "previous_rows": len(old["rows"]),
                    "previous_positive": sorted({row["dataset"] for row in old["rows"] if row["result"] in PASSED}),
                    "previous_lanes": sorted({row["lane"] for row in old["rows"] if row["result"] in PASSED}),
                    "source_access": old["account"]["status"]}
        # Reserve final IDs only for windows with exact independent trading date verification.
        for row in tests.values():
            if "expected_days" in row:
                row["object_id"] = journal.new_id()
        health = drive_health(access)
        require(health["status"] == "POSITIVE_CAPACITY_AND_BRONZE_ACCESS" and
                health["available_bytes"] >= MAX_BYTES * len(tests) + 1048576, "PREFLIGHT_STORAGE")
        write_json(root, MANIFEST, manifest, access)
    journal.initialize({"manifest_sha": digest(encode(manifest)), "tasks": {
        key: {"status": "READY", "attempts": 0} for key in manifest["tests"]
    }, "events": [], "pause": 0, "packets": [], "inflight": []})
    return manifest


def read_source(task, entry, auth):
    try:
        raw = fetch_original(auth, task, byte_limit=MAX_BYTES)
        if len(raw) > MAX_BYTES:
            return Sample("SIZE_LIMIT"), None
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return Sample("INVALID_REPLY"), None
        code = obj.get("status")
        if code == 429:
            return Sample("RATE_LIMITED"), None
        if code in (401, 403):
            return Sample("ACCESS_DENIED"), None
        if code != 200 or set(obj) - {"msg", "status", "data"}:
            return Sample("STATUS_OR_PAGINATION"), None
        rows = obj.get("data")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            return Sample("INVALID_REPLY"), None
        if not rows:
            return Sample("EMPTY_WINDOW", raw=raw), None
        if task.start:
            try:
                days = [observation_date(task.dataset, row.get("date")).isoformat() for row in rows]
            except (TypeError, ValueError):
                return Sample("DATE_KEY_UNVERIFIED", raw=raw, rows=tuple(rows)), None
            if not task.start <= min(days) <= max(days) <= task.end:
                return Sample("OUTSIDE_WINDOW", raw=raw, rows=tuple(rows)), None
        if task.data_id:
            identities = {"stock_id", "cb_id", "option_id", "futures_id"} & set(entry["fields"])
            if len(identities) == 1:
                field, = identities
                if any(row.get(field) != task.data_id for row in rows):
                    return Sample("IDENTITY_MISMATCH", raw=raw, rows=tuple(rows)), None
        result = "VALID_SAMPLE" if all(set(entry["fields"]) <= row.keys() for row in rows) else "FIELD_DIFFERENCES"
        return Sample(result, raw=raw, rows=tuple(rows)), None
    except HTTPError as error:
        if error.code == 429:
            return Sample("RATE_LIMITED"), error.headers.get("Retry-After") if error.headers else None
        if error.code in (401, 403):
            return Sample("ACCESS_DENIED"), None
        return Sample("TRANSIENT" if error.code in (408, 500, 502, 503, 504) else "HTTP_REJECTED"), None
    except RehearsalError:
        return Sample("PARTIAL_HTTP_RESPONSE"), None
    except OSError:
        return Sample("TRANSIENT"), None
    except (ValueError, EOFError):
        return Sample("INVALID_REPLY"), None


def wave_keys(state, now):
    account = state.get("account")
    if not account or now < state["pause"] or not 0 <= now - account["at"] <= 150:
        return []
    remaining = account["headroom"] - (len(state["events"]) - account["first_event"])
    events = [(entry["at"], entry["kind"]) for entry in state["events"]]
    selected = []
    for key, entry in state["tasks"].items():
        if len(selected) >= min(WORKERS, remaining):
            break
        if entry["status"] != "READY" or entry.get("retry_at", 0) > now:
            continue
        if not can_reserve(events, now, "download", normal_limit=account["limit"], total_limit=account["limit"],
                           paused_until=state["pause"]):
            break
        selected.append(key)
        events.append((now, "download"))
    return selected


def finish_wave(root, journal, record, access, result):
    state = record["state"]
    tasks = deepcopy(state["tasks"])
    require(set(result["outcomes"]) == set(state["inflight"]), "WAVE_OUTCOME_MEMBERSHIP")
    now = int(time())
    pause = state["pause"]
    for key, item in result["outcomes"].items():
        status = item["status"]
        if status == "RATE_LIMITED":
            pause = max(pause, retry_deadline(item["retry_after"], item["received_at"]))
        if status in ("TRANSIENT", "RATE_LIMITED", "INTERRUPTED") and tasks[key]["attempts"] < 4:
            tasks[key] = tasks[key] | {"status": "READY", "retry_at": max(now + 2 ** tasks[key]["attempts"], pause)}
        else:
            tasks[key] = tasks[key] | {"status": status, "proof": item.get("proof")}
    return advance(journal, record, tasks=tasks, inflight=[], pause=pause, packets=state["packets"] + result["packets"])


def recover_wave(root, sequence, inflight, access):
    name = f"wave-{sequence:06d}"
    if find(root, name + ".json", access):
        return read_json_file(root, name + ".json", access)
    found = {}
    for index in range(len(inflight)):
        part = f"{name}-part-{index + 1:02d}"
        folder = find(root, part, access)
        if folder is None or find(folder["id"], ARCHIVE, access) is None:
            break
        fid = ensure(root, part, FOLDER, access, create=False)
        verify_private_folder(fid, access)
        found[part] = fid
    archived = packets(access, found.__getitem__, found, include_status=True)
    require(archived.keys() <= set(inflight), "RECOVERED_PACKET_MEMBERSHIP")
    outcomes = dict.fromkeys(inflight, {"status": "INTERRUPTED"})
    for key, (raw, status) in archived.items():
        _, raw_sha, packed_sha = compress_original(raw)
        outcomes[key] = {"status": status, "proof": {"raw_sha": raw_sha, "packed_sha": packed_sha,
                                                    "rows": len(json.loads(raw)["data"])}}
    return {"outcomes": outcomes, "packets": list(found)}


def dispatch(manifest, root, journal, record, access, run):
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    deadline = monotonic() + 350
    while monotonic() < deadline:
        now = int(time())
        state = record["state"]
        if state["inflight"]:
            outcome = recover_wave(root, record["sequence"], state["inflight"], access)
            record = finish_wave(root, journal, record, access, outcome)
            continue
        if not any(row["status"] == "READY" for row in state["tasks"].values()):
            break
        if now < state["pause"]:
            if monotonic() + state["pause"] - now >= deadline:
                break
            sleep(min(15, state["pause"] - now))
            continue
        record = account_check(journal, record, run, auth)
        state = record["state"]
        wave = wave_keys(state, int(time()))
        if not wave:
            retry = min((row.get("retry_at", 0) for row in state["tasks"].values()
                         if row["status"] == "READY" and row.get("retry_at", 0) > now), default=0)
            if retry and retry - now <= 15:
                sleep(max(0, retry - int(time())))
                continue
            break
        tasks, events = deepcopy(state["tasks"]), list(state["events"])
        for key in wave:
            tasks[key]["attempts"] += 1
            events.append(stored_event(run, "download", now, key))
        record = advance(journal, record, tasks=tasks, events=events, inflight=wave)
        names = f"wave-{record['sequence']:06d}"
        queued = [(restore_task(manifest["tests"][key]), manifest["tests"][key]) for key in wave]
        print(f"finmind_source_requests={len(wave)} parallel_workers={len(wave)}")
        with ThreadPoolExecutor(max_workers=len(wave)) as pool:
            received = list(pool.map(lambda pair: read_source(pair[0], pair[1], auth), queued))
        raw = {pair[0]: result[0] for pair, result in zip(queued, received, strict=True)}
        archived, outcomes = [], {}
        for offset, packet in enumerate(pack_samples(raw)):
            name = f"{names}-part-{offset + 1:02d}"
            folder = ensure(root, name, FOLDER, access, create=True)
            verify_private_folder(folder, access)
            save_private({}, {}, packet, {"packet": name}, access=access, folder=folder)
            archived.append(name)
        for key, (sample, retry) in zip(wave, received, strict=True):
            outcome = {"status": sample.status, "retry_after": retry, "received_at": int(time())}
            if sample.raw:
                _, raw_sha, packed_sha = compress_original(sample.raw)
                outcome["proof"] = {"raw_sha": raw_sha, "packed_sha": packed_sha, "rows": len(sample.rows)}
            outcomes[key] = outcome
        outcome = {"outcomes": outcomes, "packets": archived}
        write_json(root, names + ".json", outcome, access)
        record = finish_wave(root, journal, record, access, outcome)
    return record


def accept(manifest, root, record, access, run, source_run):
    def packet_folder(name):
        folder = ensure(root, name, FOLDER, access, create=False)
        verify_private_folder(folder, access)
        return folder

    state = record["state"]
    data = packets(access, packet_folder, state["packets"])
    expected = {key for key, result in state["tasks"].items() if result.get("proof")}
    require(data.keys() == expected, "NEW_PACKET_LEDGER_MISMATCH")
    new_positive, lanes, accepted, blocked = set(), set(), {}, {}
    for key, result in state["tasks"].items():
        entry = manifest["tests"][key]
        task = restore_task(entry)
        if raw := data.get(key):
            packed, raw_sha, packed_sha = compress_original(raw)
            count = len(json.loads(raw)["data"])
            require(result["proof"] == {"raw_sha": raw_sha, "packed_sha": packed_sha, "rows": count},
                    "NEW_SAMPLE_SHA_OR_COUNT")
        if result["status"] != "VALID_SAMPLE":
            continue
        require(key in data, "POSITIVE_SAMPLE_MISSING")
        new_positive.add(task.dataset)
        lanes.add(entry["lane"])
        if "object_id" not in entry:
            continue
        window = {"dataset": task.dataset, "scope": task.data_id, "fields": entry["fields"],
                  "dates": entry["expected_days"]}
        try:
            proof = proof_for(window, packed, raw)
        except RehearsalError as error:
            blocked[key] = str(error)
            continue
        bronze = ensure(ROOT, "20_BRONZE", FOLDER, access, create=False)
        verify_private_folder(bronze, access)
        parent = month_folder(bronze, task, access, create=True)
        name = f"{task.dataset}__{task.data_id}__{task.start}__{task.end}__r12_1.json.gz"
        fid = ensure(parent, name, "application/gzip", access, packed, create=True, file_id=entry["object_id"])
        verify_private_folder(fid, access)
        persisted = read_private_file(fid, access)
        require(persisted == packed and proof_for(window, persisted) == proof, "BRONZE_RECEIPT")
        accepted[key] = {"file_id": fid, "proof": proof}
    result = summary(manifest, record, source_run) | {
        "verification_finmind_requests": 0,
        "old_and_new_positive_datasets": len(set(manifest["previous_positive"]) | new_positive),
        "old_and_new_positive_lanes": sorted(set(manifest["previous_lanes"]) | lanes),
        "new_positive_datasets": len(new_positive), "source_files_read_back": len(data),
        "formal_bronze_verified": len(accepted), "formal_bronze_rows": sum(
            item["proof"]["rows"] for item in accepted.values()), "formal_bronze_blocked": len(blocked),
    }
    write_json(root, f"B10_SUMMARY_{run}_{os.environ['GITHUB_RUN_ATTEMPT']}.json",
               result | {"receipts": accepted, "bronze_holds": blocked}, access)
    return result


def summary(manifest, record, run):
    state = record["state"]
    events = Counter(row["kind"] for row in state["events"] if row["run"] == run)
    counts = Counter(row["status"] for row in state["tasks"].values())
    return {"preflight": "COMPLETE" if not counts["READY"] and not state["inflight"] else "BLOCKED",
            "full_volume": "HOLD", "previous_windows_reconciled": manifest["previous_rows"],
            "previous_exceptions_reviewed": len(manifest["decisions"]),
            "confirmed_non_session": sum(row["status"] == "CONFIRMED_NON_SESSION" for row in manifest["decisions"]),
            "historical_held_without_new_query": sum(row["status"] == "HOLD_EVIDENCE" for row in manifest["decisions"]),
            "historical_alternatives_with_valid_sample": sum(
                state["tasks"].get(row.get("key"), {}).get("status") == "VALID_SAMPLE"
                for row in manifest["decisions"]),
            "new_windows": len(state["tasks"]), "outcomes": dict(counts),
            "account_reservations": events["probe"], "source_reservations": events["download"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("dispatch", "accept"))
    parser.add_argument("--source-run", type=int)
    args = parser.parse_args()
    phase = args.phase
    if args.source_run is not None and (phase != "accept" or args.source_run < 1):
        parser.error("Only readback can reopen another run's evidence")
    try:
        run = int(os.environ["GITHUB_RUN_ID"])
        access = token()
        source_run = args.source_run or run
        root = root_folder(access, prefix=PREFIX, run_id=source_run, attempt=1, create=phase == "dispatch")
        journal = DriveJournal(root, access)
        manifest = prepare(root, journal, access) if phase == "dispatch" else read_json_file(root, MANIFEST, access)
        record = journal.version()
        require(record and record["state"]["manifest_sha"] == digest(encode(manifest)), "TEST_MANIFEST_DIGEST")
        if phase == "dispatch":
            result = summary(manifest, dispatch(manifest, root, journal, record, access, run), run)
        else:
            result = accept(manifest, root, record, access, run, source_run)
        print(json.dumps(result, sort_keys=True))
        if phase == "accept":
            require(result["preflight"] == "COMPLETE", "PREFLIGHT_REQUESTS_PENDING")
    except Exception as error:
        label = str(error) if isinstance(error, RehearsalError) else type(error).__name__
        print(f"R12_FINISH_PREFLIGHT_HOLD phase={phase} label={label}")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
