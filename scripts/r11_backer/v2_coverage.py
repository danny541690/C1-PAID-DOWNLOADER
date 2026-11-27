"""Compile a frozen, review-only coverage manifest from witnessed query windows."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from itertools import islice
import re

from .cross_run_ledger import DriveJournal
from .dispatch import Window
from .drive_pilot import FOLDER, ensure
from .finish_preflight import MANIFEST as PROBE_MANIFEST, PREFIX as PROBE_PREFIX
from .history_pilot import BUSINESS
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import read_json_file
from .production_download import BATCH_SIZE, PREVIOUS_ROLLOUT, PROBE_ACCEPT, PROBE_RUN, candidate_windows
from .release_batch import MANIFEST_NAME, load as load_batch, private_campaign
from .release_probes import MAX_BYTES
from .release_rehearsal import root_folder
from .release_rollout import campaign as rollout_campaign

SHARD_LIMIT = 2000
VERSION = 2
TERMINAL = {"VERIFIED", "BLOCKED", "REPAIR"}


def query_key(parameters, mode, *, market=None, market_specific=False, identity=None):
    """Hash the native query independently of contract revisions and venue aliases."""
    if not parameters.get("dataset") or not mode:
        raise ValueError("SOURCE_QUERY_IDENTITY")
    payload = {"mode": "entity_range" if mode == "symbol_range" else mode, "parameters": parameters}
    if market_specific:
        if not market:
            raise ValueError("SOURCE_MARKET_REQUIRED")
        payload["native_market"] = market
    if identity is not None:
        payload["effective_identity"] = identity
    return digest(encode(payload))


def series_key(parameters, mode, *, market=None, market_specific=False, identity=None):
    return query_key({key: value for key, value in parameters.items() if key not in ("start_date", "end_date")},
                     mode, market=market, market_specific=market_specific, identity=identity)


def bounded_shards(rows):
    """Leave room below the private reader limit, regardless of row width."""
    chunk, size = [], 4096
    for row in rows:
        length = len(encode(row)) + 1
        if length > MAX_BYTES - 4096:
            raise ValueError("MANIFEST_ROW_EXCEEDS_READBACK_LIMIT")
        if chunk and (len(chunk) == SHARD_LIMIT or size + length > MAX_BYTES):
            yield chunk
            chunk, size = [], 4096
        chunk.append(row)
        size += length
    if chunk:
        yield chunk


def interval(start, end):
    lower, upper = [date.fromisoformat(value) for value in (start, end)]
    if lower > upper:
        raise ValueError("COVERAGE_WINDOW_ORDER")
    return lower.toordinal(), upper.toordinal()


def subtract_coverage(start, end, covered):
    """Return inclusive unproven ranges in ascending order."""
    first, last = interval(start, end)
    proof = sorted(interval(*pair) for pair in covered)
    remainder, cursor = [], first
    for low, high in proof:
        if high < cursor or low > last:
            continue
        if low > cursor:
            remainder.append((cursor, low - 1))
        cursor = max(cursor, high + 1)
    if cursor <= last:
        remainder.append((cursor, last))
    return [[date.fromordinal(part).isoformat() for part in pair] for pair in remainder]


def parameters_for(item):
    parameters = {"dataset": item["dataset"], "start_date": item["start"], "end_date": item["end"]}
    if item["dataset"] != BUSINESS:
        parameters["data_id"] = item["scope"]
    return parameters


def read_proof(item, receipt, *, expected_count=None):
    proof = receipt["proof"]
    if not receipt.get("file_id") or item.get("destination_id", receipt["file_id"]) != receipt["file_id"]:
        raise ValueError("LEGACY_RECEIPT_DESTINATION")
    for key in ("raw_sha", "packed_sha", "first_sha", "last_sha"):
        if not isinstance(proof.get(key), str) or not re.fullmatch("[0-9a-f]{64}", proof[key]):
            raise ValueError("LEGACY_RECEIPT_DIGEST")
    if type(proof.get("rows")) is not int or proof["rows"] <= 0 or (
        expected_count is not None and proof["rows"] != expected_count
    ) or type(proof.get("bytes")) is not int or proof["bytes"] <= 0:
        raise ValueError("LEGACY_RECEIPT_COUNTS")
    return proof


def receipt_entry(item, receipt, source):
    expected = len(item["dates"]) if "dates" in item else None
    proof = read_proof(item, receipt, expected_count=expected)
    params = parameters_for(item)
    mode = "market_range" if item["dataset"] == BUSINESS else "entity_range"
    return {"legacy_task_id": item["task_id"], "query_key": query_key(params, mode),
            "series_key": series_key(params, mode), "params": params, "mode": mode,
            "market": item.get("market"), "file_id": receipt["file_id"], "proof": proof,
            "evidence_source": source.get("folder"),
            "evidence_manifest_sha": source.get("manifest_sha"), "evidence_checkpoint": source.get("sequence")}


def terminal_inventory(manifest, record, source):
    manifest_sha = digest(encode(manifest))
    if record["state"]["manifest_sha"] != manifest_sha:
        raise ValueError("LEGACY_MANIFEST_DIGEST")
    source = source | {"manifest_sha": manifest_sha, "sequence": record["sequence"]}
    tasks = record["state"]["tasks"]
    if len(tasks) != len(manifest["items"]) or tasks.keys() != {row["task_id"] for row in manifest["items"]}:
        raise ValueError("LEGACY_TASK_INVENTORY")
    counts = Counter(task["status"] for task in tasks.values())
    accepted = [receipt_entry(row, tasks[row["task_id"]]["receipt"], source) for row in manifest["items"]
                if tasks[row["task_id"]]["status"] == "VERIFIED"]
    exceptions = {key: task["status"] for key, task in tasks.items() if task["status"] in {"BLOCKED", "REPAIR"}}
    return counts, accepted, exceptions


def completed_batch(ctx, group, cursor, plan):
    key = "otc_plan_sha" if group == "TPEx" else "plan_sha"
    name = f"{'TPEX_' if group == 'TPEx' else ''}BATCH_{cursor:06d}"
    root = ensure(ctx.campaign, name, FOLDER, ctx.access, create=False)
    verify_private_folder(root, ctx.access)
    manifest = read_json_file(root, MANIFEST_NAME, ctx.access)
    if (manifest["cursor"], manifest["end"], manifest["plan_sha"]) != (
        cursor, min(cursor + BATCH_SIZE, plan["budget"]["windows"]), ctx.master["state"][key]
    ):
        raise ValueError("LEGACY_BATCH_POSITION")
    record = DriveJournal(root, ctx.access).head()
    counts, receipt, exceptions = terminal_inventory(manifest, record, {"folder": root})
    if set(counts) - TERMINAL:
        raise ValueError("LEGACY_COMMITTED_BATCH_NOT_TERMINAL")
    expected = list(islice(candidate_windows(plan), cursor, manifest["end"]))
    actual = [(x["dataset"], x["scope"], x["market"], x["start"], x["end"], x["dates"])
              for x in manifest["items"]]
    if actual != expected:
        raise ValueError("LEGACY_BATCH_NOT_IN_LOCKED_MANIFEST")
    return counts, receipt, exceptions


def prior_receipts(access):
    base = private_campaign(access, create=False)
    manifest, record = load_batch(base, DriveJournal(base, access), access)
    _, old = rollout_campaign(access, PREVIOUS_ROLLOUT, create=False)
    previous = read_json_file(old, MANIFEST_NAME, access)
    old_record = DriveJournal(old, access).head()
    result = terminal_inventory(manifest, record, {"folder": base})[1]
    result.extend(terminal_inventory(previous, old_record, {"folder": old})[1])
    probe_root = root_folder(access, prefix=PROBE_PREFIX, run_id=PROBE_RUN, attempt=1, create=False)
    tests = read_json_file(probe_root, PROBE_MANIFEST, access)
    probe_sha = digest(encode(tests))
    if DriveJournal(probe_root, access).head()["state"]["manifest_sha"] != probe_sha:
        raise ValueError("PROBE_MANIFEST_DIGEST")
    checks = read_json_file(probe_root, PROBE_ACCEPT, access)
    for key, receipt in checks["receipts"].items():
        entry = tests["tests"][key]
        params = entry["params"]
        item = {"dataset": params["dataset"], "scope": params["data_id"], "start": params["start_date"],
                "end": entry["end"], "dates": entry["expected_days"],
                "destination_id": entry["object_id"], "task_id": key}
        result.append(receipt_entry(item, receipt, {"folder": probe_root, "manifest_sha": probe_sha}))
    return result


def observed_bounds(evidence):
    days = defaultdict(set)
    successful = {"VALID_SAMPLE", "PASS_NATIVE_SAMPLE"}
    for row in evidence:
        if row.get("result") in successful and row.get("dataset"):
            days[row["dataset"]].update(row.get("observed_dates", []))
    return {key: {"earliest_observed_sample": min(values), "latest_observed_sample": max(values)}
            for key, values in days.items() if values}


def freeze_preview(plans, receipts, exceptions):
    known = defaultdict(list)
    exact = {}
    for entry in receipts:
        params = entry["params"]
        known[entry["series_key"]].append((params["start_date"], params["end_date"]))
        previous = exact.setdefault(entry["query_key"], entry)
        if previous["file_id"] != entry["file_id"]:
            raise ValueError("CONFLICTING_LEGACY_SOURCE_QUERY")
    by_group, conflicts = defaultdict(list), []
    seen, variant = set(), {}
    for plan in plans:
        for dataset, code, venue, start, end, days in candidate_windows(plan):
            native = plan["contracts"][dataset]
            item = Window(dataset, code, date.fromisoformat(start), date.fromisoformat(end),
                          native["contract"], native["lane"])
            params = {"dataset": dataset, "data_id": code, "start_date": start, "end_date": end}
            key = query_key(params, "entity_range")
            if key in seen:
                prior = variant[key]
                if prior != days:
                    conflicts.append({"query_key": key, "legacy_task_id": item.task_id,
                                      "reason": "MARKET_ALIASES_WITH_CONFLICTING_EXPECTED_SESSIONS"})
                continue
            seen.add(key)
            variant[key] = days
            track = series_key(params, "entity_range")
            reusable = exact.get(key)
            row = {"query_key": key, "coverage_key": track, "parameters": params, "mode": "entity_range",
                   "market_evidence": venue, "contract_version": item.contract, "legacy_task_id": item.task_id,
                   "expected_dates": days, "lane": item.lane}
            if reusable:
                if reusable["proof"]["rows"] != len(days):
                    conflicts.append({"query_key": key, "reason": "LEGACY_SCOPE_COVERAGE_MISMATCH"})
                    coverage = { "decision": "HOLD_REUSE_CONFLICT"}
                else:
                    coverage = {"decision": "REUSE_VERIFIED"}
            elif item.task_id in exceptions:
                coverage = {"decision": "HOLD_B09", "prior_state": exceptions[item.task_id]}
            else:
                remaining = subtract_coverage(start, end, known[track])
                coverage = {"decision": "RANGE_PREVIEW", "unproven_intervals": remaining,
                            "request_preview": [
                                {"parameters": narrowed,
                                 "query_key": query_key(narrowed, "entity_range"),
                                 "expected_dates": local_dates}
                                for low, high in remaining
                                if (local_dates := [day for day in days if low <= day <= high])
                                for narrowed in (params | {"start_date": low, "end_date": high},)
                            ]}
            by_group[(end[:4], item.lane)].append(row | coverage)
    # Identify incompatible aliases without counting them as an available request.
    conflicted = {row["query_key"] for row in conflicts}
    for rows in by_group.values():
        for i, row in enumerate(rows):
            if row["query_key"] in conflicted:
                rows[i] = {key: value for key, value in row.items()
                           if key not in {"request_preview", "unproven_intervals"}} | {"decision": "HOLD_REUSE_CONFLICT"}
    return by_group, conflicts


def run(ctx):
    registry = ctx.read("DATASET_REGISTRY.json")
    evidence = ctx.read("CONTRACT_EVIDENCE.json")
    master = ctx.master["state"]
    output, findings, receipts, exceptions, confirmed = {}, [], [], {}, Counter()
    master_totals = Counter(master["totals"])
    segments = [(group, cursor, plan) for group, stop, plan in (
        ("BASE", master["cursor"], ctx.plan),
        ("TPEx", master.get("otc_cursor", 0), ctx.supplement),
    ) if plan for cursor in range(0, stop, BATCH_SIZE)]
    segment_errors = []

    def inspect(job):
        try:
            return completed_batch(ctx, *job), None
        except (OSError, ValueError, KeyError, TypeError) as exc:
            return None, {"group": job[0], "start_offset": job[1], "error": type(exc).__name__}

    with ThreadPoolExecutor(max_workers=6) as pool:
        for result, error in pool.map(inspect, segments):
            if error:
                segment_errors.append(error)
                continue
            counts, stored, held = result
            confirmed.update(counts)
            receipts.extend(stored)
            exceptions.update(held)
    if confirmed != master_totals:
        findings.append("CONCLUDED_BATCH_RECEIPTS_NOT_RECONCILED")
    elif sum(row["proof"]["rows"] for row in receipts) != master["rows"] or (
        sum(row["proof"]["bytes"] for row in receipts) != master["bytes"]
    ):
        findings.append("CONCLUDED_BATCH_TOTALS_MISMATCH")
    master_receipt_count = len(receipts)
    try:
        old = prior_receipts(ctx.access)
        receipts.extend(old)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        old = []
        findings.append("PRIOR_RECEIPT_READBACK_" + type(exc).__name__)
    active = []
    try:
        report = ctx.read("RECOVERY_REPORT.json")
        for record in report["active_batch"]["tasks"]:
            if record["disposition"] == "VERIFIED_RECEIPT":
                active.append(receipt_entry(record, record["receipt"], {"folder": report["active_batch"].get("folder")}))
    except (KeyError, TypeError, OSError, ValueError) as exc:
        findings.append("ACTIVE_BATCH_EVIDENCE_UNAVAILABLE_" + type(exc).__name__)
    receipts.extend(active)
    plans = [plan for plan in (ctx.plan, ctx.supplement) if plan]
    per_group, conflicts = freeze_preview(plans, receipts, exceptions)
    if conflicts:
        findings.append("AMBIGUOUS_MARKET_OR_LEGACY_COVERAGE")
    index, decisions, storage = [], Counter(), defaultdict(int)
    for entry in receipts:
        storage[entry["params"]["dataset"]] += entry["proof"]["bytes"]
    for (year, lane), rows in sorted(per_group.items(), reverse=True):
        for number, shard_rows in enumerate(bounded_shards(rows)):
            filename = f"V2_PREVIEW_MANIFEST_{year}_{lane}_{number:03d}.json"
            shard = {"plan_version": VERSION, "release": "HOLD", "rows": shard_rows}
            output[filename] = shard
            index.append({"name": filename, "sha256": digest(encode(shard)), "windows": len(shard_rows)})
            decisions.update(row["decision"] for row in shard_rows)
    bounds = observed_bounds(evidence["stored_samples"])
    seen = Counter(entry["params"]["dataset"] for entry in receipts)
    history_rows = []
    for item in registry["rows"]:
        name = item["dataset"]
        technical = [r for r in old if r["params"]["dataset"] == name == BUSINESS]
        history_rows.append({"dataset": name, "scope_decision": item["decision"],
                             "observed": bounds.get(name), "verified_receipts": seen[name],
                             "bounded_historical_ranges": [{"start": r["params"]["start_date"],
                                                            "end": r["params"]["end_date"],
                                                            "query_key": r["query_key"]}
                                                           for r in technical],
                             "earliest_supported": technical[0]["params"]["start_date"] if technical else None,
                             "latest_available": None, "full_universe_verified": False,
                             "blocked_reasons": item["scope_holds"]})
    findings.append("FULL_HISTORY_UNIVERSE_EARLIEST_LATEST_OR_NATIVE_COMPLETENESS_UNPROVEN")
    output["MANIFEST_INDEX.json"] = {
        "scope": "PROVEN_BOUNDED_WINDOWS_ONLY", "candidate_windows": sum(decisions.values()),
        "reusable_windows": decisions["REUSE_VERIFIED"], "unverified_windows": decisions["RANGE_PREVIEW"],
        "candidate_dispositions": dict(decisions), "full_history_denominator": None,
        "shards": index, "reused_packed_bytes_by_dataset": dict(storage),
        "reused_receipt_count_by_dataset": dict(seen),
        "production_manifest_hashes": {key: master.get(key) for key in ("plan_sha", "otc_plan_sha")},
        "aggregation": {"master_states": dict(master_totals), "resolved_segment_states": dict(confirmed),
                        "master_verified_receipts": master_receipt_count, "prior_verified_receipts": len(old),
                        "active_verified_receipts_not_in_master": len(active), "failed_segments": segment_errors},
        "preview_only": True, "production_dispatchable": 0,
    }
    reusable = sorted(receipts, key=lambda row: row["query_key"])
    reuse_shards = []
    for number, rows in enumerate(bounded_shards(reusable)):
        filename = f"V2_REUSE_MAP_{number:04d}.json"
        shard = {"receipts": rows}
        output[filename] = shard
        reuse_shards.append({"name": filename, "sha256": digest(encode(shard)), "receipts": len(shard["receipts"])})
    output["REUSE_MAP.json"] = {"receipt_count": len(reusable), "shards": reuse_shards}
    output["HISTORICAL_COVERAGE.json"] = {"rows": history_rows}
    output["COVERAGE_GAPS.json"] = {"findings": findings, "ambiguous_queries": conflicts,
                                    "full_history_denominator": None,
                                    "missing_scope": ["Historical market and instrument validity periods",
                                                      "Latest documented publications for each native source",
                                                      "Verified complete native pages, episodes, or snapshots"]}
    return {"status": "HOLD", "findings": findings, "candidate_windows": sum(decisions.values()),
            "master_verified_receipts": master_receipt_count, "active_verified_receipts": len(active),
            "outputs": output}
