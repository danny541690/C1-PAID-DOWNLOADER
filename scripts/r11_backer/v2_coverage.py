"""Compile a frozen, review-only coverage manifest from witnessed query windows."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import gzip
import json
from itertools import islice
import re

from .cross_run_ledger import DriveJournal
from .dispatch import Window
from .drive_pilot import FOLDER, ensure
from .finish_preflight import MANIFEST as PROBE_MANIFEST, PREFIX as PROBE_PREFIX
from .history_pilot import BUSINESS, PREFIX as NDC_ARCHIVE, read_verified_history_archive
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import read_json_file
from .production_download import BATCH_SIZE, PREVIOUS_ROLLOUT, PROBE_ACCEPT, PROBE_RUN, candidate_windows
from .release_batch import HISTORY_RUN, MANIFEST_NAME, load as load_batch, private_campaign
from .release_probes import MAX_BYTES, Sample
from .release_rehearsal import root_folder
from .release_rollout import campaign as rollout_campaign
from .tpex_calendar import Source, monthly_rows
from .tpex_closeout import CALENDAR_RUN, LIFETIME_RUN
from .tpex_history import EVIDENCE_RUN, GAP_DAY, TRANSFERS, close_rows, transfer_proof, verified_archive_files

SHARD_LIMIT = 2000
VERSION = 2
TERMINAL = {"VERIFIED", "BLOCKED", "REPAIR"}
HISTORY_REVISION = "V2.1"
PRIOR_INTEGRATED_RUN = 46030259242
TPEx_HISTORICAL_RECONCILIATION = 45742569508
PRICE_DATASET = "TaiwanStockPrice"


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
    except (KeyError, TypeError, OSError, ValueError, RuntimeError) as exc:
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


def historical_packet(access, run):
    folder = root_folder(access, prefix="R11_TPEX_HISTORY", run_id=run, attempt=1, create=False)
    files = verified_archive_files(access, folder)
    return files, json.loads(files["reconciliation.json"]), {
        "run": run, "attempt": 1, "folder": folder,
        "manifest_sha256": digest(files["reconciliation.json"]),
    }


def prior_index(ctx):
    """Bind the previous 2026 preview and receipt inventory to the current locked master."""
    old = ctx.prior("MANIFEST_INDEX.json")
    reuse = ctx.prior("REUSE_MAP.json")
    master = ctx.master["state"]
    if old["production_manifest_hashes"] != {k: master.get(k) for k in ("plan_sha", "otc_plan_sha")} or (
        old["aggregation"]["master_states"] != master["totals"] or old["aggregation"]["failed_segments"]
    ):
        raise ValueError("V21_LEGACY_MASTER_CHANGED")

    def verify(ref, key):
        blob = ctx.prior(ref["name"])
        if digest(encode(blob)) != ref["sha256"] or len(blob[key]) != ref[{"rows": "windows", "receipts": "receipts"}[key]]:
            raise ValueError("V21_PRIOR_SHARD_HASH_OR_COUNT")
        return blob[key]

    def request_count(ref):
        rows = verify(ref, "rows")
        return sum(len(row.get("request_preview", ())) for row in rows
                   if row["decision"] == "RANGE_PREVIEW")

    with ThreadPoolExecutor(max_workers=5) as pool:
        receipts = [receipt for page in pool.map(lambda ref: verify(ref, "receipts"), reuse["shards"])
                    for receipt in page]
        pending = sum(pool.map(request_count, old["shards"]))
    aggr = old["aggregation"]
    if len(receipts) != reuse["receipt_count"] or len(receipts) != sum(aggr[key] for key in (
        "master_verified_receipts", "prior_verified_receipts", "active_verified_receipts_not_in_master",
    )) or len({row["query_key"] for row in receipts}) != len(receipts):
        raise ValueError("V21_PRIOR_RECEIPT_INVENTORY")
    report = ctx.prior("RECOVERY_REPORT.json")
    active = [receipt_entry(item, item["receipt"], {"folder": report["active_batch"].get("folder")})
              for item in report["active_batch"]["tasks"] if item["disposition"] == "VERIFIED_RECEIPT"]
    actual = {(r["legacy_task_id"], r["file_id"], r["proof"]["raw_sha"]) for r in receipts}
    if len(active) != aggr["active_verified_receipts_not_in_master"] or any(
        (r["legacy_task_id"], r["file_id"], r["proof"]["raw_sha"]) not in actual for r in active
    ):
        raise ValueError("V21_ACTIVE_RECEIPT_BINDING")
    return old, reuse, receipts, pending


def macro_observations(access, receipts):
    root = root_folder(access, prefix=NDC_ARCHIVE, run_id=HISTORY_RUN, attempt=1, create=False)
    original, _, packed = read_verified_history_archive(access, root)
    start, end = (original["manifest"][key] for key in ("first", "verified_through"))
    params = {"dataset": BUSINESS, "start_date": start, "end_date": end}
    reuse = next((row for row in receipts if row["query_key"] == query_key(params, "market_range")), None)
    if reuse is None or reuse["proof"]["raw_sha"] != original["raw_sha256"] or (
        reuse["proof"]["packed_sha"] != original["gzip_sha256"] or len(packed) != reuse["proof"]["bytes"]
    ):
        raise ValueError("V21_BUSINESS_LEGACY_RECEIPT")
    periods = original["manifest"]["newest_first_month_offsets"]
    if len(periods) != reuse["proof"]["rows"] or len({month for month, _ in periods}) != len(periods):
        raise ValueError("V21_BUSINESS_PERIOD_INDEX")
    evidence = {"run": HISTORY_RUN, "attempt": 1, "folder": root, "source_file_id": original["file_id"],
                "offset_index_sha256": original["manifest_sha256"], "receipt": reuse}
    return {"dataset": BUSINESS, "scope": "TW", "mode": "market_range", "lane": "B07", "parameters": params,
            "query_key": reuse["query_key"], "decision": "REUSE_VERIFIED_ARCHIVE", "confirmed_from": start,
            "confirmed_through": end, "logical_period_offsets": periods, "original_preserved": True,
            "new_source_requests": 0, "evidence": evidence}


def transfer_observations(packet, detail, evidence):
    """Revalidate exact historical request bytes and the venue on each witnessed day."""
    originals = {digest(encode(row["request"])): row for row in detail["backer_requests"] if row.get("original")}
    result, gaps = [], []
    for code, day in sorted(TRANSFERS.items(), key=lambda item: item[1], reverse=True):
        if day.year >= 2026:
            continue
        before = day - timedelta(days=1)
        params = {"dataset": PRICE_DATASET, "data_id": code, "start_date": str(before), "end_date": str(day)}
        record = originals.get(digest(encode(params)))
        if not record or record["status"] != "VALID_SAMPLE":
            gaps.append({"scope": code, "day": str(day), "reason": "ARCHIVED_TRANSFER_REPLY_UNAVAILABLE"})
            continue
        raw = packet[record["original"]]
        reply = json.loads(raw)
        quotes = {(venue, stamp): close_rows(packet[f"official/{venue}-{stamp}.json"], stamp, venue)
                  for venue in ("TWSE", "TPEx") for stamp in (before, day)}
        status, episodes = transfer_proof(code, day, quotes,
                                          Sample("VALID_SAMPLE", raw=raw, rows=tuple(reply["data"])))
        if status != "PASS_TRANSFER_PRICE_AND_VENUE_SAMPLE":
            gaps.append({"scope": code, "day": str(day), "reason": status})
            continue
        result.append({"dataset": PRICE_DATASET, "scope": code, "mode": "entity_range", "lane": "B01",
                       "parameters": params, "query_key": query_key(params, "entity_range"),
                       "decision": "REUSE_VERIFIED_ARCHIVE", "confirmed_from": str(before),
                       "confirmed_through": str(day), "expected_dates": [str(before), str(day)],
                       "market_episodes": episodes, "new_source_requests": 0,
                       "evidence": evidence | {"member": record["original"], "raw_sha256": digest(raw),
                                                "raw_bytes": len(raw), "packed_bytes": len(gzip.compress(raw, mtime=0))}})
    return result, gaps


def symbol_months(calendar, calendar_evidence, lifetime, correction, registry, reusable):
    """Generate bounded entity queries backed by issuer and independent stock-day evidence."""
    lifetimes = defaultdict(list)
    for item in lifetime["listing_intervals"] + correction["new_verified_intervals"]:
        if item["market"] == "TPEx" and item.get("proof"):
            lifetimes[item["code"]].append(item)
    contradictory = {row["prior_interval"]["code"] for row in correction["prior_interval_conflicts"]}
    contradicted_years = set(map(int, correction["historical_year_contradictions"]))
    covered = defaultdict(list)
    for item in reusable:
        params = item["params"] if "params" in item else item["parameters"]
        if params["dataset"] == PRICE_DATASET:
            covered[params["data_id"]].append((params["start_date"], params["end_date"]))
    native = registry.get("native_candidate") or {}
    bounded_contract = registry.get("contract_status") == "READY_BOUNDED" and (
        native.get("query_mode") in {"symbol_range", "entity_range"}
        and tuple(native.get("key_fields", ())) == ("date", "stock_id")
    )
    rows, gaps = [], []
    for member, raw in sorted(calendar.items(), reverse=True):
        match = re.fullmatch(r"official/tpex-(\d{4})-(\d{4}-\d{2})\.json", member)
        if not match:
            continue
        code, month = match.groups()
        first = date.fromisoformat(month + "-01")
        scope = {"dataset": PRICE_DATASET, "market": "TPEx", "scope": code, "observation_month": month}
        if first.year >= 2026:
            continue
        if code in contradictory or first.year in contradicted_years:
            gaps.append(scope | {"reason": "CONFLICTING_TPEX_HISTORICAL_EVIDENCE"})
            continue
        observations = monthly_rows(raw, Source(first, code))
        if not observations or not all(row["traded"] and row["positive_price"] for row in observations.values()):
            gaps.append(scope | {"reason": "FINMIND_TREATMENT_OF_EMPTY_OR_ZERO_VOLUME_UNPROVEN"})
            continue
        last = (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        active = [episode for episode in lifetimes[code] if episode["start"] <= str(min(observations)) and
                  str(max(observations)) < episode["end_exclusive"]]
        if len(active) != 1 or not bounded_contract:
            gaps.append(scope | {"reason": "HISTORICAL_IDENTITY_OR_NATIVE_RANGE_CONTRACT_UNPROVEN"})
            continue
        episode, = active
        lower = max(first, date.fromisoformat(episode["start"]))
        upper = min(last, date.fromisoformat(episode["end_exclusive"]) - timedelta(days=1))
        for start, end in subtract_coverage(str(lower), str(upper), covered[code]):
            dates = [str(day) for day in sorted(observations) if start <= str(day) <= end]
            if not dates:
                continue
            params = {"dataset": PRICE_DATASET, "data_id": code, "start_date": start, "end_date": end}
            rows.append(scope | {"mode": "entity_range", "lane": "B01", "parameters": params,
                                 "query_key": query_key(params, "entity_range"), "decision": "READY_BOUNDED",
                                 "contract_version": registry["contract_version"], "expected_dates": dates,
                                 "confirmed_from": dates[0], "confirmed_through": dates[-1],
                                 "completeness_rule": {"method": "expected_keys", "keys": [[d, code] for d in dates]},
                                 "new_source_requests": 1, "evidence": calendar_evidence | {
                                     "member": member, "raw_sha256": digest(raw), "issuer_episode": episode}})
    return rows, gaps


def query_mode_comparison(history, transfers, raw_inputs):
    files, summary, source = raw_inputs
    day = str(GAP_DAY)
    bulk = next((entry for entry in summary["backer_requests"] if entry["request"] == {
        "dataset": PRICE_DATASET, "start_date": day, "end_date": day,
    } and entry["status"] == "VALID_SAMPLE" and entry.get("original")), None)
    overall = {"mode": "single_day_market", "state": "HOLD", "reason": "VENUE_AND_EXCLUSION_CONSTRAINTS_UNPROVEN"}
    if bulk:
        raw = files[bulk["original"]]
        status, closes, _ = close_rows(files[f"official/TPEx-{day}.json"], GAP_DAY, "TPEx")
        prices = {row.get("stock_id"): row.get("close") for row in json.loads(raw)["data"]}
        tpex = {code for code, close in closes.items() if close is not None}
        overall |= {"observed_request": bulk["request"], "observed_sample_bytes": len(raw),
                    "illustrative_requests_per_market_day": 1,
                    "unmatched_official_tpex_symbols": len(tpex - prices.keys()) if status == "VALID_OFFICIAL_DAY" else None,
                    "native_symbol_comparison": "historical complete daily reference, not a provider guarantee",
                    "evidence": source | {"member": bulk["original"], "raw_sha256": digest(raw)}}
    return {"plans": [overall, {"mode": "entity_range", "state": "SELECTED_FOR_RECONCILED_SYMBOL_WINDOWS",
                                 "new_requests": len(history), "archive_windows_reused": len(transfers)},
                      {"mode": "event", "state": "PER_DATASET_ONLY", "new_requests": None},
                      {"mode": "snapshot", "state": "CAPTURE_PERIOD_ONLY", "historical_requests": None}],
            "same_scope_parallel_modes": False}


def run_v21(ctx):
    registry = ctx.read("DATASET_REGISTRY.json")
    rules = {row["dataset"]: row for row in registry["rows"]}
    old, reuse, receipts, baseline_calls = prior_index(ctx)
    issues, storage_plan = [], []
    snapshot, transfers, proposed = None, [], []
    try:
        snapshot = macro_observations(ctx.access, receipts)
    except (ValueError, OSError, KeyError, TypeError, StopIteration) as exc:
        issues.append({"dataset": BUSINESS, "reason": "V21_ARCHIVE_" + type(exc).__name__})
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            paid, calendar, lifetime, correction = list(pool.map(
                lambda run: historical_packet(ctx.access, run),
                (EVIDENCE_RUN, CALENDAR_RUN, LIFETIME_RUN, TPEx_HISTORICAL_RECONCILIATION),
            ))
        transfers, held = transfer_observations(*paid)
        proposed, unresolved = symbol_months(calendar[0], calendar[2] | {
            "lifetime_source": lifetime[2], "subsequent_reconciliation": correction[2],
        }, lifetime[1], correction[1], rules[PRICE_DATASET], [*receipts, *transfers])
        issues.extend(held + unresolved)
        storage_plan = query_mode_comparison(proposed, transfers, paid)
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as exc:
        issues.append({"dataset": PRICE_DATASET, "reason": "V21_HISTORICAL_EVIDENCE_" + type(exc).__name__})

    output, index, parts = {}, [], defaultdict(list)
    candidates = list(filter(None, [snapshot, *transfers, *proposed]))
    ids = [candidate["query_key"] for candidate in candidates]
    if len(set(ids)) != len(ids):
        raise ValueError("V21_HISTORICAL_DUPLICATE_QUERY")
    for row in sorted(candidates, key=lambda item: item["confirmed_through"], reverse=True):
        # Group by the original query's year, keeping a multi-year archived response whole.
        key = row["dataset"], row["scope"], row["parameters"]["start_date"][:4], row["mode"]
        parts[key].append(row)
    for key, group in parts.items():
        for number, rows in enumerate(bounded_shards(group)):
            name = "V21_HISTORICAL_MANIFEST_" + "__".join((*key, f"{number:03d}")) + ".json"
            shard = {"revision": HISTORY_REVISION, "release": "HOLD", "rows": rows}
            output[name] = shard
            index.append({"name": name, "sha256": digest(encode(shard)), "windows": len(rows)})

    base = ctx.prior("HISTORICAL_COVERAGE.json")
    observed = {row["dataset"]: row.get("observed") for row in base["rows"]}
    waiting, summaries = [], []
    for row in registry["rows"]:
        name = row["dataset"]
        accepted = [item for item in candidates if item["dataset"] == name]
        # A sample, an approved subpartition, and a documented date do not prove a universe boundary.
        universe = row.get("full_history_universe_evidence")
        proved = bool(universe and universe.get("confirmed") and universe.get("complete_partition_keys"))
        if row["decision"] != "EXCLUDED" and not proved:
            waiting.append({"dataset": name, "reason": "UNCONFIRMED_HISTORY_OR_CURRENT_SOURCE_BOUNDARY"})
        summaries.append({"dataset": name, "decision": row["decision"], "contract_status": row.get("contract_status"),
                          "documented_boundary": universe.get("documented_boundary") if universe else None,
                          "observed_samples": observed.get(name),
                          "confirmed_bounded_partitions": [{key: item[key] for key in (
                              "scope", "mode", "confirmed_from", "confirmed_through", "query_key", "decision",
                          )} for item in accepted], "full_universe_verified": proved})
    findings = issues + waiting
    total = None if findings else sum(len(row["full_history_universe_evidence"]["complete_partition_keys"])
                                      for row in registry["rows"] if row["decision"] != "EXCLUDED")
    output["MANIFEST_INDEX.json"] = old | {
        "revision": HISTORY_REVISION, "scope": "INHERITED_2026_AND_EVIDENCED_HISTORICAL_PARTITIONS",
        "shards": [ref | {"inherited_from": PRIOR_INTEGRATED_RUN} for ref in old["shards"]],
        "historical_shards": index, "historical_candidate_windows": len(candidates),
        "historical_ready_windows": len(proposed), "historical_archive_windows": len(candidates) - len(proposed),
        "historical_covered_observations": len(snapshot["logical_period_offsets"]) if snapshot else 0,
        "bounded_request_floor": baseline_calls + sum(row["new_source_requests"] for row in candidates),
        "full_history_denominator": total, "query_mode_comparison": storage_plan,
        "full_download_release": "HOLD", "production_dispatchable": 0,
    }
    output["REUSE_MAP.json"] = reuse | {"shards": [ref | {"inherited_from": PRIOR_INTEGRATED_RUN} for ref in reuse["shards"]],
                                       "historical_archive_references": [row for row in candidates if not row["new_source_requests"]]}
    output["HISTORICAL_COVERAGE.json"] = {"rows": summaries, "historical_shards": index}
    output["COVERAGE_GAPS.json"] = {"findings": findings, "full_history_denominator": total,
                                   "missing_scope_datasets": len(waiting), "query_mode_comparison": storage_plan}
    return {"status": "HOLD" if findings else "PASS", "findings": ["V21_HISTORICAL_SCOPE_UNRESOLVED"] if findings else [],
            "candidate_windows": old["candidate_windows"], "historical_candidate_windows": len(candidates),
            "historical_ready_windows": len(proposed), "historical_archive_windows": len(candidates) - len(proposed),
            "full_history_denominator": total, "outputs": output}
