"""Build native historical review shards and reconcile existing production receipts read-only."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from itertools import chain
from .bulk_manifest import SCHEMA, compile_window, optimize, planned_capacity, window_months
from .drive_pilot import FOLDER, ROOT, ensure
from .integration_pilot import digest, encode, verify_private_folder
from .production_download import pending_group
from .release_rehearsal import require
from .test_archive import GitHub
from .v2_coverage import PRICE_DATASET, bounded_shards, historical_packet
from .v2_recovery import list_children, read_active
from .v2_registry import NativeContract
from .tpex_history import EVIDENCE_RUN
from .v24_sources import previous_bulk

SOURCES = ("TWSE", "TPEx", "TAIFEX", "TDCC", "MOPS", "NDC", "FinMind_Backer")
HISTORY_SCOPE = "FINMIND_V4_PRICE_NATIVE_ROWS"


def bronze_rules(access):
    bronze = ensure(ROOT, "20_BRONZE", FOLDER, access, create=False)
    year = ensure(bronze, "2026", FOLDER, access, create=False)
    verify_private_folder(bronze, access)
    verify_private_folder(year, access)
    folders = {}
    for source in SOURCES:
        child = ensure(year, source, FOLDER, access, create=False)
        verify_private_folder(child, access)
        children = list_children(child, access)
        months = sorted(name for name, meta in children.items() if name.startswith("2026-") and
                        meta.get("mimeType") == FOLDER and meta.get("shared") is False)
        require(months, "OFFICIAL_EXISTING_SOURCE_MONTH_PATTERN")
        folders[source] = {"id": child, "sample_month_folders": months}
    old_year = ensure(bronze, "1982", FOLDER, access, create=False)
    old_source = ensure(old_year, "FinMind_Backer", FOLDER, access, create=False)
    old_month = ensure(old_source, "1982-01", FOLDER, access, create=False)
    for item in (old_year, old_source, old_month):
        verify_private_folder(item, access)
    return {"path_template": "C1_0.98/20_BRONZE/YYYY/FinMind_Backer/YYYY-MM/", "folder_id": bronze,
            "existing_2026_sources": folders, "existing_1982_anchor_id": old_month,
            "anchor": "query_start_local_date_or_snapshot_obtained_at_Asia/Taipei", "no_scope_subfolders": True,
            "keep_received_full_market_payload": True, "retained_venues": ["TWSE", "TPEx", "Emerging"],
            "study_venues": ["TWSE", "TPEx"], "independent_emerging_tasks": False,
            "cross_period_copy_count": 1, "pagination_keeps_each_http_body": True,
            "phases": ["DAILY_OR_LOWER", "APPROVED_NON_INTRADAY_EVENTS", "NATIVE_SNAPSHOTS"],
            "transport_and_storage_hashes": ["decoded_http_body_sha256", "stored_gzip_sha256"]}


def verify_shards(ctx, refs, key, count_field):
    def one(ref):
        body = ctx.prior(ref["name"])
        require(len(body[key]) == ref[count_field] and digest(encode(body)) == ref["sha256"],
                "V24_INHERITED_SHARD_DIGEST")
        return body[key]
    with ThreadPoolExecutor(max_workers=5) as pool:
        return list(chain.from_iterable(pool.map(one, refs)))


def active_snapshot(ctx):
    group = pending_group(ctx.plan, ctx.supplement, ctx.master["state"])
    if group is None:
        return {"group": None, "tasks": [], "dispositions": {}, "recorded_states": {}}
    return read_active(ctx, group, list_children(ctx.campaign, ctx.access))[0]


def offline(ctx):
    index, reuse = ctx.prior("MANIFEST_INDEX.json"), ctx.prior("REUSE_MAP.json")
    previous = ctx.prior("FINAL_READINESS.json")["production_snapshot_after"]
    locked = ctx.read("INPUT_LOCK.json")["production"]
    require(locked == previous and ctx.master["sequence"] == locked["master_sequence"],
            "V24_PRODUCTION_ADVANCED_REQUIRES_RECEIPT_RECONCILIATION")
    active = active_snapshot(ctx)
    receipts = verify_shards(ctx, reuse["shards"], "receipts", "receipts")
    known = {(r["legacy_task_id"], r["file_id"], r["proof"]["raw_sha"]) for r in receipts}
    require(len(receipts) == len(known) == reuse["receipt_count"], "V24_RECEIPT_SNAPSHOT_COUNT")
    active_ids = {(r["task_id"], r["receipt"]["file_id"], r["receipt"]["proof"]["raw_sha"])
                  for r in active["tasks"] if r["disposition"] == "VERIFIED_RECEIPT"}
    require(active_ids <= known and len(active_ids) == index["aggregation"]["active_verified_receipts_not_in_master"],
            "V24_ACTIVE_RECEIPT_COUNT_OR_CONTENT")
    rows = verify_shards(ctx, index["historical_shards"], "rows", "windows")
    require(len(rows) == index["historical_candidate_windows"], "V24_HISTORICAL_INVENTORY")
    blockers = []
    try:
        old_bulk = previous_bulk(GitHub())
    except (OSError, RuntimeError, ValueError, TypeError, KeyError) as error:
        old_bulk = {"record_status": "HOLD_LOG_READBACK", "category": type(error).__name__}
        blockers.append("OLD_BULK_DIAGNOSTIC_READBACK")
    # Reuse the independent market-day original, without a new Backer call.
    try:
        packet, report, evidence = historical_packet(ctx.access, EVIDENCE_RUN)
        from .tpex_history import GAP_DAY

        day = str(GAP_DAY)
        original = next(item for item in report["backer_requests"] if item.get("original") and item["request"] == {
            "dataset": PRICE_DATASET, "start_date": day, "end_date": day,
        })
        raw = packet[original["original"]]
        old_bulk["verified_private_original"] = evidence | {"member": original["original"], "sha256": digest(raw),
                                                            "raw_bytes": len(raw), "day": day}
        old_bulk["prior_market_comparison"] = index.get("query_mode_comparison")
    except (OSError, RuntimeError, ValueError, TypeError, KeyError, StopIteration) as error:
        old_bulk["private_original_hold"] = type(error).__name__
        blockers.append("PRIVATE_BULK_DIAGNOSTIC_READBACK")
    if active["dispositions"].get("HOLD"):
        blockers.append("ACTIVE_BATCH_READBACK_HAS_UNRESOLVED_ITEMS")
    return ("HOLD" if blockers else "PASS"), {
        "HISTORICAL_INPUT.json": {"prior_manifest_sha256": digest(encode(index)), "rows": rows,
                                  "legacy_receipt_count": len(receipts), "active_verified": len(active_ids),
                                  "complete_live_master_coverage_checked": True,
                                  "known_active_request_keys": [r["task_id"] for r in active["tasks"]],
                                  "findings": blockers},
        "ACTIVE_BATCH_RECONCILIATION.json": active,
        "BULK_BENCHMARK_EVIDENCE.json": old_bulk,
        "BRONZE_STORAGE_RULE.json": bronze_rules(ctx.access),
        "B09_REPAIR_PLAN.json": ctx.legacy("B09_REPAIR_PLAN.json"),
    }


def issuer_for(row):
    episode = row["evidence"]["issuer_episode"]
    # Incarnation is stable across incidental evidence refreshes; an unproven transfer gets another subject.
    return f"{episode['market']}:{row['scope']}:{episode['start']}:{episode.get('issuer', '')}"


def merged_ranges(rows, *, maximum_native_days=260):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["dataset"], row["scope"], issuer_for(row), row["contract_version"])].append(row)
    partitions = []
    for key, group in sorted(groups.items()):
        run, days = [], 0
        for row in sorted(group, key=lambda value: value["parameters"]["start_date"]):
            start = date.fromisoformat(row["parameters"]["start_date"])
            count = len(row["expected_dates"])
            if run and (start != date.fromisoformat(run[-1]["parameters"]["end_date"]) + timedelta(days=1) or
                        days + count > maximum_native_days):
                partitions.append((key, run))
                run, days = [], 0
            run.append(row)
            days += count
        if run:
            partitions.append((key, run))
    return partitions


def compile_ranges(rows, registry, known, verified_receipts):
    price = registry[PRICE_DATASET]
    rule = NativeContract(**price["schema"])
    require(price["state"] == "CONTRACT_READY" and "ENTITY_RANGE" in price["mode_contracts"] and
            tuple(rule.key_fields) == ("date", "stock_id"), "V24_PRICE_RANGE_CONTRACT")
    scoped, benchmarks = [], []
    known_by_symbol = defaultdict(list)
    for row in verified_receipts:
        if row["params"]["dataset"] == PRICE_DATASET:
            p = row["params"]
            known_by_symbol[p.get("data_id")].append((p["start_date"], p["end_date"]))

    for (dataset, code, owner, version), group in merged_ranges(rows):
        def native_window(piece):
            earliest, latest = piece[0]["parameters"]["start_date"], piece[-1]["parameters"]["end_date"]
            params = {"dataset": dataset, "data_id": code, "start_date": earliest, "end_date": latest}
            observations = [{"subject": owner, "native_key": [day, code], "period": day, "venue": "TPEx"}
                            for row in piece for day in row["expected_dates"]]
            upstream = [{"query_key": r["query_key"], "raw_sha256": r["evidence"]["raw_sha256"],
                         "official_archive_member": r["evidence"]["member"]} for r in piece]
            return compile_window(rule=rule, mode="ENTITY_RANGE", params=params, lane=piece[0]["lane"], venue="TPEx",
                                  version=HISTORY_SCOPE, completeness={"method": "expected_keys",
                                  "keys": [e["native_key"] for e in observations], "evidence": "OFFICIAL_POSITIVE_DAILY_ROWS"},
                                  coverage=observations, evidence={"official_partitions": upstream,
                                  "prior_contract_version": version, "official_archive": piece[0]["evidence"]["manifest_sha256"]},
                                  raw_limit=512 * 1024, legacy_query=piece[0]["query_key"] if len(piece) == 1 else None)
        old_windows, combined = [native_window([row]) for row in group], native_window(group)
        candidates = {"M2_NATIVE_RANGE": [combined], "M2_LEGACY_MONTHS": old_windows}
        for name, windows in list(candidates.items()):
            if any(lo <= item["end"] and item["start"] <= hi
                   for item in windows for lo, hi in known_by_symbol[code]):
                del candidates[name]
        selected, windows, results = optimize(candidates, combined["coverage"]["coverage_ids"], known_requests=known)
        scoped.extend(windows)
        benchmarks.append({"dataset": dataset, "window": [group[0]["confirmed_from"], group[-1]["confirmed_through"]],
                           "old_month_windows": len(group), "selected": selected,
                           "new_native_requests": len(windows), "options": results,
                           "no_overlap_with_inherited_symbol_ranges": bool(selected)})
    return scoped, benchmarks


def finalize(ctx):
    inputs = ctx.read("HISTORICAL_INPUT.json")
    source_rows = inputs["rows"]
    old_idx, old_reuse = ctx.prior("MANIFEST_INDEX.json"), ctx.prior("REUSE_MAP.json")
    require(inputs["prior_manifest_sha256"] == digest(encode(old_idx)), "V24_PRIOR_INDEX_CHANGED")
    registry = {row["dataset"]: row for row in ctx.read("DATASET_REGISTRY.json")["rows"]}
    inherited = verify_shards(ctx, old_reuse["shards"], "receipts", "receipts")
    existing = {row["query_key"] for row in inherited}
    reusable_history = [row for row in source_rows if row["decision"] == "REUSE_VERIFIED_ARCHIVE"]
    for row in reusable_history:
        existing.add(row["query_key"])
    subject_rows = [row for row in source_rows if row["decision"] == "READY_BOUNDED"]
    windows, options = compile_ranges(subject_rows, registry, existing, inherited)
    output = {}
    shards = []
    selected = sorted(windows, key=lambda item: (item["end"], item["task_id"]), reverse=True)
    for number, rows in enumerate(bounded_shards(selected)):
        name = f"V24_MANIFEST_TPEX_{number:04d}.json"
        body = {"schema_version": SCHEMA, "review_only": True, "rows": rows}
        output[name] = body
        shards.append({"name": name, "sha256": digest(encode(body)), "windows": len(rows)})
    prior_gaps = ctx.prior("COVERAGE_GAPS.json")
    pending = [{"dataset": name, "documented_dataset_start": r["source_dates_documented_from"],
                "latest_source_publication": r["latest_published"], "market_lifetimes_and_native_periods": "PARTIAL",
                "reason": "SOURCE_NATIVE_UNIVERSE_OR_PUBLICATION_BOUNDARY_UNPROVEN"}
               for name, r in sorted(registry.items()) if r["state"] != "EXCLUDED"]
    for finding in options:
        if finding["selected"] is None:
            pending.append({"dataset": finding["dataset"], "window": finding["window"],
                            "reason": "EXISTING_QUERY_OR_PARTIAL_COVERAGE_OVERLAP"})
    hist = ctx.prior("HISTORICAL_COVERAGE.json")
    before = {row["dataset"]: row for row in hist["rows"]}
    dates = defaultdict(list)
    for item in selected:
        dates[item["dataset"]].append([item["start"], item["end"]])
    archive_intervals = [{"dataset": row["dataset"], "scope": row["scope"], "query_key": row["query_key"],
                          "months": window_months(row["confirmed_from"], row["confirmed_through"]),
                          "confirmed_from": row["confirmed_from"], "confirmed_through": row["confirmed_through"],
                          "evidence": row["evidence"], "new_http_requests": 0} for row in reusable_history]
    historical = {"rows": [{"dataset": name, "state": r["state"], "documented_start": r["source_dates_documented_from"],
                            "documented_as_dataset_scope_not_entity_start": True,
                            "historical_boundaries_verified": False, "latest_published": r["latest_published"],
                            "source_observed": before.get(name, {}).get("observed_samples"),
                            "native_prepared_intervals": dates[name]} for name, r in sorted(registry.items())],
                  "archived_verified_intervals": archive_intervals, "research_venues": ["TWSE", "TPEx"],
                  "attached_emerging_originals_allowed": True, "six_source_indexes_reused": list(SOURCES[:-1]),
                  "full_history_denominator": None}
    scope_label = "TPEx_PRICE_5301_5302_EVIDENCED_PERIODS"
    output |= {
        "HISTORICAL_UNIVERSE.json": historical,
        "HISTORICAL_COVERAGE.json": {"rows": historical["rows"], "reused": archive_intervals,
                                     "legacy_2026_scope": old_idx["candidate_windows"]},
        "MANIFEST_INDEX.json": {"schema_version": SCHEMA, "task_id": ctx.task_id, "parent_sha256": digest(encode(old_idx)),
                                "scope": scope_label, "prepared_windows": len(selected), "dispatch_shards": shards,
                                "reused_historical_archives": len(archive_intervals),
                                "bounded_historical_native_observations": sum(len(w["coverage"]["coverage_ids"]) for w in selected),
                                "frozen_2026_candidate_windows": old_idx["candidate_windows"],
                                "frozen_2026_manifest_index": {"run": 46074623286, "sha256": digest(encode(old_idx)),
                                                              "dispatchable": False, "lease_status": "UNCHANGED"},
                                "full_history_denominator": None, "full_download_release": "HOLD",
                                "production_dispatchable": 0, "forecast": planned_capacity(selected)},
        "REUSE_MAP.json": {"legacy": old_reuse, "legacy_verified_receipts": inputs["legacy_receipt_count"],
                           "active_verified_receipts_not_in_master": inputs["active_verified"],
                           "archive_intervals": archive_intervals,
                           "request_vs_coverage_identity": "SEPARATE_IDS_AND_EXACT_SOURCE_PROOF",
                           "active_batch_reserved_legacy_ids": inputs["known_active_request_keys"]},
        "COVERAGE_GAPS.json": {"full_history_denominator": None, "unknown_partitions": pending,
                               "independent_source_gaps": [r for r in prior_gaps["findings"] if
                                                           r["reason"] != "UNCONFIRMED_HISTORY_OR_CURRENT_SOURCE_BOUNDARY"],
                               "market_day_overlap_policy": "PREFER_NONOVERLAPPING_QUERY_OTHERWISE_SEPARATE_APPROVAL",
                               "remaining_legacy_blocked": ctx.master["state"]["totals"].get("BLOCKED", 0)},
        "QUERY_MODE_BENCHMARK.json": {"known_bounded_partition": scope_label, "evaluated_equivalent_ranges": options,
                                      "legacy_monthly_requests": len(subject_rows), "native_range_requests": len(selected),
                                      "market_day": ctx.read("BULK_BENCHMARK_EVIDENCE.json"),
                                      "market_range": "DOCUMENTED_PER_DATASET_WITHOUT_VERIFIED_NONOVERLAPPING_PARTITION",
                                      "series_range": "NDC_537_MONTH_ARCHIVE_REUSED", "snapshot": "CAPTURE_TIME_ANCHOR",
                                      "event_range": "NO_DISPATCH_WITHOUT_INDEPENDENT_EVENT_OR_CONFIRMED_EMPTY_EVIDENCE"},
    }
    return ("HOLD" if pending or inputs["findings"] else "PASS"), output
