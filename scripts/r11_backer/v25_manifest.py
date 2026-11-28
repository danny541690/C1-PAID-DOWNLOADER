"""Evidence-bounded native history selectors for all eligible catalog contracts."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from itertools import chain

from .bulk_manifest import SCHEMA, MODES, read_manifest, request_id
from .integration_pilot import digest, encode
from .release_rehearsal import require
from .tpex_closeout import LIFETIME_RUN
from .v2_coverage import TPEx_HISTORICAL_RECONCILIATION, historical_packet, subtract_coverage
from .v24_coverage import active_snapshot

SOURCE_REVISION = "FinMind_v4_native_history_v25"
M1_GRAINS = {"daily", "daily_observation"}


def checked_rows(ctx, refs, key, count_field, read):
    def read_one(ref):
        payload = read(ref["name"])
        require(digest(encode(payload)) == ref["sha256"] and len(payload[key]) == ref[count_field], "V25_PRIOR_SHARD_BINDING")
        return payload[key]
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(chain.from_iterable(pool.map(read_one, refs)))


def historical_axes(ctx):
    lifetime, corrected = (historical_packet(ctx.access, run) for run in (LIFETIME_RUN, TPEx_HISTORICAL_RECONCILIATION))
    old, new = lifetime[1], corrected[1]
    contradicted = {row["prior_interval"]["code"] for row in new["prior_interval_conflicts"]}
    subjects = {}
    for row in old["listing_intervals"] + new["new_verified_intervals"]:
        if row["market"] not in ("TWSE", "TPEx") or row["code"] in contradicted or not row.get("proof"):
            continue
        key = (row["market"], row["code"], row["start"], row["end_exclusive"])
        subjects[key] = {"market": row["market"], "id": row["code"], "start": row["start"], "end_exclusive": row["end_exclusive"],
                         "issuer": row.get("issuer"), "proof": row["proof"]}
    return {"issuer_intervals": [subjects[k] for k in sorted(subjects)], "official_reconciliation_sources": [lifetime[2], corrected[2]],
            "known_conflicted_issuer_codes": sorted(contradicted), "full_universe_known": False}


def offline(ctx):
    inputs = ctx.prior("HISTORICAL_INPUT.json")
    previous = ctx.prior("MANIFEST_INDEX.json")
    active = active_snapshot(ctx)
    registry = ctx.legacy("CONTRACT_EVIDENCE.json")
    require(len(registry["native_archive_readback"]) == 259, "V25_ORIGINAL_EVIDENCE_COUNT")
    frozen = ctx.prior("ACTIVE_BATCH_RECONCILIATION.json")
    require(active == frozen, "V25_LIVE_ACTIVE_TASK_RECEIPTS_CHANGED")
    index = ctx.prior("REUSE_MAP.json")
    receipt_refs = index["legacy"]["shards"]
    receipts = checked_rows(ctx, receipt_refs, "receipts", "receipts", ctx.v20)
    active_receipts = {(r["task_id"], r["receipt"]["file_id"], r["receipt"]["proof"]["raw_sha"])
                       for r in active["tasks"] if r["disposition"] == "VERIFIED_RECEIPT"}
    all_receipts = {(r["legacy_task_id"], r["file_id"], r["proof"]["raw_sha"]) for r in receipts}
    require(len(receipts) == len(all_receipts) == index["legacy_verified_receipts"] and active_receipts <= all_receipts,
            "V25_LATEST_RECEIPT_INVENTORY")
    native_history = checked_rows(ctx, previous["dispatch_shards"], "rows", "windows", ctx.prior)
    findings = []
    try:
        axes = historical_axes(ctx)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
        axes = {"issuer_intervals": [], "official_reconciliation_sources": [], "full_universe_known": False}
        findings.append("ISSUER_ARCHIVES_" + type(error).__name__)
    past_calendar = sorted({day for table in (ctx.plan, ctx.supplement) if table for markets in table["months"].values()
                            for days in markets.values() for day in days})
    source_dates = defaultdict(list)
    all_market_queries = defaultdict(set)
    # Preserve raw-witness boundaries; a test date without returned rows never sets a publication bound.
    for row in registry["stored_samples"]:
        params = row.get("request") or {}
        if row["result"] not in {"VALID_SAMPLE", "PASS_NATIVE_SAMPLE"}:
            continue
        observations = [str(date.fromisoformat(day)) for day in row.get("observed_dates", ()) if isinstance(day, str) and len(day) == 10]
        if observations:
            source_dates[row["dataset"]].append({"observations": observations, "parameters": params,
                                                "raw_sha256": row.get("raw_sha256"), "source_run": row["run"]})
        if not params.get("data_id") and params.get("start_date") and params.get("end_date", params["start_date"]) == params["start_date"]:
            all_market_queries[row["dataset"]].add(params["start_date"])
    old_markets = ctx.prior("BULK_BENCHMARK_EVIDENCE.json")
    for dataset, result in old_markets.get("bulk", {}).items():
        if any(e["market_status"] == "VALID_SAMPLE" for e in result["variants"]):
            all_market_queries[dataset].add(old_markets["day"])
    old_gaps = ctx.prior("COVERAGE_GAPS.json")
    blocked = ctx.prior("B09_REPAIR_PLAN.json")
    return ("HOLD" if findings else "PASS"), {
        "HISTORY_AXES_INPUT.json": axes | {"certified_2026_sessions": past_calendar, "source_witnesses": source_dates,
            "existing_market_query_days": {k: sorted(v) for k, v in all_market_queries.items()},
            "read_only_evidence_findings": findings, "known_legacy_scope": previous["frozen_2026_candidate_windows"],
            "active_batch_manifest_sha256": active["manifest_sha"], "legacy_receipt_refs": receipt_refs,
            "legacy_receipt_count": len(receipts), "active_verified": len(active_receipts),
            "reference_history_sha256": digest(encode(inputs)), "native_compilable_legacy_windows": native_history},
        "ACTIVE_BATCH_RECONCILIATION.json": active,
        "B09_QUEUE.json": {"independent_source_gaps": old_gaps["independent_source_gaps"],
                           "legacy_blocked": blocked["records"], "new_dispatch_authorized": False},
        "BRONZE_STORAGE_RULE.json": ctx.prior("BRONZE_STORAGE_RULE.json"),
    }


def years(first, last):
    while first <= last:
        upper = min(last, date(first.year, 12, 31))
        yield str(first), str(upper)
        first = upper + timedelta(days=1)


def query_parameters(partition, axes):
    """Expand a compact manifest selector into exact HTTP arguments, without I/O."""
    dataset, mode = partition["dataset"], partition["mode"]
    first, last = [date.fromisoformat(day) for day in partition["source_interval"]] if "source_interval" in partition else (None, None)
    if mode == "SNAPSHOT":
        # An existing snapshot is preserved. A subsequent capture needs an acquired-at identity.
        return
    if mode == "MARKET_DAY":
        excluded = set(axes["existing_market_query_days"].get(dataset, ()))
        for day in axes["trading_dates"]:
            if partition["source_interval"][0] <= day <= partition["source_interval"][1] and day not in excluded:
                yield {"dataset": dataset, "start_date": day}
        return
    known = {digest(encode(row["parameters"])) for row in axes.get("source_witnesses", {}).get(dataset, [])}
    if partition["selector"] == "OFFICIAL_ISSUER_VALIDITY":
        scopes = {(row["id"], max(first, date.fromisoformat(row["start"])),
                   min(last, date.fromisoformat(row["end_exclusive"]) - timedelta(days=1)))
                  for row in axes["issuer_intervals"]}
    elif partition["selector"] == "OBSERVED_ENTITY_BOUNDS":
        scopes = {(row["id"], date.fromisoformat(row["first"]), date.fromisoformat(row["last"]))
                  for row in partition["observed_entity_bounds"]}
    else:
        scopes = {(key, first, last) for key in partition["observed_data_ids"]}
        if not scopes:
            scopes = {(None, first, last)}
    accepted = axes["legacy_receipt_intervals"].get(dataset, {})
    for ident, begin, end in sorted(scopes):
        previous = accepted.get(ident or "", ())
        for lo, hi in years(begin, end):
            remaining = subtract_coverage(lo, hi, previous) if previous else [[lo, hi]]
            for start, finish in remaining:
                params = {"dataset": dataset, "start_date": start, "end_date": finish, **({"data_id": ident} if ident else {})}
                if digest(encode(params)) not in known:
                    yield params


def build_partition(row, axes, archive_by_dataset):
    name = row["dataset"]
    modes = set(row["mode_contracts"])
    samples = axes["source_witnesses"].get(name, [])
    observations = sorted({day for src in samples for day in src["observations"]})
    witnessed = row.get("v25_witness", {})
    if witnessed.get("observed_dates"):
        observations = sorted(set(observations + witnessed["observed_dates"]))
    frozen_archive = archive_by_dataset.get(name, [])
    if "SNAPSHOT" in modes and row["grain"] == "snapshot":
        result = {"dataset": name, "mode": "SNAPSHOT", "selector": "CAPTURE_TIME", "status": "WITNESS_REUSE" if witnessed else "SNAPSHOT_PLANNED",
                  "capture_evidence": witnessed or None, "scheduled_requests": 0, "completeness_contract": row["completeness_methods"],
                  "remaining_unknown": ["HISTORICAL_SNAPSHOTS_EXIST_ONLY_WHERE_ORIGINAL_ALREADY_ARCHIVED"]}
        return result, []
    source_start = sorted(set(row["source_dates_documented_from"]))
    if not observations:
        return None, [{"dataset": name, "reason": "NO_WITNESSED_LATEST_NATIVE_OBSERVATION"}]
    sample_only = not source_start and not frozen_archive
    documented_start = (source_start[0] if source_start else
                        min(r["confirmed_from"] for r in frozen_archive) if frozen_archive else observations[0])
    latest = max([observations[-1], *(r["confirmed_through"] for r in frozen_archive)])
    if documented_start > latest:
        return None, [{"dataset": name, "reason": "DOCUMENTED_AND_WITNESSED_BOUNDS_CONFLICT"}]
    stock_universe = row["schema"].get("identity_field") == "stock_id" and name.startswith("TaiwanStock")
    data_ids = sorted({r["parameters"]["data_id"] for r in samples if r["parameters"].get("data_id")})
    if witnessed.get("parameters", {}).get("data_id"):
        data_ids = sorted(set(data_ids) | {witnessed["parameters"]["data_id"]})
    bounds = defaultdict(set)
    for sample in samples:
        if ident := sample["parameters"].get("data_id"):
            bounds[ident].update(sample["observations"])
    if witnessed.get("parameters", {}).get("data_id"):
        bounds[witnessed["parameters"]["data_id"]].update(witnessed.get("observed_dates", ()))
    observed_entities = [{"id": ident, "first": min(days), "last": max(days)} for ident, days in sorted(bounds.items()) if days]
    unknown = ["UNOBSERVED_LATER_PUBLICATIONS", "VERIFIED_FULL_NATIVE_HISTORY_UNIVERSE"]
    if sample_only:
        unknown.append("OFFICIAL_START_NOT_WITNESSED")
    base = {"dataset": name, "source_interval": [documented_start, latest], "grain": row["grain"],
            "first_boundary_type": ("OBSERVED_SAMPLE_BOUND_ONLY" if sample_only else
                                    "OFFICIAL_DOCUMENTED_START" if source_start else "CERTIFIED_ARCHIVE_BOUND_ONLY"),
            "official_start_evidence": row["evidence"] if source_start else [], "latest_is_observed_publication_bound": True,
            "observed_data_ids": data_ids, "status": "PLANNED_REQUIRES_PER_WINDOW_SOURCE_COMPLETENESS",
            "unknown_boundaries": unknown,
            "logical_coverage_scope": "LISTED_OTC_RESEARCH" if name.startswith("TaiwanStock") else "AUTHORIZED_NATIVE_SOURCE",
            "source_replies_remain_complete": True, "contract_version": digest(encode(row["schema"])),
            "completeness_contract": row["completeness_methods"]}
    choices = []
    for mode in MODES:
        if mode not in modes or mode == "SNAPSHOT":
            continue
        need_id = "data_id" in row["mode_contracts"][mode]["required_parameters"]
        if mode == "MARKET_DAY":
            if row["grain"] not in M1_GRAINS or not axes["trading_dates"] or not name.startswith("TaiwanStock"):
                continue
            selectors = base | {"mode": mode, "selector": "REFERENCE_TRADING_CALENDAR", "observed_data_ids": []}
            if documented_start < axes["trading_dates"][0]:
                selectors["unknown_boundaries"] = base["unknown_boundaries"] + ["PRE_CALENDAR_SESSIONS_MISSING"]
        else:
            if need_id and not (data_ids or stock_universe and axes["issuer_intervals"]):
                continue
            if need_id and sample_only:
                if not observed_entities:
                    continue
                selector = "OBSERVED_ENTITY_BOUNDS"
            elif need_id and stock_universe and axes["issuer_intervals"]:
                selector = "OFFICIAL_ISSUER_VALIDITY"
            else:
                selector = "BOUNDED_NATIVE_RANGE"
            selectors = base | {"mode": mode, "selector": selector,
                                "observed_data_ids": data_ids if need_id else []}
            if selector == "OBSERVED_ENTITY_BOUNDS":
                selectors["observed_entity_bounds"] = observed_entities
        # The selectors emit unique physical requests independent of the release coverage counters.
        count = sum(1 for _ in query_parameters(selectors, axes))
        if mode == "MARKET_DAY" and axes["legacy_receipt_intervals"].get(name):
            selectors["overlap_gate"] = "MARKET_RAW_OVERLAP_REQUIRES_RELEASE_REVIEW_AND_LOGICAL_DEDUP"
        if count:
            choices.append(selectors | {"scheduled_requests": count})
    if not choices and frozen_archive:
        return {"dataset": name, "mode": "SERIES_RANGE", "selector": "VERIFIED_ARCHIVE", "status": "ARCHIVE_REUSE", "scheduled_requests": 0,
                "source_interval": [documented_start, latest], "archive_references": frozen_archive,
                "remaining_unknown": ["PUBLICATIONS_OUTSIDE_ARCHIVED_BOUNDARY"]}, []
    if not choices:
        return base | {"mode": next((mode for mode in MODES if mode in modes), None), "selector": "VERIFIED_SAMPLE_WITNESSES",
                       "status": "ARCHIVE_WITNESS_ONLY", "scheduled_requests": 0, "saved_samples": samples,
                       "unknown_boundaries": unknown + ["NO_UNCOVERED_SAFE_QUERY_WITH_EVIDENCED_NATIVE_BOUNDARY"]}, []
    # Compare only equivalent source axes. A known instrument or partial calendar isn't proof of equivalent market coverage.
    for candidate in choices:
        candidate["source_query_axis"] = ("FULL_MARKET_DAILY" if candidate["mode"] in {"MARKET_DAY", "MARKET_RANGE"} and row["grain"] in M1_GRAINS else
                                         "OFFICIAL_STOCK_EFFECTIVE_PERIODS" if candidate["selector"] == "OFFICIAL_ISSUER_VALIDITY" else
                                         "NATIVE_INTERVAL_WITH_EVIDENCED_IDENTITIES")
    eligible = [p for p in choices if p["source_query_axis"] == "FULL_MARKET_DAILY" and
                "PRE_CALENDAR_SESSIONS_MISSING" not in p["unknown_boundaries"]]
    selected = min(eligible or choices, key=lambda p: (p["scheduled_requests"], MODES.index(p["mode"])))
    if selected["mode"] == "MARKET_DAY" and selected.get("overlap_gate"):
        without_overlap = [p for p in choices if p["mode"] == "ENTITY_RANGE" and p["selector"] == "OFFICIAL_ISSUER_VALIDITY"]
        if without_overlap:
            selected = min(without_overlap, key=lambda p: p["scheduled_requests"]) | {
                "selection_reason": "PREFER_NONOVERLAPPING_VERIFIED_ISSUER_QUERY"}
    benchmark = [{"dataset": name, "mode": p["mode"], "requests": p["scheduled_requests"], "source_query_axis": p["source_query_axis"],
                  "overlap_gate": p.get("overlap_gate")} for p in choices]
    return selected, benchmark


def history_denominator(registry, partitions, gaps):
    eligible = {row["dataset"] for row in registry if row["state"] != "EXCLUDED"}
    known = {row["dataset"] for row in partitions if row.get("full_universe_verified") is True and
             type(row.get("complete_native_window_count")) is int}
    return sum(p["complete_native_window_count"] for p in partitions) if eligible == known and not gaps and known else None


def planned_request_identity(partition, params):
    # The interval shard metadata gives the source boundary. Raw/file and receipt IDs are assigned only at dispatch or reuse.
    request = request_id(partition["mode"], params, SOURCE_REVISION)
    observed = [params.get("data_id"), params.get("start_date"), params.get("end_date", params.get("start_date"))]
    return request, digest(encode([partition["dataset"], observed, SOURCE_REVISION]))


def final(ctx):
    offline = ctx.read("HISTORY_AXES_INPUT.json")
    registry = ctx.read("DATASET_REGISTRY.json")["rows"]
    reuse = ctx.prior("REUSE_MAP.json")
    receipt_rows = checked_rows(ctx, offline["legacy_receipt_refs"], "receipts", "receipts", ctx.v20)
    by_dataset = defaultdict(lambda: defaultdict(list))
    for row in receipt_rows:
        params = row["params"]
        by_dataset[params["dataset"]][params.get("data_id", "")].append([params["start_date"], params["end_date"]])
    active = ctx.read("ACTIVE_BATCH_RECONCILIATION.json")
    for row in active["tasks"]:
        if row["disposition"] == "DOWNLOAD_REQUIRED":
            by_dataset[row["dataset"]][row["scope"]].append([row["start"], row["end"]])
    axes = {"issuer_intervals": offline["issuer_intervals"], "source_witnesses": offline["source_witnesses"],
            "existing_market_query_days": offline["existing_market_query_days"], "legacy_receipt_intervals": by_dataset}
    cal = next((r.get("v25_witness", {}) for r in registry if r["dataset"] == "TaiwanStockTradingDate"), {})
    axes["trading_dates"] = cal.get("observed_dates") or offline["certified_2026_sessions"]
    archive_by_dataset = defaultdict(list)
    for row in reuse["archive_intervals"]:
        archive_by_dataset[row["dataset"]].append(row)
    partitions, gaps, comparisons = [], [], []
    for row in registry:
        if row["state"] == "EXCLUDED":
            continue
        name = row["dataset"]
        if row["state"] != "CONTRACT_READY":
            gaps.append({"dataset": name, "reason": "CONTRACT_PENDING"})
            continue
        prepared, decisions = build_partition(row, axes, archive_by_dataset)
        if prepared is None:
            gaps.extend(decisions)
            continue
        partitions.append(prepared)
        comparisons.extend(decisions)
        for reason in prepared.get("unknown_boundaries", prepared.get("remaining_unknown", ())):
            gaps.append({"dataset": name, "reason": reason})
    native = offline["native_compilable_legacy_windows"]
    native_body = {"schema_version": SCHEMA, "rows": native}
    native_name = "V24_MANIFEST_COMPATIBLE_V25.json"
    # The V2.4 native engine consumes the strict subset unchanged. Other shards contain
    # actual routable selector definitions with a separate planned/verification status.
    pending_scope = history_denominator(registry, partitions, gaps)
    planning = {"format": "V25_NATIVE_SELECTORS", "rows": partitions,
                "axis_ref": {"name": "HISTORICAL_QUERY_AXES.json", "sha256": digest(encode(axes))},
                "counted_http_requests": sum(row["scheduled_requests"] for row in partitions)}
    idx = {"schema_version": SCHEMA, "revision": "V2.5", "task_id": ctx.task_id, "prepared_windows": len(native),
           "dispatch_shards": [{"name": native_name, "sha256": digest(encode(native_body)), "windows": len(native)}],
           "planning_shards": [{"name": "V25_HISTORY_SELECTORS.json", "sha256": digest(encode(planning)), "datasets": len(partitions)}],
           "full_history_denominator": pending_scope,
           "frozen_2026_candidate_windows": offline["known_legacy_scope"],
           "frozen_legacy_plan_read_only_sha256": ctx.master["state"]["plan_sha"],
           "legacy_ready_receipts": offline["legacy_receipt_count"], "active_verified": offline["active_verified"],
           "planned_scope_http_requests": planning["counted_http_requests"], "planned_scope_datasets": len(partitions),
           "eligible_contract_count": sum(r["state"] == "CONTRACT_READY" for r in registry),
           "physical_requests_distinct_from_evidenced_logical_coverage": True,
           "full_download_release": "HOLD", "production_dispatchable": 0}
    return ("HOLD" if pending_scope is None else "PASS"), {
        native_name: native_body, "V25_HISTORY_SELECTORS.json": planning, "HISTORICAL_QUERY_AXES.json": axes,
        "MANIFEST_INDEX.json": idx,
        "HISTORICAL_UNIVERSE.json": {"rows": partitions, "unknown": gaps, "universe_complete": pending_scope is not None,
            "issuer_source_references": offline["official_reconciliation_sources"], "calendar_source": cal or "V24_OFFICIAL_2026_ONLY",
            "six_source_reverification_needed": False, "verified_receipts_keep_their_own_denominator": True},
        "REUSE_MAP.json": reuse | {"read_back_active_valid": offline["active_verified"], "new_native_witnesses_reusable": [r["v25_witness"]
                                     for r in registry if r.get("v25_witness")], "new_formal_downloads": 0},
        "COVERAGE_GAPS.json": {"rows": gaps, "full_history_denominator": pending_scope,
                              "known_independent_source_dispositions": ctx.read("B09_QUEUE.json")},
        "QUERY_MODE_BENCHMARK.json": {"comparison": comparisons, "physical_rows_measured": False,
            "witnessed_market_raw_sizes": ctx.prior("BULK_BENCHMARK_EVIDENCE.json")["bulk"],
            "uses_sponsor_features": False, "six_modes_supported": list(MODES)},
    }


def verify_planning(index, read):
    accepted = read_manifest(index, read)
    require(accepted, "V25_PRODUCTION_MANIFEST_EMPTY")
    results = []
    for ref in index["planning_shards"]:
        plan = read(ref["name"])
        require(digest(encode(plan)) == ref["sha256"] and len(plan["rows"]) == ref["datasets"], "V25_SELECTOR_INDEX_SHA")
        axes = read(plan["axis_ref"]["name"])
        require(digest(encode(axes)) == plan["axis_ref"]["sha256"], "V25_SELECTOR_AXES_SHA")
        for row in plan["rows"]:
            if row["scheduled_requests"] == 0:
                continue
            example = next(query_parameters(row, axes), None)
            require(example and len(planned_request_identity(row, example)[0]) == 64, "V25_MATERIALIZED_REQUEST_ID")
            results.append(row["dataset"])
    return {"native_windows_loaded": len(accepted), "synthesized_datasets_checked": len(results), "index_sha256": digest(encode(index))}
