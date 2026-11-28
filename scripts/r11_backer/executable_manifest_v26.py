"""Content-addressed, bounded native work for the production B01–B10 controller."""

from calendar import monthrange
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from hashlib import sha256
from itertools import islice
import re

from .bulk_manifest import MAX_RAW, MODES, SCHEMA, compile_window, request_id
from .integration_pilot import digest, encode
from .release_rehearsal import require
from .v2_coverage import subtract_coverage
from .v2_registry import NativeContract, check_parameters
from .v26_boundaries import QUARTERLY

VERSION = "FinMind_v4_native_history_v26"
FORMAT = 26
BATCH_LIMIT = 96
INDEX = "EXECUTABLE_MANIFEST_INDEX.json"
INPUTS = "EXECUTABLE_GENERATOR_INPUTS.json"
SHARD = "EXECUTABLE_GENERATOR_SHARDS.json"
CURSOR = "EXECUTABLE_CURSOR.json"
NATIVE_OLD = "V24_MANIFEST_COMPATIBLE_V25.json"
DAILY = {"daily", "daily_observation"}
RANGE_DAYS = {"weekly": 3653, "monthly": 3653, "quarterly": 3653, "annual": 7305,
              "event": 731, "event_interval": 731}
NATIVE_MARKET_PERIODS = QUARTERLY | {"TaiwanStockMonthRevenue"}
MARKET_EXCEPTIONS = {"TaiwanStockMarketValueWeight"}
DAY_ONLY_MARKET = {
    "TaiwanStockConvertibleBondDaily", "TaiwanStockConvertibleBondDailyOverview",
    "TaiwanStockConvertibleBondInstitutionalInvestors", "TaiwanAssetSwapFixedIncomeDaily",
    "TaiwanAssetSwapOptionDaily", "TaiwanStockSecuritiesLending",
}
TIER_IDS = {
    "CrudeOilPrices": ("Brent", "WTI"),
    "InterestRate": ("BCB", "BOC", "BOE", "BOJ", "CBR", "ECB", "FED", "PBOC", "RBA", "RBI", "RBNZ", "SNB"),
}
LANES = (
    ("B08", ("Futures", "Option", "ConvertibleBond", "AssetSwap", "ExchangeRate", "Oil", "Yield", "InterestRate", "Business", "Fear", "ETF")),
    ("B05", ("Financial", "BalanceSheet", "CashFlow", "Revenue", "Dividend", "Reduction", "Offering", "Split", "ParValue", "Suspended", "Delisting")),
    ("B02", ("Institutional", "Dealer", "InvestmentTrust")),
    ("B03", ("Shareholding", "HoldingShares", "Margin", "ShortSale", "Lending")),
    ("B04", ("Valuation", "PER", "MarketValue", "ReturnIndex")),
)


def lane_for(dataset):
    return next((lane for lane, names in LANES if any(name in dataset for name in names)), "B01")


def exact_query_key(params):
    copied = dict(params)
    if copied.get("start_date") and copied.get("end_date") == copied["start_date"]:
        copied.pop("end_date")
    return digest(encode(copied))


def merged_dates(intervals):
    merged = []
    for first, last in sorted(intervals):
        if first > last:
            continue
        if merged and first <= merged[-1][1] + timedelta(days=1):
            merged[-1] = (merged[-1][0], max(merged[-1][1], last))
        else:
            merged.append((first, last))
    return merged


def period_days(grain, first, last):
    for year in range(first.year, last.year + 1):
        for month in ((3, 6, 9, 12) if grain == "quarterly" else range(1, 13)):
            value = date(year, month, monthrange(year, month)[1] if grain == "quarterly" else 1)
            if first <= value <= last:
                yield str(value)


def parameters(plan, axes):
    dataset, mode = plan["dataset"], plan["mode"]
    if mode == "SNAPSHOT":
        yield {"dataset": dataset}
        return
    first, last = (date.fromisoformat(value) for value in plan["interval"])
    if first > last:
        return
    known = set(axes["exact_reuse_query_keys"].get(dataset, ()))
    if mode == "MARKET_DAY":
        days = (period_days(plan["grain"], first, last) if dataset in NATIVE_MARKET_PERIODS else
                (day for day in axes["trading_dates"] if str(first) <= day <= str(last)))
        gaps = set(plan["source_unavailable_days"])
        limit = plan["source_unavailable_saturdays_through"]
        existing_days = set(axes["existing_market_query_days"].get(dataset, ()))
        for day in days:
            if day in gaps or day in existing_days or limit and day <= limit and date.fromisoformat(day).weekday() == 5:
                continue
            item = {"dataset": dataset, "start_date": day}
            if exact_query_key(item) not in known:
                yield item
        return

    identities = defaultdict(list)
    if plan["scope"] == "ISSUER_HISTORY":
        for row in axes["issuer_intervals"]:
            identities[row["id"]].append((max(first, date.fromisoformat(row["start"])),
                                           min(last, date.fromisoformat(row["end_exclusive"]) - timedelta(days=1))))
    else:
        for identity in plan["identities"] or [""]:
            identities[identity].append((first, last))
    prior = axes["receipt_and_reservation_intervals"].get(dataset, {})
    for identity, intervals in sorted(identities.items()):
        for start, finish in merged_dates(intervals):
            before = prior.get(identity)
            ranges = subtract_coverage(str(start), str(finish), before) if before else [[str(start), str(finish)]]
            for start_text, end_text in ranges:
                left, end = date.fromisoformat(start_text), date.fromisoformat(end_text)
                while left <= end:
                    right = min(left + timedelta(days=plan["maximum_days"] - 1), end)
                    item = {"dataset": dataset, "start_date": str(left), "end_date": str(right)}
                    if identity:
                        item["data_id"] = identity
                    if exact_query_key(item) not in known:
                        yield item
                    left = right + timedelta(days=1)


def plan_for(row, axis, mode, first=None, last=None, *, work="NORMAL", ids=(), label=None):
    dataset = row["dataset"]
    active = row["mode_contracts"].get(mode) or row["schema"]
    require(active["key_policy"] in {"native_unique", "content_multiset"}, "UNKNOWN_NATIVE_KEY_POLICY")
    required_id = "data_id" in active["required_parameters"] or mode == "ENTITY_RANGE"
    historical_issuer = (required_id and row["schema"]["identity_field"] == "stock_id" and dataset.startswith("TaiwanStock"))
    stride = RANGE_DAYS.get(row["grain"], 1096)
    if mode in {"MARKET_RANGE", "SERIES_RANGE"} and not required_id and dataset.startswith(("TaiwanStock", "TaiwanFutures")):
        stride = min(stride, 366)
    quality = axis["quality_sample"] or {}
    method = ("PROVIDER_SPLIT" if mode.endswith("_RANGE") and quality.get("supports_query_comparison") else
              "OFFICIAL_EXPECTED_KEYS" if active["key_policy"] == "native_unique" else "OFFICIAL_ROW_MULTISET")
    return {"dataset": dataset, "name": label or mode, "mode": mode, "grain": row["grain"], "contract": active,
            "work": work, "lane": lane_for(dataset), "scope": "ISSUER_HISTORY" if historical_issuer and work == "NORMAL" else "EXPLICIT",
            "ids_required": required_id and mode not in {"SNAPSHOT", "MARKET_DAY", "MARKET_RANGE"},
            "identities": sorted(set(ids)) if required_id else [],
            "interval": [first, last] if mode != "SNAPSHOT" else None, "maximum_days": stride,
            "source_unavailable_days": axis["source_unavailable_days"],
            "source_unavailable_saturdays_through": axis["source_unavailable_saturdays_through"],
            "verification": method, "documentation": row["evidence"] + axis["authorized_market_day"],
            "raw_limit": MAX_RAW if mode.startswith("MARKET") else min(MAX_RAW, max(active["byte_limit"], 8 * 1024 * 1024)),
            "market_overlap_policy": "KEEP_NATIVE_BYTES_DEDUP_COVERAGE_BY_RECEIPT" if mode.startswith("MARKET") else None}


def observed_ids(dataset, selector, axes):
    archive = (entry["parameters"].get("data_id") for entry in axes["source_witnesses"].get(dataset, ()))
    return sorted({value for value in (*TIER_IDS.get(dataset, ()), *selector.get("observed_data_ids", ()), *archive) if value})


def mode_choices(row, boundary, selector, axes):
    dataset = row["dataset"]
    start, end = boundary["planning_start"], boundary["publication_target"]
    if row["grain"] == "snapshot" or dataset == "TaiwanStockSplitPrice" and any(
        item["source"] == "Fundamental" and item["anchor"] == "taiwanstocksplitprice" for item in boundary["official_sections"]
    ):
        capture = plan_for(row, boundary, "SNAPSHOT", work="SNAPSHOT", label="CURRENT_SNAPSHOT")
        return [(capture,)], []
    if not start or not end or start > end:
        return [], [{"reason": "SOURCE_BOUNDARY_UNAVAILABLE"}]
    options = set(row["mode_contracts"])
    if boundary["authorized_market_day"]:
        options.add("MARKET_DAY")
    ids = observed_ids(dataset, selector, axes)
    candidates, notes = [], []
    for mode in MODES:
        if mode not in options or mode == "SNAPSHOT":
            continue
        if mode == "MARKET_DAY":
            if dataset not in NATIVE_MARKET_PERIODS and (row["grain"] not in DAILY or dataset in MARKET_EXCEPTIONS or
                                                       not dataset.startswith(("TaiwanStock", "TaiwanAssetSwap", "TaiwanDaily"))):
                notes.append({"mode": mode, "reason": "NATIVE_MARKET_PERIOD_OR_CALENDAR_NOT_PROVEN"})
                continue
            if dataset not in NATIVE_MARKET_PERIODS and not axes["trading_dates"]:
                continue
            first = start if dataset in NATIVE_MARKET_PERIODS else max(start, axes["trading_dates"][0])
            market = plan_for(row, boundary, mode, first, end)
            before = str(date.fromisoformat(first) - timedelta(days=1))
            earlier = []
            if start <= before:
                if "ENTITY_RANGE" in options:
                    earlier.append(plan_for(row, boundary, "ENTITY_RANGE", start, before, ids=ids, label="BEFORE_TRADING_CALENDAR"))
                else:
                    notes.append({"mode": mode, "reason": "PRE_CALENDAR_REQUIRES_MARKET_EVIDENCE", "range": [start, before]})
                    continue
            candidates.append(tuple(earlier + [market]))
        else:
            candidate = plan_for(row, boundary, mode, start, end, ids=ids)
            if candidate["ids_required"] and not ids and candidate["scope"] != "ISSUER_HISTORY":
                notes.append({"mode": mode, "reason": "NO_AUTHORIZED_SERIES_IDENTITIES"})
                continue
            candidates.append((candidate,))
    return candidates, notes


def capture_token(stamp):
    captured = datetime.fromisoformat(stamp)
    return captured.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def physical_id(plan, params, captured):
    return request_id(plan["mode"], params, VERSION, capture_token(captured) if plan["mode"] == "SNAPSHOT" else None)


def enumerate_counts(plans, axes, stamp):
    tokens, channels = sha256(), Counter()
    first = last = None
    ids = set()
    for plan in plans:
        for params in parameters(plan, axes):
            key = physical_id(plan, params, stamp)
            require(key not in ids, "V26_DUPLICATE_PHYSICAL_REQUEST")
            ids.add(key)
            if first is None:
                first = key
            last = key
            tokens.update(bytes.fromhex(key))
            channels[plan["mode"]] += 1
    return {"requests": len(ids), "by_mode": dict(channels), "request_sequence_sha256": tokens.hexdigest(),
            "first_request_id": first, "last_request_id": last}


def identity_count(plans, axes, stamp):
    return enumerate_counts(plans, axes, stamp)["requests"]


def build_inputs(ctx):
    axes = ctx.previous("HISTORICAL_QUERY_AXES.json")
    previous_manifest = ctx.previous("MANIFEST_INDEX.json")
    strict_refs = previous_manifest["dispatch_shards"]
    strict = [item for ref in strict_refs for item in ctx.previous(ref["name"])["rows"]]
    accepted = axes["legacy_receipt_intervals"]
    for item in strict:
        identity = item["parameters"].get("data_id")
        if identity:
            accepted.setdefault(item["dataset"], {}).setdefault(identity, []).append([item["start"], item["end"]])
    return {"format": FORMAT, "issuer_intervals": axes["issuer_intervals"], "trading_dates": axes["trading_dates"],
            "existing_market_query_days": axes["existing_market_query_days"], "source_witnesses": axes["source_witnesses"],
            "receipt_and_reservation_intervals": accepted, "certified_windows": strict, "prior_receipt_or_reserved_axis_read_only": True,
            "exact_reuse_query_keys": {name: sorted({exact_query_key(item["parameters"]) for item in rows
                if item.get("raw_sha256") and item["parameters"].get("start_date")})
                for name, rows in axes["source_witnesses"].items()}}


def exploratory(row, boundary, main, axes):
    start = boundary["planning_start"]
    if not start or not main or main[0]["mode"] == "SNAPSHOT" or boundary["finmind_documented_start"]:
        return []
    first = date.fromisoformat(start)
    external = {item["id"]: item["start"] for item in boundary["primary_publisher_candidates"]}
    old_plan = main[0]
    ids = old_plan["identities"] or [""]
    work = []
    for identity in ids[:12]:
        begin = max(str(first - timedelta(days=366)), external.get(identity, "1900-01-01"))
        end = str(first - timedelta(days=1))
        if begin > end:
            continue
        if old_plan["mode"] == "MARKET_DAY":
            # A calendar day is one probe. A missing earlier market calendar
            # becomes separate source research rather than an arbitrary query.
            dates = [day for day in axes["trading_dates"] if begin <= day <= end]
            if not dates:
                continue
            begin = end = dates[-1]
        work.append(old_plan | {"name": f"EARLIER_{identity or 'MARKET'}", "interval": [begin, end],
                                "work": "EXPLORATION", "scope": "EXPLICIT", "identities": [identity],
                                "maximum_days": 366})
    return work


def compile_plans(ctx):
    registry = ctx.previous("DATASET_REGISTRY.json")
    inherited = ctx.previous("V25_HISTORY_SELECTORS.json")
    selectors = {row["dataset"]: row for row in inherited["rows"]}
    boundaries = ctx.read("BOUNDARY_REGISTRY.json")
    axes = ctx.read(INPUTS)
    boundary_rows = {row["dataset"]: row for row in boundaries["rows"]}
    stamp = boundaries["as_of_taipei"]
    charts, blockers, gaps, shards = [], [], [], []
    known_dates = set(axes["trading_dates"])
    require(len(known_dates) == len(axes["trading_dates"]), "V26_CALENDAR_NOT_UNIQUE")
    prev_reuse = ctx.previous("REUSE_MAP.json")
    previous_gaps = ctx.previous("COVERAGE_GAPS.json")
    for row in sorted(registry["rows"], key=lambda item: item["dataset"]):
        if row["state"] != "CONTRACT_READY":
            continue
        dataset = row["dataset"]
        boundary, selector = boundary_rows[dataset], selectors[dataset]
        alternatives, notes = mode_choices(row, boundary, selector, axes)
        choices = [(seq, identity_count(seq, axes, stamp)) for seq in alternatives]
        eligible = [(seq, count) for seq, count in choices if count]
        full_market = [(seq, count) for seq, count in eligible if any(p["mode"].startswith("MARKET") for p in seq)]
        if full_market:
            # A few observed bond or issuer codes cannot stand in for the
            # complete response scope of a documented market-wide request.
            eligible = [option for option in eligible if option in full_market or
                        dataset not in DAY_ONLY_MARKET and all(p["scope"] == "ISSUER_HISTORY" for p in option[0])]
        if eligible:
            winner, _ = min(eligible, key=lambda value: (value[1], [part["mode"] for part in value[0]]))
        else:
            winner = ()
        archive = [ref for ref in prev_reuse["archive_intervals"] if ref["dataset"] == dataset]
        if not winner and not archive and not row.get("v25_witness"):
            blockers.append({"dataset": dataset, "reason": "NO_EXECUTABLE_NORMAL_OR_VERIFIABLE_ARCHIVE"})
        probes = exploratory(row, boundary, winner, axes)
        normal = enumerate_counts(winner, axes, stamp)
        exploration = enumerate_counts(probes, axes, stamp)
        shards.append({"dataset": dataset, "normal": list(winner), "exploration": probes, "normal_counts": normal,
                       "exploration_counts": exploration, "reuse": archive, "saved_witness": row.get("v25_witness"),
                       "receipt_policy": "V01_TO_V07_RETAIN_UNVERIFIED_ORIGINALS_OUTSIDE_BRONZE",
                       "unknown_history_is_separate": True})
        charts.append({"dataset": dataset, "v25_estimate": selector["scheduled_requests"], "v26_requests": normal["requests"],
                       "alternatives": [{"modes": [p["mode"] for p in seq], "physical_requests": count} for seq, count in choices],
                       "limitations": notes, "raw_overlap_policy": [p["market_overlap_policy"] for p in winner if p["market_overlap_policy"]]})
        if not boundary["finmind_documented_start"] and row["grain"] != "snapshot":
            gaps.append({"dataset": dataset, "reason": "EARLIEST_PROVIDER_BOUNDARY_UNCONFIRMED",
                         "range": (boundary["quality_sample"] or {}).get("observed_range"), "disposition": "B09_EXPLORATION_OR_OFFICIAL_LOOKUP"})
        if winner and all(p["ids_required"] and p["scope"] == "EXPLICIT" for p in winner):
            gaps.append({"dataset": dataset, "reason": "AUTHORIZED_NATIVE_ID_UNIVERSE_REQUIRES_SOURCE_INDEX",
                         "identities": sorted({identity for p in winner for identity in p["identities"]}),
                         "disposition": "B09_VERIFY_FINMIND_ACCEPTED_IDENTITIES_WITH_OFFICIAL_SERIES_OR_CONTRACT_INDEX"})
        if row["grain"] == "snapshot":
            gaps.append({"dataset": dataset, "reason": "HISTORICAL_SNAPSHOTS_EXIST_ONLY_IN_ARCHIVE",
                         "disposition": "REUSE_ARCHIVED_AND_CAPTURE_CURRENT"})
        if boundary["archive_observation_earlier_than_documentation"]:
            gaps.append({"dataset": dataset, "reason": "ARCHIVE_PRECEDES_DOCUMENTED_CUTOFF",
                         "dates": boundary["archive_observation_earlier_than_documentation"], "disposition": "B09_SOURCE_BOUNDARY_RECONCILIATION"})
        if boundary["source_unavailable_days"] or boundary["source_unavailable_saturdays_through"]:
            gaps.append({"dataset": dataset, "reason": "SOURCE_NOT_PROVIDED", "dates": boundary["source_unavailable_days"],
                         "special_saturdays_through": boundary["source_unavailable_saturdays_through"],
                         "disposition": "VERIFY_SOURCE_NOTE_AND_ARCHIVE_NO_SYNTHETIC_ROWS"})
    body = {"format": FORMAT, "as_of_taipei": stamp, "rows": shards}
    per_mode = Counter()
    for row in shards:
        per_mode.update(row["normal_counts"]["by_mode"])
    total = sum(per_mode.values())
    assert sum(row["normal_counts"]["requests"] for row in shards) == total
    strict = axes["certified_windows"]
    strict_ids = {row["request_id"] for row in strict}
    strict_coverage = {coverage for row in strict for coverage in row["coverage"]["coverage_ids"]}
    require(len(shards) == 70 and len(strict_ids) == len(strict), "V26_MANIFEST_RECONCILIATION")
    state = ctx.read("LOCK_INPUT.json")
    active = ctx.read("ACTIVE_BATCH_RECONCILIATION.json")
    verified_active = active["dispositions"].get("VERIFIED_RECEIPT", 0)
    index = {"schema_version": FORMAT, "native_schema_version": SCHEMA, "task_id": ctx.task_id,
             "spec_sha256": ctx.spec_sha, "format": "FROZEN_EXECUTABLE_NATIVE_GENERATOR",
             "as_of_taipei": stamp, "files": {INPUTS: digest(encode(axes)), SHARD: digest(encode(body))},
             "planned_request_count": total + len(strict), "scheduled_generator_requests": total, "requests_by_mode": dict(per_mode),
             "exploration_request_count": sum(row["exploration_counts"]["requests"] for row in shards),
             "dataset_count": len(shards), "certified_native_windows": len(strict),
             "known_coverage_count": len(strict_coverage), "full_history_denominator": None,
             "actual_verified_count": state["production"]["totals"]["VERIFIED"],
             "verified_active_receipts": verified_active, "unknown_scope_count": len(gaps),
             "full_download_release": "HOLD", "production_dispatchable": 0}
    previous_b09 = previous_gaps["known_independent_source_dispositions"]
    b09 = {"legacy": [item | {"next_action": "READ_EXISTING_RECEIPT_OR_RETRY_THROUGH_B00"}
                     for item in previous_b09["legacy_blocked"]],
           "historical_source": [item | {"task_id": digest(encode(item)),
                                        "next_action": "RECONCILE_MARKET_CALENDAR_AND_ARCHIVED_SOURCE"}
                                 for item in previous_b09["independent_source_gaps"]],
           "historical_calendar": [{"range_before": axes["trading_dates"][0], "reason": "NO_INDEPENDENT_MARKET_CALENDAR",
                                     "disposition": "USE_ENTITY_RANGE_WHERE_SUPPORTED_ELSE_HOLD"}],
           "boundary_and_source": [entry | {"task_id": digest(encode(entry))} for entry in gaps],
           "exploration": [{"dataset": row["dataset"], "plans": row["exploration"], "frozen_counts": row["exploration_counts"]}
                           for row in shards if row["exploration"]], "release_authorized": False}
    return ("HOLD" if blockers else "PASS"), {INDEX: index, SHARD: body,
        "QUERY_MODE_BENCHMARK.json": {"v25_requests": 494432, "v26_planning_estimate": total + len(strict), "rows": charts,
                                      "estimated_by_mode": dict(per_mode), "calendars_and_universes_are_bounded": True},
        "REUSE_MAP.json": prev_reuse | {"v26_compiler_kept_existing_request_and_coverage_axes": True},
        "COVERAGE_GAPS.json": {"before": previous_gaps["rows"], "current": gaps, "engineering_holds": blockers,
                               "full_history_denominator": None, "blocked_partitions_excluded_from_verified_count": True},
        "B09_QUEUE.json": b09}


def read_inputs(index, read):
    require(index["schema_version"] == FORMAT and index["native_schema_version"] == SCHEMA and
            index["full_download_release"] == "HOLD" and not index["production_dispatchable"], "V26_INDEX_BINDING")
    loaded = {}
    for name in (INPUTS, SHARD):
        loaded[name] = read(name)
        require(digest(encode(loaded[name])) == index["files"][name] and loaded[name]["format"] == FORMAT,
                "V26_FROZEN_GENERATOR_INPUT_SHA")
    shards, axes = loaded[SHARD], loaded[INPUTS]
    names = [row["dataset"] for row in shards["rows"]]
    require(shards["as_of_taipei"] == index["as_of_taipei"] and len(names) == len(set(names)) == index["dataset_count"],
            "V26_GENERATOR_ROSTER")
    require(sum(row["normal_counts"]["requests"] for row in shards["rows"]) == index["scheduled_generator_requests"] and
            sum(row["exploration_counts"]["requests"] for row in shards["rows"]) == index["exploration_request_count"] and
            len(axes["certified_windows"]) == index["certified_native_windows"], "V26_GENERATOR_COUNTS")
    return index, shards, axes


def origin(index, queue="normal"):
    require(queue in {"normal", "certified", "exploration"}, "V26_QUEUE_ID")
    return {"manifest_sha256": digest(encode(index)), "queue": queue, "dataset_offset": 0, "window_offset": 0}


def item_from(plan, params, stamp):
    rule = NativeContract(**plan["contract"])
    market = params.get("data_id") or ("FULL_NATIVE_SOURCE" if plan["mode"].startswith("MARKET") else "NATIVE_SOURCE")
    completeness = {"method": "deferred_source_reconciliation", "verifier": plan["verification"],
                    "native_universe": "FULL_RESPONSE_SCOPE", "planned_class": plan["work"]}
    return compile_window(rule=rule, mode=plan["mode"], params=params, lane="B09" if plan["work"] == "EXPLORATION" else plan["lane"],
                          venue=market, version=VERSION, completeness=completeness, coverage=[],
                          evidence={"contract_sources": plan["documentation"], "mode": plan["mode"]},
                          raw_limit=plan["raw_limit"], capture=capture_token(stamp) if plan["mode"] == "SNAPSHOT" else None,
                          legacy_query=exact_query_key(params) if plan["mode"] != "SNAPSHOT" else None)


def render(index, read, cursor, *, limit=BATCH_LIMIT, known_requests=(), known_coverage=(), capture_at=None):
    """Generate a bounded slice at the saved cursor, keeping the original query independent of the run."""
    from .bulk_manifest import check_window, overlap_decision

    index, shards, axes = read_inputs(index, read)
    start = origin(index, cursor["queue"])
    require(cursor["manifest_sha256"] == start["manifest_sha256"] and type(limit) is int and 1 <= limit <= BATCH_LIMIT,
            "V26_CURSOR_BINDING")
    queue = cursor["queue"]
    group, offset = cursor["dataset_offset"], cursor["window_offset"]
    entries = shards["rows"] if queue != "certified" else [{"dataset": "CERTIFIED", "normal": ()}]
    require(type(group) is int and 0 <= group <= len(entries) and type(offset) is int and offset >= 0, "V26_CURSOR_OFFSET")
    selected, holds = [], []
    seen = set(known_requests)
    coverage = set(known_coverage)
    stamp = capture_at or index["as_of_taipei"]
    while group < len(entries) and len(selected) < limit:
        row = entries[group]
        total = len(axes["certified_windows"]) if queue == "certified" else row[queue + "_counts"]["requests"]
        require(offset <= total, "V26_CURSOR_RANGE")
        if queue == "certified":
            sequence = iter(axes["certified_windows"])
        else:
            sequence = ((plan, params) for plan in row[queue] for params in parameters(plan, axes))
        for candidate in islice(sequence, offset, None):
            offset += 1
            item = candidate if queue == "certified" else item_from(*candidate, stamp)
            check_window(item)
            distinction, intersections = overlap_decision(item, seen, coverage)
            if distinction == "UNIQUE":
                selected.append(item)
                seen.add(item["request_id"])
                if item.get("legacy_query_key"):
                    seen.add(item["legacy_query_key"])
                coverage.update(item["coverage"]["coverage_ids"])
                if len(selected) == limit:
                    break
            elif distinction == "HOLD_PARTIAL_COVERAGE_OVERLAP":
                holds.append({"task_id": item["request_id"], "reason": distinction, "overlap_count": len(intersections)})
        if offset == total:
            group += 1
            offset = 0
    next_cursor = start | {"dataset_offset": group, "window_offset": offset}
    return selected, next_cursor, group == len(entries), holds


def audit(index, read):
    """Enumerate all work identities with the production reader and check the sealed totals."""
    index, shards, axes = read_inputs(index, read)
    seen = set()
    by_work = Counter()
    sampled = set()
    for row in shards["rows"]:
        require(row["normal"] or row["reuse"] or row["saved_witness"], "V26_DATASET_WITHOUT_DISPOSITION")
        for queue in ("normal", "exploration"):
            plans = row[queue]
            require(enumerate_counts(plans, axes, index["as_of_taipei"]) == row[queue + "_counts"], "V26_NONREPRODUCIBLE_SHARD")
            for plan in plans:
                raw = next(parameters(plan, axes), None)
                if raw:
                    sample = item_from(plan, raw, index["as_of_taipei"])
                    check_parameters(NativeContract(**sample["native_contract"]), raw)
                    sampled.add((row["dataset"], queue))
                for params in parameters(plan, axes):
                    identity = physical_id(plan, params, index["as_of_taipei"])
                    require(identity not in seen, "V26_CROSS_QUEUE_REQUEST_OVERLAP")
                    seen.add(identity)
                    by_work[queue] += 1
    require(by_work["normal"] == index["scheduled_generator_requests"] and
            by_work["exploration"] == index["exploration_request_count"], "V26_AUDIT_TOTALS")
    return {"datasets": len(shards["rows"]), "seen_work": dict(by_work), "independent_window_samples": len(sampled),
            "index_sha256": digest(encode(index)), "no_cross_queue_physical_duplicates": True}


def open_checkpoint(index, journal, queue):
    initial = {"manifest_sha256": digest(encode(index)), "cursor": origin(index, queue), "active": None, "terminal_count": 0}
    record = journal.head()
    if record is None:
        journal.initialize(initial)
        record = journal.head()
    require(record["state"]["manifest_sha256"] == initial["manifest_sha256"] and
            record["state"]["cursor"]["queue"] == queue, "V26_CHECKPOINT_CAMPAIGN")
    return record


def advance_checkpoint(journal, record, state):
    require(journal.append(record, state), "V26_CHECKPOINT_CAS")
    newer = journal.head()
    require(newer["state"] == state and newer["previous"] == digest(encode(record)), "V26_CHECKPOINT_READBACK")
    return newer


def save_active(journal, record, batch_id, batch):
    require(not record["state"]["active"] and batch["v26_cursor"] == record["state"]["cursor"], "V26_ACTIVE_BATCH_CURSOR")
    active = {"file_id": batch_id, "sha256": digest(encode(batch)), "next_cursor": batch["v26_next_cursor"]}
    return advance_checkpoint(journal, record, record["state"] | {"active": active})


def finish_active(journal, record, batch, result):
    active = record["state"]["active"]
    terminal = {"VERIFIED", "BLOCKED", "REPAIR", "EXPLORATION_CAPTURED"}
    require(active and active["sha256"] == digest(encode(batch)) and
            result["state"]["manifest_sha"] == active["sha256"] and
            all(task["status"] in terminal for task in result["state"]["tasks"].values()), "V26_BATCH_NOT_DURABLY_TERMINAL")
    return advance_checkpoint(journal, record, record["state"] | {
        "active": None, "cursor": active["next_cursor"],
        "terminal_count": record["state"]["terminal_count"] + len(result["state"]["tasks"])})
