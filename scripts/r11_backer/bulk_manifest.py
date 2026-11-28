"""Versioned R12 native query, coverage, and lossless archive contracts."""

from collections import Counter
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta
import gzip
from io import BytesIO
import re
from zoneinfo import ZoneInfo

from .integration_pilot import digest, encode
from .release_rehearsal import require

SCHEMA = 24
ENDPOINT = "https://api.finmindtrade.com/api/v4/data"
MODES = ("MARKET_DAY", "ENTITY_RANGE", "MARKET_RANGE", "SERIES_RANGE", "SNAPSHOT", "EVENT_RANGE")
NATIVE_MODE = dict(zip(MODES, ("single_day", "entity_range", "market_range", "series_range", "snapshot", "event_range"),
                       strict=True))
ALLOWED_GRAINS = {"daily_observation", "daily", "weekly", "monthly", "quarterly", "annual", "event",
                  "event_interval", "snapshot", "reference"}
# Runner memory guard; official response limits are not inferred from it.
MAX_RAW = 24 * 1024 * 1024
MAX_PAGES = 8
MAX_GROUP_RAW = 32 * 1024 * 1024
LABEL = re.compile(r"[A-Za-z][A-Za-z0-9_]{1,100}")
SNAPSHOT_ZONE = ZoneInfo("Asia/Taipei")


def checked_day(value):
    require(isinstance(value, str), "INVALID_DATE")
    day = date.fromisoformat(value)
    require(str(day) == value, "INVALID_DATE")
    return day


def window_months(start, end):
    month, last = checked_day(start).replace(day=1), checked_day(end).replace(day=1)
    require(start <= end, "INVALID_WINDOW_INTERVAL")
    months = []
    while month <= last:
        months.append(str(month)[:7])
        if month == last:
            break
        month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def local_capture(value):
    stamp = datetime.fromisoformat(value)
    require(stamp.tzinfo is not None, "SNAPSHOT_CAPTURE_TIMEZONE_REQUIRED")
    return stamp.astimezone(SNAPSHOT_ZONE)


def request_id(mode, params, version, capture=None):
    require(mode in MODES and isinstance(version, str) and version and
            set(params) <= {"dataset", "data_id", "start_date", "end_date"} and
            isinstance(params.get("dataset"), str) and LABEL.fullmatch(params["dataset"]), "INVALID_REQUEST_ID_INPUT")
    require((mode == "SNAPSHOT") == (capture is not None), "SNAPSHOT_CAPTURE_IDENTITY")
    capture_key = local_capture(capture).isoformat() if capture is not None else None
    return digest(encode({"endpoint": ENDPOINT, "parameters": params, "mode": mode, "source_version": version,
                          "capture": capture_key}))


def coverage_id(dataset, version, evidence):
    """Keep request shape, physical storage, and the logical native observation distinct."""
    require(isinstance(evidence.get("subject"), str) and evidence["subject"] and
            isinstance(evidence.get("native_key"), list) and evidence["native_key"], "COVERAGE_KEY_REQUIRED")
    return digest(encode([dataset, version, evidence["subject"], evidence["native_key"]]))


def adapt_rule(rule, mode, raw_limit):
    require(mode in MODES and type(raw_limit) is int and 0 < raw_limit <= MAX_RAW, "UNSUPPORTED_MODE_OR_CAPACITY")
    allowed = {"dataset"} if mode == "SNAPSHOT" else {"dataset", "start_date", "end_date"}
    required = {"dataset"} if mode == "SNAPSHOT" else {"dataset", "start_date"}
    if mode != "SNAPSHOT" and "data_id" in rule.allowed_parameters and mode in {"ENTITY_RANGE", "SERIES_RANGE", "EVENT_RANGE"}:
        allowed.add("data_id")
    if mode != "SNAPSHOT" and ("data_id" in rule.required_parameters and mode in {"SERIES_RANGE", "EVENT_RANGE"}
                               or mode == "ENTITY_RANGE"):
        required.add("data_id")
    if mode in {"MARKET_DAY", "MARKET_RANGE"}:
        require("data_id" not in allowed, "MARKET_QUERY_SCOPE")
    return replace(rule, query_mode=NATIVE_MODE[mode], byte_limit=raw_limit,
                   allowed_parameters=tuple(sorted(allowed)), required_parameters=tuple(sorted(required)))


def compile_window(*, rule, mode, params, lane, venue, version, completeness, coverage, evidence,
                   raw_limit, max_pages=1, capture=None, legacy_query=None):
    """Build a task-shaped manifest row compatible with B01–B10."""
    from .v2_registry import check_parameters

    active = adapt_rule(rule, mode, raw_limit)
    deferred = completeness.get("method") == "deferred_source_reconciliation"
    boundary_probe = deferred and completeness.get("planned_class") == "EXPLORATION"
    require(active.grain in ALLOWED_GRAINS and (re.fullmatch(r"B0[1-8]", lane) or boundary_probe and lane == "B09") and type(max_pages) is int and
            1 <= max_pages <= MAX_PAGES and raw_limit * max_pages <= MAX_GROUP_RAW and
            (max_pages == 1 or bool(active.page_field)), "UNSUPPORTED_NATIVE_WINDOW")
    first, last = check_parameters(active, params)
    if mode == "SNAPSHOT":
        require(capture is not None and venue, "MISSING_SNAPSHOT_CAPTURE")
        first = last = str(local_capture(capture).date())
    else:
        require(first is not None and (last is not None or mode == "MARKET_DAY"), "REQUEST_END_UNPROVEN")
        last = last or first
    require(mode not in {"MARKET_DAY", "MARKET_RANGE"} or "data_id" not in params, "UNEXPECTED_MARKET_ENTITY_FILTER")
    require(mode != "EVENT_RANGE" or active.grain in {"event", "event_interval"}, "EVENT_FREQUENCY_REQUIRED")
    if active.identity_field and "data_id" not in params and mode in {"MARKET_DAY", "MARKET_RANGE"}:
        # A market acceptance set includes all expected provider keys. A research-only subset cannot certify it.
        require(completeness.get("native_universe") == "FULL_RESPONSE_SCOPE", "MARKET_COMPLETENESS_SCOPE")
    method = completeness.get("method")
    require((method in {"expected_keys", "expected_multiset", "split_reconciliation", "confirmed_empty"} or
             deferred and version == "FinMind_v4_native_history_v26" and
             completeness.get("verifier") in {"PROVIDER_SPLIT", "OFFICIAL_EXPECTED_KEYS", "OFFICIAL_ROW_MULTISET"}) and
            evidence, "INDEPENDENT_WINDOW_EVIDENCE_REQUIRED")
    require(method != "confirmed_empty" or (active.grain in {"event", "event_interval"} and
                                            completeness.get("evidence") and not coverage), "INVALID_EMPTY_POLICY")
    if method == "expected_keys":
        expected = {encode(key) for key in completeness["keys"]}
        require(expected and len(expected) == len(completeness["keys"]) and
                {encode(entry["native_key"]) for entry in coverage} <= expected, "COVERAGE_MUST_MATCH_NATIVE_KEYS")
    raw_rule = asdict(active)
    identity = request_id(mode, params, version, capture)
    logical_ids = sorted(coverage_id(active.dataset, version, entry) for entry in coverage)
    require(len(logical_ids) == len(set(logical_ids)), "DUPLICATE_COVERAGE_ID")
    spectrum = {"market": venue, "research_venues": ["TWSE", "TPEx"],
                "requested_months": window_months(first, last), "coverage_ids": logical_ids,
                "native_observations": coverage}
    return {"schema_version": SCHEMA, "task_id": identity, "request_id": identity,
            "endpoint": ENDPOINT, "source_version": version, "dataset": rule.dataset, "mode": mode,
            "scope": params.get("data_id", venue), "lane": lane, "market": venue, "start": first, "end": last,
            "anchor_month": first[:7], "capture_at": capture, "parameters": params, "native_contract": raw_rule,
            "contract_version": digest(encode(raw_rule)), "completeness": completeness, "coverage": spectrum,
            "evidence": evidence, "raw_limit": raw_limit, "max_pages": max_pages, "legacy_query_key": legacy_query}


def check_window(item):
    from .v2_registry import NativeContract

    require(item["schema_version"] == SCHEMA and item["endpoint"] == ENDPOINT and
            item["contract_version"] == digest(encode(item["native_contract"])), "NATIVE_CONTRACT_BINDING")
    rule = NativeContract(**item["native_contract"])
    regenerated = compile_window(rule=rule, mode=item["mode"], params=item["parameters"], lane=item["lane"],
                                 venue=item["market"], version=item["source_version"], completeness=item["completeness"],
                                 coverage=item["coverage"]["native_observations"], evidence=item["evidence"],
                                 raw_limit=item["raw_limit"], max_pages=item["max_pages"], capture=item["capture_at"],
                                 legacy_query=item.get("legacy_query_key"))
    require(encode({name: item[name] for name in regenerated}) == encode(regenerated), "NATIVE_WINDOW_BINDING")
    return rule


def receipt_id(request, file_ids, raw_sha256, packed_sha256):
    return digest(encode([request, file_ids, raw_sha256, packed_sha256]))


def packed_limit(item):
    raw_limit = item["raw_limit"]
    # More than the DEFLATE bound plus the actual gzip framing.
    return raw_limit + raw_limit // 256 + 1024


def page_label(item, position):
    require(0 <= position < item["max_pages"], "PAGE_OUT_OF_RANGE")
    name = f"{item['dataset']}__{item['mode']}__{item['start']}__{item['end']}__{item['request_id']}"
    return name + (f"__p{position + 1:02d}" if item["max_pages"] > 1 else "") + "__r12_1.json.gz"


def native_proof(item, packed, raw=None):
    from .v2_registry import validate_original

    rule = check_window(item)
    if raw is None:
        require(len(packed) <= packed_limit(item), "NATIVE_PACKED_SIZE")
        with gzip.GzipFile(fileobj=BytesIO(packed)) as stream:
            raw = stream.read(rule.byte_limit + 1)
    validated = validate_original(rule, item["parameters"], raw, completeness=item["completeness"])
    require(validated["packed_sha"] == digest(packed), "NATIVE_GZIP_SHA")
    return validated | {"request_id": item["request_id"], "contract_version": item["contract_version"],
                        "coverage_sha256": digest(encode(item["coverage"])), "gzip_format": "gzip-mtime-0",
                        "anchor_month": item["anchor_month"]}


def overlap_decision(item, known_requests, known_coverage):
    if item["request_id"] in known_requests or item.get("legacy_query_key") in known_requests:
        return "REUSE_REQUEST", []
    coverage = set(item["coverage"]["coverage_ids"])
    shared = coverage & known_coverage
    if not shared:
        return "UNIQUE", []
    return ("REUSE_COVERAGE" if shared == coverage else "HOLD_PARTIAL_COVERAGE_OVERLAP", sorted(shared))


def optimize(variants, required, known_requests=(), known_coverage=()):
    """Compare complete, nonoverlapping ways of covering one partition before dispatch."""
    known_requests, known_coverage = set(known_requests), set(known_coverage)
    requested = set(required) - known_coverage
    decisions, legal = [], []
    for label, windows in sorted(variants.items()):
        usable, coverage, findings = [], set(), []
        for item in windows:
            state, intersects = overlap_decision(item, known_requests, known_coverage | coverage)
            if state == "UNIQUE":
                usable.append(item)
                coverage.update(item["coverage"]["coverage_ids"])
            elif state == "HOLD_PARTIAL_COVERAGE_OVERLAP" or set(intersects) & coverage:
                findings.append({"request_id": item["request_id"], "reason": state, "overlap_count": len(intersects)})
        if coverage != requested:
            findings.append({"reason": "TARGET_COVERAGE_DIFFERS", "missing": len(requested - coverage),
                             "extra": len(coverage - requested)})
        calls = sum(item["max_pages"] for item in usable)
        size = sum(packed_limit(item) * item["max_pages"] for item in usable)
        decisions.append({"alternative": label, "request_upper_bound": calls, "packed_upper_bound": size,
                          "eligible": not findings, "findings": findings})
        if not findings:
            legal.append((calls, size, label, usable))
    if not legal:
        return None, [], decisions
    _, _, label, windows = min(legal, key=lambda value: value[:3])
    return label, windows, decisions


def read_manifest(index, read, *, known_requests=(), known_coverage=()):
    """Read only immutable, validated review shards. Never dispatch a manifest by reading it."""
    require(index["schema_version"] == SCHEMA and index["full_download_release"] == "HOLD" and
            index["production_dispatchable"] == 0, "EXPECTED_PREPARATION_MANIFEST")
    requests, coverage, names = set(known_requests), set(known_coverage), set()
    rows = []
    for shard in index["dispatch_shards"]:
        name = shard["name"]
        require(re.fullmatch(r"V24_MANIFEST_[A-Z0-9_]+\.json", name) and name not in names, "MANIFEST_SHARD_IDENTITY")
        names.add(name)
        body = read(name)
        require(body["schema_version"] == SCHEMA and digest(encode(body)) == shard["sha256"] and
                len(body["rows"]) == shard["windows"], "MANIFEST_SHARD_DIGEST")
        for item in body["rows"]:
            check_window(item)
            state, _ = overlap_decision(item, requests, coverage)
            require(state == "UNIQUE", "NATIVE_MANIFEST_WINDOW_OVERLAP")
            requests.add(item["request_id"])
            coverage.update(item["coverage"]["coverage_ids"])
            rows.append(item)
    require(len(rows) == index["prepared_windows"], "MANIFEST_WINDOW_INVENTORY")
    return rows


def planned_capacity(rows):
    per_mode = Counter()
    for item in rows:
        per_mode[item["mode"]] += item["max_pages"]
    bound = sum(packed_limit(item) * item["max_pages"] for item in rows)
    return {"windows": len(rows), "requests": sum(per_mode.values()), "requests_by_mode": dict(per_mode),
            "stored_packed_bound": bound, "decoded_bytes_bound": sum(i["raw_limit"] * i["max_pages"] for i in rows),
            "stage_peak_serial_bound": max((packed_limit(i) * i["max_pages"] for i in rows), default=0)}
