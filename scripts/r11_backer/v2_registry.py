"""Read-only FinMind inventory and lossless, native-grain source validation."""

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date
from math import isfinite
import json
import re
from urllib.request import urlopen

from .archive import compress_original
from .cross_run_ledger import DriveJournal
from .finish_preflight import (
    MANIFEST, PREFIX, RUN_HISTORY, original_evidence, packets, task_id,
)
from .drive_pilot import FOLDER, ensure
from .integration_pilot import digest, encode
from .live_recovery_canary import read_json_file
from .native_contracts import LAYOUTS, native_fields
from .production_download import PROBE_ACCEPT, PROBE_RUN, management
from .production_preflight import reconciled_catalog
from .reconcile_catalog import BACKER_COMPATIBLE, SOURCE, documented_tiers
from .release_probes import EVENT_DATASETS, MAX_BYTES, SNAPSHOTS, contracts, observation_date
from .release_readiness import PRIMARY_KEYS
from .release_rehearsal import root_folder

CURRENT_CATALOG = "https://raw.githubusercontent.com/FinMind/FinMind-MCP/HEAD/knowledge/datasets.md"
REVIEW_REASONS = {
    "TaiwanStockActiveETFInfo": "Confirm approved ETF universe and complete current snapshot.",
    "TaiwanStockInfoWithWarrant": "Confirm warrant class and historical identifiers are in the approved universe.",
    "TaiwanStockNews": "Confirm event scope and publication time granularity.",
}
WEEKLY = {"TaiwanStockWeekPrice", "TaiwanStockHoldingSharesPer"}
MONTHLY = {
    "TaiwanBusinessIndicator", "TaiwanStockMonthRevenue", "TaiwanStockMonthPrice",
    "TaiwanStockConvertibleBondMonthlyAnalysis",
}
QUARTERLY = {"TaiwanStockFinancialStatements", "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement"}
REFERENCE = {"TaiwanStockTradingDate"}
EVENTS = EVENT_DATASETS | {
    "InterestRate", "TaiwanFuturesFinalSettlementPrice", "TaiwanOptionFinalSettlementPrice",
    "TaiwanStockPriceLimit", "TaiwanStockNews",
}
NATIVE_KEYS = PRIMARY_KEYS | {
    "TaiwanStockInfo": ("stock_id", "type", "industry_category"),
    "TaiwanStockTradingDate": ("date",),
    "TaiwanFutOptDailyInfo": ("code", "type"),
    "TaiwanStockConvertibleBondInfo": ("cb_id",),
    "TaiwanStockIndustryChain": ("stock_id", "industry", "sub_industry"),
    "TaiwanStockInstitutionalInvestorsBuySell": ("date", "stock_id", "name"),
    "TaiwanStockTotalInstitutionalInvestors": ("date", "name"),
    "TaiwanStockTotalMarginPurchaseShortSale": ("date", "name"),
    "TaiwanStockMarketValueWeight": ("date", "type", "stock_id"),
    "TaiwanFuturesDaily": ("date", "futures_id", "contract_date", "trading_session"),
    "TaiwanOptionDaily": ("date", "option_id", "contract_date", "strike_price", "call_put", "trading_session"),
    "TaiwanStockConvertibleBondMonthlyAnalysis": ("date", "cb_id"),
    "TaiwanStockConvertibleBondDailyOverview": ("date", "cb_id"),
    "TaiwanStockMonthRevenue": ("date", "stock_id", "revenue_year", "revenue_month"),
    "TaiwanExchangeRate": ("date", "currency"),
    "GovernmentBondsYield": ("date", "name"),
    "CrudeOilPrices": ("date", "name"),
}
# A documentation match supports a candidate key, with completeness supplied separately.
SINGLE_SCOPE_OBSERVATIONS = {
    "TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanStockPER", "TaiwanStockDayTrading",
    "TaiwanStock10Year", "TaiwanStockWeekPrice", "TaiwanStockMonthPrice",
    "TaiwanStockMarginPurchaseShortSale", "TaiwanStockInstitutionalInvestorsBuySellWide",
    "TaiwanStockShareholding", "TaiwanStockMarketValue", "TaiwanStockDayTradingBorrowingFeeRate",
    "TaiwanStockTotalReturnIndex", "TaiwanTotalExchangeMarginMaintenance", "TaiwanBusinessIndicator",
    "CnnFearGreedIndex",
}


@dataclass(frozen=True)
class NativeContract:
    dataset: str
    grain: str
    query_mode: str
    fields: tuple[str, ...]
    key_fields: tuple[str, ...]
    identity_field: str | None = None
    date_field: str | None = "date"
    page_field: str | None = None
    byte_limit: int = MAX_BYTES
    required_parameters: tuple[str, ...] = ()
    allowed_parameters: tuple[str, ...] = ()
    date_format: str = "iso"
    cursor_parameter: str = "page"


def key_value(value):
    return type(value) in (str, int) and value != "" or type(value) is float and isfinite(value)


def iso_day(value):
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError("NON_CANONICAL_DATE")
    return day


def check_parameters(rule, params):
    if params.get("dataset") != rule.dataset:
        raise ValueError("WRONG_DATASET")
    if any(name not in params for name in rule.required_parameters):
        raise ValueError("MISSING_QUERY_PARAMETER")
    if rule.allowed_parameters and params.keys() - set(rule.allowed_parameters) - {rule.cursor_parameter}:
        raise ValueError("UNSUPPORTED_QUERY_PARAMETER")
    if rule.cursor_parameter in params and not rule.page_field:
        raise ValueError("PAGINATION_NOT_DOCUMENTED")
    first, last = (params.get(field) for field in ("start_date", "end_date"))
    if rule.query_mode == "snapshot" and (first is not None or last is not None):
        raise ValueError("SNAPSHOT_DATE_FILTER_UNSUPPORTED")
    if rule.query_mode == "single_day" and not (first and last in (None, first)):
        raise ValueError("SINGLE_DAY_REQUIRED")
    if rule.query_mode.endswith("_range") and not (first and last):
        raise ValueError("BOUNDED_WINDOW_REQUIRED")
    if rule.query_mode in {"symbol_range", "entity_range"} and not key_value(params.get("data_id")):
        raise ValueError("QUERY_ENTITY_REQUIRED")
    if first is not None:
        iso_day(first)
    if last is not None:
        iso_day(last)
        if not first or last < first:
            raise ValueError("INVALID_DATE_WINDOW")
    return first, last


def read_rows(rule, params, raw):
    if type(raw) is not bytes or not 0 < len(raw) <= rule.byte_limit:
        raise ValueError("RESPONSE_SIZE")
    first, last = check_parameters(rule, params)
    reply = json.loads(raw)
    fields = {"status", "msg", "data"}
    if rule.page_field:
        fields.add(rule.page_field)
    if not isinstance(reply, dict) or type(reply.get("status")) is not int or reply["status"] != 200:
        raise ValueError("SOURCE_REJECTED")
    if reply.keys() - fields:
        raise ValueError("UNRECOGNIZED_ENVELOPE_OR_PAGINATION")
    rows = reply.get("data")
    if not isinstance(rows, list) or not rule.key_fields:
        raise ValueError("UNVERIFIABLE_NATIVE_KEYS")
    for row in rows:
        if not isinstance(row, dict) or not set(rule.fields) <= row.keys():
            raise ValueError("SOURCE_FIELDS")
        if rule.identity_field and "data_id" in params and row.get(rule.identity_field) != params["data_id"]:
            raise ValueError("SOURCE_IDENTITY")
        if rule.date_field:
            value = row.get(rule.date_field)
            if rule.date_format == "month" and isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}", value):
                day = iso_day(value + "-01")
            elif rule.date_format == "daily_timestamp":
                day = observation_date(rule.dataset, value)
            else:
                day = iso_day(value)
            if (first and day.isoformat() < first or last and day.isoformat() > last or
                    rule.query_mode == "single_day" and day.isoformat() != first):
                raise ValueError("OUTSIDE_SOURCE_WINDOW")
    return rows, reply


def verify_complete(rule, rows, completeness):
    method = completeness.get("method")
    if not rows:
        if method != "confirmed_empty" or not completeness.get("evidence"):
            raise ValueError("UNVERIFIED_EMPTY_WINDOW")
        return "EMPTY_CONFIRMED"
    keys = []
    for row in rows:
        key = tuple(row.get(field) for field in rule.key_fields)
        if not all(map(key_value, key)):
            raise ValueError("MISSING_NATIVE_KEY")
        keys.append(key)
    if len(keys) != len(set(keys)):
        raise ValueError("DUPLICATE_NATIVE_KEY")
    if method == "expected_keys":
        expected = [tuple(key) for key in completeness["keys"]]
        if len(expected) != len(set(expected)) or set(keys) != set(expected):
            raise ValueError("INCOMPLETE_EXPECTED_KEYS")
    elif method == "split_reconciliation":
        if Counter(digest(encode(row)) for row in rows) != Counter(completeness["row_hashes"]):
            raise ValueError("INCOMPLETE_SPLIT_RECONCILIATION")
    else:
        raise ValueError("MISSING_COMPLETENESS_EVIDENCE")
    return "VERIFIED"


def proof(rows, raw, status):
    packed, raw_sha, packed_sha = compress_original(raw)
    return {"status": status, "rows": len(rows), "bytes": len(packed), "raw_sha": raw_sha,
            "packed_sha": packed_sha, "first_sha": digest(encode(rows[0])) if rows else None,
            "last_sha": digest(encode(rows[-1])) if rows else None}


def validate_original(contract, parameters, raw, *, completeness):
    rows, reply = read_rows(contract, parameters, raw)
    if contract.page_field and contract.page_field not in reply:
        raise ValueError("MISSING_PAGE_TERMINATOR")
    if contract.page_field and reply[contract.page_field] is not None:
        raise ValueError("INCOMPLETE_PAGINATION")
    return proof(rows, raw, verify_complete(contract, rows, completeness))


def validate_pages(contract, pages, *, completeness):
    """Require a documented cursor chain and separately evidenced full row coverage."""
    if not contract.page_field or not pages:
        raise ValueError("PAGINATION_NOT_DOCUMENTED")
    first = {k: v for k, v in pages[0][0].items() if k != contract.cursor_parameter}
    expected = None
    visited = set()
    joined, parts = [], []
    for position, (params, raw) in enumerate(pages):
        cursor = params.get(contract.cursor_parameter)
        base = {k: v for k, v in params.items() if k != contract.cursor_parameter}
        if cursor != expected or base != first or cursor in visited or position and cursor is None:
            raise ValueError("INVALID_PAGE_CHAIN")
        visited.add(cursor)
        rows, reply = read_rows(contract, params, raw)
        if contract.page_field not in reply:
            raise ValueError("MISSING_PAGE_TERMINATOR")
        expected = reply[contract.page_field]
        if expected is not None and not key_value(expected):
            raise ValueError("INVALID_PAGE_TOKEN")
        joined.extend(rows)
        parts.append(proof(rows, raw, "PAGE_READ"))
    if expected is not None:
        raise ValueError("INCOMPLETE_PAGINATION")
    return {"status": verify_complete(contract, joined, completeness), "rows": len(joined), "pages": parts}


def public_catalog():
    with urlopen(CURRENT_CATALOG, timeout=15) as response:
        raw = response.read(1_000_001)
    if not 0 < len(raw) <= 1_000_000:
        raise ValueError("CURRENT_CATALOG_SIZE")
    text = raw.decode("utf-8")
    return digest(raw), contracts(text), documented_tiers(text)


def candidate_rule(dataset, doc):
    required = {"dataset", *(p for p in ("start_date", "data_id") if f"`{p}`" in doc.required)}
    optional = {p for p in ("start_date", "end_date", "data_id") if f"`{p}`" in doc.optional}
    fields = doc.fields
    if dataset in LAYOUTS:
        layout = LAYOUTS[dataset]
        fields = tuple(sorted(native_fields(layout, doc.fields)))
        keys = ("date", layout.identity, *layout.dimensions)
    else:
        keys = NATIVE_KEYS.get(dataset, ())
        if not keys and dataset in SINGLE_SCOPE_OBSERVATIONS:
            keys = ("date", "stock_id") if "stock_id" in fields else ("date",)
    if dataset in SNAPSHOTS:
        grain = "calendar_snapshot" if dataset in REFERENCE else "snapshot"
        mode = "snapshot"
    else:
        grain = ("event" if dataset in EVENTS else "weekly" if dataset in WEEKLY else
                 "monthly" if dataset in MONTHLY else "quarterly" if dataset in QUARTERLY else
                 "daily_observation")
        if "end_date" in optional:
            mode = "entity_range" if "data_id" in required else "market_range"
        else:
            mode = "single_day" if "single day" in doc.required else "start_only"
    identity = next((key for key in ("stock_id", "cb_id", "futures_id", "option_id", "currency", "name")
                     if key in keys), None)
    if "data_id" not in required | optional or dataset == "InterestRate":
        identity = None  # Issuer code → country labels have to be verified separately.
    return NativeContract(dataset, grain, mode, tuple(fields), tuple(keys), identity,
                          "date" if "date" in fields and dataset not in SNAPSHOTS else None,
                          required_parameters=tuple(sorted(required)),
                          allowed_parameters=tuple(sorted(required | optional)))


def samples_for(access):
    old, _, documented = original_evidence(access)
    old_rows = old["rows"]
    root = root_folder(access, prefix=PREFIX, run_id=PROBE_RUN, attempt=1, create=False)
    manifest = read_json_file(root, MANIFEST, access)
    record = DriveJournal(root, access).head()
    if record["state"]["manifest_sha"] != digest(encode(manifest)):
        raise ValueError("RECORDED_TEST_MANIFEST_DIGEST")
    state = record["state"]
    def packet_folder(name):
        return ensure(root, name, FOLDER, access, create=False)
    current = packets(access, packet_folder, state["packets"], include_status=True)
    expected = {key for key, item in state["tasks"].items() if item.get("proof")}
    if expected != current.keys():
        raise ValueError("RECORDED_SAMPLE_INVENTORY")
    newer = []
    for key, (raw, _) in current.items():
        evidence = state["tasks"][key]
        params = manifest["tests"][key]["params"]
        _, raw_sha, packed_sha = compress_original(raw)
        rows = json.loads(raw)["data"]
        if task_id_from_parameters(params) != key or evidence["proof"] != {
            "raw_sha": raw_sha, "packed_sha": packed_sha, "rows": len(rows),
        }:
            raise ValueError("RECORDED_SAMPLE_DIGEST")
        dates = set()
        for row in rows:
            try:
                dates.add(observation_date(params["dataset"], row.get("date")).isoformat())
            except (TypeError, ValueError):
                continue
        newer.append({"dataset": params["dataset"], "request": params, "result": evidence["status"],
                      "observed_dates": sorted(dates), "raw_sha256": raw_sha, "rows": len(rows)})
    summary = read_json_file(root, PROBE_ACCEPT, access)
    return documented, old_rows, newer, summary


def task_id_from_parameters(params):
    return digest(encode(params))


def safe_evidence(rows, run):
    return [{k: row[k] for k in ("dataset", "request", "result", "observed_dates", "raw_sha256",
                                  "gzip_sha256", "rows", "sample_period") if k in row} | {"run": run}
            for row in rows]


def run(ctx):
    management_folder, _ = management(ctx.access, create=False)
    roster, _ = reconciled_catalog(ctx.access, management_folder)
    documented, old, new, summary = samples_for(ctx.access)
    findings = []
    try:
        current_sha, current_contracts, tiers = public_catalog()
        delta = {"added": sorted(current_contracts.keys() - documented.keys()),
                 "removed": sorted(documented.keys() - current_contracts.keys()),
                 "changed": sorted(n for n in current_contracts.keys() & documented.keys()
                                   if current_contracts[n] != documented[n])}
    except (OSError, ValueError, KeyError):
        current_sha, tiers, delta = None, {}, {}
        findings.append("CURRENT_OFFICIAL_CATALOG_UNAVAILABLE")
    evidence = safe_evidence(old, RUN_HISTORY) + safe_evidence(new, PROBE_RUN)
    by_name = defaultdict(list)
    for row in evidence:
        by_name[row.get("dataset")].append(row)
    rows, scope_holds = [], []
    for item in roster:
        name, state = item["dataset"], item["state"]
        doc = documented.get(name)
        admitted_venue = {venue for source in (ctx.plan, ctx.supplement) if source
                          for venue in source["contracts"].get(name, {}).get("venues", ())}
        holds = []
        if state.startswith("EXCLUDED"):
            decision = "EXCLUDED"
            reason = state
        elif state == "HOLD_SCOPE_REVIEW":
            decision, reason = "HOLD", REVIEW_REASONS.get(name, state)
            holds.append("SCOPE_NOT_APPROVED")
        else:
            decision, reason = "HOLD", "Native full-history coverage and completeness must be evidenced."
            if admitted_venue:
                decision, reason = "BOUNDED_EVIDENCE", "Existing certified stock plan covers only its enumerated windows."
            holds.extend(("FULL_HISTORY_UNIVERSE_UNPROVEN", "PAGINATION_OR_SPLIT_EQUIVALENCE_UNPROVEN_OUTSIDE_CERTIFIED_WINDOWS"))
        candidate = candidate_rule(name, doc) if doc and state == "SAMPLE_ONLY" else None
        if candidate and not candidate.key_fields:
            holds.append("NATIVE_UNIQUE_KEY_UNPROVEN")
        if candidate and candidate.query_mode == "start_only":
            holds.append("UPPER_QUERY_BOUND_UNPROVEN")
        if current_sha and not state.startswith("EXCLUDED"):
            if name not in tiers or tiers[name] not in BACKER_COMPATIBLE or name in delta["changed"]:
                holds.append("CURRENT_DOCUMENTED_SCOPE_OR_CONTRACT_CHANGED")
        if holds:
            scope_holds.append({"dataset": name, "reasons": holds, "decision": reason})
        contract = asdict(candidate) if candidate else None
        rows.append(item | {
            "decision": decision, "reason": reason, "documented_tier": doc.tier if doc else None,
            "documented_fields": doc.fields if doc else (),
            "documented_required": doc.required if doc else None,
            "documented_optional": doc.optional if doc else None,
            "native_candidate": contract, "contract_version": digest(encode(contract)) if contract else None,
            "bound_venues": sorted(admitted_venue),
            "sample_evidence_count": len(by_name[name]),
            "completeness": "CERTIFIED_WINDOWS_ONLY" if admitted_venue else "UNPROVEN",
            "scope_holds": holds,
        })
    if scope_holds:
        findings.append("DATASET_SCOPE_OR_FULL_CONTRACTS_UNRESOLVED")
    return {"status": "HOLD" if findings else "PASS", "findings": findings, "catalog_count": len(rows),
            "candidate_states": dict(Counter(row["state"] for row in rows)),
            "outputs": {
                "DATASET_REGISTRY.json": {"rows": rows, "historical_public_catalog": SOURCE,
                                          "current_public_catalog": CURRENT_CATALOG, "current_sha256": current_sha,
                                          "current_catalog_delta": delta},
                "CONTRACT_EVIDENCE.json": {"stored_samples": evidence, "initial_row_count": len(old),
                                            "supplemented_result_count": len(new), "prior_readback_summary": {
                                                k: v for k, v in summary.items() if k not in {"receipts", "bronze_holds"}}},
                "SCOPE_HOLDS.json": {"holds": scope_holds, "findings": findings},
            }}
