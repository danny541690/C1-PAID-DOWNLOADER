"""Close the V2.4 contract gaps from pinned documents and bounded original replies."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import date
import gzip
from io import BytesIO
import json
import re
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from .bulk_manifest import adapt_rule
from .drive_pilot import ensure
from .final_readiness_v2 import original
from .archive import compress_original
from .final_source_probes import PREFIX as HISTORICAL_PREFIX
from .finish_preflight import RUN_HISTORY
from .integration_pilot import digest, encode, verify_private_folder
from .release_probes import contracts, read_private_file
from .release_rehearsal import require, root_folder
from .tpex_history import ARCHIVE, verified_archive_files
from .v2_registry import NativeContract, read_rows
from .v24_sources import DATE_BOUND, TOPIC, plain, public_read

HIGH_FREQUENCY = {"TaiwanFutOptTickInfo", "TaiwanFuturesKBar", "TaiwanFuturesSpreadTick", "TaiwanStockKBar"}
PRIOR_FOUR = {"TaiwanFuturesFinalSettlementPrice", "TaiwanStockParValueChange", "TaiwanStockActiveETFInfo", "TaiwanStockTradingDate"}
TRADING_DATE = "TaiwanStockTradingDate"
CALENDAR_PACKET = "18jaIuL_-a3QS-IQBL_xo2RYDeC6QKt93"
CALENDAR_MEMBER = "backer/response-34.json"
TECHNICAL_BOUNDS = {
    "TaiwanStockMonthPrice": "k-taiwanstockmonthprice-backersponsor",
    "TaiwanStockWeekPrice": "k-taiwanstockweekprice-backersponsor",
    "TaiwanStockPER": "perpbr-taiwanstockper",
    "TaiwanStockPriceLimit": "taiwanstockpricelimit-backersponsor",
}
HISTORY_PAGES = {"CnnIndex": "Others", "TaiwanBusiness": "TaiwanMarket/Others", "ConvertibleBond": "TaiwanMarket/ConvertibleBond"}
HISTORY_BOUNDS = {
    "CnnFearGreedIndex": ("CnnIndex", "cnnfeargreedindex-backersponsor"),
    "TaiwanBusinessIndicator": ("TaiwanBusiness", "taiwanbusinessindicator-backersponsor"),
    "TaiwanAssetSwapFixedIncomeDaily": ("ConvertibleBond", "taiwanassetswapfixedincomedaily-backersponsor"),
    "TaiwanAssetSwapOptionDaily": ("ConvertibleBond", "taiwanassetswapoptiondaily-backersponsor"),
    "TaiwanStockConvertibleBondMonthlyAnalysis": ("ConvertibleBond", "taiwanstockconvertiblebondmonthlyanalysis-backersponsor"),
    "TaiwanStockConvertibleBondPutProvision": ("ConvertibleBond", "taiwanstockconvertiblebondputprovision-backersponsor"),
}
CONVERTIBLE_DAY = {
    "TaiwanStockConvertibleBondDaily": "backersponsor", "TaiwanStockConvertibleBondInstitutionalInvestors": "backersponsor_1",
    "TaiwanStockConvertibleBondDailyOverview": "backersponsor_2", "TaiwanAssetSwapFixedIncomeDaily": "backersponsor_3",
    "TaiwanAssetSwapOptionDaily": "backersponsor_4",
}
HISTORY_ORIGINALS = "OFFICIAL_HISTORY_SOURCE_ORIGINALS.zip"
PROBES = (
    ("TaiwanFuturesFinalSettlementPrice", "B08", 4, "Backer", {"data_id": "TX", "start_date": "2024-01-17", "end_date": "2024-01-17"}, "Derivative", "taiwanfuturesfinalsettlementprice-backersponsor"),
    ("TaiwanStockTradingDate", "B01", 3, "Free", {}, "Technical", "taiwanstocktradingdate"),
    ("TaiwanStockParValueChange", "B05", 2, "Free", {"start_date": "2020-08-17", "end_date": "2020-08-17"}, "Fundamental", "taiwanstockparvaluechange"),
    ("TaiwanStockActiveETFInfo", "B08", 1, "Free", {}, "Chip", "etf-taiwanstockactiveetfinfo"),
)


def official_inputs(ctx):
    lock = ctx.prior("OFFICIAL_SOURCE_LOCK.json")
    file_id = lock["originals"]["file_id"]
    verify_private_folder(file_id, ctx.access)
    packet = read_private_file(file_id, ctx.access)
    require(digest(packet) == lock["originals"]["sha256"], "V25_OFFICIAL_SOURCE_ARCHIVE_SHA")
    with ZipFile(BytesIO(packet)) as archive:
        bodies = {name: archive.read(name + (".md" if name == "MCP" else ".html"))
                  for name in ("MCP", "Technical", "Fundamental", "Chip", "Derivative", "Pricing")}
    for name, raw in bodies.items():
        require(digest(raw) == lock["sources"][name]["sha256"], "V25_OFFICIAL_MEMBER_SHA")
    return lock, bodies


def historical_sources(ctx):
    def fetch(entry):
        key, path = entry
        url = f"https://finmind.github.io/tutor/{path}/"
        try:
            data = public_read(url)
            return key, data, {"source": key, "url": url, "sha256": digest(data), "bytes": len(data)}, None
        except (OSError, RuntimeError, ValueError) as error:
            return key, None, None, type(error).__name__

    source, raw, findings = {}, {}, {}
    with ThreadPoolExecutor(max_workers=len(HISTORY_PAGES)) as pool:
        for key, data, ref, error in pool.map(fetch, HISTORY_PAGES.items()):
            if error:
                findings[key] = error
            else:
                source[key], raw[key] = ref, data
    payload = BytesIO()
    with ZipFile(payload, "w") as archive:
        for name, data in sorted(raw.items()):
            archive.writestr(ZipInfo(f"{name}.html"), data, compress_type=ZIP_DEFLATED)
    packed = payload.getvalue()
    file_id = ctx.binary(HISTORY_ORIGINALS, packed, "application/zip")
    return raw, {"sources": source, "findings": findings, "originals": {"file_id": file_id, "sha256": digest(packed)},
                 "charged_finmind_api": 0}


def documented_bound(row, raw, source, anchor):
    """Read the official section by label, including anchors with Chinese title prefixes."""
    dataset = row["dataset"]
    match = next((m for m in TOPIC.finditer(raw.decode()) if m[1] == anchor), None)
    require(match is not None and dataset in plain(match[2]), "V25_DOCUMENTED_SECTION_BINDING")
    bound = DATE_BOUND.search(plain(match[3]))
    require(bound is not None, "V25_DOCUMENTED_START")
    first = str(date.fromisoformat(bound[1]))
    assert first == bound[1]
    result = row | {"source_dates_documented_from": sorted(set(row["source_dates_documented_from"]) | {first}),
                    "evidence": row["evidence"] + [source | {"anchor": anchor, "documented_from": first}]}
    if dataset == "TaiwanStockPriceLimit":
        require("每日漲跌停價" in plain(match[2]) and "MARKET_DAY" in row["mode_contracts"] and any(
            item.get("market_day_tier") == "Backer" for item in row["source_rules"]), "V25_PRICE_LIMIT_DAILY_MODE")
        result |= {"grain": "daily", "schema": row["schema"] | {"grain": "daily"},
                   "mode_contracts": {"MARKET_DAY": row["mode_contracts"]["MARKET_DAY"] | {"grain": "daily"}}}
    return result


def convertible_day(row, raw, source, anchor):
    match = next((m for m in TOPIC.finditer(raw.decode()) if m[1] == anchor), None)
    require(match is not None and all(item in plain(match[2]).lower() for item in ("特定日期", "所有資料", "backer")) and
            row["dataset"] in match[3], "V25_CONVERTIBLE_MARKET_DAY_DOCUMENTATION")
    rule = adapt_rule(NativeContract(**row["schema"]), "MARKET_DAY", 4 * 1024 * 1024)
    return row | {"mode_contracts": row["mode_contracts"] | {"MARKET_DAY": asdict(rule)},
                  "evidence": row["evidence"] + [source | {"anchor": anchor, "mode": "MARKET_DAY", "tier": "Backer"}]}


def offline(ctx):
    baseline = ctx.prior("DATASET_REGISTRY.json")
    require(baseline["counts"] == {"CONTRACT_READY": 66, "EXCLUDED": 19, "HOLD": 18}, "V25_CONTRACT_BASELINE")
    lock, records = official_inputs(ctx)
    extra_raw, history = historical_sources(ctx)
    docs = contracts(records["MCP"].decode("utf-8"))
    ready, excluded, holds = set(), set(), set()
    rows, decisions = [], []
    for row in baseline["rows"]:
        dataset = row["dataset"]
        if row["state"] == "CONTRACT_READY":
            if dataset in TECHNICAL_BOUNDS:
                row = documented_bound(row, records["Technical"], lock["sources"]["Technical"], TECHNICAL_BOUNDS[dataset])
            if dataset in HISTORY_BOUNDS:
                page, anchor = HISTORY_BOUNDS[dataset]
                if page in extra_raw:
                    row = documented_bound(row, extra_raw[page], history["sources"][page], anchor)
            if dataset in CONVERTIBLE_DAY and "ConvertibleBond" in extra_raw:
                row = convertible_day(row, extra_raw["ConvertibleBond"], history["sources"]["ConvertibleBond"], CONVERTIBLE_DAY[dataset])
            rows.append(row | {"v25": "INHERITED_CONTRACT_READY"})
            ready.add(dataset)
            continue
        if row["state"] == "EXCLUDED":
            excluded.add(dataset)
            rows.append(row | {"v25": "INHERITED_SCOPE_EXCLUSION"})
            continue
        contract = docs.get(dataset)
        reason = None
        if dataset in HIGH_FREQUENCY:
            reason = "INTRADAY_TICK_OR_BAR_EXCLUSION"
        elif contract and contract.tier in {"Sponsor", "Sponsor Pro"}:
            reason = "OFFICIAL_SPONSOR_OR_HIGHER_EXCLUSION"
        if reason:
            decisions.append({"dataset": dataset, "status": "EXCLUDED_BY_RULE", "reason": reason,
                              "official_tier": contract.tier if contract else None,
                              "official_source": lock["sources"]["MCP"]})
            rows.append(row | {"state": "EXCLUDED", "v25": "EXCLUDED_BY_RULE", "scope_decision": reason})
        else:
            holds.add(dataset)
            rows.append(row)
    require(ready.isdisjoint(excluded) and len(ready) == 66 and len(excluded) == 19, "V25_SCOPE_PRESERVED")
    selected = []
    for dataset, lane, priority, tier, parameters, doc, anchor in PROBES:
        require(dataset in holds and docs[dataset].tier == tier and f'id="{anchor}"' in records[doc].decode(),
                "V25_PROBE_OFFICIAL_SCOPE")
        selected.append({"parameters": {"dataset": dataset, **parameters}, "lane": lane, "priority": priority,
                         "tier": tier, "evidence": lock["sources"][doc] | {"anchor": anchor}})
    require(set(row["parameters"]["dataset"] for row in selected) == PRIOR_FOUR, "V25_TARGETED_WITNESS_PLAN")
    return "PASS", {"CONTRACT_REVIEW.json": {"rows": rows, "initial_holds": sorted(holds), "new_decisions": decisions,
        "official_sources": lock["sources"], "previous_ready": len(ready), "previous_excluded": len(excluded)},
        "PROBE_PLAN.json": {"selected": selected, "purpose": "NEEDED_NONREPEATING_NATIVE_WITNESSES"},
        "OFFICIAL_HISTORY_SOURCE_LOCK.json": history}


def original_probe(ctx, record):
    fid, packed = original(ctx.access, record.get("source_folder_id", ctx.root), record["filename"])
    require(fid == record["file_id"] and digest(packed) == record["gzip_sha256"] and len(packed) == record["packed_bytes"],
            "V25_PROBE_PACKED_SHA")
    with gzip.GzipFile(fileobj=BytesIO(packed)) as decoded:
        raw = decoded.read(4 * 1024 * 1024 + 1)
    require(digest(raw) == record["raw_sha256"] and len(raw) == record["raw_bytes"], "V25_PROBE_RAW_SHA")
    return raw


def calendar_days(raw):
    reply = json.loads(raw)
    require(reply.get("status") == 200 and isinstance(reply["data"], list), "V25_CALENDAR_API_STATUS")
    dates = [row["date"] for row in reply["data"]]
    require(dates and len(set(dates)) == len(dates) and all(str(date.fromisoformat(day)) == day for day in dates),
            "V25_CALENDAR_NATIVE_KEY")
    return sorted(dates)


def archived_calendar(ctx, decision):
    """Pin the exact prior source body and hashes without repeating its snapshot request."""
    legacy = ctx.legacy("CONTRACT_EVIDENCE.json")
    parameters = {"dataset": TRADING_DATE}
    entry, = [item for item in legacy["stored_samples"] if item["run"] == RUN_HISTORY and
              item.get("request") == parameters and item["result"] == "VALID_SAMPLE"]
    folder = root_folder(ctx.access, prefix=f"{HISTORICAL_PREFIX}_B", run_id=RUN_HISTORY, attempt=1, create=False)
    fid = ensure(folder, ARCHIVE, "application/zip", ctx.access, create=False)
    require(fid == CALENDAR_PACKET, "V25_CALENDAR_ARCHIVE_IDENTITY")
    packet = verified_archive_files(ctx.access, folder)
    report = json.loads(packet["reconciliation.json"])
    match, = [item for item in report["backer_requests"] if item["request"] == parameters]
    require(match["status"] == "VALID_SAMPLE" and match["original"] == CALENDAR_MEMBER, "V25_CALENDAR_ARCHIVE_MEMBER")
    raw = packet[CALENDAR_MEMBER]
    packed, raw_sha, packed_sha = compress_original(raw)
    require(raw_sha == entry["raw_sha256"] and packed_sha == entry["gzip_sha256"] and
            len(calendar_days(raw)) == entry["rows"], "V25_CALENDAR_ARCHIVED_ROWS_AND_DUAL_SHA")
    return raw, {"raw_sha256": raw_sha, "gzip_sha256": packed_sha, "file_id": fid, "filename": ARCHIVE,
                 "archive_member": CALENDAR_MEMBER, "packed_bytes": len(packed), "raw_bytes": len(raw),
                 "source_run": RUN_HISTORY, "source_attempt": 1, "parameters": parameters,
                 "capture_time_known": False, "evidence": decision["evidence"], "status": "ARCHIVED_ORIGINAL_REUSED"}


def observed_contract(row, record, raw):
    dataset = row["dataset"]
    doc = record["evidence"]
    params = record["parameters"]
    if dataset == TRADING_DATE:
        days = calendar_days(raw)
        rule = NativeContract(dataset, "snapshot", "snapshot", ("date",), ("date",), date_field=None,
                              required_parameters=("dataset",), allowed_parameters=("dataset",))
        return rule, "SNAPSHOT", days, {"method": "AUTHORIZED_NATIVE_CALENDAR_KEYS",
                                        "native_unique_date_count": len(days), "earliest_observed": days[0],
                                        "last_observed": days[-1], "complete_market_history": False}
    rule = NativeContract(**row["schema"])
    mode = "SNAPSHOT" if dataset == "TaiwanStockActiveETFInfo" else "EVENT_RANGE"
    if dataset == "TaiwanFuturesFinalSettlementPrice":
        # The official Derivative example documents an optional instrument and a bounded filter.
        rule = replace(rule, identity_field="futures_id", key_fields=("date", "futures_id", "contract_month"),
                       key_policy="native_unique", allowed_parameters=tuple(sorted(set(rule.allowed_parameters) | {"data_id"})))
    if dataset == "TaiwanStockParValueChange":
        rule = replace(rule, key_fields=("date", "stock_id"), key_policy="native_unique")
    active = adapt_rule(rule, mode, 4 * 1024 * 1024)
    data, _ = read_rows(active, params, raw)
    require(data, "V25_POSITIVE_NATIVE_ORIGINAL_NOT_WITNESSED")
    keys = [tuple(obj[key] for key in active.key_fields) for obj in data]
    require(len(keys) == len(set(keys)), "V25_NATIVE_PROBE_KEYS_DUPLICATE")
    if dataset == "TaiwanStockActiveETFInfo":
        require(all(isinstance(r["stock_id"], str) and re.fullmatch(r"[0-9A-Z]+", r["stock_id"]) and
                    r["type"].lower() in {"twse", "tpex"} for r in data), "V25_ETF_MARKET_SCOPE")
        require(doc["anchor"] == "etf-taiwanstockactiveetfinfo", "V25_ETF_DOC_UNIVERSE")
    observations = sorted({obj["date"] for obj in data if isinstance(obj.get("date"), str)})
    return active, mode, observations, {"method": "EXPECTED_NATIVE_KEYS_OR_INDEPENDENT_MARKET_RECONCILIATION",
                                         "source_original_rows": len(data), "source_keys_sha256": digest(encode(keys)),
                                         "confirmed_scope": "TWSE_AND_TPEX_ONLY" if mode == "SNAPSHOT" else "WITNESSED_EVENT_WINDOW"}


def final(ctx):
    work = ctx.read("CONTRACT_REVIEW.json")
    try:
        result = ctx.read("PROBE_RESULTS.json")
    except (OSError, RuntimeError, ValueError):
        # The official decisions remain valid if the independent live witness stage is unavailable.
        result = {"rows": []}
    direct = {row["dataset"]: row for row in result["rows"] if row.get("file_id") and row["status"] == "HTTP_RESPONSE_VERIFIED"}
    calendar = next(item for item in ctx.read("PROBE_PLAN.json")["selected"] if item["parameters"]["dataset"] == TRADING_DATE)
    output, gaps = [], []
    for row in work["rows"]:
        dataset = row["dataset"]
        if row["state"] != "HOLD":
            output.append(row)
            continue
        candidate = direct.get(dataset)
        if not candidate and dataset != TRADING_DATE:
            gaps.append({"dataset": dataset, "reason": "NO_NEW_SUCCESSFUL_AUTHORIZED_NATIVE_ORIGINAL"})
            output.append(row)
            continue
        try:
            if candidate:
                raw = original_probe(ctx, candidate)
                witness = {key: candidate[key] for key in ("raw_sha256", "gzip_sha256", "file_id", "filename", "parameters",
                                                          "captured_at", "raw_bytes", "packed_bytes")}
            else:
                raw, candidate = archived_calendar(ctx, calendar)
                witness = candidate
            rule, mode, days, proof = observed_contract(row, candidate, raw)
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            label = str(error) if re.fullmatch(r"[A-Z0-9_]{3,90}", str(error)) else type(error).__name__
            gaps.append({"dataset": dataset, "reason": label, "file_id": (candidate or {}).get("file_id")})
            output.append(row | {"v25": "HOLD_WITH_REASON", "v25_witness_failure": label})
            continue
        output.append(row | {"state": "CONTRACT_READY", "v25": "CONTRACT_READY_WITH_ORIGINAL",
                              "mode_contracts": {mode: asdict(rule)}, "schema": asdict(rule), "grain": rule.grain,
                              "contract_blockers": [], "v25_witness": witness | {"observed_dates": days, "proof": proof},
                              "completeness_methods": [proof["method"]]})
    counters = dict(Counter(r["state"] for r in output))
    registry = {"rows": output, "counts": counters, "input_registry_count": len(work["rows"]),
                "new_exclusions": work["new_decisions"], "continuing_source_contracts": work["previous_ready"],
                "prior_exclusions_preserved": work["previous_excluded"], "fully_verified_download_windows": 0}
    status = "HOLD" if gaps else "PASS"
    return status, {"DATASET_REGISTRY.json": registry, "CONTRACT_GAP_MATRIX.json": {"rows": gaps,
                      "decisions": work["new_decisions"], "archived_raw_evidence_reused": 259}}
