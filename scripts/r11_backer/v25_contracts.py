"""Close the V2.4 contract gaps from pinned documents and bounded original replies."""

from collections import Counter
from dataclasses import asdict, replace
from datetime import date
import gzip
from io import BytesIO
import json
import re
from zipfile import ZipFile

from .bulk_manifest import adapt_rule
from .final_readiness_v2 import original
from .integration_pilot import digest, encode, verify_private_folder
from .release_probes import contracts, read_private_file
from .release_rehearsal import require
from .v2_registry import NativeContract, read_rows

HIGH_FREQUENCY = {"TaiwanFutOptTickInfo", "TaiwanFuturesKBar", "TaiwanFuturesSpreadTick", "TaiwanStockKBar"}
PRIOR_FOUR = {"TaiwanFuturesFinalSettlementPrice", "TaiwanStockParValueChange", "TaiwanStockActiveETFInfo", "TaiwanStockTradingDate"}
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


def offline(ctx):
    baseline = ctx.prior("DATASET_REGISTRY.json")
    require(baseline["counts"] == {"CONTRACT_READY": 66, "EXCLUDED": 19, "HOLD": 18}, "V25_CONTRACT_BASELINE")
    lock, records = official_inputs(ctx)
    docs = contracts(records["MCP"].decode("utf-8"))
    ready, excluded, holds = set(), set(), set()
    rows, decisions = [], []
    for row in baseline["rows"]:
        dataset = row["dataset"]
        if row["state"] == "CONTRACT_READY":
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
        "PROBE_PLAN.json": {"selected": selected, "purpose": "NEEDED_NONREPEATING_NATIVE_WITNESSES"}}


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


def observed_contract(row, record, raw):
    dataset = row["dataset"]
    doc = record["evidence"]
    params = record["parameters"]
    if dataset == "TaiwanStockTradingDate":
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
    output, gaps = [], []
    for row in work["rows"]:
        dataset = row["dataset"]
        if row["state"] != "HOLD":
            output.append(row)
            continue
        candidate = direct.get(dataset)
        if not candidate:
            gaps.append({"dataset": dataset, "reason": "NO_NEW_SUCCESSFUL_AUTHORIZED_NATIVE_ORIGINAL"})
            output.append(row)
            continue
        try:
            rule, mode, days, proof = observed_contract(row, candidate, original_probe(ctx, candidate))
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            label = str(error) if re.fullmatch(r"[A-Z0-9_]{3,90}", str(error)) else type(error).__name__
            gaps.append({"dataset": dataset, "reason": label, "file_id": candidate["file_id"]})
            output.append(row | {"v25": "HOLD_WITH_REASON", "v25_witness_failure": label})
            continue
        witness = {key: candidate[key] for key in ("raw_sha256", "gzip_sha256", "file_id", "filename", "parameters", "captured_at", "raw_bytes", "packed_bytes")}
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
