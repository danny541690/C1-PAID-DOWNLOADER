"""Combine Backer scope decisions, historical probes, sizing, and dispatch readiness."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from .archive import compress_original
from .combined_preflight import archived_reports, monthly_original, validated_pending_history
from .dispatch import Window, assignments
from .drive_pilot import BASE, FOLDER, ROOT, ensure, token as drive_token
from .history_pilot import BUSINESS, iso_date
from .live_preflight import drive_health, read_json
from .manifest_pilot import sample_data
from .market_lifetimes import digest, earlier_evidence
from .release_probes import (
    MAX_BYTES, SOURCE, Sample, Task, compare_market, contracts, observation_date, read_private_file, run_parallel,
)
from .test_archive import json_bytes
from .tpex_history import private_folder, save_private, verified_archive_files


PREFIX = "R11_RELEASE_READINESS"
REPORT = Path("r11-release-readiness.json")
HISTORY_RUN = 45752224557
SPEC_FILES = ("R11_BACKER_20261125.md", "R11_1_BACKER_20261125.md")
ACCOUNT = "https://api.web.finmindtrade.com/v2/user_info"
BOOK = "TaiwanStockStatisticsOfOrderBookAndTrade"
MARGIN = "TaiwanStockMarginPurchaseShortSale"
REDUCTION = "TaiwanStockCapitalReductionReferencePrice"
BROKERS = "TaiwanSecuritiesTraderInfo"
FED = "InterestRate"
QUARTER = "2026-09-30"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFEDTAR&cosd=1982-09-24&coed=1982-10-03"
FRED_MEMBER = "fred-early-policy-target.csv"
DOCS_MEMBER = "finmind-datasets.md"
PROBES = (
    Task(BROKERS, None, None),
    Task(REDUCTION, "1969-12-01", "1970-02-01"),
    Task(REDUCTION, "2013-10-01", "2013-10-31"),
    Task(REDUCTION, "2026-10-26", "2026-11-25"),
    Task(FED, "1982-09-24", "1982-10-03", "FED"),
    Task("TaiwanStockFinancialStatements", QUARTER, QUARTER, "2330"),
    Task("TaiwanStockBalanceSheet", QUARTER, QUARTER, "2330"),
    Task("TaiwanStockCashFlowsStatement", QUARTER, QUARTER, "2330"),
    Task("TaiwanStockHoldingSharesPer", "2026-10-01", "2026-10-31", "2330"),
    *(Task(MARGIN, day, day, scope) for day in ("2001-01-02", "2013-03-19", "2026-11-25") for scope in (None, "2330")),
)
PRIMARY_KEYS = {
    BROKERS: ("date", "securities_trader_id"),
    REDUCTION: ("date", "stock_id"),
    FED: ("date", "country"),
    "TaiwanStockFinancialStatements": ("date", "stock_id", "type"),
    "TaiwanStockBalanceSheet": ("date", "stock_id", "type"),
    "TaiwanStockCashFlowsStatement": ("date", "stock_id", "type"),
    "TaiwanStockHoldingSharesPer": ("date", "stock_id", "HoldingSharesLevel"),
    MARGIN: ("date", "stock_id"),
}


def public_capture(url, limit):
    with urlopen(url, timeout=15) as response:
        result = response.read(limit + 1)
    if not result or len(result) > limit:
        raise ValueError("public reference exceeds its documented bounds")
    return result


def rate_reference():
    try:
        return public_capture(FRED, 65_536)
    except (ValueError, OSError):
        return None


def specs(access):
    folder = ROOT
    for name in ("00_COMMAND", "R11_BACKER"):
        folder = ensure(folder, name, FOLDER, access, create=False)
    documents = {}
    for name in SPEC_FILES:
        file_id = ensure(folder, name, "text/markdown", access, create=False)
        meta = read_json(f"{BASE}/files/{file_id}?fields=parents,shared,trashed", access)
        if meta.get("shared") is not False or meta.get("trashed") is not False or meta.get("parents") != [folder]:
            raise ValueError("formal specification is outside the private R11 folder")
        documents[name] = read_private_file(file_id, access)
    return documents


def scope_register(documents, catalog):
    original, revision = (documents[name].decode("utf-8-sig") for name in SPEC_FILES)
    if "非必要排除，未知 HOLD" not in original or "排除逐筆、主筆成交明細、分鐘、秒級、小時及即時快照" not in revision:
        raise ValueError("R11 research and frequency rules need to be reviewed")
    section = re.search(rf"(?m)^### {BOOK}\s*\n(.*?)(?=^### |^## |\Z)", catalog, re.S)
    if section is None or not all(mark in section[1] for mark in ("Time", "每分鐘", "每 5 秒")):
        raise ValueError("documented order-book granularity needs to be reviewed")
    rows = sample_data()
    for row in rows:
        if row["dataset"] == BOOK:
            if row["state"] != "HOLD_SCOPE_REVIEW":
                raise ValueError("the historical review roster has changed")
            row.update(state="EXCLUDED_SUBDAILY", resolution="R11.1 §03 + documented seconds/minutes", reference=SOURCE)
    return rows


def account_probes(auth, registry):
    try:
        info = read_json(ACCOUNT, auth)
    except (ValueError, OSError) as error:
        state = "HOLD_RATE_LIMITED" if isinstance(error, HTTPError) and error.code == 429 else "HOLD_ACCOUNT_LOOKUP"
        return {"status": state, "exact_expiry": "HOLD_NO_EXACT_DATE"}, dict.fromkeys(PROBES, Sample("NOT_SENT_ACCOUNT"))
    limit, used = (info.get("api_request_limit"), info.get("user_count")) if isinstance(info, dict) else (None, None)
    usable = type(limit) is int and limit == 1600 and type(used) is int and 0 <= used <= limit - len(PROBES)
    account = {"status": "PASS_BACKER_REFERENCE" if usable else "HOLD_ACCOUNT_OR_HEADROOM",
               "reference_limit": limit if type(limit) is int else None, "exact_expiry": "HOLD_NO_EXACT_DATE"}
    samples = run_parallel(PROBES, registry, auth) if usable else dict.fromkeys(PROBES, Sample("NOT_SENT_ACCOUNT"))
    return account, samples


def sample_receipt(task, sample):
    result = {"parameters": task.parameters(), "state": sample.status}
    if not sample.raw:
        return result
    values = json.loads(sample.raw)
    if values.get("status") != 200 or not values["data"]:
        raise ValueError("archived response does not match its successful API state")
    rows = values["data"]
    observed, invalid_dates = [], 0
    for row in rows:
        try:
            observed.append(observation_date(task.dataset, row.get("date")))
        except (ValueError, TypeError):
            invalid_dates += 1
    primary = PRIMARY_KEYS[task.dataset]
    keyed = all(all(type(row.get(field)) in (str, int) and row[field] != "" for field in primary) for row in rows)
    duplicate_keys = len(rows) - len({tuple(row[field] for field in primary) for row in rows}) if keyed else None
    packed, raw_sha, packed_sha = compress_original(sample.raw)
    return result | {
        "rows": len(rows), "observed_first": str(min(observed)) if observed else None,
        "observed_last": str(max(observed)) if observed else None, "invalid_observation_dates": invalid_dates,
        "candidate_key": list(primary), "duplicate_candidate_keys": duplicate_keys,
        "field_layouts": sorted({",".join(sorted(row)) for row in rows}),
        "raw_bytes": len(sample.raw), "gzip_bytes": len(packed), "raw_sha256": raw_sha, "gzip_sha256": packed_sha,
    }


def fred_reconciliation(fred, sample):
    if fred is None:
        return {"status": "HOLD_FRED_SOURCE_UNAVAILABLE"}
    dates = {iso_date(row["observation_date"]): Decimal(row["DFEDTAR"])
             for row in csv.DictReader(fred.decode("utf-8-sig").splitlines()) if row["DFEDTAR"] not in {".", ""}}
    if min(dates) != date(1982, 9, 27):
        raise ValueError("FRED historical reference start does not match its source series")
    if sample.status != "VALID_SAMPLE":
        return {"status": sample.status, "fred_first_day": str(min(dates))}
    try:
        common = {iso_date(row["date"]): Decimal(str(row["interest_rate"])) for row in sample.rows}
    except ArithmeticError:
        return {"status": "HOLD_RATE_CONTRACT", "fred_first_day": str(min(dates))}
    matches = sum(day in dates and dates[day] == interest for day, interest in common.items())
    return {"status": "PASS_DATED_RATE_SAMPLE" if matches == len(common) == len(sample.rows) else "HOLD_RATE_COMPARISON",
            "fred_first_day": str(min(dates)), "compared_dates": len(common), "matched_dates": matches,
            "scope": "FED_TARGET_RATE_SAMPLE_ONLY"}


def sizing(baseline, margin_samples, drive):
    calendar = baseline["capacity"][0]["trading_calendar"]
    margin_reference = baseline["acceptance"][0]["sweep"]["all_market"][MARGIN]
    if margin_reference["matching_days"] != margin_reference["tested_days"] or not margin_reference["tested_days"]:
        raise ValueError("prior margin sizing sample consensus is missing")
    benchmarks = [margin_reference["observed_compressed_bytes_per_market_day_max"]]
    benchmarks.extend(len(compress_original(sample.raw)[0]) for sample in margin_samples)
    days = calendar["known_days_for_sizing"]
    per_day = {MARGIN: max(benchmarks), "TaiwanStockPrice": calendar["one_day_compressed_bytes"]}
    ranges = {name: amount * days for name, amount in per_day.items()}
    available = drive["available_bytes"]
    return {"known_sessions_reference": days, "market_day_gzip_bytes": per_day,
            "illustrative_sized_bytes": ranges, "illustrative_total_bytes": sum(ranges.values()),
            "example_fits": isinstance(available, int) and sum(ranges.values()) <= available,
            "status": "HOLD_REMAINING_DATASETS_AND_FULL_COVERAGE"}


def preview_business(report, archive, contract):
    coverage = report["historical_business"]
    if (coverage["status"] != "PASS_PROVIDER_WINDOW" or coverage["verified_months"] != archive["indexed_months"]
            or archive["compressed_bytes"] != report["storage"]["monthly_original_gzip_bytes"]):
        raise ValueError("verified macro original does not match the historical coverage report")
    first, last = map(iso_date, (coverage["earliest_observed"], coverage["through_official_period"]))
    window = Window(BUSINESS, "TW", first, last, contract, "B07")
    assigned, = assignments([window], {BUSINESS: last})
    return {"dataset": BUSINESS, "lane": assigned.lane, "task_id": assigned.task_id,
            "covered_start": str(assigned.start), "covered_end": str(assigned.end),
            "existing_original": archive, "queue": "HOLD_RELEASE_GATES"}


def reconcile(inputs, sources, account, samples, drive):
    reports, history_raw, monthly, documents = inputs
    catalog = sources[DOCS_MEMBER].decode("utf-8")
    scope = scope_register(documents, catalog)
    history = validated_pending_history(json.loads(history_raw))
    receipts = [sample_receipt(task, samples[task]) for task in PROBES]
    complete_fields = [row for row in receipts if row["state"] == "VALID_SAMPLE"
                       and row["invalid_observation_dates"] == 0 and row["duplicate_candidate_keys"] == 0]
    margin = {}
    for task in PROBES:
        if task.dataset == MARGIN and task.data_id is None:
            reference = Task(MARGIN, task.start, task.end, "2330")
            market, single = samples[task], samples[reference]
            margin[task.start] = {"market_status": market.status, "symbol_status": single.status,
                                  "symbol_match": compare_market(single, market)}
    rate = fred_reconciliation(sources.get(FRED_MEMBER), next(sample for task, sample in samples.items() if task.dataset == FED))
    selected = [samples[task] for task in PROBES if task.dataset == MARGIN and task.data_id is None
                and margin[task.start]["symbol_match"]]
    capacity = sizing(reports, selected, drive)
    contract = digest(json_bytes({"provider_fields": list(contracts(catalog)[BUSINESS].fields),
                                 "date_grain": "MONTH", "unique_key": ["date"]}))
    preview = preview_business(reports["capacity"][0], monthly, contract)
    public = {
        "status": "PASS_DIAGNOSTIC", "production": "HOLD", "production_dispatchable": 0,
        "candidate_statuses": dict(Counter(row["state"] for row in scope)),
        "historical_pending": history["pending"], "historical_pending_by_market": history["by_market"],
        "backer_price_gaps": reports["stock_gaps"][0]["historical_tpex"]["missing_from_bulk"],
        "live_entitlement": account["status"],
        "finmind_requests": 1 + sum(not sample.status.startswith("NOT_SENT_") for sample in samples.values()),
        "sample_statuses": dict(Counter(row["state"] for row in receipts)),
        "sampled_datasets_with_verified_candidate_keys": len({row["parameters"]["dataset"] for row in complete_fields}),
        "market_day_same_symbol_comparisons": sum(row["symbol_match"] for row in margin.values()),
        "rate_check": rate["status"], "preflight_months": monthly["indexed_months"],
        "sizing_example_bytes": capacity["illustrative_total_bytes"],
        "full_volume": capacity["status"],
    }
    details = {
        "spec_sha256": {name: digest(body) for name, body in documents.items()},
        "reference_sha256": {name: digest(body) for name, body in sources.items()},
        "datasets": scope, "scope_review_remaining": [row["dataset"] for row in scope if row["state"] == "HOLD_SCOPE_REVIEW"],
        "archive_references": {name: item[1] for name, item in reports.items()},
        "history_ledger_run": HISTORY_RUN, "history_ledger_sha256": digest(history_raw),
        "history": history, "account": account, "drive_measurement": drive,
        "sample_receipts": receipts, "market_day_checks": margin, "reference_rate_check": rate,
        "sizing": capacity, "existing_original_manifest_preview": preview,
        "remaining": {
            "historical_identity_and_backer_gaps": "HOLD",
            "whitelist_and_per_era_production_contracts": "HOLD",
            "exact_backer_expiry": account["exact_expiry"],
            "full_volume": capacity["status"],
            "deployment_and_production_g0_g5": "HOLD",
        },
    }
    return public, details


def load_saved_samples(stored, expected):
    ledger = expected["backer_requests"]
    if len(PROBES) != len(ledger):
        raise ValueError("archive does not contain the complete probe plan")
    result = {}
    for task, entry in zip(PROBES, ledger, strict=True):
        if task.parameters() != entry["request"]:
            raise ValueError("archived probe parameters changed")
        raw = stored[entry["original"]] if entry["original"] else b""
        result[task] = Sample(entry["status"], raw=raw, rows=tuple(json.loads(raw)["data"]) if raw else ())
    return result


def run(*, verify_only):
    access = drive_token()
    with ThreadPoolExecutor(max_workers=6) as pool:
        reports = pool.submit(archived_reports, access)
        history = pool.submit(earlier_evidence, HISTORY_RUN)
        monthly = pool.submit(monthly_original, access)
        documents = pool.submit(specs, access)
        if verify_only:
            saved = verified_archive_files(access, private_folder(access, create=False, prefix=PREFIX))
            expected = json.loads(saved["reconciliation.json"])
            sources = {name: saved[f"official/{name}"] for name in (DOCS_MEMBER, FRED_MEMBER)
                       if f"official/{name}" in saved}
            account, samples, drive = expected["account"], load_saved_samples(saved, expected), expected["drive_measurement"]
        else:
            catalog = pool.submit(public_capture, SOURCE, 1_000_000)
            fed = pool.submit(rate_reference)
            docs = catalog.result()
            scope_register(documents.result(), docs.decode("utf-8"))
            probes = pool.submit(account_probes, os.environ["FINMIND_BACKER_TOKEN"], contracts(docs.decode("utf-8")))
            drive = drive_health(access) | {"measured_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat()}
            account, samples = probes.result()
            sources = {DOCS_MEMBER: docs}
            if rate := fed.result():
                sources[FRED_MEMBER] = rate
        public, details = reconcile((reports.result(), history.result()["reconciliation.json"],
                                     monthly.result(), documents.result()), sources, account, samples, drive)
    result = public | details
    if verify_only:
        if json_bytes(result) != json_bytes({key: expected[key] for key in result}):
            raise ValueError("private readiness replay disagrees with the archived result")
        return public
    originals = {name: ("VALID", raw) for name, raw in sources.items()}
    urls = {name: url for name, url in ((DOCS_MEMBER, SOURCE), (FRED_MEMBER, FRED)) if name in sources}
    save_private(originals, urls, {task: samples[task] for task in PROBES}, result, prefix=PREFIX)
    return public | {"private_archive": "STAGED"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        report = run(verify_only=args.verify_only)
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
    if not args.verify_only:
        REPORT.write_bytes(json_bytes(report))
    elif report["status"] == "PASS_DIAGNOSTIC":
        print("release_readiness_drive_readback=PASS")
    print(json.dumps(report, sort_keys=True))
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
