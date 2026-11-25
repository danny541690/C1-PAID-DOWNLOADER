"""Bounded Backer release probes and all-market throughput measurement."""

from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, urlopen
from zoneinfo import ZoneInfo

from .archive import compress_original
from .drive_pilot import BASE, FOLDER, ROOT, ensure, find, token as drive_token
from .live_preflight import NoRedirect, read_json
from .preflight import read_catalog
from .reconcile_catalog import BACKER_COMPATIBLE, SOURCE

MAX_BYTES = 4 * 1024 * 1024
WORKERS = 20
REPORT_PATH = Path("r11-release-probes.json")
ARCHIVE_REPORT = "private-readback-index.json"
RECOVERY_ROOT = "1EfIyMc6x-S0ij1QEj5iD5Ez9JhqiuSPw"
SNAPSHOTS = {
    "TaiwanStockInfo", "TaiwanStockTradingDate", "TaiwanSecuritiesTraderInfo",
    "TaiwanFutOptDailyInfo", "TaiwanStockConvertibleBondInfo", "TaiwanStockIndustryChain",
}
SCOPE_HOLDS = {"TaiwanOptionVix": "HOLD_INTRADAY_FREQUENCY_UNVERIFIED"}
QUARTER_DATES = ("2025-12-31", "2025-12-31")
EVENT_DATASETS = {
    "TaiwanStockSuspended", "TaiwanStockDayTradingSuspension", "TaiwanStockMarginShortSaleSuspension",
    "TaiwanStockDispositionSecuritiesPeriod", "TaiwanStockDividend", "TaiwanStockDividendResult",
    "TaiwanStockCapitalReductionReferencePrice", "TaiwanStockDelisting", "TaiwanStockSplitPrice",
    "TaiwanStockParValueChange", "TaiwanStockConvertibleBondPutProvision",
}
BULK_CANDIDATES = (
    "TaiwanStockPrice", "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockMarginPurchaseShortSale", "TaiwanStockAfterHoursFixedPriceTrading",
)


@dataclass(frozen=True)
class Contract:
    tier: str
    required: str
    optional: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class Task:
    dataset: str
    start: str | None
    end: str | None
    data_id: str | None = None
    send_end: bool = True

    def parameters(self):
        params = {"dataset": self.dataset}
        if self.start:
            params["start_date"] = self.start
        if self.end and self.send_end:
            params["end_date"] = self.end
        if self.data_id:
            params["data_id"] = self.data_id
        return params


@dataclass
class Sample:
    status: str
    elapsed: float = 0
    raw: bytes = b""
    rows: tuple = ()
    documented_missing: tuple = ()


def contracts(markdown):
    result = {}
    for section in re.split(r"(?m)^### ", markdown)[1:]:
        name, _, _ = section.partition("\n")
        name = name.strip()
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]+", name):
            continue
        labels = dict(re.findall(r"(?m)^- \*\*(Endpoint|Tier|Required|Optional|Key columns):\*\*\s*([^\n]+)", section))
        if labels.get("Endpoint") != "`/api/v4/data`":
            continue
        fields = tuple(
            item.group(0) for column in labels["Key columns"].split(",")
            if (item := re.match(r"\w+", column.strip()))
        )
        if not fields or name in result:
            raise ValueError("duplicate or incomplete public contract")
        result[name] = Contract(labels["Tier"], labels["Required"], labels["Optional"], fields)
    if not result:
        raise ValueError("empty public contract snapshot")
    return result


def request_sample(task, contract, auth):
    started = monotonic()
    url = "https://api.finmindtrade.com/api/v4/data?" + urlencode(task.parameters())
    req = Request(url, headers={"Authorization": "Bearer " + auth, "Accept": "application/json"})
    try:
        with build_opener(NoRedirect()).open(req, timeout=25) as response:
            raw = response.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            return Sample("SIZE_LIMIT", monotonic() - started)
        data = json.loads(raw)
    except HTTPError as error:
        status = {
            400: "INVALID_PARAMETER", 401: "ACCESS_DENIED", 403: "ACCESS_DENIED",
            429: "RATE_LIMITED", 500: "HTTP_TRANSIENT", 502: "HTTP_TRANSIENT",
            503: "HTTP_TRANSIENT", 504: "HTTP_TRANSIENT",
        }.get(error.code, "HTTP_ERROR")
        return Sample(status, monotonic() - started)
    except (URLError, TimeoutError):
        return Sample("HTTP_TRANSIENT", monotonic() - started)
    except (ValueError, UnicodeError):
        return Sample("INVALID_RESPONSE", monotonic() - started)

    if not isinstance(data, dict):
        return Sample("INVALID_RESPONSE", monotonic() - started)
    api_code = data.get("status")
    if api_code != 200:
        status = {400: "INVALID_PARAMETER", 401: "ACCESS_DENIED", 403: "ACCESS_DENIED",
                  429: "RATE_LIMITED"}.get(api_code, "API_REJECTED") if type(api_code) is int else "API_REJECTED"
        return Sample(status, monotonic() - started)
    rows = data.get("data")
    if not isinstance(rows, list):
        return Sample("INVALID_RESPONSE", monotonic() - started)
    if not rows:
        return Sample("EMPTY_WINDOW", monotonic() - started)
    if not all(isinstance(row, dict) for row in rows):
        return Sample("INVALID_RESPONSE", monotonic() - started)

    if task.start and task.end:
        try:
            days = [date.fromisoformat(row["date"]) for row in rows]
            if min(days).isoformat() < task.start or max(days).isoformat() > task.end:
                return Sample("OUTSIDE_WINDOW", monotonic() - started)
        except (KeyError, TypeError, ValueError):
            return Sample("DATE_UNVERIFIED", monotonic() - started)

    missing = tuple(sorted(set(contract.fields).difference(set.intersection(*(set(r) for r in rows)))))
    status = "FIELD_DIFFERENCES" if missing else "VALID_SAMPLE"
    return Sample(status, monotonic() - started, raw, tuple(rows), missing)


def run_parallel(tasks, documented, auth, workers=WORKERS):
    """Return one outcome per dispatched request and stop dispatch on a provider 429."""
    tasks = list(dict.fromkeys(tasks))
    pending = iter(tasks)
    results = {}
    rate_limited = False
    with ThreadPoolExecutor(max_workers=workers) as pool:
        active = {pool.submit(request_sample, item, documented[item.dataset], auth): item
                  for item in tasks[:workers]}
        for _ in active:
            next(pending)
        while active:
            finished, _ = wait(active, return_when=FIRST_COMPLETED)
            for future in finished:
                task = active.pop(future)
                results[task] = future.result()
                rate_limited |= results[task].status == "RATE_LIMITED"
            if not rate_limited:
                for _ in finished:
                    item = next(pending, None)
                    if item is not None:
                        active[pool.submit(request_sample, item, documented[item.dataset], auth)] = item
    for task in tasks:
        results.setdefault(task, Sample("NOT_SENT_RATE_LIMIT"))
    return results


def recent_trading_days(calendar, as_of):
    days = set()
    for row in calendar.rows:
        try:
            day = date.fromisoformat(row["date"])
        except (KeyError, TypeError, ValueError):
            continue
        if day < as_of:
            days.add(day.isoformat())
    return sorted(days)[-5:]


def active_bond(snapshot, start, end):
    """Require valid conversion dates; one documented test code is enough."""
    for row in snapshot.rows:
        try:
            code = row["cb_id"]
            first = date.fromisoformat(row["InitialDateOfConversion"]).isoformat()
            last = date.fromisoformat(row["DueDateOfConversion"]).isoformat()
        except (KeyError, TypeError, ValueError):
            continue
        if isinstance(code, str) and re.fullmatch(r"[0-9]{5,6}", code) and first <= start <= end <= last:
            return code
    return None


def primary_task(dataset, doc, days, as_of, bond):
    """Use documented identity and frequency, and keep unsupported inputs on HOLD."""
    if dataset in SCOPE_HOLDS:
        return SCOPE_HOLDS[dataset]

    code = None
    if "`data_id`" in doc.required:
        if dataset == "TaiwanStockTotalReturnIndex":
            code = "TAIEX"
        elif dataset == "InterestRate":
            code = "FED"
        elif dataset == "CrudeOilPrices":
            code = "WTI"
        elif dataset == "GovernmentBondsYield":
            code = "United States 10-Year"
        elif "可轉債" in doc.required:
            if bond is None:
                return "HOLD_BOND_ID_UNVERIFIED"
            code = bond
        elif "選擇權" in doc.required:
            code = "TXO"
        elif "期貨" in doc.required:
            code = "TX"
        elif "股票" in doc.required:
            code = "2330"
        else:
            return "HOLD_QUERY_CONTRACT"
    elif dataset in {"TaiwanStockAfterHoursFixedPriceTrading", "TaiwanStockDayTradingBorrowingFeeRate"}:
        code = "2330"
    elif dataset == "TaiwanExchangeRate":
        code = "USD"
    elif dataset == "TaiwanStockConvertibleBondMonthlyAnalysis" and bond:
        code = bond

    if dataset in SNAPSHOTS:
        return Task(dataset, None, None, code)
    if "`start_date`" not in doc.required or "date" not in doc.fields:
        return "HOLD_QUERY_CONTRACT"
    if not days:
        return "HOLD_TRADING_CALENDAR"

    end = days[-1]
    start = days[0]
    if dataset in {"TaiwanStockFinancialStatements", "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement"}:
        start, end = QUARTER_DATES
    elif dataset in EVENT_DATASETS or dataset in {"InterestRate", "TaiwanBusinessIndicator", "TaiwanStockMonthPrice",
                                                   "TaiwanStockMonthRevenue", "TaiwanStockConvertibleBondMonthlyAnalysis",
                                                   "TaiwanFuturesFinalSettlementPrice", "TaiwanOptionFinalSettlementPrice"}:
        start = (as_of - timedelta(days=46)).isoformat()
    elif dataset in {"TaiwanOptionDaily", "TaiwanFuturesDealerTradingVolumeDaily",
                     "TaiwanOptionDealerTradingVolumeDaily", "TaiwanStockMarketValueWeight",
                     "TaiwanAssetSwapFixedIncomeDaily", "TaiwanAssetSwapOptionDaily"}:
        start = end
    send_end = "`end_date`" in doc.optional or "`end_date`" in doc.required
    if not send_end:
        start, end = days[-1], as_of.isoformat()
    return Task(dataset, start, end, code, send_end)


def compare_market(single, market):
    """Confirm the same symbol rows are present in an all-market response."""
    if single.status not in {"VALID_SAMPLE", "FIELD_DIFFERENCES"}:
        return False
    if market.status not in {"VALID_SAMPLE", "FIELD_DIFFERENCES"}:
        return False
    if {row.get("stock_id") for row in single.rows} != {"2330"}:
        return False
    if len({row.get("stock_id") for row in market.rows if row.get("stock_id")}) < 2:
        return False

    def projected(rows):
        return Counter(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows
                       if row.get("stock_id") == "2330")

    return projected(single.rows) == projected(market.rows)


def blast_summary(benchmark_results, days):
    report = {}
    for dataset in BULK_CANDIDATES:
        equivalent = []
        sizes, symbols, market_times, single_times = [], [], [], []
        for day in days:
            single = benchmark_results.get(Task(dataset, day, day, "2330"), Sample("UNTESTED"))
            market = benchmark_results.get(Task(dataset, day, day), Sample("UNTESTED"))
            same = compare_market(single, market)
            equivalent.append(same)
            if same:
                packed, _, _ = compress_original(market.raw)
                sizes.append(len(packed))
                symbols.append(len({row["stock_id"] for row in market.rows if row.get("stock_id")}))
                market_times.append(market.elapsed)
                single_times.append(single.elapsed)
        report[dataset] = {
            "matching_days": sum(equivalent), "tested_days": len(days),
            "recommendation": "MARKET_DAY_CANDIDATE" if days and all(equivalent) else "KEEP_CONTRACT_QUERY",
            "matched_market_symbols_min": min(symbols, default=0),
            "matched_market_symbols_max": max(symbols, default=0),
            "matching_market_seconds_max": round(max(market_times, default=0), 3),
            "matching_one_symbol_seconds_max": round(max(single_times, default=0), 3),
            "observed_compressed_bytes_per_market_day_max": max(sizes, default=0),
        }
    return report


def recovery_location(access):
    project = read_json(f"{BASE}/files/{ROOT}?fields=shared", access)
    recovery = read_json(
        f"{BASE}/files/{RECOVERY_ROOT}?fields=id,name,parents,shared,mimeType,trashed", access
    )
    if (
        project.get("shared") is not False
        or recovery.get("id") != RECOVERY_ROOT or recovery.get("parents") != [ROOT]
        or recovery.get("name") != "99_RECOVERY" or recovery.get("mimeType") != FOLDER
        or recovery.get("trashed") is not False or recovery.get("shared") is not False
    ):
        raise ValueError("private recovery location is not verified")
    run_id = int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    return f"R11_RELEASE_PREFLIGHT_{run_id}_{attempt}"


def read_private_file(file_id, access):
    req = Request(f"{BASE}/files/{file_id}?alt=media", headers={
        "Authorization": "Bearer " + access, "Accept-Encoding": "identity",
    })
    with build_opener(NoRedirect()).open(req, timeout=25) as response:
        contents = response.read(MAX_BYTES + 65537)
    if len(contents) > MAX_BYTES + 65536:
        raise ValueError("private readback exceeds pilot size bound")
    return contents


def verify_private_archive():
    access = drive_token()
    name = recovery_location(access)
    folder = find(RECOVERY_ROOT, name, access)
    if (not folder or folder["mimeType"] != FOLDER or folder["parents"] != [RECOVERY_ROOT]
            or read_json(f'{BASE}/files/{folder["id"]}?fields=shared', access).get("shared") is not False):
        raise ValueError("private pilot folder missing or shared")
    root = folder["id"]
    receipt_id = ensure(root, ARCHIVE_REPORT, "application/json", access, create=False)
    index = json.loads(read_private_file(receipt_id, access))
    if not isinstance(index, list) or not 1 <= len(index) <= 3:
        raise ValueError("private receipt count is invalid")
    seen = set()
    for entry in index:
        if not re.fullmatch(r"[A-Za-z0-9_]+__[0-9-]{10}__sample\.json\.gz", entry["name"]):
            raise ValueError("unexpected paid pilot file name")
        file_id = ensure(root, entry["name"], "application/gzip", access, create=False)
        if entry["file_id"] != file_id or file_id in seen:
            raise ValueError("ambiguous paid pilot file")
        seen.add(file_id)
        raw = read_private_file(file_id, access)
        if len(raw) != entry["content_bytes"] or hashlib.sha256(raw).hexdigest() != entry["gzip_sha256"]:
            raise ValueError("compressed paid payload SHA mismatch")
        if hashlib.sha256(gzip.decompress(raw)).hexdigest() != entry["raw_sha256"]:
            raise ValueError("original paid payload SHA mismatch")
    print(f"private_paid_samples_readback={len(seen)}")


def private_archive(results):
    """Small paid samples go only into a non-shared recovery directory."""
    valid = [(task, sample) for task, sample in results.items()
             if sample.status == "VALID_SAMPLE" and task.dataset in BULK_CANDIDATES and task.data_id is None]
    if not valid:
        valid = [(task, sample) for task, sample in results.items()
                 if sample.status == "VALID_SAMPLE" and task.dataset in BULK_CANDIDATES]
    if not valid:
        return "HOLD_NO_ARCHIVE_SAMPLE"
    access = drive_token()
    try:
        name = recovery_location(access)
    except ValueError:
        return "HOLD_PRIVATE_RECOVERY_UNVERIFIED"
    folder = ensure(RECOVERY_ROOT, name, FOLDER, access, create=True)
    if read_json(f"{BASE}/files/{folder}?fields=shared", access).get("shared") is not False:
        return "HOLD_PRIVATE_FOLDER_UNVERIFIED"

    index = []
    selected = set()
    for task, sample in valid:
        if task.dataset in selected:
            continue
        selected.add(task.dataset)
        packed, raw_sha, gzip_sha = compress_original(sample.raw)
        filename = f"{task.dataset}__{task.start}__sample.json.gz"
        file_id = ensure(folder, filename, "application/gzip", access, packed, create=True)
        index.append({
            "dataset": task.dataset, "parameters": task.parameters(), "file_id": file_id,
            "name": filename, "raw_sha256": raw_sha, "gzip_sha256": gzip_sha,
            "content_bytes": len(packed),
        })
        if len(index) == 3:
            break
    receipt = json.dumps(index, indent=2).encode("utf-8")
    ensure(folder, ARCHIVE_REPORT, "application/json", access, receipt, create=True)
    return "STAGED_PRIVATE_SAMPLES"


def make_report():
    started = monotonic()
    token = os.environ["FINMIND_BACKER_TOKEN"]
    with urlopen(SOURCE, timeout=20) as response:
        content = response.read(1_000_001)
    if len(content) > 1_000_000:
        raise ValueError("catalog exceeds inclusive size bound")
    documented = contracts(content.decode("utf-8"))
    rows = read_catalog()
    as_of = datetime.now(ZoneInfo("Asia/Taipei")).date()
    snapshots = [Task(name, None, None) for name in SNAPSHOTS if name != "TaiwanStockIndustryChain"]
    snapshot_results = run_parallel(snapshots, documented, token)
    days = recent_trading_days(snapshot_results[Task("TaiwanStockTradingDate", None, None)], as_of)
    bond = None
    if days:
        bond = active_bond(snapshot_results[Task("TaiwanStockConvertibleBondInfo", None, None)], days[0], days[-1])
    direct, holds = {}, {}
    for row in rows:
        dataset, lane = row["dataset"], row["routing"]
        if lane in {"EXCLUDE", "REVIEW"}:
            holds[dataset] = "EXCLUDED_BY_SCOPE" if lane == "EXCLUDE" else "HOLD_SCOPE_REVIEW"
            continue
        doc = documented.get(dataset)
        if doc is None or doc.tier not in BACKER_COMPATIBLE:
            holds[dataset] = "HOLD_PUBLIC_CONTRACT"
            continue
        task = primary_task(dataset, doc, days, as_of, bond)
        if isinstance(task, str):
            holds[dataset] = task
        else:
            direct[dataset] = task

    benchmarks = [Task(dataset, day, day, code) for dataset in BULK_CANDIDATES
                  for day in days for code in ("2330", None)]
    halt = any(item.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for item in snapshot_results.values())
    cross_month = Task("TaiwanStockPrice", "2026-10-30", "2026-11-03", "2330")
    symbol_month = Task("TaiwanStockPrice", "2026-10-01", "2026-10-31", "2330")
    pending = list(dict.fromkeys(task for task in [*direct.values(), *benchmarks, cross_month, symbol_month]
                                 if task not in snapshot_results))
    current = ({task: Sample("NOT_SENT_RATE_LIMIT") for task in pending} if halt
               else run_parallel(pending, documented, token))
    results = snapshot_results | current
    details = []
    for row in rows:
        dataset = row["dataset"]
        observation = results.get(direct.get(dataset))
        status = holds.get(dataset) if observation is None else observation.status
        details.append({
            "dataset": dataset, "lane": row["routing"], "status": status,
            "missing_documented_field_count": len(observation.documented_missing) if observation else None,
        })

    try:
        archive = private_archive({task: results[task] for task in benchmarks}) if benchmarks else "HOLD_NO_ARCHIVE_SAMPLE"
    except (HTTPError, URLError, ValueError, RuntimeError, KeyError):
        archive = "HOLD_PRIVATE_ARCHIVE_IO"
    cross = results[cross_month]
    month = results[symbol_month]
    cross_month_verified = cross.status == "VALID_SAMPLE" and len({
        row["date"][:7] for row in cross.rows
    }) == 2
    symbol_month_bytes = len(compress_original(month.raw)[0]) if month.raw else 0
    sent = sum(item.status != "NOT_SENT_RATE_LIMIT" for item in results.values())
    report = {
        "candidate_count": len(rows), "routed_count": len(direct) + sum(
            row["routing"].startswith("B") and row["dataset"] in holds for row in rows
        ),
        "request_count": sent, "parallel_workers": WORKERS,
        "response_statuses": dict(Counter(item.status for item in results.values())),
        "dataset_statuses": dict(Counter(row["status"] for row in details)),
        "closed_trading_days_tested": len(days),
        "cross_month_single_symbol_verified": cross_month_verified,
        "single_symbol_month": {
            "status": month.status, "seconds": round(month.elapsed, 3),
            "compressed_bytes": symbol_month_bytes,
        }, "active_convertible_bond_sample": bool(bond),
        "all_market": blast_summary(results, days),
        "archive": archive, "release": "HOLD",
        "historical_exact_start_proven": False,
        "workflow_seconds": round(monotonic() - started, 2),
        "subscription_expiry_verified": False,
        "dataset_sweep_complete": len(days) == 5 and all(
            item.status not in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for item in results.values()
        ),
        "datasets": details,
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "datasets"}))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if args.verify_only:
        try:
            verify_private_archive()
        except Exception as error:
            print(f"private_paid_samples_readback=HOLD_{type(error).__name__}")
            raise SystemExit(1) from None
    else:
        make_report()
