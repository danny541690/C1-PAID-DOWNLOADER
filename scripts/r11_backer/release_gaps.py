"""Targeted follow-up of the open R11 release probes; public statuses only."""

from collections import Counter
import json
import os
from pathlib import Path
from time import monotonic
from urllib.request import urlopen

from .release_probes import (
    MAX_BYTES, SOURCE, Sample, Task, active_bond, compare_market, contracts, request_sample, run_parallel,
)

PREVIOUS_RUN = 45597304811
BENCHMARK_DAY = "2026-11-24"
BULK = ("TaiwanStockPrice", "TaiwanStockInstitutionalInvestorsBuySell",
        "TaiwanStockAfterHoursFixedPriceTrading")
BULK_MAX_BYTES = 24 * 1024 * 1024
HISTORICAL_HOLDS = {
    "TaiwanOptionVix": "HOLD_INTRADAY_FREQUENCY_UNVERIFIED",
}
BOND_NAMES = (
    "TaiwanStockConvertibleBondDaily", "TaiwanStockConvertibleBondInstitutionalInvestors",
    "TaiwanStockConvertibleBondMonthlyAnalysis",
)


def gap_tasks(bond):
    tasks = [
        Task("TaiwanStockWeekPrice", "2025-10-01", "2025-11-30", "2330"),
        Task("TaiwanStockWeekPrice", "2026-10-01", "2026-11-24", "2330"),
        Task("TaiwanStockMonthPrice", "2025-08-01", "2025-11-30", "2330"),
        Task("TaiwanStockMonthPrice", "2026-08-01", "2026-11-24", "2330"),
        Task("TaiwanStockPriceLimit", BENCHMARK_DAY, BENCHMARK_DAY, "2330", send_end=False),
        Task("TaiwanStockMarginShortSaleSuspension", "2026-09-01", "2026-11-24", "2330"),
        Task("TaiwanDailyShortSaleBalances", "2026-11-02", "2026-11-06", "2330"),
        Task("TaiwanStockDividend", "2025-01-01", "2025-12-31", "2330"),
        Task("TaiwanStockDividendResult", "2025-06-01", "2025-06-30", "2330"),
        Task("TaiwanStockParValueChange", "2025-01-01", "2025-12-31"),
        Task("TaiwanFuturesInstitutionalInvestors", "2026-11-02", "2026-11-06", "TX"),
        Task("TaiwanOptionInstitutionalInvestors", "2026-11-02", "2026-11-06", "TXO"),
        Task("TaiwanFuturesFinalSettlementPrice", "2025-10-01", "2025-12-31"),
        Task("TaiwanFuturesFinalSettlementPrice", "2026-09-01", "2026-11-24"),
        Task("TaiwanOptionFinalSettlementPrice", "2025-10-01", "2025-12-31"),
        Task("TaiwanOptionFinalSettlementPrice", "2026-09-01", "2026-11-24"),
        Task("TaiwanBusinessIndicator", "2026-07-01", "2026-11-24"),
        Task("InterestRate", "2025-06-01", "2025-12-31", "FED"),
        Task("InterestRate", "2026-01-01", "2026-11-24", "FED"),
        Task("GoldPrice", "2026-10-01", "2026-10-07"),
        Task("GoldPrice", "2025-11-03", "2025-11-07"),
        Task("CrudeOilPrices", "2025-11-03", "2025-11-14", "WTI"),
        Task("CrudeOilPrices", "2026-09-01", "2026-10-15", "Brent"),
    ]
    if bond:
        tasks += [
            Task(BOND_NAMES[0], "2026-11-02", "2026-11-06", bond),
            Task(BOND_NAMES[1], "2026-11-02", "2026-11-06", bond),
        ]
    # The monthly method documents market-wide queries without a bond code.
    tasks.append(Task(BOND_NAMES[2], "2026-08-01", "2026-09-30"))
    return tasks


def safe_gap_sample(task, contract, token):
    is_bulk = task.dataset in BULK and task.data_id is None
    return request_sample(task, contract, token, max_bytes=BULK_MAX_BYTES if is_bulk else MAX_BYTES)


def bulk_outcome(results, dataset):
    single = results[Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY, "2330")]
    variants = []
    for with_end in (True, False):
        market = results[Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY, send_end=with_end)]
        matches = compare_market(single, market)
        variants.append({
            "send_end_date": with_end, "market_status": market.status,
            "date_issue": market.date_issue, "reference_matches": matches,
            "seconds": round(market.elapsed, 3),
            "response_mib": round(len(market.raw) / 1024**2, 3),
            "matched_market_symbols": len({row["stock_id"] for row in market.rows
                                           if row.get("stock_id")}) if matches else 0,
        })
    return {
        "single_status": single.status,
        "candidate_mode": "MARKET_DAY_CANDIDATE" if any(row["reference_matches"] for row in variants)
        else "KEEP_VERIFIED_SYMBOL_QUERY",
        "variants": variants,
    }


def main():
    started = monotonic()
    token = os.environ["FINMIND_BACKER_TOKEN"]
    with urlopen(SOURCE, timeout=20) as response:
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000:
        raise ValueError("public contract exceeds expected size")
    documented = contracts(raw.decode("utf-8"))

    snapshot_task = Task("TaiwanStockConvertibleBondInfo", None, None)
    snapshot = request_sample(snapshot_task, documented[snapshot_task.dataset], token)
    bond = active_bond(snapshot, "2026-11-02", "2026-11-06")
    tasks = gap_tasks(bond)
    holds = dict(HISTORICAL_HOLDS)
    if not bond:
        for dataset in BOND_NAMES[:2]:
            holds[dataset] = "HOLD_BOND_ID_UNVERIFIED"
    bulk_tasks = [
        task for dataset in BULK for task in (
            Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY, "2330"),
            Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY),
            Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY, send_end=False),
        )
    ]
    pending = [*tasks, *bulk_tasks]
    if snapshot.status == "RATE_LIMITED":
        results = dict.fromkeys(pending, Sample("NOT_SENT_RATE_LIMIT"))
    else:
        results = run_parallel(pending, documented, token, reader=safe_gap_sample)

    dataset_checks = {}
    for task in tasks:
        observed = results[task]
        detail = {
            "start": task.start, "end": task.end, "status": observed.status,
            "missing_documented_fields": observed.documented_missing, "date_issue": observed.date_issue,
        }
        dataset_checks.setdefault(task.dataset, []).append(detail)
    conclusions = {
        dataset: ("VALID_SAMPLE" if any(item["status"] == "VALID_SAMPLE" for item in records)
                  else "HOLD")
        for dataset, records in dataset_checks.items()
    } | holds
    report = {
        "previous_run": PREVIOUS_RUN, "production": "HOLD",
        "parallel_workers": 20, "requests_sent": 1 + sum(x.status != "NOT_SENT_RATE_LIMIT" for x in results.values()),
        "seconds": round(monotonic() - started, 3), "snapshot_status": snapshot.status,
        "dataset_conclusions": conclusions, "dataset_checks": dataset_checks,
        "request_statuses": dict(Counter(value.status for value in results.values())),
        "bulk_response_limit_mib": BULK_MAX_BYTES // 1024**2,
        "bulk": {dataset: bulk_outcome(results, dataset) for dataset in BULK},
    }
    Path("r11-release-gaps.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "dataset_checks"}))
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as out:
            out.write(f"## Follow-up of {len(conclusions)} candidates\n\n")
            out.write(f"- Request outcomes: `{json.dumps(report['request_statuses'])}`\n")
            out.write(f"- Newly confirmed sample datasets: {sum(x == 'VALID_SAMPLE' for x in conclusions.values())}\n")
            out.write("- Production: **HOLD**\n")
            for dataset, outcome in report["bulk"].items():
                out.write(f"- {dataset}: {outcome['candidate_mode']}\n")
    return int(snapshot.status == "RATE_LIMITED" or any(
        value.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for value in results.values()
    ))


if __name__ == "__main__":
    raise SystemExit(main())
