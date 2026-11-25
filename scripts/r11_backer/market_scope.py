"""Audit market-wide payload membership before enabling bulk downloads."""

from collections import Counter
import json
import os
from pathlib import Path
import re
from urllib.request import urlopen

from .release_gaps import BENCHMARK_DAY, BULK_MAX_BYTES
from .release_probes import BULK_CANDIDATES, MAX_BYTES, SOURCE, Task, contracts, request_sample, run_parallel

TARGET_MARKETS = {"twse", "tpex"}
REFERENCES = ("2330", "2317", "6488", "8069")
ROW_KEYS = {
    "TaiwanStockPrice": ("date", "stock_id"),
    "TaiwanStockMarginPurchaseShortSale": ("date", "stock_id", "name"),
    "TaiwanStockInstitutionalInvestorsBuySell": ("date", "stock_id", "name"),
    "TaiwanStockAfterHoursFixedPriceTrading": ("date", "stock_id", "market"),
}


def market_universe(rows):
    members = {}
    for row in rows:
        code = row.get("stock_id")
        exchange = row.get("type")
        if isinstance(code, str) and isinstance(exchange, str):
            exchange = exchange.strip().lower()
            if exchange in TARGET_MARKETS and re.fullmatch(r"[0-9]{4}", code):
                members.setdefault(code, set()).add(exchange)
    return {code: next(iter(exchanges)) for code, exchanges in members.items() if len(exchanges) == 1}


def rows_for(rows, code):
    return Counter(
        json.dumps(row, sort_keys=True, separators=(",", ":"))
        for row in rows if row.get("stock_id") == code
    )


def audit_market(market, references, universe, row_key):
    eligible = [row for row in market.rows if row.get("stock_id") in universe]
    keys = [tuple(row.get(field) for field in row_key) for row in eligible]
    match = {}
    for code, sample in references.items():
        reference_rows = rows_for(sample.rows, code)
        match[code] = bool(reference_rows) and reference_rows == rows_for(eligible, code)
    duplicate = len(keys) - len(set(keys))
    reference_matches = sum(match.values())
    exchanged = Counter(universe[row["stock_id"]] for row in eligible)
    verdict = "HOLD_SAMPLE_REQUIRED"
    if market.status == "VALID_SAMPLE" and eligible:
        if len(eligible) != len(market.rows):
            verdict = "HOLD_OUT_OF_SCOPE_ROWS"
        elif duplicate or any(None in key for key in keys):
            verdict = "HOLD_PRIMARY_KEY"
        elif reference_matches != len(references):
            verdict = "HOLD_REFERENCE_MISMATCH"
        else:
            verdict = "SCOPE_SAMPLE_PASS"
    return {
        "status": market.status, "verdict": verdict,
        "total_rows": len(market.rows), "rows_matching_current_master": len(eligible),
        "rows_unmatched_to_current_master": len(market.rows) - len(eligible),
        "target_symbols_observed": len({row["stock_id"] for row in eligible}),
        "rows_by_target_exchange": dict(exchanged), "duplicate_target_keys": duplicate,
        "matched_reference_queries": reference_matches, "reference_queries": len(references),
    }


def main():
    token = os.environ["FINMIND_BACKER_TOKEN"]
    with urlopen(SOURCE, timeout=20) as stream:
        content = stream.read(1_000_001)
    if len(content) > 1_000_000:
        raise ValueError("public contract exceeds expected size")
    doc = contracts(content.decode("utf-8"))
    master = request_sample(Task("TaiwanStockInfo", None, None), doc["TaiwanStockInfo"], token)
    universe = market_universe(master.rows)
    result = {
        "prior_run": 45623673196, "production": "HOLD", "benchmark_day": BENCHMARK_DAY,
        "master_status": master.status, "master_scoped_symbols": len(universe),
        "master_exchange_counts": dict(Counter(universe.values())),
    }
    if universe and master.status != "RATE_LIMITED":
        tasks = [Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY, code)
                 for dataset in BULK_CANDIDATES for code in (None, *REFERENCES)]
        gold = Task("GoldPrice", "2026-10-01", "2026-10-07")
        tasks.append(gold)

        def reader(task, contract, auth):
            large = task.dataset in BULK_CANDIDATES and task.data_id is None
            return request_sample(task, contract, auth, max_bytes=BULK_MAX_BYTES if large else MAX_BYTES)

        replies = run_parallel(tasks, doc, token, reader=reader)
        benchmarks = {}
        for dataset in BULK_CANDIDATES:
            bulk = replies[Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY)]
            references = {code: replies[Task(dataset, BENCHMARK_DAY, BENCHMARK_DAY, code)]
                          for code in REFERENCES}
            benchmarks[dataset] = audit_market(bulk, references, universe, ROW_KEYS[dataset])
        result["scope_comparison"] = benchmarks
        gold_sample = replies[gold]
        result["gold_date_validation"] = gold_sample.status
        if gold_sample.rows:
            # Detect multiple observations for one claimed daily period.
            result["gold_one_row_per_day"] = (
                len({row["date"][:10] for row in gold_sample.rows}) == len(gold_sample.rows)
            )
        result["requests_sent"] = 1 + sum(s.status != "NOT_SENT_RATE_LIMIT" for s in replies.values())
    Path("r11-market-scope.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
