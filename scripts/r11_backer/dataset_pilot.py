"""Bounded dataset probes using the Backer credential without logging records."""

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date
import hashlib
import json
from pathlib import Path
import os
from urllib.error import HTTPError
from urllib.parse import urlencode

from .live_preflight import read_json

PROBES = (
    ("TaiwanStockPrice", "2330", "stock_id", "2026-11-02", "2026-11-10",
     {"open", "close", "Trading_Volume"}),
    ("TaiwanStockHoldingSharesPer", "2330", "stock_id", "2026-11-02", "2026-11-14",
     {"HoldingSharesLevel", "people", "percent", "unit"}),
    ("TaiwanStockMonthRevenue", "2330", "stock_id", "2026-10-01", "2026-10-31",
     {"revenue", "revenue_month", "revenue_year"}),
    ("TaiwanStockInstitutionalInvestorsBuySell", "2330", "stock_id", "2026-11-02", "2026-11-06",
     {"buy", "sell", "name"}),
    ("TaiwanStockDividendResult", "2330", "stock_id", "2025-06-01", "2025-06-30",
     {"before_price", "after_price", "reference_price"}),
    ("TaiwanFuturesDaily", "TX", "futures_id", "2026-11-02", "2026-11-04",
     {"contract_date", "close", "volume", "open_interest"}),
    ("TaiwanExchangeRate", "USD", "currency", "2026-11-02", "2026-11-06",
     {"cash_buy", "cash_sell", "spot_buy", "spot_sell"}),
    ("TaiwanStockBalanceSheet", "2330", "stock_id", "2025-12-31", "2025-12-31",
     {"type", "value", "origin_name"}),
    ("TaiwanStockPriceAdj", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"Trading_Volume", "close", "open"}),
    ("TaiwanStockPER", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"PBR", "PER", "dividend_yield"}),
    ("TaiwanStockDayTrading", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"BuyAfterSale", "Volume"}),
    ("TaiwanStockMarginPurchaseShortSale", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"MarginPurchaseTodayBalance", "ShortSaleTodayBalance"}),
    ("TaiwanStockShareholding", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"ForeignInvestmentShares", "ForeignInvestmentSharesRatio"}),
    ("TaiwanStockSecuritiesLending", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"transaction_type", "volume"}),
    ("TaiwanStockFinancialStatements", "2330", "stock_id", "2025-12-31", "2025-12-31",
     {"origin_name", "type", "value"}),
    ("TaiwanStockCashFlowsStatement", "2330", "stock_id", "2025-12-31", "2025-12-31",
     {"origin_name", "type", "value"}),
    ("TaiwanStockMarketValue", "2330", "stock_id", "2026-11-02", "2026-11-04",
     {"market_value"}),
    ("TaiwanStockDividend", "2330", "stock_id", "2025-01-01", "2025-12-31",
     {"CashEarningsDistribution", "year"}),
)


# Earlier windows are separate evidence from the already verified recent samples.
HISTORICAL_WINDOWS = (
    ("TaiwanStockPrice", "1994-09-01", "1994-09-30"),
    ("TaiwanStockPrice", "2008-10-01", "2008-10-10"),
    ("TaiwanStockPrice", "2016-01-04", "2016-01-08"),
    ("TaiwanStockPriceAdj", "1994-09-01", "1994-09-30"),
    ("TaiwanStockPriceAdj", "2008-10-01", "2008-10-10"),
    ("TaiwanStockFinancialStatements", "1991-10-01", "1991-12-31"),
    ("TaiwanStockFinancialStatements", "2012-12-31", "2012-12-31"),
    ("TaiwanStockFinancialStatements", "2013-03-31", "2013-03-31"),
    ("TaiwanStockBalanceSheet", "2013-03-31", "2013-03-31"),
    ("TaiwanStockCashFlowsStatement", "2013-03-31", "2013-03-31"),
    ("TaiwanStockMonthRevenue", "2002-01-01", "2002-03-31"),
    ("TaiwanStockMonthRevenue", "2013-01-01", "2013-03-31"),
    ("TaiwanStockInstitutionalInvestorsBuySell", "2004-10-01", "2004-10-08"),
    ("TaiwanStockMarginPurchaseShortSale", "2001-01-02", "2001-01-08"),
    ("TaiwanStockHoldingSharesPer", "2019-01-01", "2019-01-15"),
    ("TaiwanFuturesDaily", "2008-10-01", "2008-10-03"),
    ("TaiwanExchangeRate", "2006-01-02", "2006-01-08"),
    ("TaiwanStockDividendResult", "2015-06-01", "2015-07-31"),
)


def classify(payload, start, end, fields, *, identity_field="stock_id", identity="2330"):
    if not isinstance(payload, dict):
        return "INVALID_RESPONSE"
    if payload.get("status") in (401, 403):
        return "ACCESS_DENIED"
    if payload.get("status") == 429:
        return "RATE_LIMITED"
    if payload.get("status") != 200:
        return "API_REJECTED"
    rows = payload.get("data")
    if not isinstance(rows, list):
        return "INVALID_RESPONSE"
    if not rows:
        return "EMPTY_WINDOW"
    for row in rows:
        if not isinstance(row, dict) or not fields <= row.keys() or row.get(identity_field) != identity:
            return "SCHEMA_UNVERIFIED"
        try:
            observed = date.fromisoformat(row["date"])
        except (KeyError, TypeError, ValueError):
            return "DATE_UNVERIFIED"
        if not date.fromisoformat(start) <= observed <= date.fromisoformat(end):
            return "DATE_OUTSIDE_WINDOW"
    return "VALID_SAMPLE"


def probe(dataset, identity, identity_field, start, end, fields, token):
    params = urlencode({"dataset": dataset, "data_id": identity, "start_date": start, "end_date": end})
    try:
        payload = read_json(f"https://api.finmindtrade.com/api/v4/data?{params}", token)
    except HTTPError as error:
        return {401: "ACCESS_DENIED", 403: "ACCESS_DENIED", 429: "RATE_LIMITED"}.get(
            error.code, "HTTP_ERROR"
        )
    except Exception:
        return "CHECK_FAILED"
    return classify(payload, start, end, fields, identity_field=identity_field, identity=identity)


def probe_key(spec):
    *params, fields = spec
    encoded = json.dumps([*params, sorted(fields)], separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def split_probes(specs, evidence):
    reused, pending = [], []
    for spec in specs:
        record = evidence.get(probe_key(spec), {})
        valid = (
            record.get("dataset") == spec[0]
            and record.get("status") == "VALID_SAMPLE"
            and type(record.get("run_id")) is int and record["run_id"] > 0
            and type(record.get("job_id")) is int and record["job_id"] > 0
        )
        (reused if valid else pending).append(spec)
    return reused, pending


def run_pending(pending, token, workers=12):
    """Keep the worker pool full until a provider limit stops new submissions."""
    remaining = iter(pending)
    completed = {}
    rate_limited = False
    with ThreadPoolExecutor(max_workers=workers) as pool:
        active = {pool.submit(probe, *spec, token): spec for spec in pending[:workers]}
        for _ in active:
            next(remaining)
        while active:
            done, _ = wait(active, return_when=FIRST_COMPLETED)
            for future in done:
                spec = active.pop(future)
                status = future.result()
                completed[probe_key(spec)] = status
                rate_limited |= status == "RATE_LIMITED"
            if not rate_limited:
                for _ in done:
                    spec = next(remaining, None)
                    if spec is not None:
                        active[pool.submit(probe, *spec, token)] = spec
    return completed


def pilot_probes():
    by_dataset = {spec[0]: spec for spec in PROBES}
    historical = []
    for dataset, start, end in HISTORICAL_WINDOWS:
        _, code, key, _, _, fields = by_dataset[dataset]
        historical.append((dataset, code, key, start, end, fields))
    return (*PROBES, *historical)


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--count", action="store_true")
    args = parser.parse_args()
    evidence = json.loads(Path(__file__).with_name("sample_evidence.json").read_text())
    reused, pending = split_probes(pilot_probes(), evidence)
    if args.count:
        print(len(pending))
        return 0
    if args.plan:
        print(json.dumps({"reused": [spec[0] for spec in reused],
                          "pending": [spec[0] for spec in pending]}))
        return 0
    for spec in reused:
        print(f"{spec[0]}=REUSED_WINDOW_EVIDENCE")
    if not pending:
        print("dataset_pilot=PASS_NO_NEW_REQUESTS")
        return 0
    token = os.environ.get("FINMIND_BACKER_TOKEN")
    if not token:
        print("dataset_pilot=HOLD_MISSING_BACKER_TOKEN")
        return 1
    results = run_pending(pending, token)
    for spec in pending:
        status = results.get(probe_key(spec), "NOT_SENT_RATE_LIMIT")
        print(f"window {probe_key(spec)} {spec[0]} {spec[3]} {spec[4]} {status}")
    print(f"dataset_requests_sent={len(results)}")
    passed = len(results) == len(pending) and all(
        status == "VALID_SAMPLE" for status in results.values()
    )
    print("dataset_pilot=" + ("PASS" if passed else "HOLD"))
    return int(not passed)


if __name__ == "__main__":
    raise SystemExit(main())
