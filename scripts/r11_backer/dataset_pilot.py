"""Bounded dataset probes using the Backer credential without logging records."""

from datetime import date
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


def main():
    token = os.environ.get("FINMIND_BACKER_TOKEN")
    if not token:
        print("dataset_pilot=HOLD_MISSING_BACKER_TOKEN")
        return 1
    results = []
    for spec in PROBES:
        status = probe(*spec, token)
        print(f"{spec[0]}={status}")
        results.append(status)
        if status in {"RATE_LIMITED", "ACCESS_DENIED"}:
            break
    passed = results == ["VALID_SAMPLE"] * len(PROBES)
    print("dataset_pilot=" + ("PASS" if passed else "HOLD"))
    return int(not passed)


if __name__ == "__main__":
    raise SystemExit(main())
