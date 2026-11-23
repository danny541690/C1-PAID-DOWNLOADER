#!/usr/bin/env python3
"""JARVIS v0.98 FinMind Backer read-only API schema canary.
Only observed successful API responses are treated as actual data evidence.
Candidate dataset names come from the existing C1-RAW-DOWNLOADER script
scripts/v093_finmind_free_2026_canary.py; NOT a Backer coverage claim.
Never prints tokens or sends payloads to GitHub source.
"""
import csv
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.finmindtrade.com/api/v4/data"
DATASETS = [
    ("TaiwanStockInfo", "stock list / identity", False),
    ("TaiwanStockPrice", "daily equity prices", True),
    ("TaiwanStockInstitutionalInvestorsBuySell", "institutional trading", True),
    ("TaiwanStockMarginPurchaseShortSale", "margin / short sale", True),
    ("TaiwanStockMonthRevenue", "monthly revenue", True),
    ("TaiwanStockFinancialStatements", "financial statements", True),
    ("TaiwanStockBalanceSheet", "balance sheet", True),
    ("TaiwanStockCashFlowsStatement", "cash flows", True),
    ("TaiwanStockPER", "valuation ratios", True),
    ("TaiwanStockShareholding", "shareholding distribution", True),
]
# Descriptions are merely JARVIS classification labels; observed API columns
# and records, not labels, are the evidence of content and schema.
def fetch(dataset, token, needs_id):
    query = {"dataset": dataset}
    if needs_id:
        query.update(data_id="2330", start_date="2024-01-01",
                     end_date="2024-12-31")
    url = API + "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(
        url, headers={"Authorization": "Bearer " + token,
                      "Accept": "application/json",
                      "User-Agent": "C1-PAID-DOWNLOADER-schema-canary/0.98"})
    try:
        with urllib.request.urlopen(request, timeout=50) as resp:
            # Strict response bound to prevent unexpectedly large data jobs.
            raw = resp.read(8_000_001)
            if len(raw) > 8_000_000:
                raise ValueError("RESPONSE_EXCEEDS_8MB")
            status = resp.status
    except urllib.error.HTTPError as e:
        # No raw server body: it can be arbitrary and potentially sensitive.
        return {"state": "HTTP_ERROR", "http_status": e.code}
    except Exception as e:
        return {"state": "TRANSPORT_ERROR", "error_type": type(e).__name__}
    try:
        decoded = json.loads(raw.decode("utf-8-sig"))
        if not isinstance(decoded, dict):
            return {"state": "INVALID_RESPONSE_OBJECT", "http_status": status}
        data = decoded.get("data")
        if decoded.get("status") != 200:
            return {"state": "API_REJECTED", "http_status": status,
                    "api_status": decoded.get("status")}
        if not isinstance(data, list):
            return {"state": "INVALID_DATA_ARRAY", "http_status": status}
        if not data:
            return {"state": "EMPTY_VALID_RESPONSE", "http_status": status, "row_count": 0}
        if any(not isinstance(row, dict) for row in data):
            return {"state": "NON_OBJECT_ROW", "http_status": status}
        columns = sorted({name for row in data for name in row})
        example = data[:2]  # Actual data cells; no credentials in market rows.
        types = {k: sorted({type(row[k]).__name__ for row in data if k in row})
                 for k in columns}
        missing = {k: sum(k not in row for row in data) for k in columns}
        return {"state": "PASS_OBSERVED", "http_status": status,
                "api_status": 200, "row_count": len(data),
                "observed_columns": columns, "observed_types": types,
                "missing_column_counts": missing,
                "sample_rows": example, "response_sha256": hashlib.sha256(raw).hexdigest(),
                "response_bytes": len(raw)}
    except Exception as e:
        return {"state": "PARSE_ERROR", "error_type": type(e).__name__}

def main():
    token = os.environ.get("FINMIND_BACKER_TOKEN", "").strip()
    if not token:
        raise SystemExit("MISSING_FINMIND_BACKER_TOKEN")
    out = Path("finmind_canary_report")
    out.mkdir(exist_ok=True)
    entries = []
    for idx, (dataset, jarvis_label, needs_id) in enumerate(DATASETS, 1):
        result = fetch(dataset, token, needs_id)
        record = {"dataset": dataset, "jarvis_classification": jarvis_label,
                  "test_params": {"dataset": dataset, **({"data_id": "2330",
                     "start_date": "2024-01-01", "end_date": "2024-12-31"}
                     if needs_id else {})}, **result}
        entries.append(record)
        print(json.dumps({"index": idx, "dataset": dataset, "state": result["state"],
                          "rows": result.get("row_count"), "http_status": result.get("http_status"),
                          "columns": len(result.get("observed_columns", []))},
                         ensure_ascii=False), flush=True)
        if result.get("http_status") in (402, 429):
            print("QUOTA_OR_RATE_LIMIT_STOP", flush=True)
            break
        if idx < len(DATASETS):
            time.sleep(1)
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    summary = {"timestamp_utc": now, "repo": "C1-PAID-DOWNLOADER",
        "auth_token_present": True, "stock_id_test_scope": "2330",
        "historical_test_period": "2024-01-01 to 2024-12-31",
        "dataset_candidate_origin": "existing C1-RAW-DOWNLOADER source code",
        "complete_backer_dataset_catalog": False,
        "backer_subscription_entitlement_independently_verified": False,
        "read_only": True, "drive_write_performed": False,
        "candidates_tested": len(entries),
        "datasets_pass_observed": sum(e["state"] == "PASS_OBSERVED" for e in entries),
        "entries": entries}
    (out/"report.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2),
                                  encoding="utf-8")
    with (out/"dataset_columns.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["dataset","jarvis_classification","state","row_count",
                         "field_name","observed_types","missing_row_count"])
        for e in entries:
            if not e.get("observed_columns"):
                writer.writerow([e["dataset"],e["jarvis_classification"],e["state"],
                                 e.get("row_count",0),"","",""])
            for k in e.get("observed_columns",[]):
                writer.writerow([e["dataset"], e["jarvis_classification"],e["state"],
                     e["row_count"],k,"|".join(e["observed_types"][k]),
                     e["missing_column_counts"][k]])
    (out/"README.txt").write_text(
        "Observed sample schemas ONLY. This is not a complete FinMind Backer catalog.\n"
        "PASS_OBSERVED means 2024 stock 2330 response contained records.\n"
        "EMPTY and API_REJECTED are not evidence a dataset is absent.\n"
        "sample_rows contain only up to two public market records, no access tokens.\n"
        "No original Google Drive / Bronze files were modified.\n",encoding="utf-8")
    print("RESULT_FILE=finmind_canary_report/report.json")
    print("OBSERVED_DATASETS="+str(summary["datasets_pass_observed"]))
    if summary["datasets_pass_observed"] == 0:
        raise SystemExit("NO_SUCCESSFUL_DATASETS; inspect report artifact")

if __name__ == "__main__":
    main()
