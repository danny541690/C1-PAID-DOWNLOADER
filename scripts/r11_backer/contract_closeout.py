"""Consolidate outstanding date, field and historical identity checks."""

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import re
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from .market_scope import market_universe
from .release_probes import SOURCE, Sample, Task, active_bond, contracts, request_sample, run_parallel

VIX_TUTORIAL = "https://finmind.github.io/tutor/TaiwanMarket/Derivative/#taiwanoptionvix-backersponsor"
BASELINE = 45623673196
DATE_INSPECTION = {"GoldPrice", "TaiwanStockPriceLimit"}


def source_days(dataset, rows):
    days = []
    for row in rows:
        value = row.get("date")
        try:
            day = date.fromisoformat(value)
        except (TypeError, ValueError):
            if dataset != "GoldPrice" or not isinstance(value, str):
                raise
            # Keep the source's calendar day for counting. This does not infer
            # release time, timezone conversion, or the point-in-time rule.
            day = datetime.fromisoformat(value).date()
        days.append(day)
    return days


def inspect_dates(task, sample, cutoff):
    if not sample.rows:
        return sample.status
    try:
        days = source_days(task.dataset, sample.rows)
    except (ValueError, TypeError):
        return "HOLD_DATE_FORMAT"
    first, last = date.fromisoformat(task.start), date.fromisoformat(task.end)
    if sample.documented_missing:
        return "HOLD_COLUMNS"
    if not any(first <= day <= last for day in days) or min(days) < first or max(days) > cutoff:
        return "HOLD_OUTSIDE_WINDOW"
    if len(days) != len(set(days)):
        return "HOLD_MULTIPLE_RECORDS_PER_SOURCE_DAY"
    if max(days) > last:
        return "OPEN_ENDED_RESPONSE_VERIFIED" if not task.send_end else "HOLD_OUTSIDE_WINDOW"
    return "VALID_SAMPLE"


def dated_market_members(periods, day):
    """A code belongs to one market at the given effective date."""
    members = {}
    seen = set()
    for code, market, start, end in periods:
        if end is not None and end <= start:
            raise ValueError("invalid listing validity period")
        if start <= day and (end is None or day < end):
            if code in seen:
                raise ValueError("overlapping listing periods")
            seen.add(code)
            if market in {"twse", "tpex"}:
                members[code] = market
    return members


def layout_diagnostic(sample):
    # Report only public contract field names. Response rows and values remain in memory.
    return {
        "status": sample.status,
        "missing_documented_fields": sample.documented_missing,
        "record_count": len(sample.rows),
    }


def historical_events(sample, current):
    options = []
    for row in sample.rows:
        code = row.get("stock_id")
        if not isinstance(code, str) or not re.fullmatch(r"[0-9]{4}", code):
            continue
        try:
            day = date.fromisoformat(row["date"])
        except (ValueError, TypeError, KeyError):
            continue
        options.append((day, code))
    return sorted(set(options), key=lambda item: (item[1] in current, -item[0].toordinal(), item[1]))[:4]


def proof_of_delisted_coverage(results, events):
    outcomes = Counter()
    for day, code in events:
        def window(offset_a, offset_b):
            return Task("TaiwanStockPrice", (day + timedelta(days=offset_a)).isoformat(),
                        (day + timedelta(days=offset_b)).isoformat(), code)
        before, after = results[window(-21, -1)], results[window(1, 14)]
        if (before.status == "VALID_SAMPLE" and before.rows
                and all(row.get("stock_id") == code for row in before.rows)
                and after.status == "EMPTY_WINDOW"):
            outcomes["PRICE_BEFORE_ANNOUNCED_DELISTING_AND_EMPTY_AFTER"] += 1
        else:
            outcomes["EVENT_SAMPLE_UNCONFIRMED"] += 1
    return dict(outcomes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity-only", action="store_true")
    args = parser.parse_args()
    token = os.environ["FINMIND_BACKER_TOKEN"]
    with urlopen(SOURCE, timeout=20) as response:
        content = response.read(1_000_001)
    if len(content) > 1_000_000:
        raise ValueError("public catalog exceeds expected size")
    registry = contracts(content.decode("utf-8"))
    cutoff = datetime.now(ZoneInfo("Asia/Taipei")).date()
    initial = [
        Task("TaiwanStockInfo", None, None),
        Task("TaiwanStockDelisting", "2024-01-01", "2024-12-31"),
    ]
    if args.identity_only:
        initial.append(Task("TaiwanStockDelisting", "2026-09-01", "2026-11-24"))
    else:
        initial.append(Task("TaiwanStockConvertibleBondInfo", None, None))
    results = run_parallel(initial, registry, token)
    bond = None if args.identity_only else active_bond(results[initial[2]], "2026-11-02", "2026-11-06")
    current = market_universe(results[initial[0]].rows)
    event_feeds = [task for task in initial if task.dataset == "TaiwanStockDelisting"]
    events = []
    for task in event_feeds:
        events.extend(item for item in historical_events(results[task], current) if item not in events)
    events = events[:6]
    date_tasks = [
        Task("TaiwanStockPriceLimit", "2026-11-24", "2026-11-24", "2330", send_end=False),
        Task("GoldPrice", "2026-10-01", "2026-10-07"),
        Task("GoldPrice", "2025-11-03", "2025-11-07"),
    ]
    fields = [
        Task("TaiwanDailyShortSaleBalances", "2026-11-02", "2026-11-06", "2330"),
        Task("TaiwanDailyShortSaleBalances", "2025-11-03", "2025-11-07", "6488"),
        Task("TaiwanFuturesInstitutionalInvestors", "2026-11-02", "2026-11-06", "TX"),
        Task("TaiwanFuturesInstitutionalInvestors", "2025-11-03", "2025-11-07", "TX"),
        Task("TaiwanOptionInstitutionalInvestors", "2026-11-02", "2026-11-06", "TXO"),
        Task("TaiwanOptionInstitutionalInvestors", "2025-11-03", "2025-11-07", "TXO"),
    ]
    bond_datasets = ("TaiwanStockConvertibleBondDaily", "TaiwanStockConvertibleBondInstitutionalInvestors")
    if bond:
        fields.extend(Task(dataset, "2026-11-02", "2026-11-06", bond) for dataset in bond_datasets)

    if args.identity_only:
        date_tasks, fields, bond_datasets = [], [], ()
    price_tasks = []
    for day, code in events:
        for offset_a, offset_b in ((-21, -1), (1, 14)):
            price_tasks.append(Task("TaiwanStockPrice", (day + timedelta(days=offset_a)).isoformat(),
                                    (day + timedelta(days=offset_b)).isoformat(), code))

    def reader(task, contract, auth):
        return request_sample(task, contract, auth, inspect_dates=task.dataset in DATE_INSPECTION)

    throttled = any(sample.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for sample in results.values())
    planned = [*date_tasks, *fields, *price_tasks]
    if throttled:
        checked = dict.fromkeys(planned, Sample("NOT_SENT_RATE_LIMIT"))
    else:
        checked = run_parallel(planned, registry, token, reader=reader)

    checks = {}
    for task in date_tasks:
        status = inspect_dates(task, checked[task], cutoff)
        checks.setdefault(task.dataset, []).append({
            "status": status, "start": task.start, "end": task.end,
        })
    for task in fields:
        checks.setdefault(task.dataset, []).append(layout_diagnostic(checked[task]))
    for name in bond_datasets:
        checks.setdefault(name, [{"status": "HOLD_BOND_ID_UNVERIFIED"}])

    # The official tutorial demonstrates multiple intraday times on one date.
    if not args.identity_only:
        checks["TaiwanOptionVix"] = [{"status": "EXCLUDED_INTRADAY", "documentation": VIX_TUTORIAL}]
    conclusions = {}
    for dataset, windows in checks.items():
        states = {item["status"] for item in windows}
        if states <= {"VALID_SAMPLE", "OPEN_ENDED_RESPONSE_VERIFIED"}:
            conclusions[dataset] = "PASS_SAMPLE_WITH_WINDOW_INDEX" if "OPEN_ENDED_RESPONSE_VERIFIED" in states else "PASS_SAMPLE"
        elif states == {"EXCLUDED_INTRADAY"}:
            conclusions[dataset] = "EXCLUDED_INTRADAY"
        else:
            conclusions[dataset] = "HOLD"

    all_requests = [*results.values(), *checked.values()]
    report = {
        "prior_run": BASELINE, "production": "HOLD",
        "requests_sent": sum(s.status != "NOT_SENT_RATE_LIMIT" for s in all_requests),
        "conclusions": conclusions,
        "checks": checks,
        "historical_identity": {
            "current_master_matches": len(current),
            "delisting_feeds": [
                {"start": task.start, "end": task.end, "status": results[task].status,
                 "rows": len(results[task].rows)} for task in event_feeds
            ],
            "delisting_event_windows_tested": len(events),
            "observations": proof_of_delisted_coverage(checked, events),
            "full_effective_identity_history": "HOLD",
        },
        "rate_limited": any(s.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for s in all_requests),
    }
    Path("r11-contract-closeout.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))
    return int(report["rate_limited"])


if __name__ == "__main__":
    raise SystemExit(main())
