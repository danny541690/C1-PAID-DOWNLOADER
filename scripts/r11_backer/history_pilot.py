"""Check historical boundaries, publication dates and archive capacity in one Backer run."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import gzip
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .archive import archive_plan, compress_original
from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .exchange_scope import price_value
from .live_preflight import read_json
from .release_probes import MAX_BYTES, RECOVERY_ROOT, Task, read_private_file, recovery_location, run_parallel
from .release_remediation import pinned_contracts


BUSINESS = "TaiwanBusinessIndicator"
BUSINESS_START = date(1982, 1, 1)
NDC_PERIOD = date(2026, 9, 1)
NDC_PUBLICATION = date(2026, 10, 29)
NDC_NEXT_PUBLICATION = date(2026, 11, 27)
NDC_REPORT = "https://www.ndc.gov.tw/nc_332_40618"
NDC_HISTORICAL_REPORT = "https://ws.ndc.gov.tw/001/administrator/10/relfile/6141/30014/b204ea46-2860-4af4-ab9f-fc61e4205485.pdf"
FINMIND_BUSINESS_DOC = "https://finmind.github.io/tutor/TaiwanMarket/Others/"
PRICE = "TaiwanStockPrice"
BENCHMARK_DAY = date(2026, 11, 24)
BENCHMARK_ARCHIVE_BYTES = 475771
CALENDAR = Task("TaiwanStockTradingDate", None, None)
EARLY_PRICE = Task(PRICE, "1994-01-01", "1998-12-31", "2330")
REVENUE = Task("TaiwanStockMonthRevenue", "2026-10-01", "2026-11-01", "2330")
REVENUE_RELEASES = {
    date(2026, 9, 1): (date(2026, 10, 8), 511857, "https://pr.tsmc.com/english/news/3343"),
    date(2026, 10, 1): (date(2026, 11, 10), 586890, "https://pr.tsmc.com/english/news/3349"),
}
TWSE_CALENDAR = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
TWSE_CALENDAR_ALTERNATE = "https://wwwc.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
TWSE_CALENDAR_DOC = "https://www.twse.com.tw/zh/trading/historical/fmtqik.html"
TWSE_INDEX = Path("r11-twse-early-session-index.json")
PREVIOUS_INDEX_SHA256 = "af4bb26d782565d7150934a0a1cf54bcb76ac25324bec0c0218c913ec65c950c"
PREFIX = "R11_HISTORY_PILOT"
RECEIPT_FILE = "history-index.json"
PUBLIC_REPORT = Path("r11-history-capacity.json")


def iso_date(value):
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date does not match source ISO contract")
    return parsed


def months(first, last):
    while first <= last:
        yield first
        first = next_month(first)


def next_month(day):
    return (day.replace(day=28) + timedelta(days=4)).replace(day=1)


def business_rows(rows):
    indexed = {}
    for position, row in enumerate(rows):
        period = iso_date(row["date"])
        if period.day != 1 or period in indexed:
            raise ValueError("non-monthly or duplicate business month")
        if not {"leading", "coincident", "lagging", "monitoring", "monitoring_color"} <= row.keys():
            raise ValueError("incomplete Business Indicator schema")
        indexed[period] = (position, row)
    return indexed


def classify_business(full, recent, earlier, trial_day):
    try:
        if full.status != "VALID_SAMPLE" or recent.status != "VALID_SAMPLE" or not full.rows or not recent.rows:
            raise ValueError("monthly provider samples unavailable")
        indexed, recent_index = business_rows(full.rows), business_rows(recent.rows)
    except (ValueError, KeyError, TypeError):
        return {"status": "HOLD_MONTHLY_SOURCE_CONTRACT", "probe_statuses": [full.status, recent.status, earlier.status]}, None
    expected = set(months(BUSINESS_START, NDC_PERIOD))
    observed = indexed.keys() & expected
    missing = expected - observed
    suffix = {period: item[1] for period, item in indexed.items() if period >= min(recent_index)}
    same_suffix = suffix == {period: item[1] for period, item in recent_index.items()}
    ndc_matches = NDC_PERIOD in indexed and price_value(indexed[NDC_PERIOD][1]["monitoring"]) == 41
    recent_is_current = NDC_PUBLICATION <= trial_day < NDC_NEXT_PUBLICATION
    first_vendor_month = min(indexed)
    early_scope = "OFFICIAL_PRE_PROVIDER_GAP" if earlier.status == "EMPTY_WINDOW" else "HOLD_PRE_PROVIDER_SCOPE"
    status = "PASS_PROVIDER_WINDOW" if not missing and same_suffix and ndc_matches and recent_is_current and earlier.status == "EMPTY_WINDOW" else "HOLD_UNVERIFIED_MONTHLY_WINDOW"
    report = {
        "status": status, "provider_document_start": BUSINESS_START.isoformat(),
        "earliest_observed": first_vendor_month.isoformat(), "latest_observed": max(indexed).isoformat(),
        "through_official_period": NDC_PERIOD.isoformat(), "expected_months": len(expected),
        "verified_months": len(observed), "missing_months": len(missing), "suffix_matches": same_suffix,
        "latest_official_monitoring_score_matches": ndc_matches, "as_of_within_published_window": recent_is_current,
        "older_official_window": early_scope, "earlier_finmind_status": earlier.status,
        "official_1981_months_outside_finmind": 12 if earlier.status == "EMPTY_WINDOW" else None,
        "later_rows_excluded_from_verified_index": sum(period > NDC_PERIOD for period in indexed),
        "official_reference": NDC_REPORT, "earlier_official_reference": NDC_HISTORICAL_REPORT,
        "provider_reference": FINMIND_BUSINESS_DOC,
    }
    return report, indexed


def calendar_evidence(calendar_sample, early_price):
    if calendar_sample.status != "VALID_SAMPLE" or early_price.status != "VALID_SAMPLE":
        return {"status": "HOLD_CALENDAR_AND_EARLY_TRADES", "calendar": calendar_sample.status, "daily_price": early_price.status}
    try:
        calendar = [iso_date(row["date"]) for row in calendar_sample.rows]
        old_days = {iso_date(row["date"]) for row in early_price.rows if row["stock_id"] == EARLY_PRICE.data_id}
    except (ValueError, KeyError, TypeError):
        return {"status": "HOLD_DATE_PARSING"}
    if not calendar or not old_days or len(calendar) != len(set(calendar)) or len(old_days) != len(early_price.rows):
        return {"status": "HOLD_CALENDAR_UNIQUE_KEYS"}
    cutoff = BENCHMARK_DAY
    missing = old_days - set(calendar)
    projected_days = len({day for day in calendar if day <= cutoff} | missing)
    return {
        "status": "HOLD_CALENDAR_ROSTER_GAP" if missing else "PASS_OBSERVED_TRADES_ON_CALENDAR",
        "first_calendar_day": min(calendar).isoformat(), "last_calendar_day": max(calendar).isoformat(),
        "first_early_daily_price": min(old_days).isoformat(), "last_early_daily_price": max(old_days).isoformat(),
        "early_trade_days": len(old_days), "early_trade_days_missing_from_calendar": len(missing),
        "known_through": cutoff.isoformat(), "known_days_for_sizing": projected_days,
        "one_day_sample_run": 45649612210, "one_day_compressed_bytes": BENCHMARK_ARCHIVE_BYTES,
        "illustrative_known_day_bytes": projected_days * BENCHMARK_ARCHIVE_BYTES,
        "estimate_status": "BENCHMARK_FOR_KNOWN_DAYS_ONLY",
    }


def revenue_evidence(sample):
    if sample.status != "VALID_SAMPLE":
        return {"status": sample.status}
    periods = {}
    try:
        for row in sample.rows:
            period = date(int(row["revenue_year"]), int(row["revenue_month"]), 1)
            if period not in REVENUE_RELEASES or period in periods or row["stock_id"] != REVENUE.data_id:
                return {"status": "HOLD_REVENUE_KEYS"}
            source_date = iso_date(row["date"])
            release, official_millions, link = REVENUE_RELEASES[period]
            if source_date != next_month(period):
                return {"status": "HOLD_QUERY_DATE_RULE"}
            value = row["revenue"]
            if type(value) is not int or abs(value - official_millions * 1_000_000) > 500_000:
                return {"status": "HOLD_OFFICIAL_REVENUE_RECONCILIATION"}
            periods[period] = {"period": period.strftime("%Y-%m"), "provider_query_date": source_date.isoformat(),
                               "official_announced_on": release.isoformat(),
                               "pit_calendar_lower_bound": (release + timedelta(days=1)).isoformat(),
                               "publication_reference": link, "matches_company_revenue_rounded_to_millions": True}
    except (TypeError, ValueError, KeyError):
        return {"status": "HOLD_REVENUE_DATE_CONTRACT"}
    return {"status": "PASS_PIT_PUBLICATION_DATE_SAMPLE" if periods.keys() == REVENUE_RELEASES.keys() else "HOLD_INCOMPLETE_RELEASE_SAMPLE",
            "periods": [periods[key] for key in sorted(periods)],
            "finmind_create_time_meaning": "DATABASE_INGESTION_DATE"}


def canonical_json(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def parse_twse_sessions(document, month):
    if document.get("stat") != "OK" or document.get("date") != month.strftime("%Y%m%d"):
        raise ValueError("exchange calendar does not identify the requested month")
    headers = document["fields"]
    date_index = headers.index("日期")
    close_index = headers.index("成交股數")
    days = set()
    for row in document["data"]:
        year, mm, dd = map(int, row[date_index].strip().split("/"))
        day = date(year + 1911, mm, dd)
        if day.replace(day=1) != month or day in days or price_value(row[close_index]) is None:
            raise ValueError("invalid or duplicated exchange session")
        days.add(day)
    if not days:
        raise ValueError("exchange calendar is empty")
    return days


def twse_month(month):
    query = urlencode({"date": month.strftime("%Y%m%d"), "response": "json"})
    for endpoint in (TWSE_CALENDAR, TWSE_CALENDAR_ALTERNATE):
        request = Request(f"{endpoint}?{query}", headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        try:
            with urlopen(request, timeout=12) as response:
                body = response.read(131073)
            if len(body) > 131072:
                raise ValueError("unexpected calendar response size")
            return "PASS", parse_twse_sessions(json.loads(body), month)
        except HTTPError as error:
            if endpoint == TWSE_CALENDAR and error.code in (301, 302, 307, 308, 502, 503, 504):
                continue
            return ("RATE_LIMITED" if error.code == 429 else f"HOLD_EXCHANGE_HTTP_{error.code}"), set()
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return "HOLD_EXCHANGE_FORMAT_OR_NETWORK", set()
    raise RuntimeError("exchange source selection exhausted")


def twse_history(start, end, confirmed):
    results = dict(confirmed)
    schedule = [month for month in months(start, end) if month not in results]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for offset in range(0, len(schedule), 4):
            batch = schedule[offset:offset + 4]
            results.update(zip(batch, pool.map(twse_month, batch)))
            if any(results[month][0] == "RATE_LIMITED" for month in batch):
                break
    return results


def verified_exchange_months(document, first_month, last_month):
    if (sha(canonical_json(document)) != PREVIOUS_INDEX_SHA256 or document["venue"] != "TWSE"
            or document["start_month"] != first_month.isoformat() or document["end_month"] != last_month.isoformat()):
        raise ValueError("prior exchange index is not the verified test artifact")
    confirmed = {}
    for value in document["newest_first_sessions"]:
        day = iso_date(value)
        month = day.replace(day=1)
        if not first_month <= month <= last_month:
            raise ValueError("prior exchange session is out of scope")
        _, dates = confirmed.setdefault(month, ("PASS", set()))
        if day in dates:
            raise ValueError("duplicate session in prior exchange index")
        dates.add(day)
    return confirmed


def followup(prior_index=None):
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    first_month, last_month = date(1994, 9, 1), date(1998, 12, 1)
    expected_months = len(tuple(months(first_month, last_month)))
    confirmed = verified_exchange_months(json.loads(prior_index.read_bytes()), first_month, last_month) if prior_index else {}
    tasks = [EARLY_PRICE] if prior_index else [EARLY_PRICE, REVENUE]
    with ThreadPoolExecutor(max_workers=1) as pool:
        calendar = pool.submit(twse_history, first_month, last_month, confirmed)
        received = run_parallel(tasks, pinned_contracts(), auth, workers=len(tasks))
        results = calendar.result()
    days = set().union(*(sessions for _, sessions in results.values()))
    prices = received[EARLY_PRICE]
    price_days = {iso_date(row["date"]) for row in prices.rows if row["stock_id"] == EARLY_PRICE.data_id}
    outcomes = Counter(status for status, _ in results.values())
    complete = outcomes["PASS"] == expected_months
    consistent = prices.status == "VALID_SAMPLE" and bool(price_days) and len(price_days) == len(prices.rows) and price_days <= days
    index = {"venue": "TWSE", "source": TWSE_CALENDAR_DOC, "start_month": first_month.isoformat(),
             "end_month": last_month.isoformat(), "all_months_confirmed": complete,
             "newest_first_sessions": [day.isoformat() for day in sorted(days, reverse=True)]}
    TWSE_INDEX.write_bytes(canonical_json(index) + b"\n")
    return {"status": "PASS_DIAGNOSTIC", "production": "HOLD", "prior_run": 45651767533 if prior_index else 45651295300,
            "revenue_pit": {"status": "REUSED_FROM_PRIOR_RUN"} if prior_index else revenue_evidence(received[REVENUE]),
            "twse_early_sessions": {"status": "PASS_TWSE_SAMPLE_RECONCILED" if complete and consistent else "HOLD_TWSE_RECONCILIATION",
                                    "prior_months_reused": len(confirmed), "official_reports_requested": len(results) - len(confirmed),
                                    "monthly_outcomes": dict(outcomes),
                                    "official_sessions": len(days), "early_finmind_stock_dates": len(price_days),
                                    "finmind_dates_on_official_calendar": len(price_days & days),
                                    "index_sha256": sha(canonical_json(index)), "tpex_calendar": "HOLD_SEPARATE_VENUE_REQUIRED"},
            "requests": sum(sample.status != "NOT_SENT_RATE_LIMIT" for sample in received.values()),
            "rate_limited": any(sample.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for sample in received.values())}


def raw_month_manifest(indexed):
    actual_first, actual_last = min(indexed), max(indexed)
    month_offsets = [[day.isoformat()[:7], indexed[day][0]] for day in sorted(indexed, reverse=True)
                     if BUSINESS_START <= day <= NDC_PERIOD]
    return {"dataset": BUSINESS, "anchor": archive_plan(list(indexed)).anchor,
            "first": actual_first.isoformat(), "last": actual_last.isoformat(),
            "verified_through": NDC_PERIOD.isoformat(), "newest_first_month_offsets": month_offsets}


def private_run_folder(access, *, create):
    name = recovery_location(access, prefix=PREFIX)
    folder = ensure(RECOVERY_ROOT, name, FOLDER, access, create=create)
    if read_json(f"{BASE}/files/{folder}?fields=shared", access).get("shared") is not False:
        raise ValueError("private recovery folder could not be confirmed")
    return folder


def archive_parent(root, manifest, access, *, create):
    anchor = archive_plan([iso_date(manifest["first"])]).anchor
    if manifest["anchor"] != anchor:
        raise ValueError("archive location changed")
    parent = root
    for label in anchor.split("/"):
        parent = ensure(parent, label, FOLDER, access, create=create)
    return parent


def archive_name(manifest):
    return f"FinMind_Backer__{BUSINESS}__{manifest['first']}__{manifest['last']}__R11.1.json.gz"


def storage_and_stage(sample, indexed, calendar):
    access = drive_token()
    quota = read_json(f"{BASE}/about?fields=storageQuota(limit%2Cusage)", access)["storageQuota"]
    free = int(quota["limit"]) - int(quota["usage"])
    if free <= 0:
        return {"archive": "HOLD_NO_CAPACITY", "full_volume": "HOLD"}
    projection = calendar.get("illustrative_known_day_bytes")
    outcome = {"archive": "HOLD_MONTHLY_INPUT", "full_volume": "HOLD_UNESTIMATED_DATASETS",
               "daily_sizing_example_fits": projection is not None and projection <= free}
    if indexed is None:
        return outcome
    if not sample.raw:
        raise ValueError("original response bytes missing")
    manifest = raw_month_manifest(indexed)
    packed, raw_sha, packed_sha = compress_original(sample.raw)
    if len(packed) + len(canonical_json(manifest)) >= free:
        return outcome | {"archive": "HOLD_SAMPLE_CAPACITY"}
    root = private_run_folder(access, create=True)
    parent = archive_parent(root, manifest, access, create=True)
    filename = archive_name(manifest)
    file_id = ensure(parent, filename, "application/gzip", access, packed, create=True)
    receipt = {"manifest": manifest, "manifest_sha256": sha(canonical_json(manifest)), "file_id": file_id,
               "compressed_bytes": len(packed), "raw_sha256": raw_sha, "gzip_sha256": packed_sha,
               "drive_available_bytes": free}
    ensure(root, RECEIPT_FILE, "application/json", access, canonical_json(receipt), create=True)
    return outcome | {"archive": "STAGED", "monthly_original_gzip_bytes": len(packed),
                      "indexed_months": len(manifest["newest_first_month_offsets"])}


def verify_readback():
    access = drive_token()
    root = private_run_folder(access, create=False)
    receipt_id = ensure(root, RECEIPT_FILE, "application/json", access, create=False)
    receipt = json.loads(read_private_file(receipt_id, access))
    manifest = receipt["manifest"]
    parent = archive_parent(root, manifest, access, create=False)
    file_id = ensure(parent, archive_name(manifest), "application/gzip", access, create=False)
    compressed = read_private_file(file_id, access)
    if file_id != receipt["file_id"] or sha(compressed) != receipt["gzip_sha256"] or len(compressed) != receipt["compressed_bytes"]:
        raise ValueError("archive file identity or gzip digest mismatch")
    with gzip.GzipFile(fileobj=BytesIO(compressed)) as source:
        raw = source.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or sha(raw) != receipt["raw_sha256"]:
        raise ValueError("source digest mismatch")
    expected = raw_month_manifest(business_rows(json.loads(raw)["data"]))
    if expected != manifest or sha(canonical_json(expected)) != receipt["manifest_sha256"]:
        raise ValueError("independent month index/digest mismatch")
    print(f"independent_history_archive=PASS indexed_months={len(expected['newest_first_month_offsets'])}")


def run():
    today = datetime.now(ZoneInfo("Asia/Taipei")).date()
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    account = read_json("https://api.web.finmindtrade.com/v2/user_info", auth)
    limit, used = account.get("api_request_limit"), account.get("user_count")
    if limit != 1600:
        return {"status": "HOLD_BACKER_ENTITLEMENT", "production": "HOLD"}
    # The 1981 comparison comes from an earlier NDC archive. The 1982 date
    # is specific to this vendor dataset's published range.
    primary = Task(BUSINESS, BUSINESS_START.isoformat(), today.isoformat())
    overlap = Task(BUSINESS, f"{NDC_PERIOD.year}-01-01", today.isoformat())
    preceding = Task(BUSINESS, "1981-01-01", "1981-12-31")
    tasks = [primary, overlap, preceding, CALENDAR, EARLY_PRICE, REVENUE]
    if type(used) is not int or not 0 <= used <= limit - len(tasks):
        return {"status": "HOLD_ACCOUNT_USAGE", "production": "HOLD"}
    received = run_parallel(tasks, pinned_contracts(), auth, workers=len(tasks))
    business, index = classify_business(received[primary], received[overlap], received[preceding], today)
    calendar = calendar_evidence(received[CALENDAR], received[EARLY_PRICE])
    drive = storage_and_stage(received[primary], index, calendar)
    return {"status": "PASS_DIAGNOSTIC", "production": "HOLD", "as_of_taipei": today.isoformat(),
            "historical_business": business, "trading_calendar": calendar, "revenue_pit": revenue_evidence(received[REVENUE]),
            "storage": drive, "entitlement": "BACKER_REFERENCE_CONFIRMED", "expiration": "HOLD_NO_EXACT_DATE",
            "requests": 1 + sum(sample.status != "NOT_SENT_RATE_LIMIT" for sample in received.values()),
            "rate_limited": any(sample.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for sample in received.values())}


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--verify-only", action="store_true")
    modes.add_argument("--followup", action="store_true")
    modes.add_argument("--retry-calendar", type=Path)
    args = parser.parse_args()
    if args.verify_only:
        try:
            verify_readback()
        except Exception as error:
            print(f"independent_history_archive=FAIL:{type(error).__name__}")
            return 1
        return 0
    try:
        report = followup(args.retry_calendar) if args.followup or args.retry_calendar else run()
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "production": "HOLD", "error_class": type(error).__name__}
    PUBLIC_REPORT.write_bytes(canonical_json(report) + b"\n")
    print(json.dumps(report, sort_keys=True))
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a", encoding="utf-8") as outputs:
            outputs.write(f"archive={report.get('storage', {}).get('archive', 'HOLD')}\n")
    if args.retry_calendar:
        settled = report.get("twse_early_sessions", {}).get("status") == "PASS_TWSE_SAMPLE_RECONCILED"
        return int(not settled or report.get("rate_limited", False))
    if args.followup:
        return int(report["status"] != "PASS_DIAGNOSTIC" or report.get("rate_limited", False))
    return int(report["status"] != "PASS_DIAGNOSTIC" or report.get("rate_limited", False)
               or report.get("storage", {}).get("archive") != "STAGED")


if __name__ == "__main__":
    raise SystemExit(main())
