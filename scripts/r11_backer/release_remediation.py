"""One bounded Backer run for the outstanding contract, scope and storage issues."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
import gzip
import json
import os
from pathlib import Path
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from .archive import archive_plan, compress_original
from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .exchange_scope import EXCHANGES, official_close, price_value, reconcile_close
from .live_preflight import read_json
from .native_contracts import BOND_INVESTORS, LAYOUTS, native_result, numeric
from .release_gaps import BULK_MAX_BYTES
from .release_probes import (
    MAX_BYTES, RECOVERY_ROOT, SOURCE, Sample, Task, active_bond, contracts, read_private_file,
    recovery_location, request_sample, run_parallel,
)


PLIMIT = "TaiwanStockPriceLimit"
PRICE = "TaiwanStockPrice"
DAYS = (date(2010, 11, 24), date(2026, 11, 24))
PRICE_LIMIT_TASKS = (
    Task(PLIMIT, "1999-12-20", None, "2330", send_end=False),
    Task(PLIMIT, "2026-11-13", None, "2330", send_end=False),
)
GOLD_TASKS = (Task("GoldPrice", "2025-11-03", "2025-11-07"), Task("GoldPrice", "2026-10-01", "2026-10-07"))
MARKET_TASKS = {day: Task(PRICE, day.isoformat(), day.isoformat()) for day in DAYS}
BOND_SNAPSHOT = Task("TaiwanStockConvertibleBondInfo", None, None)
OUTPUT = Path("r11-release-remediation.json")


def open_ended_rows(task, sample):
    if sample.status != "VALID_SAMPLE" or not sample.rows:
        return {}
    rows = {}
    for position, row in enumerate(sample.rows):
        day = date.fromisoformat(row["date"])
        if day.isoformat() != row["date"] or row["stock_id"] != task.data_id:
            return {}
        if day.isoformat() < task.start or day in rows:
            return {}
        if not all(numeric(row.get(field)) for field in ("reference_price", "limit_up", "limit_down")):
            return {}
        rows[day] = (position, row)
    return rows


def suffix_index(tasks, samples, verified_close, today):
    try:
        full, recent = (open_ended_rows(task, samples[task]) for task in tasks)
    except (TypeError, ValueError, KeyError):
        return {"status": "HOLD_INVALID_DATES_OR_FIELDS"}, None
    if not full or not recent:
        return {"status": "HOLD_START_ONLY_LAYOUT", "probe_statuses": [samples[task].status for task in tasks]}, None
    boundary = date.fromisoformat(tasks[1].start)
    same_suffix = {day: row for day, (_, row) in full.items() if day >= boundary} == {
        day: row for day, (_, row) in recent.items()
    }
    summary = {"observed_first": min(full).isoformat(), "observed_last": max(full).isoformat(),
               "raw_rows": len(full), "recent_rows": len(recent), "suffix_matches": same_suffix}
    if not same_suffix or not any(day < boundary for day in full):
        return summary | {"status": "HOLD_START_ONLY_SUFFIX_MISMATCH"}, None
    # A future trading date may exist in the provider response. Keep the bytes,
    # and exclude that row from the currently verified archive index.
    if verified_close is None or verified_close > today or verified_close not in full:
        return summary | {"status": "HOLD_CLOSED_SESSION_NOT_COVERED"}, None
    months = defaultdict(list)
    followup = []
    for day, (position, _) in full.items():
        if day > verified_close:
            followup.append(position)
        else:
            months[day.isoformat()[:7]].append(position)
    summary |= {"status": "PASS_START_ONLY_SNAPSHOT_SAMPLE", "indexed_months": len(months),
                "rows_after_verified_close": len(followup), "verified_close": verified_close.isoformat()}
    index = {"months": dict(months), "after_verified_close": followup,
             "anchor": archive_plan([day for day in full if day <= verified_close]).anchor}
    return summary, index


def gold_outcome(sample):
    if sample.status != "VALID_SAMPLE" or not sample.rows:
        return {"status": sample.status}
    try:
        times = [datetime.fromisoformat(row["date"]) for row in sample.rows]
    except (TypeError, ValueError, KeyError):
        return {"status": "HOLD_GOLD_TIME_FORMAT"}
    per_day = defaultdict(set)
    for stamp in times:
        per_day[stamp.date()].add(stamp.hour * 3600 + stamp.minute * 60 + stamp.second)
    gaps = Counter()
    for observations in per_day.values():
        ordered = sorted(observations)
        gaps.update(later - earlier for earlier, later in zip(ordered, ordered[1:]))
    common_interval = gaps.most_common(1)[0][0] if gaps else None
    status = "EXCLUDE_SUBDAILY_SOURCE_SERIES" if common_interval else "HOLD_DAILY_OBSERVATION_AND_PUBLICATION_CONTRACT"
    return {"status": status, "days_with_multiple_observations": sum(len(values) > 1 for values in per_day.values()),
            "most_common_interval_seconds": common_interval,
            "non_midnight_timestamps": sum(stamp.time() != datetime.min.time() for stamp in times),
            "timestamps_with_explicit_timezone": sum(stamp.tzinfo is not None for stamp in times)}


def field_tasks(bond):
    eras = (("2025-11-03", "2025-11-07"), ("2026-11-02", "2026-11-06"))
    tasks = [Task(dataset, start, end, code) for dataset, code in (
        ("TaiwanDailyShortSaleBalances", "2330"), ("TaiwanFuturesInstitutionalInvestors", "TX"),
        ("TaiwanOptionInstitutionalInvestors", "TXO"),
    ) for start, end in eras]
    for dataset in ("TaiwanStockConvertibleBondDaily", BOND_INVESTORS):
        tasks.append(Task(dataset, "2020-04-06", "2020-04-10", "14361"))
        if bond:
            tasks.append(Task(dataset, "2026-11-02", "2026-11-06", bond))
    return tasks


def official_benchmarks(pool):
    return {(day, venue): pool.submit(official_close, day, venue) for day in DAYS for venue in EXCHANGES}


def collect_benchmarks(futures):
    results = {day: {} for day in DAYS}
    for (day, venue), future in futures.items():
        try:
            results[day][venue] = future.result()
        except Exception:
            results[day][venue] = {}
    return results


def pinned_contracts():
    with urlopen(SOURCE, timeout=15) as response:
        body = response.read(1_000_001)
    if len(body) > 1_000_000:
        raise ValueError("public contract snapshot exceeds bound")
    return contracts(body.decode())


def storage_and_archive(task, sample, index):
    access = drive_token()
    quota = read_json(f"{BASE}/about?fields=storageQuota(limit%2Cusage)", access).get("storageQuota", {})
    if not {"limit", "usage"} <= quota.keys():
        return {"status": "HOLD_DRIVE_CAPACITY_UNKNOWN"}
    limit, usage = int(quota["limit"]), int(quota["usage"])
    available = limit - usage
    if available <= 0:
        return {"status": "HOLD_NO_FREE_DRIVE_CAPACITY"}
    packed, original_sha, gzip_sha = compress_original(sample.raw) if index else (b"", None, None)
    if available <= len(packed) + len(json.dumps(index).encode()):
        return {"status": "HOLD_INSUFFICIENT_PILOT_CAPACITY"}
    name = recovery_location(access, prefix="R11_REMEDIATION")
    parent = ensure(RECOVERY_ROOT, name, FOLDER, access, create=True)
    if read_json(f"{BASE}/files/{parent}?fields=shared", access).get("shared") is not False:
        return {"status": "HOLD_PRIVATE_DRIVE_SCOPE"}
    evidence = {"limit_bytes": limit, "usage_bytes": usage, "available_bytes": available,
                "sample_compressed_bytes": len(packed), "full_volume_projection": "HOLD_FULL_MANIFEST_REQUIRED"}
    evidence_bytes = json.dumps(evidence, sort_keys=True).encode()
    evidence_id = ensure(parent, "drive-capacity.json", "application/json", access, evidence_bytes, create=True)
    if read_private_file(evidence_id, access) != evidence_bytes:
        raise ValueError("capacity evidence failed readback")

    archive = "HOLD_NO_VERIFIED_START_ONLY_SAMPLE"
    if index:
        filename = f"FinMind_Backer__{task.dataset}__{task.data_id}__start-{task.start}__snapshot__R11.1.json.gz"
        file_id = ensure(parent, filename, "application/gzip", access, packed, create=True)
        receipt = json.dumps(index | {"file": filename, "file_id": file_id, "parameters": task.parameters(),
                                      "raw_sha256": original_sha, "gzip_sha256": gzip_sha}, sort_keys=True).encode()
        receipt_id = ensure(parent, "price-limit-month-index.json", "application/json", access, receipt, create=True)
        readback = read_private_file(file_id, access)
        if readback != packed or gzip.decompress(readback) != sample.raw or read_private_file(receipt_id, access) != receipt:
            raise ValueError("price limit pilot readback failed")
        archive = "PASS_PRIVATE_ORIGINAL_AND_INDEX_READBACK"
    return {"status": "PASS_FREE_CAPACITY_FOR_PILOT", "full_volume_projection": evidence["full_volume_projection"],
            "sample_compressed_bytes": len(packed), "private_archive": archive}


def run():
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    with ThreadPoolExecutor(max_workers=6) as pool:
        planned_official = official_benchmarks(pool)
        account = pool.submit(read_json, "https://api.web.finmindtrade.com/v2/user_info", auth)
        docs = pinned_contracts()
        info = account.result()
        limit, used = info.get("api_request_limit"), info.get("user_count")
        usable = type(limit) is int and limit >= 1600 and type(used) is int and 0 <= used <= limit - 18
        seed = request_sample(BOND_SNAPSHOT, docs[BOND_SNAPSHOT.dataset], auth) if usable else Sample("NOT_SENT")
        bond = active_bond(seed, "2026-11-02", "2026-11-06")
        fields = field_tasks(bond)
        tasks = [*fields, *PRICE_LIMIT_TASKS, *GOLD_TASKS, *MARKET_TASKS.values()]

        def read(task, contract, token):
            if task in MARKET_TASKS.values():
                return request_sample(task, contract, token, max_bytes=BULK_MAX_BYTES)
            return request_sample(task, contract, token, inspect_dates=task.dataset == "GoldPrice", max_bytes=MAX_BYTES)

        dispatch = usable and seed.status != "RATE_LIMITED"
        if dispatch:
            samples = run_parallel(tasks, docs, auth, workers=min(16, len(tasks)), reader=read)
        else:
            reason = "NOT_SENT_RATE_LIMIT" if seed.status == "RATE_LIMITED" else "NOT_SENT_ACCOUNT_UNVERIFIED"
            samples = dict.fromkeys(tasks, Sample(reason))
        official = collect_benchmarks(planned_official)

    checks = defaultdict(list)
    for task in fields:
        checks[task.dataset].append({"start": task.start, "end": task.end,
                                     "status": native_result(task, docs[task.dataset], samples[task])})
    natives = {name: {"public_tutorial": LAYOUTS[name].tutorial, "windows": checked,
                      "status": "PASS_TWO_NATIVE_SAMPLES" if len(checked) == 2 and
                      all(check["status"] == "PASS_NATIVE_SAMPLE" for check in checked) else "HOLD_INCOMPLETE_NATIVE_EVIDENCE"}
               for name, checked in checks.items()}
    comparisons = {}
    for day, task in MARKET_TASKS.items():
        result, _ = reconcile_close(samples[task], official[day])
        result["compressed_sample_bytes"] = len(compress_original(samples[task].raw)[0]) if samples[task].raw else None
        comparisons[day.isoformat()] = result
    historical, recent = (set().union(*official[day].values()) for day in DAYS)
    benchmark = DAYS[-1] if all(official[DAYS[-1]].values()) else None
    price_limit, index = suffix_index(PRICE_LIMIT_TASKS, samples, benchmark, datetime.now(ZoneInfo("Asia/Taipei")).date())
    try:
        drive = storage_and_archive(PRICE_LIMIT_TASKS[0], samples[PRICE_LIMIT_TASKS[0]], index)
    except Exception as error:
        drive = {"status": "HOLD_DRIVE_CHECK_FAILED", "error_class": type(error).__name__}
    rate_limited = any(sample.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for sample in (seed, *samples.values()))
    return {
        "prior_contract_run": 45626725236, "prior_market_run": 45624026361, "production": "HOLD",
        "account": "BACKER_QUOTA_AND_PRIVATE_DATA_OBSERVED" if usable and seed.status == "VALID_SAMPLE"
        else "HOLD_CURRENT_ENTITLEMENT_CHECK", "exact_subscription_expiry": "HOLD_NO_CONFIRMED_EXPIRY",
        "quota_matches_backer_reference": limit == 1600,
        "native_contracts": natives, "start_only_price_limit": price_limit,
        "gold": [gold_outcome(samples[task]) for task in GOLD_TASKS], "dated_market_scope": comparisons,
        "historic_codes_missing_from_recent_reference": len(historical - recent) if all(
            all(official[day].values()) for day in DAYS) else None,
        "drive": drive, "requests": 1 + usable + sum(not s.status.startswith("NOT_SENT_") for s in samples.values()),
        "rate_limited": rate_limited,
    }


def direct_close_result(task, sample, official_price):
    if sample.status != "VALID_SAMPLE":
        return sample.status
    if len(sample.rows) != 1:
        return "HOLD_DIRECT_ROW_COUNT"
    row = sample.rows[0]
    if row.get("stock_id") != task.data_id or row.get("date") != task.start:
        return "HOLD_DIRECT_IDENTITY_OR_DATE"
    return "PASS_DIRECT_CLOSE" if price_value(row.get("close")) == official_price else "HOLD_DIRECT_CLOSE_MISMATCH"


def followup():
    auth = os.environ["FINMIND_BACKER_TOKEN"]
    day = DAYS[0]
    market_task = MARKET_TASKS[day]
    docs = pinned_contracts()

    def read(task, contract, token):
        return request_sample(task, contract, token, inspect_dates=task.dataset == "GoldPrice",
                              max_bytes=BULK_MAX_BYTES if task == market_task else MAX_BYTES)

    with ThreadPoolExecutor(max_workers=2) as pool:
        listing = pool.submit(official_close, day, "TPEx")
        received = run_parallel([market_task, *GOLD_TASKS], docs, auth, reader=read)
        tpex = listing.result()
    bulk = received[market_task]
    if bulk.status != "VALID_SAMPLE":
        raise ValueError("historical bulk baseline unavailable")
    primary_symbols = {row["stock_id"] for row in bulk.rows if isinstance(row.get("stock_id"), str)}
    missing = sorted(code for code in tpex.keys() - primary_symbols if tpex[code] is not None)
    tasks = [Task(PRICE, day.isoformat(), day.isoformat(), code) for code in missing[:6]]
    throttled = any(sample.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for sample in received.values())
    direct = run_parallel(tasks, docs, auth) if not throttled else dict.fromkeys(tasks, Sample("NOT_SENT_RATE_LIMIT"))
    outcomes = Counter(direct_close_result(task, direct[task], tpex[task.data_id]) for task in tasks)
    return {
        "prior_run": 45649612210, "production": "HOLD",
        "historical_tpex": {"day": day.isoformat(), "missing_from_bulk": len(missing), "direct_outcomes": dict(outcomes),
                            "recovery": "PASS_ALL_MISSING_DIRECT" if missing and outcomes["PASS_DIRECT_CLOSE"] == len(missing)
                            else "HOLD_UNRECOVERED_HISTORICAL_COVERAGE"},
        "gold": [gold_outcome(received[task]) for task in GOLD_TASKS],
        "requests": sum(s.status != "NOT_SENT_RATE_LIMIT" for s in (*received.values(), *direct.values())),
        "rate_limited": throttled or any(s.status in {"RATE_LIMITED", "NOT_SENT_RATE_LIMIT"} for s in direct.values()),
    }


def main():
    followup_only = os.environ.get("R11_REMEDIATION_FOLLOWUP") == "true"
    try:
        report = followup() if followup_only else run()
    except Exception as error:
        report = {"production": "HOLD", "status": "INFRASTRUCTURE_ERROR", "error_class": type(error).__name__}
    OUTPUT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as summary:
            summary.write("## Backer release remediation\n\n")
            for name, outcome in report.get("native_contracts", {}).items():
                summary.write(f"- `{name}` — {outcome['status']}\n")
            for name in ("start_only_price_limit", "drive", "historical_tpex"):
                if name in report:
                    result = report[name].get("status") or report[name]["recovery"]
                    summary.write(f"- {name} — {result}\n")
            summary.write("- Production — HOLD\n")
    if followup_only:
        return int(report.get("status") == "INFRASTRUCTURE_ERROR" or report.get("rate_limited", False))
    return int(report.get("status") == "INFRASTRUCTURE_ERROR" or report.get("rate_limited", False)
               or report.get("account") != "BACKER_QUOTA_AND_PRIVATE_DATA_OBSERVED"
               or report.get("drive", {}).get("status") != "PASS_FREE_CAPACITY_FOR_PILOT")


if __name__ == "__main__":
    raise SystemExit(main())
