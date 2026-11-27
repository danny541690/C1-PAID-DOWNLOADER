"""Run one bounded Backer sampling batch and archive the exact responses privately."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
from string import ascii_uppercase
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

from .archive import compress_original
from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .history_pilot import BUSINESS
from .live_preflight import drive_health, read_json
from .market_lifetimes import digest
from .native_contracts import LAYOUTS, native_result
from .release_probes import (
    BACKER_COMPATIBLE, RECOVERY_ROOT, SNAPSHOTS, SOURCE, Sample, Task, contracts, observation_date,
    primary_task, recent_trading_days, request_sample, run_parallel,
)
from .release_readiness import ACCOUNT, PRIMARY_KEYS, public_capture, scope_register, specs
from .test_archive import json_bytes
from .tpex_history import private_folder, save_private, verified_archive_files


PREFIX = "R11_FINAL_SOURCES"
REPORT = Path("r11-final-source-probes.json")
PRIOR = 45754253067
ANCHORS = {
    "TaiwanStockPrice": "1994-09-13",
    "TaiwanStockPriceAdj": "1994-09-14",
    "TaiwanStockFinancialStatements": "1991-12-31",
    "TaiwanStockMonthRevenue": "2002-01-01",
    "TaiwanStockInstitutionalInvestorsBuySell": "2004-10-11",
    "TaiwanStockHoldingSharesPer": "2019-01-01",
    "TaiwanStockCapitalReductionReferencePrice": "1969-12-01",
    "TaiwanStockMarginPurchaseShortSale": "2001-01-02",
    BUSINESS: "1982-01-01",
    "InterestRate": "1982-09-24",
}
QUARTERLY = {"TaiwanStockFinancialStatements", "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement"}
MAX_SAMPLE_BYTES = 768 * 1024
MAX_PACKET_BYTES = 2 * 1024 * 1024
DATE_SEED = Task("TaiwanStockTradingDate", None, None)
BOND_SEED = Task("TaiwanStockConvertibleBondInfo", None, None)


def previous_capture(access):
    root = ensure(RECOVERY_ROOT, f"R11_RELEASE_READINESS_{PRIOR}_1", FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{root}?fields=shared", access).get("shared") is not False:
        raise ValueError("earlier Backer evidence is shared")
    files = verified_archive_files(access, root)
    prior = json.loads(files["reconciliation.json"])
    return files, prior


def request_key(task):
    return json.dumps(task.parameters(), sort_keys=True)


def previous_samples(files, prior):
    cached = {}
    for entry in prior["backer_requests"]:
        if entry["status"] not in {"VALID_SAMPLE", "EMPTY_WINDOW"}:
            continue
        raw = files[entry["original"]] if entry["original"] else b""
        cached[json.dumps(entry["request"], sort_keys=True)] = (
            Sample(entry["status"], raw=raw, rows=tuple(json.loads(raw)["data"]) if raw else ()),
            {"run_id": PRIOR, "original": entry["original"], "raw_sha256": digest(raw) if raw else None},
        )
    return cached


def applicable_bond(snapshot, start, end):
    for row in snapshot.rows:
        identifier, first, last = row.get("cb_id"), row.get("InitialDateOfConversion"), row.get("DueDateOfConversion")
        if (not isinstance(identifier, str) or not identifier.isdecimal()
                or not isinstance(first, str) or not isinstance(last, str)):
            continue
        if first <= (end - timedelta(days=3)).isoformat() and last >= (start + timedelta(days=3)).isoformat():
            return identifier
    return None


def quarter_end(day):
    first = day.replace(month=(day.month - 1) // 3 * 3 + 1, day=1)
    return first - timedelta(days=1)


def sampling_windows(name, era, latest, as_of):
    if name in QUARTERLY:
        if era == "early":
            chosen = date.fromisoformat(ANCHORS.get(name, "2005-03-31"))
        elif era == "middle":
            chosen = date(2013, 3, 31)
        else:
            chosen = quarter_end(as_of - timedelta(days=28))
        return chosen, chosen
    if era == "recent":
        return latest - timedelta(days=29), latest
    if name == "TaiwanStockCapitalReductionReferencePrice":
        return (date(1969, 12, 1), date(1970, 2, 1)) if era == "early" else (date(2013, 10, 1), date(2013, 10, 31))
    if name == "InterestRate" and era == "early":
        return date(1982, 9, 24), date(1982, 10, 3)
    if name == "TaiwanStockHoldingSharesPer" and era == "early":
        return date(2019, 1, 1), date(2019, 1, 31)
    if name == "TaiwanStockHoldingSharesPer" and era == "middle":
        return date(2022, 6, 1), date(2022, 6, 30)
    anchor = date.fromisoformat(ANCHORS.get(name, "2005-01-03")) if era == "early" else date(2015, 6, 1)
    if name == "TaiwanStockMarginPurchaseShortSale" and era == "early":
        return anchor, anchor + timedelta(days=15)
    return anchor, anchor + timedelta(days=30)


def build_plan(roster, registry, calendar, bonds, as_of):
    days = recent_trading_days(calendar, as_of)
    calendar_verified = bool(days)
    # No dated samples are dispatched without a returned calendar.
    if not days:
        days = [as_of.isoformat()]
    latest = date.fromisoformat(days[-1])
    plan = []
    for row in roster:
        name = row["dataset"]
        if row["state"] != "SAMPLE_ONLY":
            continue
        contract = registry.get(name)
        for era in (("snapshot",) if name in SNAPSHOTS else ("early", "middle", "recent")):
            window = None
            if contract is None or contract.tier not in BACKER_COMPATIBLE:
                reason = "HOLD_DOCUMENTED_AUTHORIZATION"
            elif name in SNAPSHOTS:
                window = Task(name, None, None)
                reason = None
            elif not calendar_verified:
                reason = "HOLD_TRADING_CALENDAR"
            else:
                lo, hi = sampling_windows(name, era, latest, as_of)
                bond = applicable_bond(bonds, lo, hi)
                template = primary_task(name, contract, days, as_of, bond)
                if isinstance(template, str):
                    reason = template
                elif not template.send_end and era != "recent" and "single day" not in contract.required:
                    reason = "HOLD_UNBOUNDED_START_ONLY_QUERY"
                else:
                    reason = None
                    if "single day" in contract.required or name in {
                        "TaiwanOptionDaily", "TaiwanOptionDealerTradingVolumeDaily",
                        "TaiwanAssetSwapFixedIncomeDaily", "TaiwanAssetSwapOptionDaily",
                    }:
                        lo = hi if era == "recent" else lo
                        hi = lo
                    if not template.send_end and era == "recent" and "single day" not in contract.required:
                        lo, hi = latest, as_of
                    window = Task(name, str(lo), str(hi), template.data_id, send_end=template.send_end)
            plan.append((row["routing"], name, era, window, reason))
    return plan


def measure(task, sample, contract):
    receipt = {"provider_result": sample.status, "result": sample.status}
    if not sample.raw:
        return receipt
    receipt.update(raw_sha256=digest(sample.raw), raw_bytes=len(sample.raw))
    packed, _, gzip_sha = compress_original(sample.raw)
    receipt.update(gzip_bytes=len(packed), gzip_sha256=gzip_sha, rows=len(sample.rows),
                   field_sets=sorted({",".join(sorted(row)) for row in sample.rows}))
    shape = Counter()
    for row in sample.rows:
        for name, value in row.items():
            shape[name, type(value).__name__] += 1
    receipt["field_type_counts"] = [[name, kind, count] for (name, kind), count in sorted(shape.items())]
    days = []
    for row in sample.rows:
        try:
            days.append(observation_date(task.dataset, row.get("date")))
        except (TypeError, ValueError):
            continue
    receipt["date_values_unverified"] = len(sample.rows) - len(days)
    if days:
        receipt["observed_dates"] = [str(min(days)), str(max(days))]
    if task.dataset in LAYOUTS and task.data_id and task.start:
        receipt["result"] = native_result(task, contract, sample)
    if task.dataset in PRIMARY_KEYS:
        fields = PRIMARY_KEYS[task.dataset]
        keys = [tuple(row[field] for field in fields) for row in sample.rows
                if all(type(row.get(field)) in {int, str} and row[field] != "" for field in fields)]
        receipt["unverified_keys"] = len(sample.rows) - len(keys)
        receipt["duplicate_keys"] = len(keys) - len(set(keys))
    return receipt


def reader(task, contract, auth):
    if task in (DATE_SEED, BOND_SEED):
        return request_sample(task, contract, auth, retain_empty=True)
    return request_sample(task, contract, auth, max_bytes=MAX_SAMPLE_BYTES, retain_empty=True)


def split_window(task):
    if not task.start or not task.send_end or task.start >= task.end:
        return ()
    first, last = map(date.fromisoformat, (task.start, task.end))
    middle = first + (last - first) // 2
    return (Task(task.dataset, str(middle + timedelta(days=1)), task.end, task.data_id),
            Task(task.dataset, task.start, str(middle), task.data_id))


def pack_samples(samples):
    packet, size = {}, 0
    for task, sample in sorted(samples.items(), key=lambda item: request_key(item[0])):
        if not sample.raw:
            continue
        if packet and size + len(sample.raw) > MAX_PACKET_BYTES:
            yield packet
            packet, size = {}, 0
        packet[task] = sample
        size += len(sample.raw)
    if packet:
        yield packet


def write_packets(samples):
    packets = list(pack_samples(samples))
    if len(packets) > len(ascii_uppercase):
        raise ValueError("Backer packet count exceeds the bounded pilot")

    def write(paired):
        number, packet = paired
        name = f"{PREFIX}_{ascii_uppercase[number]}"
        save_private({}, {}, packet, {"status": "STAGED", "archive_prefix": name}, prefix=name)
        return name

    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(write, enumerate(packets)))


def sample_batch(roster, registry, cached, as_of, auth):
    try:
        account = read_json(ACCOUNT, auth)
    except (ValueError, OSError) as error:
        account = {}
        account_state = "HOLD_ACCOUNT_RATE_LIMITED" if isinstance(error, HTTPError) and error.code == 429 else "HOLD_ACCOUNT"
    else:
        account_state = "HOLD_ACCOUNT"
    limit, used = (account.get("api_request_limit"), account.get("user_count")) if isinstance(account, dict) else (None, None)
    usable = type(limit) is int and limit == 1600 and type(used) is int and 0 <= used <= limit - 5
    sent = {}
    if usable:
        account_state = "BACKER_REFERENCE_CONFIRMED"
        sent = run_parallel((DATE_SEED, BOND_SEED), registry, auth, reader=reader)
    ready = not any(s.status == "RATE_LIMITED" for s in sent.values())
    plan = build_plan(roster, registry, sent.get(DATE_SEED, Sample("NOT_SENT")),
                      sent.get(BOND_SEED, Sample("NOT_SENT")), as_of)
    pending = list(dict.fromkeys(task for _, _, _, task, _ in plan
                                 if task is not None and task not in sent and request_key(task) not in cached))
    sufficient = usable and ready and limit - used >= len(pending) + len(sent) + 1
    split = {}
    if sufficient:
        sent.update(run_parallel(pending, registry, auth, reader=reader))
        if not any(s.status == "RATE_LIMITED" for s in sent.values()):
            split = {task: children for task, sample in sent.items()
                     if sample.status in {"SIZE_LIMIT", "HTTP_TRANSIENT"} and (children := split_window(task))}
            followups = list(dict.fromkeys(child for children in split.values() for child in children if child not in sent))
            if len(followups) <= limit - used - len(sent) - 1:
                sent.update(run_parallel(followups, registry, auth, reader=reader))
    results = []
    for lane, dataset, era, task, reason in plan:
        row = {"lane": lane, "dataset": dataset, "sample_period": era}
        if task is None:
            results.append(row | {"result": reason})
        elif item := cached.get(request_key(task)):
            sample, source = item
            results.append(row | {"request": task.parameters(), "reused_evidence": source} | measure(task, sample, registry[dataset]))
        elif task in sent:
            receipt = row | {"request": task.parameters()} | measure(task, sent[task], registry[dataset])
            if task in split:
                receipt["shards"] = [{"request": child.parameters()} | measure(child, sent[child], registry[dataset])
                                     for child in split[task] if child in sent]
            results.append(receipt)
        else:
            results.append(row | {"request": task.parameters(), "result": "HOLD_NO_ACCOUNT_HEADROOM_OR_COOLDOWN"})
    account_result = {"status": account_state, "reference_limit": limit, "remaining_for_planned_samples": sufficient,
                      "exact_expiry": "HOLD_NO_EXACT_DATE",
                      "requests": 1 + sum(not s.status.startswith("NOT_SENT_") for s in sent.values())}
    return results, sent, account_result


def main():
    try:
        access = drive_token()
        drive = drive_health(access)
        files, prior = previous_capture(access)
        public_doc = public_capture(SOURCE, 1_000_000)
        documents = specs(access)
        text = public_doc.decode("utf-8")
        roster = scope_register(documents, text)
        as_of = datetime.now(ZoneInfo("Asia/Taipei")).date()
        rows, samples, account = sample_batch(roster, contracts(text), previous_samples(files, prior), as_of,
                                              os.environ["FINMIND_BACKER_TOKEN"])
        packets = write_packets(samples)
        public = {
            "status": "PASS_DIAGNOSTIC", "production": "HOLD", "as_of_taipei": str(as_of),
            "candidates": dict(Counter(row["state"] for row in roster)),
            "sample_decisions": len(rows), "datasets_test_planned": len({row["dataset"] for row in rows}),
            "results": dict(Counter(row["result"] for row in rows)), "finmind_requests": account["requests"],
            "reused_probes": sum("reused_evidence" in row for row in rows), "private_packets": len(packets),
        }
        private = {"rows": rows, "prior_result_sha": digest(files["reconciliation.json"]), "account": account,
                   "drive": drive, "packets": packets, "spec_sha256": {key: digest(doc) for key, doc in documents.items()}}
        save_private({"finmind-datasets.md": ("VALID", public_doc)}, {"finmind-datasets.md": SOURCE}, {},
                     public | private, prefix=PREFIX)
    except Exception as error:
        public = {"status": "HOLD_INFRASTRUCTURE", "production": "HOLD", "failure_class": type(error).__name__}
    REPORT.write_bytes(json_bytes(public))
    print(json.dumps(public, sort_keys=True))
    return int(public["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
