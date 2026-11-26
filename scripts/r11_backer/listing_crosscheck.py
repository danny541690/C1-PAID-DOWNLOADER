"""Cross-check official OTC listing identity and preserve conflicting TWSE dates and codes."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from html import unescape
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from .drive_pilot import token as drive_token
from .history_pilot import iso_date
from .market_chronology import legal_stem, market_events
from .market_lifetimes import digest, earlier_evidence, name_key, nonoverlapping
from .test_archive import json_bytes
from .tpex_calendar import SOURCE_ROOT, Source, fetch_official, monthly_rows
from .tpex_closeout import LIFETIME_RUN, market_amounts, market_table, same_prices
from .tpex_history import private_folder, save_private, verified_archive_files
from .twse_historical_codes import OTC_CLOSEOUT_RUN, PREVIOUS_RUN as CHRONOLOGY_RUN


BASE_RUN = 45733326506
INPUT_RUNS = (LIFETIME_RUN, OTC_CLOSEOUT_RUN, CHRONOLOGY_RUN, BASE_RUN)
ISSUER_LINK = "HOLD_ISSUER_NAME_LINK"
KIND = "OFFICIAL_CLOSED_INTERVAL_WITH_TPEX_TRADE"
REPORT = Path("r11-listing-crosscheck.json")
MANAGED = "(管理股票)"
NOTICE_ROOT = "https://www.tpex.org.tw/storage/eb_data/"
NOTICES = {
    "6747": ("START", "11001/11000002831.html"),
    "6457": ("START", "10406/10400146681.htm"),
    "8476": ("START", "10603/10600064341.htm"),
    "3426": ("START", "10512/10500357571.htm"),
    "4762": ("START", "10412/10400359681.htm"),
    "6594": ("END", "11203/11200007241.html"),
    "8420": ("END", "11310/11300095221.html"),
}
HUA_SHIN_REPORT = "sinopac-hua-shin-2000-audited.pdf"
BANK_SOURCES = {
    HUA_SHIN_REPORT: "https://bank.sinopac.com/upload/sinopac/spbtBookMark/15ea8c94188000008bf2.pdf",
}


def notice_member(code):
    return f"tpex-notice-{code}.html"


def current_ledger(inputs, as_of):
    lifetimes = inputs[0]
    base, otc, chain, report = [json.loads(files["reconciliation.json"]) for files in inputs]
    accepted = (base["listing_intervals"] + otc["additional_closed_intervals"]
                + chain["additional_yearbook_intervals"] + chain["code_assignment_intervals"]
                + report["new_verified_intervals"])
    _, overlaps = nonoverlapping(accepted)
    if overlaps:
        raise ValueError("the accepted baseline already contains interval overlaps")
    pending = {row["end_source"]: row for row in report["pending_listing_ledger"]}
    if len(pending) != report["historic_intervals_open_after"]:
        raise ValueError("the pending market ledger did not reconcile")
    starts, ends = market_events(lifetimes, base, as_of)
    probes = {}
    shortlist = [row for row in pending.values() if row["market"] == "TPEx" and row["disposition"] == ISSUER_LINK]
    for row in sorted(shortlist, key=lambda item: max(event["date"] for event in item["existing_starts"]), reverse=True):
        key = row["market"], row["code"]
        endings = ends[key]
        end, = [event for event in endings if event.source == row["end_source"]]
        if (end.issuer, end.day.isoformat()) != (row["issuer"], row["end_exclusive"]):
            raise ValueError("pending terminal event changed since the latest verified archive")
        lower = max((event.day for event in endings if event.day < end.day), default=date.min)
        choices = [event for event in starts[key] if lower <= event.day < end.day]
        if len(choices) == 1:
            probes[end.source] = choices[0], end
    return accepted, pending, probes, report


def month_source(event):
    return Source(event.day.replace(day=1), event.code)


def compact_html(raw):
    try:
        html = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        html = raw.decode("cp950")
    return name_key(unescape(re.sub(r"<[^>]+>", " ", html)))


def notice_matches(raw, start, end, event_kind):
    try:
        content = compact_html(raw)
    except UnicodeDecodeError:
        return False
    issuer = re.escape(name_key(end.issuer))
    day = start.day if event_kind == "START" else end.day
    effective = rf"(?<![0-9]){day.year - 1911}年0?{day.month}月0?{day.day}日"
    code = rf"(?:股票)?代[號碼]:?{start.code}(?![0-9])"
    alias = re.escape(name_key(start.issuer))
    if event_kind == "START":
        return (bool(re.search(rf"{issuer}(?:\([^。]{{1,120}}\))?初次申請上櫃普通股股票", content))
                and bool(re.search(rf"{code}[。.,]*(?:\(二\))?簡稱:{alias}(?=[)。,;]|$)", content))
                and bool(re.search(rf"(?:(?:開始櫃檯買賣日期|上櫃股票開始買賣日期):(?:民國)?{effective}|"
                                   rf"{effective}起開始以一般類股為櫃檯買賣)", content)))
    return (bool(re.search(rf"{issuer}(?:上櫃(?:之)?有價證券|普通股股票)?"
                           rf"\({code},(?:以下簡稱|下稱){alias}(?:公司)?\)", content))
            and bool(re.search(rf"{effective}起終止櫃檯買賣", content)))


def trade_rows(raw, source):
    if not raw:
        return "HOLD_MONTHLY_SOURCE_UNAVAILABLE", {}, []
    try:
        doc = json.loads(raw)
        if doc.get("stat") == "no data" and not doc.get("tables"):
            return "HOLD_MONTHLY_SOURCE_EMPTY", {}, []
        rows = monthly_rows(raw, source)
        if doc["tables"][0]["title"] != "個股日成交資訊" or not any(
            "不含上櫃股票鉅額交易" in note for note in doc["tables"][0]["notes"]
        ):
            raise ValueError("unexpected TPEx stock quote scope")
    except (ValueError, TypeError, KeyError, IndexError):
        return "HOLD_MONTHLY_SOURCE_CONTRACT", {}, []
    return "VALID", doc["tables"][0], [(day, row) for day, row in sorted(rows.items())
                                        if row["traded"] and row["positive_price"]]


def managed_trade(raw, quotes, table, start, end):
    # A managed-stock effective date can precede the next open session.
    selected = next(((day, proof) for day, proof in quotes if start.day <= day < end.day), None)
    if not selected:
        return {"decision": "HOLD_MANAGED_NO_TRADE_IN_EFFECTIVE_MONTH"}
    day, proof = selected
    if raw is None:
        return {"decision": "HOLD_MANAGED_OFFICIAL_CLASSIFICATION", "first_quote_date_in_probe": str(day)}
    try:
        index, quote, checksum = market_table(raw, "管理股票", day)[start.code]
        pos = int(proof["source"].rsplit("#", 1)[1])
        monthly = dict(zip(table["fields"], table["data"][pos - 1], strict=True))
        if not same_prices(quote, monthly) or market_amounts(quote)[2] <= 0:
            raise ValueError("market category and monthly stock trade do not agree")
    except (ValueError, TypeError, KeyError, IndexError):
        return {"decision": "HOLD_MANAGED_OFFICIAL_CLASSIFICATION", "first_quote_date_in_probe": str(day)}
    return {"decision": "HOLD_MANAGED_ORIGINAL_LISTING_START",
            "first_managed_trade_verified_in_probe": str(day), "effective": str(start.day),
            "proof": [start.source, proof["source"], f"{managed_member(day)}#{index}"],
            "daily_quote_row_sha256": checksum}


def managed_member(day):
    return f"tpex-managed-day-{day}.json"


def managed_origins(probes, responses):
    origins = {}
    for start, end in probes.values():
        if not name_key(start.issuer).endswith(MANAGED):
            continue
        source = month_source(start)
        _, _, quotes = trade_rows(responses.get(source.name), source)
        day = next((day for day, _ in quotes if start.day <= day < end.day), None)
        if day:
            query = urlencode({"date": day.strftime("%Y/%m/%d"), "response": "json"})
            origins[managed_member(day)] = f"{SOURCE_ROOT}/dailyQuotes?{query}"
    return origins


def otc_decision(start, end, responses):
    source = month_source(start)
    status, table, quotes = trade_rows(responses.get(source.name), source)
    if status != "VALID":
        return {"decision": status}, None
    if name_key(start.issuer).endswith(MANAGED):
        day = next((day for day, _ in quotes if start.day <= day < end.day), None)
        return managed_trade(responses.get(managed_member(day)), quotes, table, start, end), None
    if not quotes or quotes[0][0] != start.day:
        return {"decision": "HOLD_TRADE_START_DISAGREEMENT",
                "first_quote_date_in_probe": str(quotes[0][0]) if quotes else None}, None
    proof = [start.source, end.source, quotes[0][1]["source"]]
    if legal_stem(start.issuer) == legal_stem(end.issuer):
        route = "EXACT_ISSUER_AFTER_LEGAL_SUFFIX"
    else:
        notice = notice_member(start.code)
        if notice not in responses or not notice_matches(responses[notice], start, end, NOTICES[start.code][0]):
            return {"decision": ISSUER_LINK, "trade_proof": proof[-1]}, None
        route = "DATED_EXCHANGE_NOTICE"
        proof.append(notice)
    return {"decision": "PASS", "identity_proof": route}, {
        "code": end.code, "market": end.market, "issuer": end.issuer, "start": str(start.day),
        "end_exclusive": str(end.day), "kind": KIND, "proof": proof, "exiting_source": end.source,
        "identity_proof": route,
    }


def bank_conflicts(latest, payloads):
    records = {}
    for row in latest["pending_listing_ledger"]:
        if row["market"] != "TWSE" or row["code"] not in {"2837", "2839", "2843"}:
            continue
        reviewed = latest["candidate_decisions"][row["end_source"]]
        records[row["code"]] = reviewed | {
            "end_source": row["end_source"], "end_exclusive": row["end_exclusive"],
            "chronicle_source": row["start_source"],
        }
    if set(records) != {"2837", "2839", "2843"}:
        raise ValueError("three bank collision cases were not preserved from the baseline")
    raw = payloads.get(HUA_SHIN_REPORT)
    if raw and raw.startswith(b"%PDF-"):
        extracted = subprocess.run(["pdftotext", "-enc", "UTF-8", "-", "-"], input=raw, capture_output=True,
                                   check=False, timeout=20)
        pages = [name_key(page) for page in extracted.stdout.decode().split("\f")] if extracted.returncode == 0 else []
        intro = next((page for page in pages if "本銀行股票自八十五年一月十八日起" in page
                      and "八十七年六月二十九日起轉於台灣證券交易所掛牌交易" in page), None)
        if pages and intro and all(value in pages[0] for value in (
            "股票代碼:2839", "華信商業銀行股份有限公司", "民國八十九及八十八年度"
        )):
            records["2839"]["issuer_retrospective"] = {
                "source": HUA_SHIN_REPORT, "document_sha256": digest(raw), "cover_page": 1,
                "effective_otc_to_twse": "1998-06-29", "reporting_year": 2000,
                "history_page": pages.index(intro) + 1, "code_on_report": "2839",
                "decision": "CORROBORATES_DATE_AND_LATER_CODE_ONLY",
            }
    return records


def evaluate(inputs, responses, as_of):
    accepted, pending, probes, latest = current_ledger(inputs, as_of)
    before = len(pending)
    decisions, proposals = {}, []
    for source, (start, end) in probes.items():
        decision, interval = otc_decision(start, end, responses)
        decisions[source] = decision | {"opening_source": start.source, "start": str(start.day), "code": start.code}
        if interval:
            proposals.append(interval)
    combined, overlaps = nonoverlapping(accepted + proposals)
    for row in overlaps:
        if row["kind"] == KIND:
            decisions[row["exiting_source"]]["decision"] = "HOLD_OVERLAPPING_INTERVAL"
    additions = [row for row in combined if row["kind"] == KIND]
    for row in additions:
        del pending[row["exiting_source"]]
    if len(pending) + len(additions) != before:
        raise ValueError("pending market count did not conserve the baseline")
    banks = bank_conflicts(latest, responses)
    report = {"status": "PASS_DIAGNOSTIC", "production": "HOLD", "finmind_requests": 0, "previous_run": BASE_RUN,
              "as_of_taipei": str(as_of), "historical_intervals_before": before,
              "historical_intervals_after": len(pending), "tpex_intervals_added": len(additions),
              "tpex_alias_hold_tested": len(probes),
              "tpex_decisions": dict(Counter(row["decision"] for row in decisions.values())),
              "tpex_acceptance_proofs": dict(Counter(row["identity_proof"] for row in additions)),
              "twse_bank_conflicts_held": len(banks),
              "twse_issuer_history_corroborated": sum("issuer_retrospective" in record for record in banks.values())}
    details = {"new_verified_intervals": additions, "pending_listing_ledger": list(pending.values()),
               "tpex_candidate_decisions": decisions, "twse_bank_conflict_decisions": banks,
               "baseline_archive_sha256": {str(run): digest(files["reconciliation.json"])
                                           for run, files in zip(INPUT_RUNS, inputs, strict=True)}}
    return report, details


def fetch_origins(origins):
    names = list(origins)
    results = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        for i in range(0, len(names), 6):
            batch = names[i:i + 6]
            outcomes = list(pool.map(fetch_official, [origins[name] for name in batch]))
            results.update(zip(batch, outcomes, strict=True))
            if any(status == "HOLD_OFFICIAL_HTTP_429" for status, _ in outcomes):
                break
    return results


def load_inputs():
    with ThreadPoolExecutor(max_workers=len(INPUT_RUNS)) as pool:
        return list(pool.map(earlier_evidence, INPUT_RUNS))


def run(inputs, as_of):
    _, _, probes, _ = current_ledger(inputs, as_of)
    origins = {source.name: source.url for source in (month_source(start) for start, _ in probes.values())}
    origins.update({notice_member(code): NOTICE_ROOT + name for code, (_, name) in NOTICES.items()})
    origins.update(BANK_SOURCES)
    fetched = fetch_origins(origins)
    responses = {name: raw for name, (_, raw) in fetched.items() if raw}
    daily = managed_origins(probes, responses)
    origins.update(daily)
    if not any(status == "HOLD_OFFICIAL_HTTP_429" for status, _ in fetched.values()):
        fetched.update(fetch_origins(daily))
    responses = {name: raw for name, (_, raw) in fetched.items() if raw}
    report, details = evaluate(inputs, responses, as_of)
    report.update(private_archive="STAGED", official_fetch_outcomes=dict(Counter(status for status, _ in fetched.values())))
    save_private(fetched, origins, {}, report | details)
    return report


def verify(inputs):
    access = drive_token()
    files = verified_archive_files(access, private_folder(access, create=False))
    expected = json.loads(files["reconciliation.json"])
    responses = {name.removeprefix("official/"): raw for name, raw in files.items() if name.startswith("official/")}
    public, details = evaluate(inputs, responses, iso_date(expected["as_of_taipei"]))
    computed = public | details
    if json_bytes(computed) != json_bytes({key: expected[key] for key in computed}):
        raise ValueError("archived official sources did not reproduce the listing cross-check")
    print(f"listing_crosscheck_drive_readback=PASS files={len(files)} "
          f"added={public['tpex_intervals_added']} pending={public['historical_intervals_after']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        inputs = load_inputs()
        if args.verify_only:
            verify(inputs)
            return 0
        report = run(inputs, datetime.now(ZoneInfo("Asia/Taipei")).date())
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
        if args.verify_only:
            print(json.dumps(report, sort_keys=True))
            return 1
    REPORT.write_bytes(json_bytes(report))
    print(json.dumps(report, sort_keys=True))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"archive={report.get('private_archive', 'HOLD')}\n")
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
