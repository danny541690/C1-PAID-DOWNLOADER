"""Test point-in-time TWSE security codes against audited official annual-report pages."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from http.client import IncompleteRead
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .drive_pilot import BASE, FOLDER, ensure, token as drive_token
from .history_pilot import iso_date
from .live_preflight import read_json
from .market_chronology import PREVIOUS_RUN as OTC_CLOSEOUT_RUN
from .market_lifetimes import digest, earlier_evidence, name_key, nonoverlapping, twse_events
from .release_probes import RECOVERY_ROOT
from .test_archive import json_bytes
from .tpex_calendar import fetch_official
from .tpex_closeout import LIFETIME_RUN
from .tpex_history import private_folder, save_private, verified_archive_files


PREVIOUS_RUN = 45724784275
ANNUAL_ROOT = "https://wwwc.twse.com.tw/downloads/zh/about/company/"
REPORT = Path("r11-twse-historical-codes.json")
HISTORICAL_SYMBOL = "HOLD_HISTORICAL_SYMBOL_LINK"
DAY_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
MAX_PDF_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class Publication:
    year: int
    path: str
    page: int
    sha256: str
    section: str

    @property
    def member(self):
        return f"twse-annual-{self.year}-page-{self.page}.pdf"

    @property
    def url(self):
        return ANNUAL_ROOT + self.path


PUBLICATIONS = (
    Publication(2009, "Annual/98/P32.pdf", 1, "8e66b8fabc4bb0f557a618f564e57d4ce47e949242c885115f4826f72c9aec76",
                "LISTED_TAIWAN_DEPOSITARY_RECEIPTS"),
    Publication(2000, "annual_89.pdf", 13, "476d14261692de17d9b4fbcffb3e06d3bd76385939d0a1368017c005034a3857",
                "LISTED_COMMON_SHARES"),
    Publication(1999, "annual_88.pdf", 13, "6dc4711f56ed18fe8e082714976a5b5961d81536e6be31d6299513454efc9db6",
                "LISTED_COMMON_SHARES"),
    Publication(1998, "annual_87.pdf", 30, "492080cecf2f7ef43e56b1648ec7e0d1fb518019f52311a5245e084546324e30",
                "LISTED_COMMON_SHARES"),
    Publication(1997, "annual_86.pdf", 28, "646db29458370fc7fd22d071120073e11fefb9ff79a896209618faf4fd8d70e7",
                "NARRATIVE_WITHOUT_CODE_COLUMN_IN_SAMPLE"),
)

# Visually transcribed from the audited listing columns of the SHA-bound pages.
# The last field records the equivalent name in the dated chronology and terminal docket.
AUDITED_ROWS = {
    2009: (
        ("9188", "10-08", "精熙國際", "精熙-DR"),
        ("9106", "10-12", "新焦點", "新焦點-DR"),
        ("9157", "12-11", "陽光能", "陽光能源-DR"),
    ),
    2000: (
        ("2403", "03-30", "友尚", "友尚"),
        ("2407", "06-22", "陞技", "陞技電腦"),
    ),
    1999: (
        ("2846", "01-08", "中興商業銀行", "中興銀行"),
        ("2847", "02-04", "大眾商業銀行", "大眾銀行"),
        ("1469", "02-05", "理隆纖維", "理隆纖維"),
        ("2389", "03-04", "世昕企業", "世昕企業"),
        ("2848", "05-20", "華南產物保險", "華南產保"),
        ("2391", "08-12", "合勤科技", "合勤科技"),
        ("2396", "11-29", "精碟科技", "精碟科技"),
        ("2398", "12-18", "博達科技", "博達科技"),
    ),
    1998: (
        ("2837", "06-29", "華信商業銀行", "華信銀行"),
        ("2838", "06-29", "萬泰商業銀行", "萬泰銀行"),
        ("2839", "06-29", "聯邦商業銀行", "聯邦銀行"),
        ("2840", "08-19", "玉山商業銀行", "玉山銀行"),
        ("1523", "08-27", "臺灣開億工業", "臺灣開億"),
        ("2370", "09-07", "匯僑工業", "匯僑工業"),
        ("2842", "10-01", "富邦商業銀行", "富邦銀行"),
        ("2378", "10-14", "鴻運電子", "鴻運電子"),
        ("2843", "10-17", "亞太商業銀行", "亞太銀行"),
        ("2844", "10-20", "台新國際商業銀行", "台新銀行"),
        ("2384", "12-19", "勝華科技", "勝華科技"),
        ("2386", "12-23", "國碁電子", "國碁電子"),
    ),
}


def source_rows(publications):
    result = defaultdict(list)
    for doc in publications:
        seen = set()
        for code, month_day, printed_name, docket_name in AUDITED_ROWS.get(doc.year, ()):
            if code in seen:
                raise ValueError("duplicate code in audited annual listing rows")
            seen.add(code)
            day = iso_date(f"{doc.year}-{month_day}")
            result[doc.year].append({"code": code, "date": day.isoformat(), "printed_issuer": printed_name,
                                     "docket_issuer": docket_name, "section": doc.section, "source": doc.member,
                                     "publication_sha256": doc.sha256})
    return dict(result)


def fetch_publication(doc):
    request = Request(doc.url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/pdf"})
    try:
        with urlopen(request, timeout=60) as response:
            original = response.read(MAX_PDF_BYTES + 1)
    except HTTPError as error:
        return f"HOLD_OFFICIAL_HTTP_{error.code}", b"", {}
    except (IncompleteRead, URLError, TimeoutError):
        return "HOLD_OFFICIAL_NETWORK", b"", {}
    sha = digest(original)
    evidence = {"url": doc.url, "full_document_bytes": len(original), "full_document_sha256": sha,
                "original_pdf_page": doc.page, "section": doc.section}
    if len(original) > MAX_PDF_BYTES or not original.startswith(b"%PDF-"):
        return "HOLD_OFFICIAL_FORMAT", b"", evidence
    if sha != doc.sha256:
        return "HOLD_OFFICIAL_EDITION_CHANGED", b"", evidence
    if doc.year == 2009:
        return "VERIFIED_OFFICIAL_PAGE", original, evidence
    with TemporaryDirectory() as directory:
        base = Path(directory)
        original_file = base / "source.pdf"
        original_file.write_bytes(original)
        subprocess.run(["pdfseparate", "-f", str(doc.page), "-l", str(doc.page), str(original_file),
                        str(base / "page-%d.pdf")], capture_output=True, check=True, timeout=30)
        evidence_bytes = (base / f"page-{doc.page}.pdf").read_bytes()
    return "VERIFIED_OFFICIAL_PAGE", evidence_bytes, evidence


def reviewed_case(case, rows):
    day = case["dated_name_only_start"]
    name = name_key(case["issuer"])
    identified = [row for row in rows if name_key(row["docket_issuer"]) == name]
    code_bound = [row for row in identified if row["code"] == case["code"]]
    matched = [row for row in code_bound if row["date"] == day]
    if len(matched) == 1 and len(code_bound) == 1:
        return "PASS", matched
    if code_bound:
        return "HOLD_OFFICIAL_START_DATE_CONFLICT", code_bound
    if identified:
        return "HOLD_HISTORICAL_CODE_CONFLICT", identified
    return HISTORICAL_SYMBOL, []


def exact_end_events(lifetimes, as_of):
    return {row.source: row for row in twse_events(lifetimes["official/twse-ended.csv"], as_of, start=False)}


def reconcile(prior, accepted, endings, pages, publication_metadata):
    known = [doc for doc in PUBLICATIONS if pages.get(doc.member)]
    for doc in known:
        meta = publication_metadata[doc.member]
        if meta["full_document_sha256"] != doc.sha256 or meta["original_pdf_page"] != doc.page:
            raise ValueError("archived page is not tied to the audited publication edition")
    rows = source_rows(known)
    pending, proposals, decisions = {}, [], {}
    for case in prior["pending_listing_ledger"]:
        source = case["end_source"]
        if source in pending:
            raise ValueError("duplicate terminal reference in the pending market ledger")
        pending[source] = case
        if case["disposition"] != HISTORICAL_SYMBOL:
            continue
        end = endings.get(source)
        if end is None or (end.code, end.issuer, str(end.day)) != (case["code"], case["issuer"], case["end_exclusive"]):
            raise ValueError("pending reference no longer matches the official terminal docket")
        day = iso_date(case["dated_name_only_start"])
        verdict, evidence = reviewed_case(case, rows.get(day.year, []))
        decisions[source] = {"code": case["code"], "issuer": case["issuer"], "date": str(day),
                             "disposition": verdict, "annual_rows": evidence}
        if verdict != "PASS":
            pending[source] = case | {"disposition": verdict}
            continue
        row = evidence[0]
        if day >= end.day:
            raise ValueError("proposed start does not precede termination")
        proposals.append({"market": "TWSE", "code": end.code, "issuer": end.issuer, "start": str(day),
                          "end_exclusive": str(end.day), "kind": "OFFICIAL_CLOSED_INTERVAL_WITH_DATED_CODE",
                          "security_type": row["section"], "proof": [row["source"], case["start_source"], source],
                          "exiting_source": source, "printed_issuer": row["printed_issuer"]})
    combined, collisions = nonoverlapping(accepted + proposals)
    for row in collisions:
        if row["kind"] == "OFFICIAL_CLOSED_INTERVAL_WITH_DATED_CODE":
            key = row["exiting_source"]
            pending[key] = pending[key] | {"disposition": "HOLD_OVERLAPPING_INTERVAL"}
            decisions[key]["disposition"] = "HOLD_OVERLAPPING_INTERVAL"
    additions = [row for row in combined if row["kind"] == "OFFICIAL_CLOSED_INTERVAL_WITH_DATED_CODE"]
    for row in additions:
        del pending[row["exiting_source"]]
    if len(pending) + len(additions) != prior["historic_intervals_open_after"]:
        raise ValueError("pending interval total was not conserved")
    report = {"historical_code_candidates": len(decisions), "historical_code_acceptances": len(additions),
              "historical_code_outcomes": dict(Counter(x["disposition"] for x in decisions.values())),
              "accepted_by_security_type": dict(Counter(row["security_type"] for row in additions)),
              "verified_publication_years": sorted(doc.year for doc in known),
              "historic_intervals_open_before": prior["historic_intervals_open_after"], "historic_intervals_open_after": len(pending)}
    details = {"new_verified_intervals": additions, "pending_listing_ledger": list(pending.values()),
               "candidate_decisions": decisions, "audited_listing_rows": rows}
    return report, details


def daily_probe_urls(prior):
    targets = [row for row in prior["pending_listing_ledger"] if row["disposition"] == HISTORICAL_SYMBOL
               and row["dated_name_only_start"] >= "1999-01-01"]
    result = {}
    for row in sorted(targets, key=lambda x: (x["dated_name_only_start"], x["code"]), reverse=True):
        day, code = row["dated_name_only_start"], row["code"]
        query = urlencode({"date": day.replace("-", ""), "stockNo": code, "response": "json"})
        result[f"twse-day-{code}-{day}.json"] = f"{DAY_URL}?{query}"
    return result


def daily_probe_results(responses, urls):
    results = {}
    for name in urls:
        raw = responses.get(name)
        if not raw:
            results[name] = "HOLD_SOURCE_UNAVAILABLE"
            continue
        try:
            payload = json.loads(raw)
        except ValueError:
            results[name] = "HOLD_OFFICIAL_CONTRACT"
            continue
        message = payload.get("stat", "")
        if isinstance(message, str) and "查詢日期小於99年1月4日" in message and not payload.get("data"):
            results[name] = "HOLD_OFFICIAL_DATE_FLOOR_2010_01_04"
        else:
            results[name] = "REVIEW_OFFICIAL_RESPONSE"
    return results


def input_archives():
    return earlier_evidence(LIFETIME_RUN), earlier_evidence(OTC_CLOSEOUT_RUN), earlier_evidence(PREVIOUS_RUN)


def evaluate(inputs, responses, metadata, as_of):
    lifetimes, closeout, chronology = inputs
    prior = json.loads(chronology["reconciliation.json"])
    accepted = (json.loads(lifetimes["reconciliation.json"])["listing_intervals"]
                + json.loads(closeout["reconciliation.json"])["additional_closed_intervals"]
                + prior["additional_yearbook_intervals"] + prior["code_assignment_intervals"])
    report, details = reconcile(prior, accepted, exact_end_events(lifetimes, as_of), responses, metadata)
    probes = daily_probe_results(responses, daily_probe_urls(prior))
    report.update(status="PASS_DIAGNOSTIC", production="HOLD", finmind_requests=0, previous_run=PREVIOUS_RUN,
                  as_of_taipei=as_of.isoformat(), daily_probe_outcomes=dict(Counter(probes.values())))
    details.update(publication_provenance=metadata, daily_probe_details=probes, input_archive_hashes={
        str(run): digest(archive["reconciliation.json"])
        for run, archive in zip((LIFETIME_RUN, OTC_CLOSEOUT_RUN, PREVIOUS_RUN), inputs, strict=True)
    })
    return report, details


def download_responses(probe_urls):
    harvested, metadata = {}, {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        for doc, (status, raw, proof) in zip(PUBLICATIONS, pool.map(fetch_publication, PUBLICATIONS), strict=True):
            harvested[doc.member] = status, raw
            metadata[doc.member] = proof | {"status": status, "page_sha256": digest(raw) if raw else None}
    with ThreadPoolExecutor(max_workers=4) as pool:
        entries = list(probe_urls)
        for offset in range(0, len(entries), 4):
            batch = entries[offset:offset + 4]
            values = list(pool.map(fetch_official, [probe_urls[name] for name in batch]))
            harvested.update(zip(batch, values, strict=True))
            if any(status == "HOLD_OFFICIAL_HTTP_429" for status, _ in values):
                break
    return harvested, metadata


def archived_responses(files):
    ledger = json.loads(files["reconciliation.json"])
    responses = {name.removeprefix("official/"): body for name, body in files.items() if name.startswith("official/")}
    metadata = ledger["publication_provenance"]
    for name, proof in metadata.items():
        raw = responses.get(name)
        if proof["page_sha256"] != (digest(raw) if raw else None):
            raise ValueError("official publication page digest did not match the captured proof")
    return responses, metadata


def cached_input(run_id):
    access = drive_token()
    parent = ensure(RECOVERY_ROOT, f"R11_TWSE_CODE_INPUT_{run_id}_1", FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{parent}?fields=shared", access).get("shared") is not False:
        raise ValueError("historical code input folder is shared")
    return verified_archive_files(access, parent)


def run(resume_from):
    inputs = input_archives()
    prior = json.loads(inputs[-1]["reconciliation.json"])
    probe_urls = daily_probe_urls(prior)
    urls = {doc.member: doc.url for doc in PUBLICATIONS} | probe_urls
    if resume_from is not None:
        staged = cached_input(resume_from)
        responses, metadata = archived_responses(staged)
        if responses.keys() != urls.keys():
            raise ValueError("staged source packet does not match the current publication and probe plan")
        harvested = {}
        outcomes = {"REUSED_PRIVATE_DRIVE": len(responses)}
    else:
        harvested, metadata = download_responses(probe_urls)
        responses = {name: raw for name, (_, raw) in harvested.items()}
        outcomes = dict(Counter(status for status, _ in harvested.values()))
    as_of = datetime.now(ZoneInfo("Asia/Taipei")).date()
    report, details = evaluate(inputs, responses, metadata, as_of)
    report.update(private_archive="STAGED", official_fetch_outcomes=outcomes)
    if resume_from is not None:
        details.update(cached_input_run=resume_from, cached_input_ledger_sha256=digest(staged["reconciliation.json"]))
    save_private(harvested, urls, {}, report | details)
    return report


def verify():
    inputs = input_archives()
    access = drive_token()
    files = verified_archive_files(access, private_folder(access, create=False))
    expected = json.loads(files["reconciliation.json"])
    sources = files
    if cached_run := expected.get("cached_input_run"):
        sources = cached_input(cached_run)
        if digest(sources["reconciliation.json"]) != expected["cached_input_ledger_sha256"]:
            raise ValueError("historical code input provenance changed after execution")
    responses, metadata = archived_responses(sources)
    public, details = evaluate(inputs, responses, metadata, iso_date(expected["as_of_taipei"]))
    computed = public | details
    if json_bytes(computed) != json_bytes({key: expected[key] for key in computed}):
        raise ValueError("Drive sources did not reproduce the historical code decisions")
    print(f"twse_historical_codes_drive_readback=PASS files={len(files)} "
          f"acceptances={public['historical_code_acceptances']} pending={public['historic_intervals_open_after']}")


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--verify-only", action="store_true")
    mode.add_argument("--resume-from", type=int)
    args = parser.parse_args()
    try:
        if args.verify_only:
            verify()
            return 0
        report = run(args.resume_from)
    except Exception as error:
        report = {"status": "HOLD_INFRASTRUCTURE", "failure_class": type(error).__name__, "production": "HOLD"}
        if args.verify_only:
            print(json.dumps(report, sort_keys=True))
            return 1
    REPORT.write_bytes(json_bytes(report))
    print(json.dumps(report, sort_keys=True))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as stream:
            stream.write(f"archive={report.get('private_archive', 'HOLD')}\n")
    return int(report["status"] != "PASS_DIAGNOSTIC")


if __name__ == "__main__":
    raise SystemExit(main())
