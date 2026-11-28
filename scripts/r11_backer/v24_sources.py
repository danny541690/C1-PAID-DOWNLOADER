"""Ground eligible native queries in locked FinMind documentation and witnessed responses."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from html import unescape
from io import BytesIO
import json
import re
from urllib.request import Request, urlopen
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from .bulk_manifest import ALLOWED_GRAINS, ENDPOINT, MODES, adapt_rule
from .drive_retry import retry_read
from .integration_pilot import digest, encode
from .reconcile_catalog import BACKER_COMPATIBLE
from .release_gaps import BENCHMARK_DAY, BULK, BULK_MAX_BYTES
from .release_probes import contracts
from .release_rehearsal import require
from .v2_registry import ACTIVE_ETF, V21_SCOPE_EXCLUSIONS, v21_rule

PAGES = {
    "Pricing": "https://finmind.github.io/Pricing/", "Quickstart": "https://finmind.github.io/quickstart/",
    "WhatIsNew": "https://finmind.github.io/WhatIsNew/", "Quota": "https://finmind.github.io/api_usage_count/",
    **{page: f"https://finmind.github.io/tutor/TaiwanMarket/{page}/" for page in
       ("DataList", "Technical", "Chip", "Fundamental", "Derivative")},
}
REPO_DOCS = {"MCP": ("FinMind/FinMind-MCP", "knowledge/datasets.md"),
             "SDK": ("FinMind/FinMind", "FinMind/data/finmind_api.py")}
BULK_RUN = 45623673196
BULK_JOB = 139238130298
BULK_ARTIFACT = 15027001535
MODE_TIER = {"MARKET_DAY": "Backer", "MARKET_RANGE": "Backer"}
TOPIC = re.compile(r'<h4 id="([^"]+)"[^>]*>(.*?)</h4>(.*?)(?=<h[1-4]\b|</article>|\Z)', re.S)
DATE_BOUND = re.compile(r"(?:資料區間|自)\s*[：:]?\s*(\d{4}-\d{2}-\d{2})\s*(?:[~～]|至|起)")
PRIORITY = {
    "TaiwanStockPriceLimit": "PROVE_NATIVE_DAY_RESPONSE_AND_COMPLETE_KEYS_FOR_WITNESSED_SESSION",
    "TaiwanStockParValueChange": "CROSSCHECK_KNOWN_TWSE_TPEX_EFFECTIVE_EVENTS_AND_NATIVE_FIELDS",
    "TaiwanFuturesFinalSettlementPrice": "CROSSCHECK_KNOWN_TAIFEX_SETTLEMENT_DATES_AND_NATIVE_FIELDS",
    "TaiwanStockActiveETFInfo": "PROVE_FULL_NATIVE_SNAPSHOT_AND_TWSE_TPEX_ELIGIBILITY",
}


def public_read(url):
    def once():
        with urlopen(Request(url, headers={"User-Agent": "R12-Bronze-readiness"}), timeout=18) as response:
            require(response.url.startswith(("https://finmind.github.io/", "https://api.github.com/",
                                             "https://raw.githubusercontent.com/FinMind/")), "OFFICIAL_HOST")
            raw = response.read(3 * 1024 * 1024 + 1)
            require(0 < len(raw) <= 3 * 1024 * 1024, "OFFICIAL_DOCUMENT_BOUND")
            return raw
    return retry_read(once, "OFFICIAL_EVIDENCE")


def get_source(name):
    commit = None
    if name in REPO_DOCS:
        repo, path = REPO_DOCS[name]
        commit = json.loads(public_read(f"https://api.github.com/repos/{repo}/commits/HEAD"))["sha"]
        require(re.fullmatch(r"[0-9a-f]{40}", commit), "OFFICIAL_COMMIT")
        url = f"https://raw.githubusercontent.com/{repo}/{commit}/{path}"
    else:
        url = PAGES[name]
    raw = public_read(url)
    return {"source": name, "url": url, "commit": commit, "sha256": digest(raw), "bytes": len(raw)}, raw


def archive_sources(ctx):
    refs, raw, findings = {}, {}, {}

    def retrieve(name):
        try:
            ref, body = get_source(name)
            return name, (ref, body), None
        except (OSError, ValueError, KeyError, TypeError) as error:
            return name, None, type(error).__name__

    with ThreadPoolExecutor(max_workers=6) as pool:
        for name, result, error in pool.map(retrieve, (*PAGES, *REPO_DOCS)):
            if result:
                refs[name], raw[name] = result
            else:
                findings[name] = error
    packed = BytesIO()
    with ZipFile(packed, "w", compression=ZIP_DEFLATED) as bundle:
        for name, body in raw.items():
            # Fixed timestamps make a transport retry idempotent.
            entry = ZipInfo(name + (".html" if name in PAGES else ".md" if name == "MCP" else ".py"))
            bundle.writestr(entry, body, compress_type=ZIP_DEFLATED)
    file_name = "OFFICIAL_SOURCE_ORIGINALS.zip"
    sha = digest(packed.getvalue())
    archive_id = ctx.binary(file_name, packed.getvalue(), "application/zip")
    return raw, {"status": "HOLD" if findings else "PASS", "sources": refs, "findings": findings,
                 "originals": {"file_id": archive_id, "sha256": sha}, "source_endpoint": ENDPOINT,
                 "finmind_data_requests": 0, "sdk_async_counted_per_http": True}


def plain(value):
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]*>", "", value))).strip()


def topics(raw):
    result = defaultdict(list)
    for page in ("DataList", "Technical", "Chip", "Fundamental", "Derivative"):
        body = raw.get(page, b"").decode("utf-8")
        preceding = None
        for anchor, title_html, section in TOPIC.findall(body):
            title = plain(title_html)
            if preceding and "特定日期" in title and "所有資料" in title and re.search(rf"\b{re.escape(preceding)}\b", section):
                entry = result[preceding][-1]
                tier = "SponsorPro" if "sponsorpro" in title.lower() else "Backer" if "backer" in title.lower() else "UNVERIFIED"
                entry.update(market_day_tier=tier, market_day_evidence=title, market_day_anchor=anchor)
                continue
            names = re.findall(r"\b[A-Z][a-zA-Z0-9]{5,}\b", title)
            name = next((name for name in names if anchor.startswith(name.lower())), None)
            preceding = name
            if not name:
                continue
            head = plain(section.split('<div class="admonition', 1)[0][:9000])
            region_bulk = re.search(r"省略\s*data_id.{0,120}日期區間.{0,140}backer", head, re.I)
            match = DATE_BOUND.search(head)
            restricted = re.search(r"只限\s*(?:backer.{0,40}sponsor|backer|sponsorpro|sponsor|贊助)", title, re.I)
            result[name].append({"page": page, "anchor": anchor, "title": title,
                                 "documented_from": match[1] if match else None,
                                 "published_latest": None, "description": head[:1100],
                                 "market_day_tier": None, "market_day_evidence": None,
                                 "market_range_backer": bool(region_bulk),
                                 "dataset_title_restriction": restricted[0] if restricted else None})
    return result


def scope_and_modes(previous, docs, topical, sources):
    exclusions = {row["dataset"] for row in previous["rows"] if row["decision"] == "EXCLUDED"}
    require(len(exclusions) == 19 and set(V21_SCOPE_EXCLUSIONS) <= exclusions, "V24_SCOPE_BASELINE")
    entitlements, modes, contracts_out, missing = [], [], [], []
    old = {row["dataset"]: row for row in previous["rows"]}
    for dataset in sorted(old.keys() | docs.keys()):
        row, doc = old.get(dataset, {}), docs.get(dataset)
        topic = topical.get(dataset, [])
        def source_refs():
            return [{"source": t["page"], "sha256": sources[t["page"]]["sha256"], "anchor": t["anchor"]} for t in topic]
        origin = source_refs() + ([{"source": "MCP", "sha256": sources["MCP"]["sha256"], "dataset": dataset}]
                                  if doc else [])
        excepted = dataset in exclusions
        disputed = bool(doc and doc.tier == "Free(w/ data_id)" and any(
            t["dataset_title_restriction"] and "backer" in t["dataset_title_restriction"].lower() for t in topic))
        range_backer = any(t["market_range_backer"] for t in topic)
        modes_known = []
        mode_holds = {}
        rule = v21_rule(dataset, doc) if doc and not excepted else None
        if rule and rule.grain in ALLOWED_GRAINS:
            history = "`end_date`" in doc.optional or "`end_date`" in doc.required
            series = rule.grain not in {"event", "event_interval"} and rule.query_mode != "snapshot"
            entity = "`data_id`" in doc.required
            if rule.query_mode == "snapshot":
                modes_known.append("SNAPSHOT")
            if history and entity and series:
                modes_known.append("ENTITY_RANGE")
            if history and not entity and series:
                modes_known.append("SERIES_RANGE")
            if history and rule.grain in {"event", "event_interval"}:
                modes_known.append("EVENT_RANGE")
            if range_backer:
                modes_known.append("MARKET_RANGE")
            if any(t["market_day_tier"] == "Backer" for t in topic):
                modes_known.append("MARKET_DAY")
            if any(t["market_day_tier"] == "SponsorPro" for t in topic):
                mode_holds["MARKET_DAY"] = "SPONSOR_PRO_ONLY"
            if rule.query_mode == "start_only" and not history:
                mode_holds["ENTITY_RANGE"] = "OFFICIAL_END_PARAMETER_UNPROVEN"
        grain_ok = rule and rule.grain in ALLOWED_GRAINS
        published = sorted({t["documented_from"] for t in topic if t["documented_from"]})
        trusted_shape = bool(row.get("native_example_shapes", {}).get("NATIVE_SHAPE_OBSERVED") or
                             row.get("bound_venues"))
        code = "EXCLUDED" if excepted else "HOLD"
        blockers = []
        if not excepted:
            if dataset not in old:
                blockers.append("OFFICIAL_ADDITION_REQUIRES_SCOPE_REVIEW")
            if not doc or doc.tier not in BACKER_COMPATIBLE:
                blockers.append("CURRENT_BACKER_CONTRACT_UNPROVEN")
            if not grain_ok or not modes_known:
                blockers.append("APPROVED_NATIVE_GRAIN_OR_BOUNDED_QUERY_UNPROVEN")
            if not trusted_shape:
                blockers.append("NATIVE_SHAPE_NOT_YET_WITNESSED")
            if dataset == ACTIVE_ETF:
                blockers.append("ETF_SNAPSHOT_ELIGIBLE_UNIVERSE_UNPROVEN")
            if not blockers:
                code = "CONTRACT_READY"
        policy = "INDEPENDENT_EMPTY_EVENT_PROOF_REQUIRED" if rule and rule.grain in {"event", "event_interval"} \
            else "RAW_NULLS_AND_PROVIDER_ZEROS_PRESERVED_EXPECTED_KEYS_REQUIRED"
        if mode_holds:
            followup = sorted(mode_holds.values())
        else:
            followup = []
        if gap := PRIORITY.get(dataset):
            followup.append(gap)
        if not excepted and not row.get("bound_venues"):
            followup.append("PARTITION_NATIVE_TOTALS_OR_KEYS_AND_PROVIDER_BOUNDARIES_REQUIRED")
        if blockers or followup:
            missing.append({"dataset": dataset, "contract_blockers": blockers, "partition_evidence_needed": followup})
        tier_rows = {mode: (MODE_TIER.get(mode, "Backer" if disputed else doc.tier)) for mode in modes_known} if doc else {}
        entitlements.append({"dataset": dataset, "prior_state": row.get("state"), "official_tier": doc.tier if doc else None,
                             "free": bool(doc and doc.tier.startswith("Free") and not disputed),
                             "free_condition": ("data_id" if doc and doc.tier == "Free(w/ data_id)" and not disputed else None),
                             "tier_disagreement": disputed, "mode_entitlements": tier_rows,
                             "account_entitlement": "NEEDS_SAME_ACCOUNT_EVIDENCE", "references": origin,
                             "decision": code})
        modes.append({"dataset": dataset, "documented": sorted(tier_rows), "held_modes": mode_holds,
                      "effective_modes_for_dispatch": [], "candidate_requires_partition_evidence": True,
                      "evidence": origin})
        contracts_out.append({"dataset": dataset, "state": code, "prior_state": row.get("state"),
                              "scope_decision": row.get("reason") if excepted else "V2.4_APPROVED_MODES_AND_NATIVE_GRAIN",
                              "source_dates_documented_from": published, "latest_published": None,
                              "grain": rule.grain if rule else None, "endpoint": ENDPOINT,
                              "schema": asdict(rule) if rule else None, "documented_required": doc.required if doc else None,
                              "documented_optional": doc.optional if doc else None, "mode_contracts": {
                                  mode: asdict(adapt_rule(rule, mode, min(rule.byte_limit, BULK_MAX_BYTES)))
                                  for mode in modes_known}, "source_rules": topic,
                              "null_policy": policy, "pagination": "EXPLICIT_PROVIDER_CURSOR_EVIDENCE_REQUIRED",
                              "completeness_methods": ["expected_keys", "split_reconciliation"] if rule and rule.key_fields
                              else ["expected_multiset", "split_reconciliation"],
                              "archived_native_shape_proven": trusted_shape, "contract_blockers": blockers,
                              "known_bounded_evidence": row.get("bound_venues", []), "evidence": origin})
    return {"rows": contracts_out, "counts": dict(Counter(r["state"] for r in contracts_out)),
            "baseline_count": len(old), "official_inventory_count": len(docs),
            "official_new": sorted(docs.keys() - old.keys()), "official_removed": sorted(old.keys() - docs.keys()),
            "exclusions": sorted(exclusions)}, entitlements, modes, missing


def official(ctx):
    raw, lock = archive_sources(ctx)
    docs = contracts(raw.get("MCP", b"").decode("utf-8"))
    previous, history = ctx.prior("DATASET_REGISTRY.json"), ctx.prior("CONTRACT_EVIDENCE.json")
    require(len(history["native_archive_readback"]) == 259 and len(history["stored_samples"]) == 266,
            "V24_ARCHIVED_RAW_EVIDENCE_COUNT")
    registry, entitlement, modes, missing = scope_and_modes(previous, docs, topics(raw), lock["sources"])
    keyouts = {"OFFICIAL_SOURCE_LOCK.json": lock, "ENTITLEMENT_MATRIX.json": {"rows": entitlement,
               "sdk_async": "ONE_HTTP_PER_REQUEST_EVEN_WHEN_CONCURRENT"},
               "OFFICIAL_DATASET_REVIEW.json": registry, "OFFICIAL_QUERY_REVIEW.json": {"modes": list(MODES), "rows": modes},
               "CONTRACT_GAP_MATRIX.json": {"rows": missing, "priority": PRIORITY,
                                            "archived_raw_reused": len(history["native_archive_readback"])}}
    return ("PASS" if lock["status"] == "PASS" and docs else "HOLD"), keyouts


def previous_bulk(github):
    reply = github.json(f"/actions/runs/{BULK_RUN}/jobs?per_page=100")
    require(any(row["id"] == BULK_JOB and row["conclusion"] == "success" for row in reply["jobs"]), "BULK_JOB_BINDING")
    raw = github.get(f"/actions/jobs/{BULK_JOB}/logs", download=True)
    require(0 < len(raw) < 1024**2, "BULK_LOG_BOUND")
    summaries = []
    for line in raw.decode().splitlines():
        if '"bulk_response_limit_mib"' in line and '"bulk"' in line and '{' in line:
            try:
                value = json.loads(line[line.find("{"):])
            except ValueError:
                continue
            summaries.append(value)
    report, = summaries
    require(report["bulk_response_limit_mib"] == BULK_MAX_BYTES // 1024**2 and set(report["bulk"]) == set(BULK),
            "BULK_REPORT_CONTRACT")
    return {"run_id": BULK_RUN, "attempt": 1, "job_id": BULK_JOB, "artifact_id": BULK_ARTIFACT, "day": BENCHMARK_DAY,
            "log_sha256": digest(raw), "scope": "SINGLE_SYMBOL_EQUIVALENCE_ONLY", "is_official_limit": False,
            "reference_matches_do_not_establish_native_market_completeness": True, "bulk": report["bulk"]}
