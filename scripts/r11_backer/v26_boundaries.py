"""Lock source boundary evidence and derive native publication targets."""

from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from io import BytesIO
import re
from urllib.request import Request, urlopen
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from .bulk_manifest import SNAPSHOT_ZONE
from .integration_pilot import digest
from .release_rehearsal import require
from .v24_sources import DATE_BOUND, TOPIC, plain

FINMIND = "https://finmind.github.io"
PAGES = {name: f"{FINMIND}/{path}/" for name, path in {
    "Quality": "DataQuality", "Methodology": "DataQuality/Methodology", "WhatIsNew": "WhatIsNew",
    "Technical": "tutor/TaiwanMarket/Technical", "Fundamental": "tutor/TaiwanMarket/Fundamental",
    "Chip": "tutor/TaiwanMarket/Chip", "ConvertibleBond": "tutor/TaiwanMarket/ConvertibleBond",
    "Derivative": "tutor/TaiwanMarket/Derivative", "Materials": "tutor/Materials",
    "InterestRate": "tutor/InterestRate", "ExchangeRate": "tutor/ExchangeRate",
    "GovernmentBondsYield": "tutor/GovernmentBondsYield", "DataList": "tutor/TaiwanMarket/DataList",
}.items()} | {
    "EIA_WTI": "https://www.eia.gov/dnav/pet/hist/RWTCD.htm",
    "EIA_BRENT": "https://www.eia.gov/dnav/pet/hist/RBRTED.htm",
    "US_TREASURY": "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives",
    "FED": "https://www.federalreserve.gov/monetarypolicy/openmarket.htm",
    "ECB": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/key_ecb_interest_rates/html/index.en.html",
    "TAIWAN_CBC": "https://www.cbc.gov.tw/tw/cp-645-1081-533d6-1.html",
    "TPEX_CB": "https://www.tpex.org.tw/en-us/bond/info/statistics-cb/inst-summary/day.html",
    "TWSE_SPLIT": "https://wwwc.twse.com.tw/zh/announcement/split/twtcau.html",
}
TARGETS = {
    "CrudeOilPrices": ("Materials", "EIA_WTI", "EIA_BRENT"),
    "GovernmentBondsYield": ("GovernmentBondsYield", "US_TREASURY"),
    "InterestRate": ("InterestRate", "FED", "ECB"),
    "TaiwanExchangeRate": ("ExchangeRate", "TAIWAN_CBC"),
    "TaiwanStockConvertibleBondDaily": ("ConvertibleBond", "TPEX_CB"),
    "TaiwanStockConvertibleBondDailyOverview": ("ConvertibleBond", "TPEX_CB"),
    "TaiwanStockConvertibleBondInstitutionalInvestors": ("ConvertibleBond", "TPEX_CB"),
    "TaiwanStockSplitPrice": ("Fundamental", "TWSE_SPLIT"),
}
ORIGINALS = "V26_OFFICIAL_SOURCE_ORIGINALS.zip"
ISO = r"\d{4}-\d{2}-\d{2}"
QUARTERLY = {"TaiwanStockFinancialStatements", "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement"}
EXTERNAL_BOUNDS = {"CrudeOilPrices": {"WTI": ["1986-01-02", "EIA_WTI"], "Brent": ["1987-05-20", "EIA_BRENT"]},
                   "GovernmentBondsYield": {"US_TREASURY": ["1990-01-02", "US_TREASURY"]}}


def article(raw):
    return raw.decode("utf-8", errors="replace").split("<article", 1)[-1].split("</article>", 1)[0]


def quality_record(dataset, raw):
    page = article(raw)
    title = plain(page)
    require(title.startswith("class=") and f"{dataset} 資料品質報告" in title[:260], "V26_QUALITY_REPORT_IDENTITY")
    dates = re.search(rf"資料期間\s+({ISO})\s*[～~至]\s*({ISO})", title)
    observed = re.search(rf"最新資料日\s*({ISO})", title)
    sample_date = re.search(rf"檢測日期\s*({ISO})", title)
    feature = title.partition("資料特性說明¶")[2].partition("資料維護紀錄¶")[0].strip()
    return {"observed_range": list(dates.groups()) if dates else None,
            "last_reported_published": observed[1] if observed else None,
            "sampled_at": sample_date[1] if sample_date else None,
            "supports_query_comparison": bool(re.search(r"查詢方式一致性\s+通過", title)),
            "features": feature[:900], "proof_scope": "FINMIND_QUALITY_REPORT_SAMPLED_NOT_FULL_HISTORY"}


def fetch_page(item):
    name, url = item
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 R12-Readiness"})
        with urlopen(req, timeout=14) as response:
            raw = response.read(3 * 1024 * 1024 + 1)
            require(0 < len(raw) <= 3 * 1024 * 1024, "V26_SOURCE_SIZE")
            if url.startswith(FINMIND):
                require(response.url.startswith(FINMIND + "/"), "V26_SOURCE_REDIRECT")
            return name, raw, {"url": url, "final_url": response.url, "bytes": len(raw), "sha256": digest(raw)}
    except (OSError, ValueError, RuntimeError) as error:
        code = f"HTTP_{error.code}" if hasattr(error, "code") else type(error).__name__
        return name, None, {"url": url, "error": code}


def sources(ctx, datasets):
    docs, refs = {}, {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        for name, raw, ref in pool.map(fetch_page, PAGES.items()):
            refs[name] = ref
            if raw:
                docs[name] = raw
        if "Quality" in docs:
            links = set(re.findall(r'href="([A-Za-z0-9_]+)/"', article(docs["Quality"])))
            qualified = (set(datasets) | set(TARGETS)) & links
            jobs = {f"Quality_{name}": f"{FINMIND}/DataQuality/{name}/" for name in sorted(qualified)}
            for name, raw, ref in pool.map(fetch_page, jobs.items()):
                refs[name] = ref
                if raw:
                    docs[name] = raw
    bundle = BytesIO()
    with ZipFile(bundle, "w") as archive:
        for name, raw in sorted(docs.items()):
            archive.writestr(ZipInfo(name + ".html"), raw, compress_type=ZIP_DEFLATED)
    raw = bundle.getvalue()
    key = ctx.binary(ORIGINALS, raw, "application/zip")
    return docs, {"originals": {"file_id": key, "filename": ORIGINALS, "sha256": digest(raw)},
                  "sources": refs, "locked_pages": len(docs), "data_api_requests": 0}


def official_sections(names, raw, refs):
    sections = {name: [] for name in names}
    alternatives = {name: [] for name in names}
    for label in ("Technical", "Fundamental", "Chip", "ConvertibleBond", "Derivative", "Materials",
                  "InterestRate", "ExchangeRate", "GovernmentBondsYield"):
        if label not in raw:
            continue
        for anchor, title, detail in TOPIC.findall(raw[label].decode()):
            heading = plain(title)
            tokens = set(re.findall(r"\b[A-Z][A-Za-z0-9]{4,}\b", heading))
            if hit := tokens.intersection(names):
                early = plain(detail.split('<div class="admonition', 1)[0])
                bound = DATE_BOUND.search(early)
                publish = re.search(r"(?:資料更新時間|更新時間)[^¶。]{0,100}?(\d\d:\d\d)", early)
                for name in sorted(hit):
                    sections[name].append({"source": label, "anchor": anchor, "sha256": refs[label]["sha256"],
                        "documented_start": bound[1] if bound else None, "publish_at_taipei": publish[1] if publish else None,
                        "notes": early[:550]})
            title_authorized = ("特定日期" in heading and "所有資料" in heading and
                                "backer" in heading.lower() and "sponsorpro" not in heading.lower())
            if title_authorized:
                for name in sorted(names.intersection(re.findall(r"\b[A-Z][A-Za-z0-9]{4,}\b", detail))):
                    alternatives[name].append({"source": label, "anchor": anchor, "sha256": refs[label]["sha256"],
                                                "mode": "MARKET_DAY", "tier": "Backer"})
    return sections, alternatives


def prior_month(day):
    return day.replace(day=1) - timedelta(days=1)


def release_target(dataset, grain, as_of, sessions, published_at=None):
    day = as_of.date()
    if grain == "snapshot":
        return None, {"kind": "ACQUISITION_TIME_ASIA_TAIPEI"}
    if dataset in QUARTERLY or grain == "quarterly":
        # Native observations are fiscal period keys. Ordinary filing deadlines
        # establish a planning cutoff, and late/exempt returns remain verifiable exceptions.
        quarters = (date(year, month, monthrange(year, month)[1])
                    for year in (day.year - 2, day.year - 1, day.year) for month in (3, 6, 9, 12))
        completed = [period for period in quarters if period + timedelta(days=90 if period.month == 12 else 45) <= day]
        return str(max(completed)), {"kind": "FISCAL_PERIOD_END", "planning_reporting_days": {"Q1_Q2_Q3": 45, "Q4": 90}}
    if dataset == "TaiwanStockMonthRevenue":
        anchor = day.replace(day=1) if day.day >= 11 else prior_month(day).replace(day=1)
        return str(anchor), {"kind": "REVENUE_MONTH_PUBLICATION_LABEL", "planning_day_of_following_month": 11}
    if grain == "monthly":
        return str(prior_month(day).replace(day=1)), {"kind": "LAST_COMPLETED_MONTH"}
    if grain == "annual":
        return str(date(day.year - 1, 12, 31)), {"kind": "LAST_COMPLETED_ANNUAL_PERIOD"}
    if grain == "weekly":
        return str(day - timedelta(days=day.weekday() + 1)), {"kind": "LAST_COMPLETED_ISO_WEEK"}
    if grain in {"event", "event_interval"}:
        return str(day), {"kind": "OCCURRENCE_OR_ANNOUNCEMENT", "announced_future_originals_preserved": True}
    if dataset == "CrudeOilPrices":
        return str(day - timedelta(days=7)), {"kind": "SOURCE_LAG_ESTIMATE", "days": 7}
    if dataset.startswith(("TaiwanStock", "TaiwanAssetSwap", "TaiwanDaily", "TaiwanTotal", "TaiwanFutures", "TaiwanOption")):
        time_reached = published_at and as_of.strftime("%H:%M") >= published_at
        eligible = [session for session in sessions if session < str(day) or time_reached and session == str(day)]
        if eligible:
            return eligible[-1], {"kind": "ARCHIVED_TAIWAN_SESSION_AND_RELEASE_TIME", "published_at_taipei": published_at,
                                  "market_specific_exceptions_retained": True}
    return str(day - timedelta(days=1)), {"kind": "CONSERVATIVE_PRIOR_DAY", "observations_must_be_verified": True}


def run(ctx):
    registry = ctx.previous("DATASET_REGISTRY.json")
    names = {row["dataset"] for row in registry["rows"] if row["state"] == "CONTRACT_READY"}
    require(len(names) == 70, "V26_DATASET_SCOPE")
    axes = ctx.previous("HISTORICAL_QUERY_AXES.json")
    snapshot = ctx.previous("HISTORICAL_UNIVERSE.json")
    as_of = datetime.fromisoformat(ctx.read("LOCK_INPUT.json")["as_of_taipei"]).astimezone(SNAPSHOT_ZONE)
    raw, lock = sources(ctx, names)
    sections, options = official_sections(names, raw, lock["sources"])
    rows = []
    for row in sorted(registry["rows"], key=lambda item: item["dataset"]):
        name = row["dataset"]
        if name not in names:
            continue
        report_name = f"Quality_{name}"
        quality = quality_record(name, raw[report_name]) if report_name in raw else {}
        declared = sorted(set(row["source_dates_documented_from"]) |
                          {s["documented_start"] for s in sections[name] if s["documented_start"]})
        observed = sorted({day for sample in axes["source_witnesses"].get(name, ()) for day in sample["observations"]} |
                          set(row.get("v25_witness", {}).get("observed_dates", ())))
        reports = quality.get("observed_range")
        inferred_start = declared[0] if declared else min([reports[0], *observed]) if reports else observed[0] if observed else None
        earlier = [value for value in ([reports[0]] if reports else []) + observed[:1] if declared and value < declared[0]]
        publish = next((s["publish_at_taipei"] for s in sections[name] if s["publish_at_taipei"]), None)
        latest, rule = release_target(name, row["grain"], as_of, axes["trading_dates"], publish)
        external = [
            {"id": key, "start": start, "source_key": source, "proof_scope": "PRIMARY_SOURCE_CANDIDATE",
             "artifact": lock["sources"][source]}
            for key, (start, source) in EXTERNAL_BOUNDS.get(name, {}).items() if source in raw
        ]
        features = quality.get("features", "")
        source_unavailable = (["2010-02-06", "2014-12-27"] if name == "TaiwanStockConvertibleBondDailyOverview"
                              and all(value in features for value in ("2010-02-06", "2014-12-27")) else [])
        rows.append({"dataset": name, "grain": row["grain"], "finmind_documented_start": declared or None,
                     "quality_sample": quality or None, "quality_source": lock["sources"].get(report_name),
                     "primary_publisher_candidates": external, "other_official_sources": [
                         lock["sources"][label] for label in TARGETS.get(name, ()) if label in lock["sources"]],
                     "stored_native_observed": [observed[0], observed[-1]] if observed else None,
                     "planning_start": inferred_start, "publication_target": latest, "publication_rule": rule,
                     "historical_start_class": "DOCUMENTED" if declared else "QUALITY_OR_ARCHIVE_BOUND",
                     "archive_observation_earlier_than_documentation": earlier,
                     "source_unavailable_days": source_unavailable,
                     "source_unavailable_saturdays_through": ("2018-12-31" if name == "TaiwanStockConvertibleBondInstitutionalInvestors"
                                                          and "以前的週六補行交易日" in features else None),
                     "valuation_missing_indicator": "-1" if name in {"TaiwanExchangeRate", "GovernmentBondsYield"} and "-1" in features else None,
                     "official_sections": sections[name], "authorized_market_day": options[name],
                     "previous_coverage_complete": False})
    status = "PASS" if len(rows) == 70 and all(row["quality_sample"] for row in rows if row["dataset"] in TARGETS) else "HOLD"
    return status, {"BOUNDARY_REGISTRY.json": {"as_of_taipei": as_of.isoformat(), "rows": rows},
                    "OFFICIAL_SOURCE_LOCK.json": lock,
                    "HISTORICAL_CALENDAR.json": {"calendar_reconciliation": snapshot["calendar_reconciliation"],
                        "source": snapshot["calendar_source"] | {"observed_dates": None}, "sessions": axes["trading_dates"],
                        "market_calendars_kept_separate_where_known": True,
                        "before_first_session": "B09_unconfirmed_market_calendar"}}
