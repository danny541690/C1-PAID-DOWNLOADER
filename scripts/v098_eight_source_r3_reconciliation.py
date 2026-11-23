#!/usr/bin/env python3
"""JARVIS v0.98 R3 eight-source source/native/schema reconciliation & Drive evidence publish.
Consumes immutable independent GitHub run artifacts, never re-fetches market data.
No false claim of full 2000-2026 official dataset completeness.
"""
import collections,csv,datetime as dt,hashlib,json,os,pathlib
from v098_publish_source_period_rules import get_token,ensure_spec_folder,upload_check,write_csv,write_json
OUT=pathlib.Path("eight_source_r3");OUT.mkdir(exist_ok=True)
P=pathlib.Path("r3_input")
def load(path):return json.loads((P/path).read_text(encoding="utf-8"))
free=load("free_matched/report.json")
backer=load("backer_matched/report.json")
pilot=load("six_official/six_official_bronze_era_pilot.json")
r2=load("previous/11_八大來源資料規則主帳_20261124_R2.json")
if free["plan"]!="FREE" or backer["plan"]!="BACKER":raise RuntimeError("PLAN_ARTIFACT_SWAP")
if free["registry_sha256"]!=backer["registry_sha256"]:raise RuntimeError("FINMIND_DOC_DRIFT")
if len(free["queries"])!=78 or len(backer["queries"])!=78:raise RuntimeError("QUERY_COHORT_INCOMPLETE")
if pilot.get("schema")!="JARVIS_V098_SIX_OFFICIAL_BRONZE_ERA_PILOT_V1":raise RuntimeError("BAD_OFFICIAL_PILOT")
def key(r):return (r["dataset"],r["year"])
fa={key(r):r for r in free["queries"]}
ba={key(r):r for r in backer["queries"]}
if set(fa)!=set(ba):raise RuntimeError("UNMATCHED_DATASET_VINTAGE_REQUESTS")
comparisons=[]
for key_ in sorted(fa):
 f=fa[key_];b=ba[key_]
 params_identical=f.get("params")==b.get("params")
 if not params_identical:raise RuntimeError("PARAMETER_MISMATCH")
 if f["state"]=="PASS" and b["state"]=="PASS":
  consistent=f.get("canonical_sha256")==b.get("canonical_sha256") and f.get("columns")==b.get("columns") and f.get("rows")==b.get("rows")
  verdict="MATCHED_CONTENT_AND_SCHEMA" if consistent else "DATA_OR_SCHEMA_DIFFERENCE_INVESTIGATE"
 elif f["state"]=="EMPTY" and b["state"]=="EMPTY":verdict="BOTH_EMPTY_IN_WINDOW"
 elif f["state"]=="PASS" and b["state"]=="EMPTY":verdict="FREE_HAS_DATA_BACKER_EMPTY"
 elif f["state"]=="EMPTY" and b["state"]=="PASS":verdict="BACKER_HAS_DATA_FREE_EMPTY"
 elif b["state"]=="PASS":verdict="BACKER_PASS_FREE_UNAVAILABLE"
 else:verdict="REQUEST_INCONCLUSIVE"
 comparisons.append({"source_pair":"FinMind_Free|FinMind_Backer","dataset":key_[0],"sample_year":key_[1],
  "same_parameters":params_identical,"free_state":f["state"],"backer_state":b["state"],
  "free_rows":f.get("rows"),"backer_rows":b.get("rows"),
  "free_columns":"|".join(f.get("columns",[])),"backer_columns":"|".join(b.get("columns",[])),
  "free_sample_min":f.get("first_date",""),"backer_sample_min":b.get("first_date",""),
  "free_sample_max":f.get("last_date",""),"backer_sample_max":b.get("last_date",""),
  "free_canonical_sha256":f.get("canonical_sha256",""),
  "backer_canonical_sha256":b.get("canonical_sha256",""),"verdict":verdict,
  "G2_certified":False,"note":"Sample period, one stock 2330, no announcement/PIT validation"})
doc_links={
"TWSE":["https://www.twse.com.tw/en/trading/historical/stock-day.html","https://www.twse.com.tw/en/trading/historical/fmtqik.html"],
"TPEx":["https://www.tpex.org.tw/en-us/mainboard/trading/info/stock-pricing.html"],
"MOPS":["https://mops.twse.com.tw/mops/web/index","https://www.twse.com.tw/rwd/IFRS/aboutIFRS"],
"NDC":["https://www.ndc.gov.tw/EN/Content_List.aspx?n=8DCFE8CF6FB8173D"],
"TDCC":["https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"],
"TAIFEX":["https://www.taifex.com.tw/enl/eng3/futDailyMarketView","https://www.taifex.com.tw/enl/eng3/optDailyMarketView"]}
era_by_source=collections.defaultdict(list)
for x in r2.get("six_source_historical_format_eras",[]):era_by_source[x["source"]].append(x)
official_rows=[]
for x in pilot["entries"]:
 sample=x.get("sample") or {}
 observed=x.get("state","NOT_TESTED")
 era=next((e["era_id"] for e in era_by_source[x["source"]] if
   e["start_month"]<=str(x["year"])+"-01"<=e["end_month"]),"ERA_NOT_MAPPED")
 official_rows.append({"source":x["source"],"year":x["year"],"year_source_status":observed,
  "first_year_of_this_dataset_verified":False,"first_date_of_this_dataset_verified":False,
  "source_folder_exists":x.get("source_folder_exists",False),"child_count":x.get("child_count"),
  "direct_file_count":x.get("file_count_at_source_root"),
  "sample_content_available":x.get("has_any_file_sample",False),
  "sample_format":sample.get("format",""),"sample_encoding":sample.get("encoding",""),
  "sample_columns":"|".join(map(str,sample.get("columns",[]))),
  "sample_rows":sample.get("record_count_sample"),"sample_state":sample.get("state",""),
  "sample_sha256":sample.get("raw_sha256",""),
  "historical_schema_template":era,"official_reference":"|".join(doc_links[x["source"]]),
  "schema_comparison":"ERA_ROUTING_CANDIDATE_NOT_EXACT_DOC_COLUMN_PASS" if sample else "NOT_COMPARABLE_NO_CONTENT_SAMPLE",
  "G1_verified":False,"G2_verified":False,
  "issue":"FOLDER_OR_FILE_SCOPE_NOT_DATASET_HISTORY" if observed!="SCAN_ERROR" else "SCAN_ERROR",
  "followup":"Classify native dataset, record per-file primary keys/date range, compare exact documented layout"})
issues=[
 {"priority":"P0","problem":"Priced Free 45 / API doc Free 54 and Backer 81 / API doc Free+Backer 89","root_cause":"Different catalog counting and entitlement drift","remedy":"Persist canonical registry snapshot; authorization=actual per-dataset/per-query + official price mapping; do not equate counts","evidence":"FinMind official pricing and GitHub registry"},
 {"priority":"P0","problem":"1991 financial statement used to infer 1991 stock price","root_cause":"Dataset/category dates conflated; financial period != trade date","remedy":"Track start/end per source, endpoint, dataset, symbol, exchange, publication and PIT; no inferred earliest for other datasets","evidence":"Prior 1991/1994 evidence"},
 {"priority":"P0","problem":"HTTP 400 or EMPTY treated as missing dataset","root_cause":"Missing data_id/invalid permissions/single-day/wrong effective market code","remedy":"Classify AUTH_DENIED,INVALID_PARAM,SINGLE_DAY_REQUIRED,EMPTY_VALID,NOT_YET_PUBLISHED,HTTP_TRANSIENT separately","evidence":"Free/Backer 78 identical calls"},
 {"priority":"P0","problem":"Oversized all-history response or server 504","root_cause":"1900-2026 broad query, unbounded response and server latency","remedy":"Adaptive year-month-day partitions; stop on 429; preserve manifest checkpoint; retry max 3 for transient faults only","evidence":"Previous Backer repair22 run"},
 {"priority":"P0","problem":"Historical schema incorrectly inferred from current header","root_cause":"Top-level JSON keys unchanged but statement type/unit/GAAP-IFRS changed","remedy":"Dataset×era×market×encoding×columns×units×type/field semantic version; sample each actual era and official documents","evidence":"Backer 1991-2026 schema sampled"},
 {"priority":"P0","problem":"Drive 2000-2026 year folders counted as dataset completeness","root_cause":"Directory existence is not file content nor expected market coverage","remedy":"G1 derive actual each dataset file min/max from immutable Bronze/Manifest, match official release and market calendar","evidence":"Six official year/source metadata audit"},
 {"priority":"P1","problem":"Comparing different Free vs Backer dates and parameters","root_cause":"Earlier tests were not identical scopes","remedy":"This R3 compares exact same 78 query parameter sets, row count, native columns, canonical SHA","evidence":"Matched Free/Backer runs"},
 {"priority":"P1","problem":"PIT leakage from financial period-end","root_cause":"data.date is effective/filing period not first publication","remedy":"Enforce publication date +1 day rule; unavailable timestamp=PIT_UNVERIFIED","evidence":"JARVIS v0.98 policy"},
 {"priority":"P1","problem":"Official field docs/old datasets not directly scraped","root_cause":"Some sites lack uniform JSON, downloads differ by era and access rights","remedy":"Attach exact official product page and versioned native table as G2 comparator; no blanket certification","evidence":"TWSE/TPEx/MOPS/NDC/TDCC/TAIFEX official references"},
 {"priority":"P1","problem":"Repeated full refetch to audit already-saved Bronze","root_cause":"Downloader and QA not isolated; lack immutable source SHA/Checkpoint","remedy":"Download only genuine missing units; G1/G2 read existing SHA and compare no-mutating raw byte samples","evidence":"User C1 acceptance rules"},
]
summary_counts=dict(collections.Counter(x["verdict"] for x in comparisons))
official_counts=dict(collections.Counter(x["year_source_status"] for x in official_rows))
output={"schema":"JARVIS_V098_EIGHT_SOURCE_SCHEMA_R3_20261124",
 "run_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
 "sources":["TWSE","TPEx","MOPS","NDC","TDCC","TAIFEX","FinMind_Free","FinMind_Backer"],
 "source_branches":{"TWSE":"FREE","TPEx":"FREE","MOPS":"FREE","NDC":"FREE","TDCC":"FREE","TAIFEX":"FREE","FinMind_Free":"FREE","FinMind_Backer":"PAID"},
 "finmind_matched_queries_per_plan":78,
 "finmind_free_pass":free["success"],"finmind_backer_pass":backer["success"],
 "finmind_comparison_status_counts":summary_counts,
 "six_official_year_source_status_counts":official_counts,
 "official_record_count":len(official_rows),"official_raw_dataset_catalog_complete":False,
 "eight_source_data_catalog_complete":False,
 "source_docs_canonical_snapshot":free["registry_sha256"],
 "official_page_proof_is_native_dataset_proof":False,
 "audit_windows":[2000,2008,2013,2018,2024,2026],
 "Bronze_originals_mutated":False,"Silver_Gold_run":False,
 "PIT_rule":"published_at_date_plus_one","unverified_pit_label":"PIT_UNVERIFIED",
 "comparison":comparisons,"official":official_rows,"error_taxonomy":issues,
 "reference_master_r2":"11_八大來源資料規則主帳_20261124_R2.json",
 "workflow_evidence":{"free":"https://github.com/danny541690/C1-RAW-DOWNLOADER/actions/runs/45219906365",
 "backer":"https://github.com/danny541690/C1-PAID-DOWNLOADER/actions/runs/45219922979",
 "six_official":"https://github.com/danny541690/C1-RAW-DOWNLOADER/actions/runs/45220041535"}}
files=[]
edition="20261124_R3"
files.append(write_csv("13_八來源六來源Bronze年代實測_"+edition+".csv",
 ["source","year","year_source_status","source_folder_exists","child_count","direct_file_count",
  "sample_content_available","sample_format","sample_encoding","sample_columns","sample_rows","sample_state",
  "sample_sha256","historical_schema_template","official_reference","schema_comparison","G1_verified","G2_verified","issue","followup"],
 official_rows))
files.append(write_csv("14_Free_BackER_同參數歷史資料對照_"+edition+".csv",
 ["source_pair","dataset","sample_year","same_parameters","free_state","backer_state","free_rows","backer_rows",
 "free_columns","backer_columns","free_sample_min","backer_sample_min","free_sample_max","backer_sample_max",
 "free_canonical_sha256","backer_canonical_sha256","verdict","G2_certified","note"],comparisons))
files.append(write_csv("15_八來源歷史下載與格式錯誤根因修復_"+edition+".csv",
 ["priority","problem","root_cause","remedy","evidence"],issues))
files.append(write_json("16_八來源格式核對與變更主帳_"+edition+".json",output))
text_md=[
 "# JARVIS v0.98 八來源資料與歷史格式R3：實際驗收、未驗、修復",
 "",
 "## 這次正式執行的三個互不共用Token的流程",
 "- 六大官方：C1-RAW-DOWNLOADER；只讀Google Drive既有20_BRONZE，按2000至2026年原始年份×來源盤點，抽7年代少量原件驗證格式。",
 "- FinMind Free：C1-RAW-DOWNLOADER匿名API；13種資料集各6個年份，合計78次。",
 "- FinMind Backer：C1-PAID-DOWNLOADER專用Backer Secret；相同13×6共78次。",
 "- 整合：付費GitHub只合併先前Artifacts，寫入C1_0.98/10_SOURCES/00_資料規則表，不重複下載API。",
 "",
 "## 實測與證據限制",
 f"- Free有資料：{free['success']}/78；Backer有資料：{backer['success']}/78。",
 f"- FinMind同參數對照分類：{json.dumps(summary_counts,ensure_ascii=False)}。",
 f"- 六大官方年份×來源抽樣分類：{json.dumps(official_counts,ensure_ascii=False)}。",
 "- 官網方案數和公開技術文件資料集候選數不同，不得直接混加；前次規格總帳R2仍保留全部54 Free候選與89 Backer候選。",
 "- 六大官方年份資料夾不等於資料集最早實際可用日期，未逐檔/逐Dataset驗證的最早日期都保留unknown。",
 "- 本輪六大官方欄位採樣僅代表被抽到的原件；未取得精確官方歷史版本欄位定義的，不核發G2 PASS。",
 "- 財報period_end不是announced_at；PIT遵守公告日期隔日可用。",
 "",
 "## 反覆錯誤與採用解法","",
]
for i in issues:text_md+=["### "+i["priority"]+" "+i["problem"],"根因："+i["root_cause"],"解法："+i["remedy"],""]
text_md+=["## 後續上線驗收門檻",
 "1. 對每筆Bronze讀取SHA/來源metadata，僅未知年代或異常內容才重新打開本體，不可重抓原網站作為搬運驗收。",
 "2. 精確官方表頭映射應是dataset×版次×歷史有效區間，候選語義映射需人工/官方文獻證據。",
 "3. 建G1搬運PASS、G2 Schema PASS、G3 native shape PASS、G4 Silver Ready；四者完全分離。",
 "4. 金額儲存原單位，在Gold展示時依JARVIS慣例轉成億元；市場範圍以上市/上櫃為正式母體。",
 "5. 任何無法證明的歷史起點都標記NOT_VERIFIED，不得直接用資料集最早日期推斷其他Dataset。",
 "",
 "## 參考來源",
 "https://finmind.github.io/Pricing/",
 "https://finmind.github.io/WhatIsNew/",
 "https://www.twse.com.tw/en/trading/historical/stock-day.html",
 "https://www.tpex.org.tw/en-us/mainboard/trading/info/stock-pricing.html",
 "https://www.ndc.gov.tw/EN/Content_List.aspx?n=8DCFE8CF6FB8173D",
 "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock",
 "https://www.taifex.com.tw/enl/eng3/optDailyMarketView"
]
p=OUT/("00_八來源R3驗收摘要_"+edition+".md");p.write_text("\n".join(text_md),encoding="utf-8");files.append(p)
token=get_token();dest=ensure_spec_folder(token)
receipts=[upload_check(token,dest,f) for f in files]
rp=OUT/("17_八來源R3遠端回讀履歷_"+edition+".json")
rp.write_text(json.dumps({"status":"PASS","files":receipts,"all_readback_verified":True,
 "run_id":os.getenv("GITHUB_RUN_ID"),"counts":{"matched":summary_counts,"official":official_counts}},ensure_ascii=False,indent=2),encoding="utf-8")
receipts.append(upload_check(token,dest,rp))
print("JARVIS_R3_UPLOAD_PASS "+json.dumps({"files":len(receipts),"Free_success":free["success"],
 "Backer_success":backer["success"],"official_year_source_count":len(official_rows),
 "matched_digest_agreement":summary_counts.get("MATCHED_CONTENT_AND_SCHEMA",0),
 "source_count":8,"google_drive_readback":"PASS"},ensure_ascii=False),flush=True)
