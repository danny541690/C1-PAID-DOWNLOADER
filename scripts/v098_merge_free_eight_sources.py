#!/usr/bin/env python3
"""Merge FinMind Free empirical report, prior Backer and six official registry.
Versioned write-only specs to existing Drive rules folder, SHA-checked.
"""
import csv,datetime as dt,json,pathlib
from v098_publish_source_period_rules import get_token,ensure_spec_folder,upload_check,write_csv,write_json
ROOT=pathlib.Path("source_spec_output");ROOT.mkdir(exist_ok=True)
OLD=pathlib.Path("eight_source_inputs/backer_official/06_JARVIS_v098_來源資料總規格_20261124.json")
FREE=pathlib.Path("eight_source_inputs/free/report.json")
if not OLD.is_file() or not FREE.is_file():raise SystemExit("MISSING_INPUT_SPEC_ARTIFACTS")
old=json.loads(OLD.read_text(encoding="utf-8"));free=json.loads(FREE.read_text(encoding="utf-8"))
if free.get("schema")!="JARVIS_V098_FINMIND_FREE_HISTORY_EVIDENCE_V1":raise ValueError("BAD_FREE_REPORT")
sources=["TWSE","TPEx","MOPS","NDC","TDCC","TAIFEX","FinMind_Free","FinMind_Backer"]
items=[]
for r in old["official"]:
 items.append({"source":r["source"],"dataset_id":r["dataset_id"],"registry_type":r["catalog_class"],
  "api_status":r.get("verify_status",""),"earliest_observed":r.get("actual_bronze_min_date",""),
  "latest_observed":r.get("actual_bronze_max_date",""),"official_page_start":r.get("documented_earliest",""),
  "date_basis":"OFFICIAL_DOC_ENDPOINT_ONLY" if r.get("documented_earliest") not in ("","UNVERIFIED_AT_THIS_DATASET_ID") else "NOT_AUDITED_PER_DATASET",
  "raw_format":r.get("raw_format",""),"field_names":"",
  "historical_era_rules":r.get("era_rules",""),"sample_data_id":"",
  "evidence_url":r.get("official_document",""),"all_dataset_full_history_certified":False,
  "notes":"Bronze individual file date bounds have NOT been audited"})
for r in old["finmind"]:
 items.append({"source":"FinMind_Backer","dataset_id":r["dataset_id"],"registry_type":"BACKER_89_CANDIDATE",
  "api_status":r.get("initial_status",""),"earliest_observed":r.get("first_observed_date",""),
  "latest_observed":r.get("last_observed_date",""),"official_page_start":"",
  "date_basis":r.get("first_observed_date_basis",""),"raw_format":r.get("raw_format","JSON"),
  "field_names":r.get("actual_fields",""),"historical_era_rules":"","sample_data_id":r.get("sample_stock_code",""),
  "evidence_url":"https://github.com/danny541690/C1-PAID-DOWNLOADER/actions/runs/45178606721",
  "all_dataset_full_history_certified":False,"notes":r.get("fixed_probe_status","")})
for r in free["rows"]:
 items.append({"source":"FinMind_Free","dataset_id":r["dataset"],"registry_type":r["registry_tier"],
  "api_status":r["final_state"],"earliest_observed":r.get("earliest_observed_date") or "",
  "latest_observed":r.get("latest_observed_date") or "","official_page_start":"",
  "date_basis":r["historical_basis"],"raw_format":"JSON_DATA_ARRAY",
  "field_names":"|".join(r.get("actual_columns",[])),"historical_era_rules":"",
  "sample_data_id":r.get("data_id_used") or "",
  "evidence_url":"https://github.com/danny541690/C1-RAW-DOWNLOADER/actions/runs/45216009366",
  "all_dataset_full_history_certified":False,"notes":"Anonymous Free mode. Pricing Free45 vs official registry candidates discrepancy"})
fb={r["dataset"]:r for r in free["rows"]};bb={r["dataset_id"]:r for r in old["finmind"]}
comparisons=[]
for ds in sorted(set(fb)|set(bb)):
 f=fb.get(ds,{});b=bb.get(ds,{})
 if not f:comparison="BACKER_ONLY_CANDIDATE"
 elif not b:comparison="FREE_ONLY_CANDIDATE"
 elif f.get("final_state")=="PASS" and b.get("initial_status")=="PASS":comparison="BOTH_OBSERVED_DATA"
 elif f.get("final_state")=="PASS":comparison="FREE_ONLY_OBSERVED"
 elif b.get("initial_status")=="PASS":comparison="BACKER_ONLY_OBSERVED"
 else:comparison="NEITHER_PROVEN"
 comparisons.append({"dataset_id":ds,"comparison":comparison,
  "free_earliest":f.get("earliest_observed_date") or "","backer_earliest":b.get("first_observed_date") or "",
  "free_latest":f.get("latest_observed_date") or "","backer_latest":b.get("last_observed_date") or "",
  "free_status":f.get("final_state","NOT_TESTED"),"backer_status":b.get("initial_status","NOT_TESTED"),
  "same_parameters":False,"note":"Different sample period or data_id; dates are not directly comparable"})
spec={"schema":"JARVIS_V098_EIGHT_SOURCE_RULES_R2","generated_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
 "sources":sources,"all_eight_sources_registered":len({x["source"] for x in items})==8,
 "official_candidate_contracts":sum(x["registry_type"]=="JARVIS_V095_202609_CANDIDATE_CONTRACT" for x in items),
 "official_documented_date_references":sum(x["registry_type"]=="OFFICIAL_PAGE_DATE_REFERENCE_NOT_ACTIVE_CONTRACT" for x in items),
 "backer_candidates":len(old["finmind"]),"free_candidates_tested":len(free["rows"]),
 "free_official_registry_count":free["official_registry_free_candidates"],
 "pricing_free":45,"pricing_backer":81,"pricing_registry_one_to_one_confirmed":False,
 "free_successful_data":free["pass"],"free_empty":free["empty"],"free_errors":free["no_data_or_error"],
 "all_six_official_bronze_data_ranges_verified":False,
 "all_finmind_symbols_date_ranges_verified":False,
 "PIT":"PUBLICATION_PLUS_ONE_DAY","records":items,"comparison":comparisons,
 "six_source_historical_format_eras":old["eras"],"taifex_candidate_endpoints":old["taifex_candidate"],
 "evidence_urls":["https://github.com/danny541690/C1-RAW-DOWNLOADER/actions/runs/45216009366",
 "https://github.com/danny541690/C1-PAID-DOWNLOADER/actions/runs/45214627608"]}
edition="20261124_R2"
files=[]
files.append(write_csv("08_八來源Dataset日期格式總帳_"+edition+".csv",
 ["source","dataset_id","registry_type","api_status","earliest_observed","latest_observed",
 "official_page_start","date_basis","raw_format","field_names","historical_era_rules",
 "sample_data_id","evidence_url","all_dataset_full_history_certified","notes"],items))
files.append(write_csv("09_FinMind_Free和Backer樣本日期差異_"+edition+".csv",
 ["dataset_id","comparison","free_earliest","backer_earliest","free_latest","backer_latest",
 "free_status","backer_status","same_parameters","note"],comparisons))
files.append(write_csv("10_FinMind_Free逐項原始日期_"+edition+".csv",
 ["dataset","registry_tier","final_state","earliest_observed_date","latest_observed_date",
 "historical_basis","data_id_used","actual_columns","actual_types","trials"],free["rows"]))
files.append(write_json("11_八大來源資料規則主帳_"+edition+".json",spec))
readme="""# JARVIS v0.98 八來源資料規則R2
TWSE、TPEx、MOPS、NDC、TDCC、TAIFEX、FinMind_Free、FinMind_Backer。
免費資料由C1-RAW-DOWNLOADER獨立匿名API測試；Backer與六官方沿用此前證據。
官方定價Free45、Backer81；API文件free候選清單數量可能不同，不能擅自認定每項有權限。
所列日期只代表指定資料集及商品的最早／最晚觀察日期；不代表全股票、全歷史完整。
股票財報1991年不代表Free台股股價1991年可用。
六大官方已有39個既有候選契約、12個官方頁面起點及23種年代格式路由；未逐筆審計原始Bronze，因此仍須保留NOT_AUDITED_PER_DATASET。
相同資料集Free和Backer並未使用完全相同查詢期間，不可據此推斷值不同。
PIT遵循公布隔日可用；不得覆蓋Bronze原件或直接放行Silver/Gold。
"""
f=ROOT/("00_八大來源規格說明_"+edition+".md");f.write_text(readme,encoding="utf-8");files.append(f)
tok=get_token();target=ensure_spec_folder(tok)
receipts=[upload_check(tok,target,f) for f in files]
receipt={"status":"PASS","files":receipts,"source_count":8,"free_tested":len(free["rows"]),
 "free_api_pass":free["pass"],"six_official_per_dataset_dates_proven":False,"all_remote_readback_pass":True}
r=ROOT/("12_八來源上傳回讀證明_"+edition+".json");r.write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding="utf-8")
receipts.append(upload_check(tok,target,r))
print("EIGHT_SOURCE_UPLOAD_PASS "+json.dumps({"files":len(receipts),"source_count":8,
 "free_tested":len(free["rows"]),"free_pass":free["pass"],"official_contracts":spec["official_candidate_contracts"],
 "sha_readback":"PASS"},ensure_ascii=False))
