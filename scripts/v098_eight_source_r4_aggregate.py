#!/usr/bin/env python3
"""JARVIS v0.98 eight parallel-lane merge to source/format/history/Gold443 registry.
Consumes eight upstream immutable GitHub artifacts and existing R2 history contract.
R4 is an evidence ledger; no dataset is declared fully covered without official &
native verified rows. Original Bronze/Silver/Gold never modified.
"""
import csv,datetime as dt,hashlib,io,json,os,pathlib,re,urllib.request,urllib.parse,collections
from v098_publish_source_period_rules import get_token,ensure_spec_folder,upload_check,write_csv,write_json
ROOT=pathlib.Path("eight_source_r4");ROOT.mkdir(exist_ok=True)
INPUT=pathlib.Path("r4_input")
DAYS=["2026-11-16","2026-11-17","2026-11-18","2026-11-19","2026-11-20"]
SOURCES=["TWSE","TPEx","MOPS","NDC","TDCC","TAIFEX","FinMind_Free","FinMind_Backer"]
def read_report(path):
 p=INPUT/path
 if not p.exists():raise RuntimeError("MISSING_ARTIFACT_"+path.replace("/","_"))
 return json.loads(p.read_text(encoding="utf-8"))
reports={s:read_report(s+"/report.json") for s in SOURCES}
old=read_report("existing/11_八大來源資料規則主帳_20261124_R2.json")
for s in SOURCES:
 r=reports[s]
 if r.get("source")!=s or r.get("five_days")!=DAYS or r.get("lane_error"):raise RuntimeError("INVALID_SOURCE_REPORT_"+s)
 if not r.get("dataset_registry"):raise RuntimeError("NO_DATASET_REGISTRY_"+s)
by_old=collections.defaultdict(list)
for r in old.get("records",[]):by_old[(r.get("source"),r.get("dataset_id"))].append(r)
allrows=[];dayrows=[];fieldrows=[];states=collections.Counter()
for source in SOURCES:
 report=reports[source]
 official=source not in ("FinMind_Free","FinMind_Backer")
 for ds in report["dataset_registry"]:
  name=ds["dataset"]
  refs=by_old.get((source,name),[])
  ref=refs[0] if refs else {}
  begin=ref.get("earliest_observed","");end=ref.get("latest_observed","")
  if begin in ("NOT_AUDITED_PER_DATASET","UNVERIFIED_AT_THIS_DATASET_ID",None):begin=""
  if end in ("NOT_AUDITED_PER_DATASET","UNVERIFIED_AT_THIS_DATASET_ID",None):end=""
  meta={
   "source":source,"dataset_id":name,"official_or_doc_registry":ds.get("tier") if not official else "V095_CANDIDATE",
   "official_reference_first":ref.get("official_page_start",""),
   "observed_history_first":begin,"observed_history_last":end,
   "history_evidence_class":ref.get("date_basis") or "NO_PER_DATASET_HISTORY_PROOF",
   "data_id":ref.get("sample_data_id","2330" if not official else ""),
   "history_is_all_market_verified":False,
   "asof":"2026-11-24","latest_published_verified":False,
   "daily_trading_sample":"", "native_schema_state":""
  }
  daily=[]
  if official:
   for day in DAYS:
    o=report.get("five_day_source_evidence",{}).get(day,{})
    daily.append((day,o.get("status","NO_EVIDENCE"),[],{},0,""))
   meta["daily_trading_sample"]="SOURCE_LEVEL_EVIDENCE_UNMAPPED_TO_DATASET"
   meta["native_schema_state"]="RAW_FILE_SAMPLES_PARTIAL"
  else:
   for o in ds["trading_day_samples"]:
    day=o["day"];info=o["result"];state=info["state"]
    daily.append((day,state,info.get("fields",[]),info.get("field_types",{}),info.get("rows",0),
      info.get("canonical_sha256","")))
    if state=="PASS":
     for field in info.get("fields",[]):
      fieldrows.append({"source":source,"dataset_id":name,"day":day,"field":field,
       "observed_type":"|".join(info.get("field_types",{}).get(field,[])),
       "source_native":"FinMind_API_JSON","field_semantic_verified":False,
       "Silver_target":"MAPPING_PENDING","Gold_dependency_approved":False,
       "lineage":"20_BRONZE>30_RAW>40_SILVER>50_GOLD"})
   meta["daily_trading_sample"]="|".join(f"{day}:{state}" for day,state,*_ in daily)
   meta["native_schema_state"]="API_JSON_RETURN_FIELDS_SAMPLED"
  allrows.append(meta)
  for day,state,fields,types,count,sha in daily:
   states[(source,state)]+=1
   dayrows.append({"source":source,"dataset_id":name,"test_day":day,"status":state,
    "record_count":count,"field_count":len(fields),"fields":"|".join(fields),
    "field_types":types,"canonical_sha256":sha,
    "historical_start":begin,"historical_end":end,
    "four_layer_lineage_status":"RAW_NATIVE_EVIDENCE_NOT_SILVER_OR_GOLD_CERTIFIED",
    "PIT":"PUBLICATION_PLUS_ONE_DAY_UNVERIFIED","comparison_status":"OFFICIAL_NATIVE_FIELDS_NOT_CERTIFIED" if official else "FINMIND_OBSERVED_FIELDS"})
# All distinct historical references not in active 39+existing candidates remain reference-only.
supplement=[]
active={(x["source"],x["dataset_id"]) for x in allrows}
for r in old["records"]:
 if (r["source"],r["dataset_id"]) in active:continue
 supplement.append({"source":r["source"],"dataset_id":r["dataset_id"],
 "catalog_status":"R2_REFERENCE_ONLY_NOT_FIVE_DAY_TESTED",
 "official_page_start":r.get("official_page_start",""),
 "observed_start":r.get("earliest_observed",""),"observed_end":r.get("latest_observed",""),
 "raw_format":r.get("raw_format",""),"source_document":r.get("evidence_url",""),
 "date_status":"NOT_REVERIFIED_IN_R4"})
for r in old.get("taifex_candidate_endpoints",[]):
 supplement.append({"source":"TAIFEX","dataset_id":r.get("path",""),"catalog_status":"TAIFEX_PATH_REFERENCE_ONLY",
  "official_page_start":"","observed_start":"","observed_end":"","raw_format":"","source_document":"",
  "date_status":"NO_ENDPOINT_DATE_VERIFICATION"})
# Gold 443 original master from user's earlier historic Drive file. No formula approval or computation.
def gold443(token):
 req=urllib.request.Request("https://www.googleapis.com/drive/v3/files/1mj0-Jr46atOhDoGRTxS-G17aShKF64PS?alt=media",
  headers={"Authorization":"Bearer "+token})
 with urllib.request.urlopen(req,timeout=35) as response:raw=response.read(2000000)
 lines=raw.decode("utf-8-sig").splitlines()
 out=[];seen=set()
 for line in lines:
  if not line.startswith("| "):continue
  c=[v.strip() for v in line.strip().strip("|").split("|")]
  if len(c)!=7 or not re.fullmatch(r"[A-Z][A-Z0-9_]*",c[0]) or c[0]=="ID":continue
  fid,name,group,unit,source,evidence,gap=c
  if fid in seen:raise RuntimeError("GOLD_443_ID_DUPLICATE")
  seen.add(fid)
  xq=fid.startswith(("MAIN_FORCE_","BROKER_","CHIP_CONCENTRATION_"))
  if xq:dep="XQ_EXTERNAL_MANUAL_NOT_BACKER_699"
  elif group in ("Price_Raw","Technical","RS","Volume"):dep="TWSE|TPEx|FinMind_Free|FinMind_Backer"
  elif "Revenue" in group or "Growth" in group:dep="MOPS|FinMind_Free|FinMind_Backer"
  elif "Quality" in group or "Financial" in group or "Valuation" in group:dep="MOPS|TWSE|TPEx|FinMind_Free|FinMind_Backer"
  elif "Ownership" in group:dep="TDCC|TWSE|TPEx|MOPS|FinMind_Free|FinMind_Backer"
  elif "Chip" in group:dep="TWSE|TPEx|FinMind_Free|FinMind_Backer"
  elif "Sector" in group:dep="TWSE|TPEx|HISTORICAL_SECTOR_MASTER"
  else:dep="SOURCE_MAPPING_PENDING"
  out.append({"Gold_ID":fid,"Gold_name":name,"group":group,"original_unit":unit,
   "original_source_label":source,"existing_evidence":evidence,"gap":gap,"candidate_source_mapping":dep,
   "RAW_contract":"SOURCE_DATASET_FIELD_PENDING","Bronze_native":"SOURCE_SHA_REQUIRED",
   "Silver_field":"SCHEMA_AND_UNIT_MAP_PENDING","Gold_formula":"DEFINITION_PENDING",
   "Gold_PIT":"PIT_UNVERIFIED","Gold_ready":False,"XQ_dependency":xq,
   "status":"CANDIDATE_ONLY_NOT_443_PROVEN"})
 if len(out)!=443:raise RuntimeError("GOLD_COUNT_NOT_443_"+str(len(out)))
 return out,hashlib.sha256(raw).hexdigest()
token=get_token()
gold,original_hash=gold443(token)
official_count=sum(len(reports[s]["dataset_registry"]) for s in SOURCES[:6])
meta={"schema":"JARVIS_V098_EIGHT_SOURCE_FIVE_DAY_R4","created_at":dt.datetime.now(dt.timezone.utc).isoformat(),
 "trading_days":DAYS,"sources":SOURCES,"official_candidate_contracts":official_count,
 "finmind_free_registry":len(reports["FinMind_Free"]["dataset_registry"]),
 "finmind_backer_registry":len(reports["FinMind_Backer"]["dataset_registry"]),
 "gold443_candidates":len(gold),"gold_source_sha256":original_hash,
 "status_counts":{f"{s}:{st}":n for (s,st),n in states.items()},
 "official_bronze_only_not_livedownload":True,
 "official_exact_dataset_field_map_complete":False,
 "all_history_date_range_certified":False,
 "all_silver_contracts_certified":False,"all_gold443_formulas_certified":False,
 "exclusions":["NO_BRONZE_MODIFICATIONS","NO_SILVER_TRANSFORM","NO_GOLD_COMPUTE",
   "NO_INTRADAY_REQUIRED","NO_UNJUSTIFIED_DATE_START"],
 "readme":"These are candidate datasets and bounded five-trading-day evidence; full historical coverage needs per-dataset Bronze/official comparison",
 "source_artifacts":{"free":"45283284088","paid":"45283451250"}}
edition="20261124_R4"
files=[]
files.append(write_csv("20_八來源實際資料項目及最早最新日期_"+edition+".csv",
 ["source","dataset_id","official_or_doc_registry","official_reference_first","observed_history_first",
 "observed_history_last","history_evidence_class","data_id","history_is_all_market_verified",
 "asof","latest_published_verified","daily_trading_sample","native_schema_state"],allrows))
files.append(write_csv("21_八來源5交易日各Dataset與表頭_"+edition+".csv",
 ["source","dataset_id","test_day","status","record_count","field_count","fields",
 "field_types","canonical_sha256","historical_start","historical_end",
 "four_layer_lineage_status","PIT","comparison_status"],dayrows))
files.append(write_csv("22_八來源觀察欄位與Silver_Gold待建對照_"+edition+".csv",
 ["source","dataset_id","day","field","observed_type","source_native","field_semantic_verified",
 "Silver_target","Gold_dependency_approved","lineage"],fieldrows))
files.append(write_csv("23_Gold443逐項來源血緣與缺口_"+edition+".csv",
 ["Gold_ID","Gold_name","group","original_unit","original_source_label","existing_evidence","gap",
 "candidate_source_mapping","RAW_contract","Bronze_native","Silver_field","Gold_formula","Gold_PIT",
 "Gold_ready","XQ_dependency","status"],gold))
files.append(write_csv("24_官方及TAIFEX未驗證參考端點_"+edition+".csv",
 ["source","dataset_id","catalog_status","official_page_start","observed_start",
 "observed_end","raw_format","source_document","date_status"],supplement))
files.append(write_json("25_八來源五日與歷史日期_R4主帳_"+edition+".json",meta))
readme=ROOT/("00_R4驗收摘要_"+edition+".md")
readme.write_text("# JARVIS v0.98 八來源R4五交易日與原始資料歷史總帳\n\n"+
 "八來源分成免費七線及Backer付費一線，測試日期2026/11/16–20。\n\n"+
 f"六大官方來源：既有v0.95候選Dataset {official_count}種；其底層Bronze僅做有限抽樣，並非直接從所有官方網站完整重抓。\n"+
 f"FinMind Free候選 {len(reports['FinMind_Free']['dataset_registry'])}種；Backer候選 {len(reports['FinMind_Backer']['dataset_registry'])}種。\n"+
 "FinMind的Free45/Backer81是定價頁方案項目數，與技術文件候選不等價，需另外對帳。\n"+
 "日資料與財報、營收、集保週資料的公布頻率不同，不可拿每日EMPTY當作缺失。\n"+
 "官方來源每個Dataset的歷史最早日期若無已認證原件或官網明確的同Dataset起點，記錄UNKNOWN/NOT_CERTIFIED。\n"+
 "六大來源正式要求原始檔SHA/日期範圍/官方文件版次、主鍵/單位；目前仍未逐Dataset完成G2正式核發。\n"+
 "Gold443只提供候選來源依賴鏈，既有443項原始稿多數公式仍為DEFINITION_PENDING；57項直接依賴XQ主力/分點不可由699全包。\n"+
 "Bronze→RAW→Silver→Gold均需分別有證據，PIT依公告日隔一天可用；本輪不執行資料轉換。\n",encoding="utf-8")
files.append(readme)
folder=ensure_spec_folder(token)
rec=[upload_check(token,folder,p) for p in files]
receipt={"schema":"JARVIS_R4_DRIVE_EVIDENCE_RECEIPT","files":rec,"readback":True,
 "source_count":8,"Gold443":len(gold),"dataset_candidates":len(allrows),"day_rows":len(dayrows),
 "field_rows":len(fieldrows),"reference_only_rows":len(supplement)}
rp=ROOT/("26_R4遠端回讀與摘要_"+edition+".json");rp.write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding="utf-8")
rec.append(upload_check(token,folder,rp))
print("JARVIS_R4_UPLOAD_PASS "+json.dumps({"sources":8,"files":len(rec),"dataset_rows":len(allrows),
 "five_day_rows":len(dayrows),"raw_field_rows":len(fieldrows),"Gold443":len(gold),
 "readback":"PASS"},ensure_ascii=False),flush=True)
