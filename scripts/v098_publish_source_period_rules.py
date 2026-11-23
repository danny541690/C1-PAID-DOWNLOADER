#!/usr/bin/env python3
"""JARVIS v0.98 evidence-graded Backer + six official source rules registry.
Read GitHub-hosted original contracts, prior JARVIS API artifacts and Drive layout.
Publish versioned, immutable copies to C1_0.98/10_SOURCES/00_資料規則表.
Never rewrite Bronze/Silver/Gold, never present a guessed date as verified.
"""
import ast,csv,datetime as dt,hashlib,io,json,os,pathlib,urllib.parse,urllib.request,urllib.error,uuid
ROOT=pathlib.Path("source_spec_output");ROOT.mkdir(exist_ok=True)
RAW="https://raw.githubusercontent.com/danny541690/C1-RAW-DOWNLOADER/main/"
DRIVE_ROOT="1FzqFPBOKQH2mk324dZAtG7D5z_hGdwoa"
DRIVE_SOURCES="1tl-tp35IXSWhDPnrPA_UVuei-E5ZbZ4z"
EDITION="20261124"
def download(path):
  with urllib.request.urlopen(urllib.request.Request(RAW+path,headers={"User-Agent":"JARVIS-C1-v098-source-rule-registry"}),timeout=40) as r:return r.read().decode("utf-8-sig")
def csv_rows(content):return list(csv.DictReader(io.StringIO(content)))
def parse_eras(content):
  module=ast.parse(content)
  for node in module.body:
    if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="OFFICIAL_VERSIONS" for t in node.targets):
      return ast.literal_eval(node.value)
  raise ValueError("NO_OFFICIAL_ERA_REGISTRY")
def load_local(name):
 p=pathlib.Path("artifact_input")/name
 if not p.is_file():raise FileNotFoundError(name)
 return json.loads(p.read_text(encoding="utf-8"))
def write_csv(name,headers,rows):
 p=ROOT/name
 with p.open("w",newline="",encoding="utf-8-sig") as f:
  w=csv.DictWriter(f,fieldnames=headers,extrasaction="ignore");w.writeheader()
  for row in rows:w.writerow({k:("|".join(map(str,row[k])) if isinstance(row.get(k),list) else
                                json.dumps(row[k],ensure_ascii=False) if isinstance(row.get(k),dict) else row.get(k,""))
                               for k in headers})
 return p
def write_json(name,obj):
 p=ROOT/name;p.write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding="utf-8");return p
def get_token():
 p=urllib.parse.urlencode({"client_id":os.environ["GOOGLE_CLIENT_ID"],"client_secret":os.environ["GOOGLE_CLIENT_SECRET"],
   "refresh_token":os.environ["GOOGLE_REFRESH_TOKEN"],"grant_type":"refresh_token"}).encode()
 req=urllib.request.Request("https://oauth2.googleapis.com/token",data=p,headers={"Content-Type":"application/x-www-form-urlencoded"})
 with urllib.request.urlopen(req,timeout=35) as r:return json.load(r)["access_token"]
def drive(token,url,method="GET",data=None,ctype=None,raw=False):
 h={"Authorization":"Bearer "+token}
 if ctype:h["Content-Type"]=ctype
 req=urllib.request.Request(url,headers=h,method=method,data=data)
 with urllib.request.urlopen(req,timeout=50) as r:
  response=r.read(5000000)
  return response if raw else (json.loads(response) if response else {})
def drive_metadata(token,id):
 return drive(token,"https://www.googleapis.com/drive/v3/files/"+id+"?fields=id,name,mimeType,parents")
def list_folder(token,id):
 q="'"+id+"' in parents and trashed = false"
 url="https://www.googleapis.com/drive/v3/files?"+urllib.parse.urlencode({"q":q,"pageSize":100,
   "fields":"nextPageToken,files(id,name,mimeType,parents)"})
 obj=drive(token,url)
 if obj.get("nextPageToken"):raise RuntimeError("FOLDER_LIST_INCOMPLETE")
 return obj.get("files",[])
def ensure_spec_folder(token):
 main=drive_metadata(token,DRIVE_ROOT);src=drive_metadata(token,DRIVE_SOURCES)
 if main.get("name")!="C1_0.98" or src.get("name")!="10_SOURCES" or DRIVE_ROOT not in src.get("parents",[]):
  raise RuntimeError("TARGET_DRIVE_IDENTITY_MISMATCH")
 existing=[x for x in list_folder(token,DRIVE_SOURCES) if x["name"]=="00_資料規則表"]
 if len(existing)>1:raise RuntimeError("DUPLICATE_SPEC_FOLDERS")
 if existing:
  if existing[0]["mimeType"]!="application/vnd.google-apps.folder":raise RuntimeError("SPEC_NAME_NOT_FOLDER")
  return existing[0]["id"]
 obj=drive(token,"https://www.googleapis.com/drive/v3/files?fields=id,name",method="POST",
   data=json.dumps({"name":"00_資料規則表","mimeType":"application/vnd.google-apps.folder","parents":[DRIVE_SOURCES]}).encode(),
   ctype="application/json")
 return obj["id"]
def upload_check(token,parent,file):
 name=file.name
 if any(x["name"]==name for x in list_folder(token,parent)):raise RuntimeError("SPEC_VERSION_ALREADY_EXISTS_"+name)
 raw=file.read_bytes()
 metadata=json.dumps({"name":name,"parents":[parent],"mimeType":"text/csv" if name.endswith(".csv") else
        "application/json" if name.endswith(".json") else "text/markdown"},ensure_ascii=False).encode()
 boundary="jarvis-"+uuid.uuid4().hex
 body=("--"+boundary+"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode()+metadata+(
      "\r\n--"+boundary+"\r\nContent-Type: application/octet-stream\r\n\r\n").encode()+raw+(
      "\r\n--"+boundary+"--\r\n").encode()
 obj=drive(token,"https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,size,md5Checksum",
           "POST",body,"multipart/related; boundary="+boundary)
 copy=drive(token,"https://www.googleapis.com/drive/v3/files/"+obj["id"]+"?alt=media",raw=True)
 if hashlib.sha256(raw).digest()!=hashlib.sha256(copy).digest():raise RuntimeError("REMOTE_READBACK_MISMATCH_"+name)
 return {"name":name,"file_id":obj["id"],"bytes":len(raw),"sha256":hashlib.sha256(raw).hexdigest(),"readback":"PASS"}
DOC_DATES=[
 ("TWSE","MARKET_HIGHLIGHT_FMQIK","1990-01-04","https://www.twse.com.tw/en/trading/historical/fmtqik.html","MARKET_TOTAL_NOT_PER_STOCK"),
 ("TWSE","STOCK_DAY_AVG","1999-01-05","https://www.twse.com.tw/en/trading/historical/stock-day-avg.html","CLOSE_AND_MONTH_AVG_ONLY"),
 ("TWSE","STOCK_DAY","2010-01-04","https://www.twse.com.tw/en/trading/historical/stock-day.html","PER_STOCK_TRADING_VOLUME_AND_VALUE"),
 ("TWSE","TAIEX_INDEX_HIST","1999-01-05","https://www.twse.com.tw/en/indices/taiex/mi-5min-hist.html","INDEX_NOT_PER_STOCK"),
 ("TWSE","A05E_PAID_QUOTE_PRODUCT","1992-01-04","https://eshop.twse.com.tw/en/product/detail/ef7b7785e2cb4793baca3644c8a74d4e","PAID_PRODUCT_NOT_PUBLIC_OPENAPI"),
 ("TPEx","IndividualDailyInfo","1994-01","https://www.tpex.org.tw/en-us/mainboard/trading/info/stock-pricing.html","INDIVIDUAL_SECURITY_MONTH_PRECISION"),
 ("TPEx","DailyQuotesBulk","2003-08","https://www.tpex.org.tw/en-us/mainboard/trading/info/pricing.html","HISTORICAL_BULK_EXCLUDES_PRE_2003_08"),
 ("TDCC","ShareholderDistribution","2008-07","https://www.tdcc.com.tw/portal/zh/smWeb/qryStock","HISTORIC_CONSTRUCTED_SINCE_2008_07;PUBLIC_SEARCH_ONE_YEAR_RETENTION"),
 ("TAIFEX","FuturesDailyGeneral","1998-07","https://www.taifex.com.tw/enl/eng1/historyOfTaifex","MARKET_LAUNCH_NOT_PROOF_EVERY_DATASET"),
 ("TAIFEX","InstitutionalFutureContractDate","2008-04-07","https://www.taifex.com.tw/enl/eng3/futContractsDateView?menuid1=03","OFFICIAL_DATE_TRACE"),
 ("TAIFEX","OptionsDailySchemaChange","2022-03-04","https://www.taifex.com.tw/enl/eng3/optDailyMarketView?menuid1=03","ADDED_CHANGE_AND_CHANGE_PERCENT_COLUMNS"),
 ("NDC","BusinessSignalHistorical","1978","https://www.ndc.gov.tw/nc_335_2237","HISTORIC_PERIOD_NOT_UNIFORM_FORMULA"),
]
def main():
 contracts=csv_rows(download("governance/v095/DATASET_CONTRACTS.csv"))
 history=json.loads(download("config/history_source_registry.json"))
 eras=parse_eras(download("scripts/v097_seven_historical_format_registry.py"))
 taifex=json.loads(download("scripts/taifex_verified_catalog.json"))
 first=load_local("backer_initial/report.json")
 repair=load_local("backer_repair/repair_22.json")
 vintage=load_local("backer_repair/era_schema.json")
 f_rows=[]
 remediation={}
 for r in repair["probes"]:remediation.setdefault(r["dataset"],[]).append(r)
 for r in first["rows"]:
  cases=remediation.get(r["dataset"],[])
  success=[x for x in cases if x.get("status")=="PASS"]
  f_rows.append({"source":"FinMind_Backer","dataset_id":r["dataset"],"tier_reference":r["tier"],
    "initial_status":r["state"],"raw_format":"JSON_DATA_ARRAY","api_endpoint":r.get("endpoint","/api/v4/data"),
    "first_observed_date":r.get("first_observed_date") or "",
    "last_observed_date":r.get("last_observed_date") or "",
    "first_observed_date_basis":r.get("date_basis","UNVERIFIED"),
    "sample_stock_code":r.get("data_id") or "",
    "initial_sample_rows":r.get("rows") if r.get("rows") is not None else "",
    "actual_fields":"|".join(r.get("columns",[])),
    "fixed_probe_status":"PASS_BOUNDED_SAMPLE" if success else "NO_VALID_REPAIR_SAMPLE" if cases else "NOT_REQUIRED",
    "fixed_sample_min":min([x.get("min_date") for x in success if x.get("min_date")],default=""),
    "fixed_sample_max":max([x.get("max_date") for x in success if x.get("max_date")],default=""),
    "history_completeness":"NOT_CERTIFIED_ALL_SYMBOLS_ALL_DATES",
    "valid_from":"NOT_VERIFIED_WHOLE_MARKET",
    "valid_to":"NOT_VERIFIED_LATEST_PUBLISHED",
    "field_contract":"OBSERVED_SAMPLED_DATA_ONLY",
    "evidence_run":"45178606721|45186591900"})
 # special CB probe exists in 45187279213, not repeat FinMind calls
 for row in f_rows:
  if row["dataset_id"] in ("TaiwanStockConvertibleBondDaily",
    "TaiwanStockConvertibleBondInstitutionalInvestors","TaiwanStockConvertibleBondDailyOverview"):
   row["fixed_probe_status"]="PASS_CONFIRMED_CB_11011_2026_11_16_TO_20"
   row["fixed_sample_min"]="2026-11-16";row["fixed_sample_max"]="2026-11-20"
   row["evidence_run"]+="|45187279213"
 official=[]
 for row in contracts:
  src=row["source_class_id"]
  official.append({"source":src,"dataset_id":row["dataset_id"],"dataset_family":row["dataset_id"].split("_202609_",1)[-1],
   "catalog_class":"JARVIS_V095_202609_CANDIDATE_CONTRACT",
   "frequency":"SOURCE_DEPENDENT","raw_format":row.get("raw_format",""),
   "primary_keys":row.get("primary_keys",""),"original_unit":row.get("native_unit",""),
   "normalized_unit":row.get("standard_unit",""),"pilot_contract_effective_start":row.get("effective_start",""),
   "documented_earliest":"UNVERIFIED_AT_THIS_DATASET_ID",
   "actual_bronze_min_date":"NOT_AUDITED_PER_DATASET",
   "actual_bronze_max_date":"NOT_AUDITED_PER_DATASET",
   "latest_published":"NOT_QUERIED_IN_THIS_RUN",
   "era_rules":",".join([p["id"] for p in eras[src]["periods"]]),
   "PIT_contract":"PUBLISHED_PLUS_1_DAY_NEEDS_PUBLISHED_AT",
   "verify_status":"CANDIDATE_NEEDS_DATASET_BREADTH_CHECK",
   "official_document":"","caution":"2026-09 pilot contract date is NOT historical earliest date"})
 for src,ds,begin,url,caution in DOC_DATES:
  official.append({"source":src,"dataset_id":ds,"dataset_family":ds,
   "catalog_class":"OFFICIAL_PAGE_DATE_REFERENCE_NOT_ACTIVE_CONTRACT",
   "frequency":"SOURCE_DEPENDENT","raw_format":"SOURCE_DEPENDENT","primary_keys":"SOURCE_DEFINED",
   "original_unit":"SOURCE_DEFINED","normalized_unit":"SOURCE_DEFINED",
   "pilot_contract_effective_start":"","documented_earliest":begin,
   "actual_bronze_min_date":"NOT_AUDITED_PER_DATASET","actual_bronze_max_date":"NOT_AUDITED_PER_DATASET",
   "latest_published":"NOT_QUERIED_IN_THIS_RUN",
   "era_rules":",".join([p["id"] for p in eras[src]["periods"]]),
   "PIT_contract":"PUBLISHED_PLUS_1_DAY_NEEDS_PUBLISHED_AT",
   "verify_status":"OFFICIAL_DOCUMENTED_ENDPOINT_START_NOT_BULK_COMPLETENESS",
   "official_document":url,"caution":caution})
 historical=[]
 for src in ("TWSE","TPEx","MOPS","NDC","TDCC","TAIFEX"):
  for e in eras[src]["periods"]:
   historical.append({"source":src,"start_month":e["start"],"end_month":e["end"],
    "era_id":e["id"],"format_notes":e["format"],"historical_warning":eras[src].get("historical_warning",""),
    "quality_contract":";".join(eras[src].get("strict",[])),
    "source_links":"|".join(eras[src].get("reference",[])),
    "classification":"PARSER_TEMPLATE_ROUTING_NOT_EVIDENCE_OF_LIVE_RAW_BYTES"})
 tpaths=taifex.get("api_paths",{})
 taifex_rows=[{"source":"TAIFEX","path":k,"description":v,
   "historical_earliest":"UNVERIFIED","historical_latest":"UNVERIFIED",
   "dataset_status":"OFFICIAL_ENDPOINT_CATALOG_NOT_DOWNLOADED_OR_CERTIFIED"} for k,v in sorted(tpaths.items())]
 vintage_rows=[]
 for r in vintage["results"]:
  vintage_rows.append({"source":"FinMind_Backer","dataset":r["dataset"],"era":r["era"],"state":r["status"],
   "row_count":r.get("rows",""),"fields":"|".join(r.get("columns",[])),
   "field_types":r.get("field_types",{}),"account_type_count":r.get("account_count",""),
   "account_type_hash":r.get("account_type_hash",""),
   "sample_min_date":r.get("min_date",""),"sample_max_date":r.get("max_date",""),
   "vintage_inference":"SAMPLED_PERIOD_ONLY_NOT_ENTIRE_ERA"})
 guide="""# JARVIS v0.98 資料來源與歷史格式規則（版控）
版本：2026-11-24 | 僅供 Bronze/RAW G01、G02 验收規則參照；未核發Silver/Gold

## 證據層級
1. BACKER_API_OBSERVED：指定代表股票/商品的API回傳最早與最晚；不可代表全市場最早，也不代表全歷史無缺口。
2. OFFICIAL_DOCUMENTED：官方網站明確註明的資料頁面起始日；不能替換其他資料集、不同授權方式之日期。
3. CONTRACT_CANDIDATE：既有JARVIS六大來源2026/09試驗契約；2026/09是契約試驗日，不是資料歷史起點。
4. ERA_SCHEMA_CANDIDATE：舊v0.97不同年代模板；正式入庫仍須和實際Bronze欄位逐筆比較。
5. DRIVE_YEAR_FOLDERS：v0.98 20_BRONZE 已有2000–2026年資料夾；資料夾存在不表示各來源各dataset有內容或通過QA。

## 統計
- FinMind官方定價頁Backer為81種；FinMind官方公開dataset規格文檔這次測試為89種Free/Backer候選，兩清單尚未一一對帳。
- 前次89項候選首次67項有資料、15項空、7項HTTP/超大。
- 22個原異常項目經有界抽樣及有效可轉債代碼補驗後，已各取得成功取數樣本。
- 六來源現存v0.95 2026/09契約共39列，另有12條官方頁面歷史起點參考（非全產品目錄）。
- TAIFEX另有大量歷史/統計API候選路徑，不代表已逐個拿到數值。
- 12類FinMind × 7年代抽樣84次、51次有資料，有資料樣本未發現頂層表頭變化；type會計科目數/語意可能變更。

## 格式與可用性
- 每Dataset都需要記錄實際欄位SHA、歷史era、native_unit、主鍵、空值規則、market_scope、首次公告時間、修訂版本、source_sha與回讀SHA。
- GAAP→IFRS、XBRL/iXBRL及NDC景氣信號構成變動不可單純依表頭做完整認證。
- TDCC 2008-07起建立歷史檔，公開查詢保存一年；2000年資料夾不代表當時有TDCC週分散表。
- 期交所日夜盤歸屬交易日變動；2022/03/04選擇權日表新增漲跌欄位，須路由格式模板。
- 1994年2330價格是FinMind實測，不是六大官方的TWSE全市場股價日期。TWSE不同資訊頁起始時間各異（1990/1999/2010），不能互換。
- PIT依使用者約定公布日隔日才使用，缺published_at時標PIT_UNVERIFIED；日期為財報期末不等於公布日。
- 本版未做六大官方全歷史逐Dataset真實內容掃描，請保持DATASET_HISTORY_UNVERIFIED狀態，不要當成Gold放行。

## 不覆寫原則
原本R3架構規格和六大官方Bronze皆未修改；本規則為新增獨立版控快照，不變更production工作流。

## 證據
- GitHub Backer Run 45178606721 / 45186591900 / 45187279213。
- GitHub raw規格 governance/v095/DATASET_CONTRACTS.csv、scripts/v097_seven_historical_format_registry.py、scripts/taifex_verified_catalog.json。
- 參考來源網址詳各CSV列。
"""
 files=[]
 files.append(write_csv("01_FinMind_Backer_89項觀察日期_20261124.csv",
    ["source","dataset_id","tier_reference","initial_status","raw_format","api_endpoint","first_observed_date",
    "last_observed_date","first_observed_date_basis","sample_stock_code","initial_sample_rows","actual_fields",
    "fixed_probe_status","fixed_sample_min","fixed_sample_max","history_completeness","valid_from",
    "valid_to","field_contract","evidence_run"],f_rows))
 files.append(write_csv("02_六大官方來源Dataset_歷史日期規則_20261124.csv",
    ["source","dataset_id","dataset_family","catalog_class","frequency","raw_format","primary_keys",
    "original_unit","normalized_unit","pilot_contract_effective_start","documented_earliest",
    "actual_bronze_min_date","actual_bronze_max_date","latest_published","era_rules",
    "PIT_contract","verify_status","official_document","caution"],official))
 files.append(write_csv("03_六大官方歷史格式年代模板_20261124.csv",
    ["source","start_month","end_month","era_id","format_notes","historical_warning","quality_contract",
    "source_links","classification"],historical))
 files.append(write_csv("04_TAIFEX_API候選端點參考_20261124.csv",
    ["source","path","description","historical_earliest","historical_latest","dataset_status"],taifex_rows))
 files.append(write_csv("05_FinMind_1991到2026年代Schema抽樣_20261124.csv",
    ["source","dataset","era","state","row_count","fields","field_types","account_type_count",
    "account_type_hash","sample_min_date","sample_max_date","vintage_inference"],vintage_rows))
 master={"schema":"JARVIS_v098_Source_Dataset_History_Spec_v1","edition":EDITION,
  "objective":"DATA_SOURCE_HISTORY_AND_ERA_SCHEMA_RULES","sources":["FinMind_Backer","TWSE","TPEx","MOPS","NDC","TDCC","TAIFEX"],
  "statuses":{"finmind_candidates":len(f_rows),"official_v095_contracts":len(contracts),
       "official_date_reference_rows":len(DOC_DATES),"official_eras":len(historical),
       "taifex_candidate_endpoints":len(taifex_rows),"finmind_vintage_samples":len(vintage_rows)},
  "latest_published_source_dates_proven":False,"six_official_per_dataset_history_certified":False,
  "finmind_official_count_discrepancy":"81 plan vs 89 tested eligible candidates; unresolved",
  "finmind":f_rows,"official":official,"eras":historical,"taifex_candidate":taifex_rows,
  "vintage":vintage_rows,"raw_sources":["governance/v095/DATASET_CONTRACTS.csv",
    "config/history_source_registry.json","scripts/v097_seven_historical_format_registry.py",
    "scripts/taifex_verified_catalog.json"],"policy":"IMMUTABLE_REPORTS_PIT_D_PLUS_ONE_NO_BRONZE_MODIFICATION"}
 files.append(write_json("06_JARVIS_v098_來源資料總規格_20261124.json",master))
 gp=ROOT/"00_讀我_來源規格與證據限制_20261124.md";gp.write_text(guide,encoding="utf-8");files.append(gp)
 token=get_token()
 target=ensure_spec_folder(token)
 receipts=[]
 for f in files:
  receipt=upload_check(token,target,f)
  receipts.append(receipt)
 receipt={"status":"PASS","parent_folder_id":target,"files":receipts,
   "counts":master["statuses"],"run_id":os.getenv("GITHUB_RUN_ID"),"all_remote_readback_pass":True}
 (ROOT/"07_上傳回讀驗收履歷_20261124.json").write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding="utf-8")
 last=upload_check(token,target,ROOT/"07_上傳回讀驗收履歷_20261124.json")
 # Avoid printing Drive IDs in PUBLIC GitHub logs.
 print("SPEC_UPLOAD_VERIFIED "+json.dumps({"status":"PASS","files":len(files)+1,
  "counts":master["statuses"],"readback_verified":True},ensure_ascii=False))
if __name__=="__main__":main()
