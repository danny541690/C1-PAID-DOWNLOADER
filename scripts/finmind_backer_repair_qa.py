#!/usr/bin/env python3
"""JARVIS 0.98 FinMind Backer 22-issue repair and era-schema sample QA.
Read-only FinMind; never alter Bronze. Results are saved to GitHub artifact
and optionally to C1_0.98/FinMind_Backer_QA after exact Drive folder discovery.
All data observations are samples, not complete archive certifications.
"""
import collections,csv,datetime as dt,hashlib,io,json,os,pathlib,time,urllib.parse,urllib.request,urllib.error,uuid
API="https://api.finmindtrade.com/api/v4/data"
TOKEN=os.environ["FINMIND_BACKER_TOKEN"].strip()
ROOT=pathlib.Path("finmind_repair_qa");ROOT.mkdir(exist_ok=True)
# Each previously failing dataset gets official correction and bounded probes.
CASES=[
 ("TaiwanStockInfoWithWarrant",None,"oversize"),
 ("TaiwanStockAfterHoursFixedPriceTrading","2330","empty"),
 ("TaiwanStockTotalReturnIndex","TAIEX","wrong_id"),
 ("TaiwanStockDayTradingBorrowingFeeRate",None,"empty"),
 ("TaiwanStockMarketValueWeight",None,"empty"),
 ("TaiwanFuturesDaily","TX","oversize"),
 ("TaiwanOptionDaily","TXO","timeout"),
 ("TaiwanFuturesDealerTradingVolumeDaily",None,"empty"),
 ("TaiwanOptionDealerTradingVolumeDaily",None,"empty"),
 ("TaiwanFuturesFinalSettlementPrice",None,"empty"),
 ("TaiwanOptionFinalSettlementPrice",None,"empty"),
 ("TaiwanStockConvertibleBondDaily","15131","wrong_id"),
 ("TaiwanStockConvertibleBondInstitutionalInvestors","15131","wrong_id"),
 ("TaiwanStockConvertibleBondDailyOverview","15131","wrong_id"),
 ("TaiwanAssetSwapOptionDaily",None,"oversize"),
 ("TaiwanStockNews","2330","single_day"),
 ("USStockPriceMinute","AAPL","single_day"),
 ("EuropeStockPrice","AALB.AS","wrong_id"),
 ("TaiwanExchangeRate",None,"empty"),
 ("GoldPrice",None,"oversize"),
 ("CrudeOilPrices","WTI","wrong_id"),
 ("GovernmentBondsYield","United States 10-Year","wrong_id"),
]
# Snapshot windows cover early 1990s through 2026. Empty periods are recorded.
ERA_DATASETS=[
 ("TaiwanStockFinancialStatements","2330"),
 ("TaiwanStockBalanceSheet","2330"),
 ("TaiwanStockCashFlowsStatement","2330"),
 ("TaiwanStockPrice","2330"),
 ("TaiwanStockPriceAdj","2330"),
 ("TaiwanStockMonthRevenue","2330"),
 ("TaiwanStockInstitutionalInvestorsBuySell","2330"),
 ("TaiwanStockMarginPurchaseShortSale","2330"),
 ("TaiwanStockShareholding","2330"),
 ("TaiwanStockHoldingSharesPer","2330"),
 ("TaiwanStockPER","2330"),
 ("TaiwanStockMarketValue","2330"),
]
ERAS=[("1991","1991-01-01","1991-12-31"),("1995","1995-01-01","1995-12-31"),
      ("2000","2000-01-01","2000-12-31"),("2008","2008-01-01","2008-12-31"),
      ("2012","2012-01-01","2012-12-31"),("2018","2018-01-01","2018-12-31"),
      ("2026","2026-01-01","2026-11-20")]
# Limit requests for high volume data to bounded days, never try 1991->2026 in one call.
PROBES=[("2026","2026-11-20","2026-11-20"),("2024","2024-01-02","2024-01-08"),
        ("2018","2018-01-02","2018-01-05")]
def call(ds,stock,start,end,single=False):
 p={"dataset":ds,"start_date":start}
 if stock:p["data_id"]=stock
 if not single:p["end_date"]=end
 req=urllib.request.Request(API+"?"+urllib.parse.urlencode(p),
     headers={"Authorization":"Bearer "+TOKEN,"Accept":"application/json","User-Agent":"JARVIS-v0.98-Backer-repair-R1"})
 entry={"dataset":ds,"data_id":stock,"start_date":start,"end_date":None if single else end,
        "single_day":single}
 try:
  with urllib.request.urlopen(req,timeout=38) as f:
   raw=f.read(4000001);http=f.status
  if len(raw)>4000000:return {**entry,"status":"LIMIT_4MB","http_status":http}
  obj=json.loads(raw.decode("utf-8-sig"))
  if obj.get("status")!=200:return {**entry,"status":"API_REJECTED","api_status":obj.get("status"),"http_status":http}
  data=obj.get("data")
  if not isinstance(data,list):return {**entry,"status":"BAD_DATA","http_status":http}
  if not data:return {**entry,"status":"EMPTY","rows":0,"http_status":http}
  if not all(isinstance(x,dict) for x in data):return {**entry,"status":"BAD_ROW"}
  cols=sorted(set().union(*(set(x) for x in data)))
  types={key:sorted({type(x[key]).__name__ for x in data if key in x}) for key in cols}
  dates=sorted({str(x.get("date"))[:10] for x in data if x.get("date")})
  accounts=sorted({str(x.get("type")) for x in data if x.get("type") is not None})
  native=hashlib.sha256(raw).hexdigest()
  return {**entry,"status":"PASS","rows":len(data),"http_status":http,"columns":cols,
          "field_types":types,"min_date":dates[0] if dates else None,
          "max_date":dates[-1] if dates else None,"account_count":len(accounts),
          "account_type_hash":hashlib.sha256(json.dumps(accounts,ensure_ascii=False).encode()).hexdigest(),
          "account_type_sample":accounts[:25],"first_row":data[0],"response_sha256":native}
 except urllib.error.HTTPError as e:
  return {**entry,"status":"HTTP_ERROR","http_status":e.code}
 except Exception as e:
  return {**entry,"status":"TRANSPORT_OR_PARSE_ERROR","error_type":type(e).__name__}
def write_report(name,obj):
 p=ROOT/name;p.write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding="utf-8")
 return p
def upload_drive(files):
 creds=[os.environ.get(k,"").strip() for k in ("GOOGLE_CLIENT_ID","GOOGLE_CLIENT_SECRET","GOOGLE_REFRESH_TOKEN")]
 if not all(creds):return {"state":"SKIPPED_MISSING_OAUTH_SECRET"}
 try:
  body=urllib.parse.urlencode({"client_id":creds[0],"client_secret":creds[1],
        "refresh_token":creds[2],"grant_type":"refresh_token"}).encode()
  req=urllib.request.Request("https://oauth2.googleapis.com/token",data=body,headers={"Content-Type":"application/x-www-form-urlencoded"})
  with urllib.request.urlopen(req,timeout=25) as f:access=json.load(f)["access_token"]
  def drive(endpoint,method="GET",data=None,ctype=None):
   h={"Authorization":"Bearer "+access}
   if ctype:h["Content-Type"]=ctype
   req=urllib.request.Request(endpoint,data=data,headers=h,method=method)
   with urllib.request.urlopen(req,timeout=30) as f:return json.load(f)
  base="https://www.googleapis.com/drive/v3/files"
  def find(name,parent=None):
   q="name = '"+name.replace("'","\\'")+"' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
   if parent:q+=" and '"+parent+"' in parents"
   url=base+"?"+urllib.parse.urlencode({"q":q,"fields":"nextPageToken,files(id,name,parents),incompleteSearch","pageSize":"100","spaces":"drive"})
   res=drive(url)
   if res.get("nextPageToken") or res.get("incompleteSearch"):raise RuntimeError("INCOMPLETE_FOLDER_SEARCH")
   return res.get("files",[])
  roots=find("C1_0.98")
  if len(roots)!=1:return {"state":"BLOCKED_C1_0_98_FOLDER_NOT_UNIQUE","folder_count":len(roots)}
  parent=roots[0]["id"]
  existing=find("FinMind_Backer_QA",parent)
  if len(existing)>1:return {"state":"BLOCKED_QA_FOLDER_DUPLICATE"}
  if existing:parent=existing[0]["id"]
  else:
   req=json.dumps({"name":"FinMind_Backer_QA","mimeType":"application/vnd.google-apps.folder","parents":[parent]}).encode()
   parent=drive(base+"?fields=id,name","POST",req,"application/json")["id"]
  result=[]
  for file in files:
   raw=file.read_bytes()
   metadata=json.dumps({"name":file.name.replace(".json","_")+os.getenv("GITHUB_RUN_ID","local")+".json","parents":[parent]},ensure_ascii=False).encode()
   boundary=uuid.uuid4().hex
   data=(("--"+boundary+"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode()+metadata+
      ("\r\n--"+boundary+"\r\nContent-Type: application/json\r\n\r\n").encode()+raw+
      ("\r\n--"+boundary+"--\r\n").encode())
   endpoint="https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,md5Checksum,size"
   obj=drive(endpoint,"POST",data,"multipart/related; boundary="+boundary)
   result.append({"file_id":obj["id"],"name":obj["name"],"sha256":hashlib.sha256(raw).hexdigest(),"bytes":len(raw)})
  return {"state":"PASS_UPLOADED","qa_folder_id":parent,"files":result}
 except Exception as e:return {"state":"DRIVE_UPLOAD_FAILED","error_type":type(e).__name__}
def main():
 issues=[];era=[]
 for ds,ident,reason in CASES:
  for label,start,end in PROBES:
   r=call(ds,ident,start,end,single=reason=="single_day")
   r.update({"original_error_class":reason,"probe_label":label})
   issues.append(r)
   print(json.dumps({"category":"repair","dataset":ds,"year":label,"status":r["status"],"rows":r.get("rows")},ensure_ascii=False),flush=True)
   time.sleep(.25)
 for ds,ident in ERA_DATASETS:
  for label,start,end in ERAS:
   # For daily series and financial periods, use short windows within the era
   # to keep bounded payload; do not imply an entire year's schema is proven.
   if label=="1991": e="1991-12-31"
   elif label=="1995":e="1995-12-31"
   elif label=="2000":e="2000-12-31"
   elif label=="2008":e="2008-12-31"
   elif label=="2012":e="2012-12-31"
   elif label=="2018":e="2018-12-31"
   else:e="2026-09-30"
   start=(label+"-01-01") if "Financial" in ds or "BalanceSheet" in ds or "CashFlows" in ds else label+"-01-02"
   end=e if ("Financial" in ds or "BalanceSheet" in ds or "CashFlows" in ds or "MonthRevenue" in ds) else label+"-02-15"
   r=call(ds,ident,start,end);r["era"]=label;era.append(r)
   print(json.dumps({"category":"schema","dataset":ds,"era":label,"status":r["status"],"rows":r.get("rows"),"column_count":len(r.get("columns",[]))},ensure_ascii=False),flush=True)
   time.sleep(.2)
 # Structural difference: base field union and type changes; account sets change
 by=collections.defaultdict(list)
 for x in era:by[x["dataset"]].append(x)
 deltas={}
 for ds,rs in by.items():
  present=[x for x in rs if x["status"]=="PASS"]
  union=set().union(*(set(x["columns"]) for x in present)) if present else set()
  deltas[ds]={"successful_eras":[x["era"] for x in present],
   "field_union":sorted(union),"era_missing_columns":{x["era"]:sorted(union-set(x["columns"])) for x in present},
   "different_field_sets":len({tuple(x["columns"]) for x in present})>1,
   "account_type_hash_by_era":{x["era"]:x.get("account_type_hash") for x in present if x.get("account_count")},
   "caveat":"Sampled windows only; account-list changes are legitimate vintage changes unless contract says otherwise"}
 paths=[write_report("repair_22.json",{"target_count":len(CASES),"probes":issues}),
        write_report("era_schema.json",{"eras":ERAS,"results":era,"comparison":deltas})]
 drive=upload_drive(paths)
 control={"run_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
  "original_issues":22,"repair_queries":len(issues),"repair_pass":sum(x["status"]=="PASS" for x in issues),
  "repaired_dataset_names":sorted({x["dataset"] for x in issues if x["status"]=="PASS"}),
  "era_queries":len(era),"era_pass":sum(x["status"]=="PASS" for x in era),
  "drive":drive,"official_references":["https://finmind.github.io/WhatIsNew/","https://finmind.github.io/tutor/TaiwanMarket/ConvertibleBond/",
    "https://finmind.github.io/DataQuality/TaiwanStockFinancialStatements/"],
  "limitations":["This is bounded sample QA, not full historical completeness",
   "A different account-type list between years does not prove erroneous schema",
   "No Bronze writes, no Silver or Gold conversion","No raw token values exported"]}
 write_report("summary.json",control)
 print("FINAL_SUMMARY "+json.dumps(control,ensure_ascii=False),flush=True)
if __name__=="__main__":main()
