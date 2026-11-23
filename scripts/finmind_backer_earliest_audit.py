#!/usr/bin/env python3
import csv,datetime as dt,json,re,os,time,urllib.parse,urllib.request,urllib.error,hashlib
from pathlib import Path
DOC="https://raw.githubusercontent.com/FinMind/FinMind-MCP/master/knowledge/datasets.md"
API="https://api.finmindtrade.com/api/v4/data"
TOKEN=os.environ["FINMIND_BACKER_TOKEN"].strip()
TODAY=dt.date.today().isoformat(); EARLIEST="1900-01-01"; DEFAULT_DAY="2026-11-20"
ID_MAP={"股票代號":"2330","2330":"2330","期貨代號":"TX","選擇權代號":"TXO","可轉債代號":"23301","美股代號":"AAPL","英股代號":"HSBA.L","歐股代號":"SIE.DE","日股代號":"7203.T","央行代號":"FED","WTI 或":"WTI","TAIEX 或":"TAIEX"}
SINGLE={"TaiwanStockPriceTick":"2018-12-07","TaiwanFuturesTick":"2011-01-03","TaiwanOptionTick":"2011-01-03"}
def read_doc():
 req=urllib.request.Request(DOC,headers={"User-Agent":"JARVIS-v0.98-Backer-Audit"})
 with urllib.request.urlopen(req,timeout=40) as r:return r.read().decode()
def parse_registry(md):
 out=[]
 for block in re.split(r"(?m)^### ",md)[1:]:
  name=block.splitlines()[0].strip()
  tm=re.search(r"(?m)^\\s*\\* Tier:\\s*(.+)$",block)
  rm=re.search(r"(?m)^\\s*\\* Required:\\s*(.+)$",block)
  cm=re.search(r"(?m)^\\s*\\* Key columns:\\s*(.+)$",block)
  if not tm or not rm:continue
  tier=tm.group(1).strip()
  if tier not in ("Free","Free(w/ data_id)","Backer"):continue
  req=rm.group(1);data_id=None
  if "data_id" in req:
   for label,value in ID_MAP.items():
    if label in req:data_id=value;break
   if data_id is None:data_id="2330"
  single="single day" in req.lower()
  span=re.search(r"(?:資料區間|涵蓋|回溯至)\\s*((?:19|20)\\d{2}-\\d{2}-\\d{2})\\s*(?:~|至)\\s*(?:now|(?:19|20)\\d{2}-\\d{2}-\\d{2})",block,re.I)
  out.append({"dataset":name,"tier":tier,"endpoint":"/api/v4/data","required":req,
   "expected_columns":cm.group(1).strip() if cm else "","data_id":data_id,"single_day":single,
   "documented_start":span.group(1) if span else None})
 return out
def query(d):
 p={"dataset":d["dataset"]}
 if d["data_id"]:p["data_id"]=d["data_id"]
 if d["dataset"] in SINGLE:p["start_date"]=SINGLE[d["dataset"]]
 elif d["single_day"]:p["start_date"]=DEFAULT_DAY
 else:p.update(start_date=EARLIEST,end_date=TODAY)
 req=urllib.request.Request(API+"?"+urllib.parse.urlencode(p),headers={"Authorization":"Bearer "+TOKEN,"Accept":"application/json","User-Agent":"JARVIS-v0.98-Backer-81-Audit"})
 try:
  with urllib.request.urlopen(req,timeout=75) as r:status=r.status;raw=r.read(20000001)
  if len(raw)>20000000:return p,{"state":"RESPONSE_SIZE_LIMIT","http_status":status}
  obj=json.loads(raw.decode("utf-8-sig"))
  if obj.get("status")!=200:return p,{"state":"API_REJECTED","http_status":status,"api_status":obj.get("status"),"message":str(obj.get("msg") or obj.get("message") or "")[:300]}
  data=obj.get("data")
  if not isinstance(data,list):return p,{"state":"INVALID_RESPONSE_SHAPE","http_status":status}
  if not data:return p,{"state":"EMPTY","http_status":status,"rows":0,"columns":[]}
  if not all(isinstance(x,dict) for x in data):return p,{"state":"NON_OBJECT_ROWS","http_status":status}
  cols=sorted(set().union(*(set(x) for x in data)))
  dates=sorted({str(x.get("date") or x.get("trade_date") or x.get("datetime") or "")[:10] for x in data if x.get("date") or x.get("trade_date") or x.get("datetime")})
  first=dates[0] if dates else None
  sha=hashlib.sha256(json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()).hexdigest()
  exact=not d["single_day"] and bool(first)
  return p,{"state":"PASS","http_status":status,"rows":len(data),"columns":cols,"first_observed_date":first,
   "last_observed_date":dates[-1] if dates else None,"unique_dates":len(dates),
   "date_basis":"API_RANGE_MIN_OBSERVED" if exact else ("SINGLE_DAY_PROBE_ONLY" if d["single_day"] else "NO_DATE_FIELD"),
   "earliest_date_certified":exact,"response_sha256":sha,"sample":data[0]}
 except urllib.error.HTTPError as e:return p,{"state":"HTTP_ERROR","http_status":e.code,"message":e.read(1500).decode("utf-8","replace")[:500]}
 except Exception as e:return p,{"state":"TRANSPORT_OR_PARSE_ERROR","error":type(e).__name__+":"+str(e)[:250]}
def main():
 md=read_doc();registry=parse_registry(md);rows=[]
 for i,d in enumerate(registry,1):
  params,res=query(d);rows.append({**d,"request_params":params,**res})
  print(json.dumps({"i":i,"total":len(registry),"dataset":d["dataset"],"tier":d["tier"],"state":res["state"],"rows":res.get("rows"),"earliest":res.get("first_observed_date"),"basis":res.get("date_basis")},ensure_ascii=False),flush=True);time.sleep(.35)
 tiers={t:sum(x["tier"]==t for x in registry) for t in ("Free","Free(w/ data_id)","Backer")}
 summary={"schema":"JARVIS_V098_FINMIND_BACKER_DATASET_EARLIEST_AUDIT","run_date":TODAY,"plan":"Backer NT$699",
  "pricing_page_advertised_count":81,"published_registry_count":len(registry),"registry_tier_counts":tiers,
  "registry_source":DOC,"registry_sha256":hashlib.sha256(md.encode()).hexdigest(),
  "count_discrepancy":"Pricing page advertises 81; current published dataset registry parses to "+str(len(registry))+" eligible Free/Free-with-data_id/Backer entries. No silent truncation.",
  "tested":len(rows),"pass":sum(x["state"]=="PASS" for x in rows),"empty":sum(x["state"]=="EMPTY" for x in rows),
  "failed":sum(x["state"] not in ("PASS","EMPTY") for x in rows),
  "exact_earliest_certified":sum(x.get("earliest_date_certified",False) for x in rows),
  "not_exact":sum(not x.get("earliest_date_certified",False) for x in rows),"rows":rows}
 out=Path("finmind_backer_earliest_audit");out.mkdir(exist_ok=True)
 (out/"report.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
 fields=["dataset","tier","state","rows","first_observed_date","last_observed_date","date_basis","earliest_date_certified","http_status","required","expected_columns","actual_columns","request_params","response_sha256","message"]
 with (out/"dataset_audit.csv").open("w",newline="",encoding="utf-8-sig") as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
  for x in rows:w.writerow({**x,"actual_columns":"|".join(x.get("columns",[])),"request_params":json.dumps(x.get("request_params",{}),ensure_ascii=False)})
 print("FINAL "+json.dumps({k:v for k,v in summary.items() if k!="rows"},ensure_ascii=False),flush=True)
if __name__=="__main__":main()
