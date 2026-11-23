#!/usr/bin/env python3
"""Targeted CB identity + Google OAuth/Drive evidence access probe; no raw overwrites."""
import os,json,urllib.request,urllib.parse,urllib.error,datetime
BASE="https://api.finmindtrade.com/api/v4/data"
def stock_query(ds,cb,start,end):
 p={"dataset":ds,"data_id":cb,"start_date":start,"end_date":end}
 try:
  req=urllib.request.Request(BASE+"?"+urllib.parse.urlencode(p),headers={"Authorization":"Bearer "+os.environ["FINMIND_BACKER_TOKEN"],"User-Agent":"JARVIS-finmind-CB-repair"})
  with urllib.request.urlopen(req,timeout=35) as f:obj=json.load(f)
  rows=obj.get("data",[])
  return {"dataset":ds,"id":cb,"period":start+".."+end,"status":obj.get("status"),"rows":len(rows) if isinstance(rows,list) else None,
          "fields":sorted({k for x in rows[:30] for k in x}) if isinstance(rows,list) else []}
 except urllib.error.HTTPError as e:return {"dataset":ds,"id":cb,"http":e.code}
 except Exception as e:return {"dataset":ds,"id":cb,"error_type":type(e).__name__}
def drive():
 result={"stage":"none"}
 try:
  result["stage"]="OAUTH_REFRESH"
  p=urllib.parse.urlencode({"client_id":os.environ["GOOGLE_CLIENT_ID"],"client_secret":os.environ["GOOGLE_CLIENT_SECRET"],
    "refresh_token":os.environ["GOOGLE_REFRESH_TOKEN"],"grant_type":"refresh_token"}).encode()
  req=urllib.request.Request("https://oauth2.googleapis.com/token",data=p,headers={"Content-Type":"application/x-www-form-urlencoded"})
  with urllib.request.urlopen(req,timeout=25) as f: token=json.load(f)["access_token"]
  result["stage"]="DRIVE_LIST_FOLDERS"
  q="name = 'C1_0.98' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
  url="https://www.googleapis.com/drive/v3/files?"+urllib.parse.urlencode({"q":q,"fields":"nextPageToken,files(id,name,parents)","pageSize":100})
  req=urllib.request.Request(url,headers={"Authorization":"Bearer "+token})
  with urllib.request.urlopen(req,timeout=30) as f:obj=json.load(f)
  result.update({"state":"PASS","matching_folder_count":len(obj.get("files",[])),"next_page":bool(obj.get("nextPageToken"))})
 except urllib.error.HTTPError as e:
  try:
   safe=json.loads(e.read(4096)).get("error","unknown")
   if safe not in ("invalid_grant","invalid_client","unauthorized_client","access_denied","invalid_request","invalid_scope"):safe="other"
  except Exception:safe="unknown"
  result.update({"state":"HTTP_ERROR","http_status":e.code,"oauth_error_code":safe})
 except Exception as e: result.update({"state":"ERROR","error_type":type(e).__name__})
 return result
def main():
 names=["TaiwanStockConvertibleBondDaily","TaiwanStockConvertibleBondInstitutionalInvestors","TaiwanStockConvertibleBondDailyOverview"]
 out=[]
 for name in names:
  for cb,a,b in [("11011","2026-11-16","2026-11-20"),("15131","2022-01-03","2022-01-07")]:
   row=stock_query(name,cb,a,b);out.append(row);print(json.dumps({"kind":"CB",**row},ensure_ascii=False),flush=True)
 d=drive();print("DRIVE_DIAGNOSTIC "+json.dumps(d),flush=True)
 import pathlib
 o=pathlib.Path("finmind_followup");o.mkdir(exist_ok=True)
 (o/"diagnostic.json").write_text(json.dumps({"date":"2026-11-23","cb_cases":out,"drive":d},ensure_ascii=False,indent=2),encoding="utf-8")
if __name__=="__main__":main()
