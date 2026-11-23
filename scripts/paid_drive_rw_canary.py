#!/usr/bin/env python3
"""JARVIS paid Google Drive isolated OAuth, list, write/readback/delete canary.
No FinMind calls; no edits to existing Drive originals. Logs contain no secrets.
"""
import datetime as dt, hashlib, json, os, pathlib, urllib.error, urllib.parse, urllib.request

REPORT=pathlib.Path("drive_canary_report.json")
OUT={"run_utc":dt.datetime.now(dt.timezone.utc).isoformat(),"repo":"C1-PAID-DOWNLOADER",
     "tests":{},"result":"INCOMPLETE","target":"C1_0.98"}
def mark(name,status,**details):OUT["tests"][name]={"status":status,**details}
def api(url,token,method="GET",body=None,content_type=None):
    h={"Authorization":"Bearer "+token}
    if content_type:h["Content-Type"]=content_type
    req=urllib.request.Request(url,data=body,method=method,headers=h)
    with urllib.request.urlopen(req,timeout=30) as response:
        data=response.read(500000)
        return json.loads(data) if data else {}
def main():
    token=None
    try:
        names=("GOOGLE_CLIENT_ID","GOOGLE_CLIENT_SECRET","GOOGLE_REFRESH_TOKEN")
        values=[os.environ.get(n,"").strip() for n in names]
        if not all(values):
            mark("secret_presence","FAIL",missing_names=[n for n,v in zip(names,values) if not v])
            return
        mark("secret_presence","PASS",names=list(names))
        payload=urllib.parse.urlencode({"client_id":values[0],"client_secret":values[1],
                 "refresh_token":values[2],"grant_type":"refresh_token"}).encode()
        req=urllib.request.Request("https://oauth2.googleapis.com/token",data=payload,
               headers={"Content-Type":"application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req,timeout=25) as r:auth=json.load(r)
        except urllib.error.HTTPError as e:
            try:error=json.loads(e.read(4000)).get("error")
            except Exception:error="UNKNOWN"
            if error not in ("invalid_grant","invalid_client","invalid_request","unauthorized_client","invalid_scope","access_denied"):error="OTHER"
            mark("oauth_refresh","FAIL",http_status=e.code,oauth_error_code=error)
            return
        token=auth["access_token"]
        mark("oauth_refresh","PASS",token_type=auth.get("token_type"),scope=auth.get("scope"))
        q="name = 'C1_0.98' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        url="https://www.googleapis.com/drive/v3/files?"+urllib.parse.urlencode({
             "q":q,"pageSize":100,"fields":"nextPageToken,incompleteSearch,files(id,name,parents,webViewLink)"})
        result=api(url,token)
        folders=result.get("files",[])
        count=len(folders)
        if result.get("nextPageToken") or result.get("incompleteSearch") or count!=1:
            mark("folder_lookup","BLOCKED",matches=count,pagination=bool(result.get("nextPageToken")),
                 incomplete_search=bool(result.get("incompleteSearch")))
            return
        folder=folders[0];fid=folder["id"]
        mark("folder_lookup","PASS",matches=1,folder_id=fid,folder_name=folder.get("name"))
        # Test specifically in the uniquely identified folder. No existing file modified.
        name="_JARVIS_PAID_RW_CANARY_"+str(os.getenv("GITHUB_RUN_ID","manual"))+".txt"
        data=("JARVIS_PAID_RW_CANARY|"+str(os.getenv("GITHUB_RUN_ID","manual"))+"|"+
               dt.datetime.now(dt.timezone.utc).isoformat()).encode()
        meta=json.dumps({"name":name,"mimeType":"text/plain","parents":[fid]}).encode()
        import uuid
        boundary="jarvis-"+uuid.uuid4().hex
        blob=("--"+boundary+"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode()+meta+(
              "\r\n--"+boundary+"\r\nContent-Type: text/plain\r\n\r\n").encode()+data+(
              "\r\n--"+boundary+"--\r\n").encode()
        created=None
        try:
            created=api("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,md5Checksum,parents",
                 token,"POST",blob,"multipart/related; boundary="+boundary)
            created_id=created["id"]
            mark("isolated_write","PASS",file_id=created_id,filename=name,bytes=len(data))
            req=urllib.request.Request("https://www.googleapis.com/drive/v3/files/"+
                    urllib.parse.quote(created_id,safe="")+"?alt=media",
                    headers={"Authorization":"Bearer "+token})
            with urllib.request.urlopen(req,timeout=30) as resp:got=resp.read(2000)
            if got!=data:
                mark("readback_sha256","FAIL",expected=hashlib.sha256(data).hexdigest(),
                     observed=hashlib.sha256(got).hexdigest())
            else:
                mark("readback_sha256","PASS",sha256=hashlib.sha256(data).hexdigest())
        finally:
            if created and created.get("id"):
                req=urllib.request.Request("https://www.googleapis.com/drive/v3/files/"+
                     urllib.parse.quote(created["id"],safe=""),method="DELETE",
                     headers={"Authorization":"Bearer "+token})
                with urllib.request.urlopen(req,timeout=30) as response:response.read()
                mark("test_file_cleanup","PASS",file_id=created["id"])
    except urllib.error.HTTPError as e:
        OUT["error"]={"stage":"DRIVE_API","http_status":e.code}
    except Exception as e:
        OUT["error"]={"stage":"CANARY","error_type":type(e).__name__}
    finally:
        required=("secret_presence","oauth_refresh","folder_lookup","isolated_write","readback_sha256","test_file_cleanup")
        OUT["result"]="PASS" if all(OUT["tests"].get(k,{}).get("status")=="PASS" for k in required) else "INCOMPLETE"
        REPORT.write_text(json.dumps(OUT,ensure_ascii=False,indent=2),encoding="utf-8")
        print("DRIVE_CANARY_RESULT "+json.dumps(OUT,ensure_ascii=False),flush=True)
        if OUT["result"]!="PASS":raise SystemExit(1)
if __name__=="__main__":main()
