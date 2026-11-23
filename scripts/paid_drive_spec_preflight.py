import os, urllib.request, urllib.parse, json
p=urllib.parse.urlencode({"client_id":os.environ["GOOGLE_CLIENT_ID"],"client_secret":os.environ["GOOGLE_CLIENT_SECRET"],"refresh_token":os.environ["GOOGLE_REFRESH_TOKEN"],"grant_type":"refresh_token"}).encode()
req=urllib.request.Request("https://oauth2.googleapis.com/token",data=p,headers={"Content-Type":"application/x-www-form-urlencoded"})
with urllib.request.urlopen(req,timeout=25) as r:tok=json.load(r)["access_token"]
hdr={"Authorization":"Bearer "+tok}
root="1FzqFPBOKQH2mk324dZAtG7D5z_hGdwoa"
def get(q):
 url="https://www.googleapis.com/drive/v3/files?"+urllib.parse.urlencode({"q":q,"fields":"nextPageToken,files(id,name,mimeType,parents)","pageSize":100})
 with urllib.request.urlopen(urllib.request.Request(url,headers=hdr),timeout=40) as r:return json.load(r)
data=get("'"+root+"' in parents and trashed = false")
files=data.get("files",[])
print("ROOT_CHILDREN",json.dumps({"count":len(files),"next_page":bool(data.get("nextPageToken")),"items":[{"name":x["name"],"id":x["id"],"mime":x["mimeType"]} for x in files[:100]]},ensure_ascii=False))
match=get("name contains '規格' and trashed = false")
print("SPEC_CANDIDATES",json.dumps({"count":len(match.get("files",[])),"items":[{"name":x["name"],"id":x["id"],"mime":x["mimeType"],"parents":x.get("parents")} for x in match.get("files",[])[:50]]},ensure_ascii=False))

for label,fid in [("SOURCES","1tl-tp35IXSWhDPnrPA_UVuei-E5ZbZ4z"),("REPORTS","1iHKk6KcdIKCVK9TjLbbS8XUiImwtBqlt")]:
 o=get("'"+fid+"' in parents and trashed = false")
 print("CHILD_"+label,json.dumps({"count":len(o.get("files",[])),"next_page":bool(o.get("nextPageToken")),"items":[{"id":v["id"],"name":v["name"],"mime":v["mimeType"]} for v in o.get("files",[])[:80]]},ensure_ascii=False))

for label,fid in [("BRONZE","13DY2S3TuC4KYr8JKiptzUeXG9xZyIzam"),("OPERATIONS","1JIngK7pijr9CyRjamccxNeD5t38WrBN1")]:
 o=get("'"+fid+"' in parents and trashed = false")
 print("CHILD_"+label,json.dumps({"count":len(o.get("files",[])),"next_page":bool(o.get("nextPageToken")),"items":[{"id":v["id"],"name":v["name"],"mime":v["mimeType"]} for v in o.get("files",[])[:80]]},ensure_ascii=False))

for label,fid in [("Y2000","1EiUXq-sD_IP6iLZt4Zrxdt45MIIUB0kI"),("Y2008","16r7V0Hsck1cyaxHjnpnUJ6aTRwPW13XV"),("Y2026","18I_NQk9la6OR-AKYCPbMQeCk7mgumlWp")]:
 o=get("'"+fid+"' in parents and trashed = false")
 print("CHILD_"+label,json.dumps({"count":len(o.get("files",[])),"next_page":bool(o.get("nextPageToken")),"items":[{"id":v["id"],"name":v["name"],"mime":v["mimeType"]} for v in o.get("files",[])[:100]]},ensure_ascii=False))
