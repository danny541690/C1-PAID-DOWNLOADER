"""Date/grain audit for FinMind Backer all-market query; summary only, no raw rows."""
import os,urllib.parse,urllib.request,json,collections,datetime
q=urllib.parse.urlencode({"dataset":"TaiwanStockPrice","start_date":"2024-01-02","end_date":"2024-01-02"})
req=urllib.request.Request("https://api.finmindtrade.com/api/v4/data?"+q,headers={"Authorization":"Bearer "+os.environ["FINMIND_BACKER_TOKEN"],"User-Agent":"JARVIS-date-grain-audit"})
with urllib.request.urlopen(req,timeout=65) as res:
    raw=res.read(12_000_001)
    if len(raw)>12_000_000:raise ValueError("SIZE_LIMIT")
x=json.loads(raw);rows=x.get("data",[])
if x.get("status")!=200 or not isinstance(rows,list):raise RuntimeError("API_BAD_RESPONSE")
dates=collections.Counter(str(y.get("date")) for y in rows)
ids=collections.Counter(str(y.get("stock_id")) for y in rows)
report={"api_status":x["status"],"requested_dates":["2024-01-02","2024-01-02"],"rows":len(rows),
        "unique_dates":len(dates),"date_counts":dict(sorted(dates.items())),
        "unique_stock_ids":len(ids),"duplicate_date_stock_pairs":len(rows)-len({(str(y.get("date")),str(y.get("stock_id"))) for y in rows}),
        "only_requested_date":set(dates)=={"2024-01-02"},
        "columns":sorted(set().union(*(y.keys() for y in rows))) if rows else []}
print(json.dumps(report,ensure_ascii=False))
