#!/usr/bin/env python3
import json,datetime as dt,pathlib,collections
from v098_publish_source_period_rules import get_token,ensure_spec_folder,upload_check
src=pathlib.Path("tdcc_r3b_input/tdcc_deep_schema.json")
data=json.loads(src.read_text(encoding="utf-8"))
assert data["schema"]=="JARVIS_TDCC_DEPTH_SAMPLES_R3B"
root=pathlib.Path("tdcc_r3b_report");root.mkdir(exist_ok=True)
payload={"schema":"JARVIS_TDCC_DEEP_REPAIR_AUDIT_R3B",
 "run_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
 "original_six_source_pilot_run":"45220041535",
 "focused_followup_run":"45221828481",
 "limits":{"first_page_only":True,"max_depth":4,"max_samples_per_year":3},
 "years_with_sample":[r["year"] for r in data["reports"] if r["sampled"]>0],
 "provenance":"FREE_REPOSITORY_ACTUAL_BRONZE_READ_ONLY","g2_official_schema_certified":False,
 "zero_samples_in_older_years_mean_no_files_in_bounded_paths_not_absence_of_official_history":True,
 "evidence":data}
p=root/"18_TDCC深層來源格式抽樣_20261124_R3B.json"
p.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
token=get_token();folder=ensure_spec_folder(token)
receipt=upload_check(token,folder,p)
(root/"19_TDCC_R3B上傳讀回履歷_20261124.json").write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding="utf-8")
print("TDCC_R3B_DRIVE_PASS "+json.dumps({"uploaded":1,"samples":sum(r["sampled"] for r in data["reports"]),
 "years_with_samples":payload["years_with_sample"],"sha_verified":True,"not_complete":True},ensure_ascii=False))
