#!/usr/bin/env python3
"""Upload existing FinMind QA GitHub artifact to unique C1_0.98/FinMind_Backer_QA.
Never calls FinMind. No writes to Bronze. Uses paid Google OAuth only.
"""
import json,os,pathlib,sys
from scripts.finmind_backer_repair_qa import upload_drive
source=pathlib.Path("qa_input")
files=[source/"repair_22.json",source/"era_schema.json",source/"summary.json"]
missing=[str(p) for p in files if not p.is_file()]
if missing:raise SystemExit("MISSING_QA_ARTIFACT:"+",".join(missing))
result=upload_drive(files)
print("DRIVE_UPLOAD_RECEIPT "+json.dumps(result,ensure_ascii=False),flush=True)
pathlib.Path("drive_upload_receipt.json").write_text(json.dumps(result,ensure_ascii=False,indent=2))
if result["state"]!="PASS_UPLOADED":raise SystemExit("UPLOAD_BLOCKED:"+result["state"])
