"""Preserve Backer test outcomes and source evidence in the private project Drive."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from functools import partial
import hashlib
from io import BytesIO, StringIO
import json
import os
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, build_opener
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile, is_zipfile

from .drive_pilot import BASE, FOLDER, ROOT, ensure, find, token as drive_token
from .live_preflight import NoRedirect, read_json


REPO = "danny541690/C1-PAID-DOWNLOADER"
BRANCH = "codex/r11-1-backer-preflight"
WORKFLOW = "r11-backer-bootstrap.yml"
ARCHIVE_JOB = "Save Backer test history to private Drive"
HISTORICAL_LAST = 45652349945
HISTORICAL_RUNS = 79


class ArchiveError(RuntimeError):
    pass


class GitHub:
    def __init__(self):
        if os.environ["GITHUB_REPOSITORY"] != REPO:
            raise ArchiveError("UNEXPECTED_REPOSITORY")
        self.headers = {"Authorization": f"Bearer {os.environ['GH_TOKEN']}",
                        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}

    def get(self, path, *, download=False):
        request = Request(f"https://api.github.com/repos/{REPO}{path}", headers=self.headers)
        for retry in range(3):
            try:
                opener = build_opener(NoRedirect())
                try:
                    with opener.open(request, timeout=40) as response:
                        return response.read()
                except HTTPError as error:
                    if not download or error.code not in {301, 302, 303, 307, 308}:
                        raise
                    location = error.headers.get("Location", "")
                    url = urlparse(location)
                    host = url.hostname or ""
                    if url.scheme != "https" or not host.endswith((".blob.core.windows.net", ".actions.githubusercontent.com",
                                                                  ".blob.storage.azure.net")):
                        raise ArchiveError("UNEXPECTED_GITHUB_DOWNLOAD_HOST") from None
                    with opener.open(location, timeout=60) as response:
                        return response.read()
            except HTTPError as error:
                if error.code not in {408, 429, 500, 502, 503, 504} or retry == 2:
                    raise ArchiveError(f"GITHUB_HTTP_{error.code}") from None
                if error.code == 429:
                    delay = error.headers.get("Retry-After")
                    if not delay or not delay.isdigit() or int(delay) > 60:
                        raise ArchiveError("GITHUB_RATE_LIMITED") from None
                    time.sleep(int(delay))
                    continue
            except (URLError, TimeoutError):
                if retry == 2:
                    raise ArchiveError("GITHUB_NETWORK_ERROR") from None
            time.sleep(2 ** retry)
        raise ArchiveError("GITHUB_FETCH_FAILED")

    def json(self, path):
        return json.loads(self.get(path))

    def pages(self, path, key, **params):
        page = 1
        while True:
            query = urlencode(params | {"per_page": 100, "page": page})
            items = self.json(f"{path}?{query}")[key]
            yield from items
            if len(items) < 100:
                return
            page += 1

    def runs(self):
        rows = self.pages(f"/actions/workflows/{WORKFLOW}/runs", "workflow_runs", branch=BRANCH)
        return sorted((row for row in rows if row.get("head_branch") == BRANCH
                       and (row.get("head_repository") or {}).get("full_name") == REPO), key=lambda row: row["id"])


def private_drive_folder(access):
    root = read_json(f"{BASE}/files/{ROOT}?fields=name,shared,trashed,mimeType", access)
    if (root.get("name") != "C1_0.98" or root.get("shared") is not False
            or root.get("trashed") is not False or root.get("mimeType") != FOLDER):
        raise ArchiveError("PRIVATE_PROJECT_NOT_VERIFIED")
    location = ROOT
    for name in ("00_COMMAND", "R11_BACKER", "TEST_RUNS"):
        location = ensure(location, name, FOLDER, access, create=False)
        if read_json(f"{BASE}/files/{location}?fields=shared", access).get("shared") is not False:
            raise ArchiveError("PRIVATE_ARCHIVE_FOLDER_NOT_VERIFIED")
    return location


def archive_name(run_id, attempt):
    return f"R11_BACKER_Run_{run_id}_Attempt_{attempt}.zip"


def recovery_index(access):
    recovery = ensure(ROOT, "99_RECOVERY", FOLDER, access, create=False)
    if read_json(f"{BASE}/files/{recovery}?fields=shared", access).get("shared") is not False:
        raise ArchiveError("PRIVATE_RECOVERY_NOT_VERIFIED")
    references, page = [], {}
    while True:
        params = {"q": f"'{recovery}' in parents and trashed = false and mimeType = '{FOLDER}'",
                  "fields": "files(id,name,shared),nextPageToken,incompleteSearch", "pageSize": 100}
        result = read_json(f"{BASE}/files?{urlencode(params | page)}", access)
        if result.get("incompleteSearch"):
            raise ArchiveError("PRIVATE_RECOVERY_INDEX_INCOMPLETE")
        for folder in result["files"]:
            match = re.fullmatch(r"R11_[A-Z_]+_(\d+)(?:_(\d+))?", folder["name"])
            if not match:
                continue
            if folder.get("shared") is not False:
                raise ArchiveError("SHARED_TEST_RECOVERY_FOLDER")
            run, attempt = match.groups()
            references.append({"run_id": int(run), "attempt": int(attempt) if attempt else None,
                               "name": folder["name"], "url": f"https://drive.google.com/drive/folders/{folder['id']}"})
        if not (token := result.get("nextPageToken")):
            return sorted(references, key=lambda row: (row["run_id"], row["name"]))
        page = {"pageToken": token}


def expect_log(job):
    return job.get("conclusion") != "skipped" and bool(job.get("runner_id") or job.get("steps"))


def response_zip(body):
    if not is_zipfile(BytesIO(body)):
        raise ArchiveError("GITHUB_DOWNLOAD_NOT_ZIP")
    return body


def download_reports(github, artifacts, files, missing):
    for artifact in artifacts:
        key = artifact["id"]
        try:
            if artifact.get("expired"):
                raise ArchiveError("GITHUB_ARTIFACT_EXPIRED")
            payload = github.get(f"/actions/artifacts/{key}/zip", download=True)
            files[f"artifacts/artifact-{key}.zip"] = response_zip(payload)
        except ArchiveError as error:
            missing.append({"artifact_id": key, "status": str(error)})


def job_logs(github, jobs, files, missing, folder):
    for job in jobs:
        if not expect_log(job):
            continue
        job_id = job["id"]
        try:
            log = github.get(f"/actions/jobs/{job_id}/logs", download=True)
            if not log:
                raise ArchiveError("GITHUB_EMPTY_JOB_LOG")
            files[f"{folder}/job-{job_id}.txt"] = log
        except ArchiveError as error:
            missing.append({"job_id": job_id, "status": str(error)})


def run_snapshot(github, run, *, current=False, attempts=None):
    run_id = run["id"]
    attempts = list(attempts or range(1, run["run_attempt"] + 1))
    files, missing = {}, []
    artifacts = list(github.pages(f"/actions/runs/{run_id}/artifacts", "artifacts"))
    outcomes = Counter()
    for number in attempts:
        endpoint = f"/actions/runs/{run_id}/attempts/{number}"
        folder = f"attempt-{number}"
        attempt = github.json(endpoint)
        jobs = list(github.pages(f"{endpoint}/jobs", "jobs"))
        test_jobs = [job for job in jobs if not current or job.get("name") != ARCHIVE_JOB]
        if current and any(job.get("status") != "completed" for job in test_jobs):
            raise ArchiveError("TEST_JOBS_NOT_FINAL")
        outcomes.update(job.get("conclusion") for job in test_jobs)
        files[f"{folder}/attempt.json"] = json_bytes(attempt)
        files[f"{folder}/jobs.json"] = json_bytes(test_jobs)
        if current:
            job_logs(github, test_jobs, files, missing, folder)
        elif any(map(expect_log, test_jobs)):
            try:
                payload = github.get(f"{endpoint}/logs", download=True)
                files[f"{folder}/workflow-logs.zip"] = response_zip(payload)
            except ArchiveError:
                job_logs(github, test_jobs, files, missing, folder)
    if len(attempts) == 1 and (current or run["run_attempt"] > 1):
        start = attempt["run_started_at"]
        end = None
        if attempts[0] < run["run_attempt"]:
            next_attempt = github.json(f"/actions/runs/{run_id}/attempts/{attempts[0] + 1}")
            end = next_attempt["run_started_at"]
        artifacts = [entry for entry in artifacts if start <= entry["created_at"] and (end is None or entry["created_at"] < end)]
    download_reports(github, artifacts, files, missing)
    files["run.json"] = json_bytes(run)
    files["artifacts.json"] = json_bytes(artifacts)
    report = {"run_id": run_id, "event": run["event"], "outcome": run["conclusion"], "attempts": attempts,
              "job_records": sum(outcomes.values()), "artifact_records": len(artifacts),
              "unavailable_evidence": missing, "test_job_outcomes": dict(outcomes)}
    return files, report


def json_bytes(value):
    return (json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode()


def write_snapshot(archive, prefix, files, report):
    coverage = []
    for name, body in sorted(files.items()):
        mode = ZIP_STORED if name.endswith(".zip") else ZIP_DEFLATED
        archive.writestr(f"{prefix}{name}", body, compress_type=mode)
        coverage.append({"path": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    archive.writestr(f"{prefix}evidence.json", json_bytes(report | {"files": coverage}))


def archive_single(github, run, attempt, references, *, current):
    payload = BytesIO()
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive:
        files, report = run_snapshot(github, run, current=current, attempts=(attempt,))
        files["private-recovery-index.json"] = json_bytes([
            item for item in references if item["run_id"] == run["id"] and item["attempt"] in {None, attempt}
        ])
        write_snapshot(archive, "", files, report)
    return payload.getvalue(), report


def archive_historical(github, through, references):
    runs = [run for run in github.runs() if run["id"] <= through and run["status"] == "completed"]
    if len(runs) != HISTORICAL_RUNS or runs[-1]["id"] != through:
        raise ArchiveError("HISTORICAL_END_RUN_NOT_VERIFIED")
    payload, reports = BytesIO(), []
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive, ThreadPoolExecutor(max_workers=6) as pool:
        index = StringIO()
        table = csv.writer(index)
        table.writerow(["Run ID", "執行事件", "GitHub 結果", "嘗試次數", "提交 SHA", "開始時間 UTC", "GitHub 連結"])
        for run, (files, report) in zip(runs, pool.map(partial(run_snapshot, github), runs)):
            write_snapshot(archive, f"runs/{run['id']}/", files, report)
            table.writerow([run["id"], run["event"], run["conclusion"], run["run_attempt"], run["head_sha"],
                            run["run_started_at"], run["html_url"]])
            reports.append(report)
        archive.writestr("runs.csv", index.getvalue().encode("utf-8-sig"))
        archive.writestr("history-summary.json", json_bytes(reports))
        run_ids = {run["id"] for run in runs}
        archive.writestr("private-recovery-index.json", json_bytes([item for item in references if item["run_id"] in run_ids]))
    summary = {"runs": len(reports), "attempts": sum(len(report["attempts"]) for report in reports),
               "job_records": sum(report["job_records"] for report in reports),
               "artifact_records": sum(report["artifact_records"] for report in reports),
               "unavailable_evidence": sum(len(report["unavailable_evidence"]) for report in reports)}
    return payload.getvalue(), summary


def confirmed_drive_zip(access, parent, name, body):
    file_id = ensure(parent, name, "application/zip", access, body, create=True)
    metadata = read_json(f"{BASE}/files/{file_id}?fields=shared,size", access)
    if metadata.get("shared") is not False or int(metadata["size"]) != len(body):
        raise ArchiveError("PRIVATE_ARCHIVE_READBACK_METADATA")
    digest = hashlib.sha256()
    request = Request(f"{BASE}/files/{file_id}?alt=media", headers={"Authorization": f"Bearer {access}"})
    with build_opener(NoRedirect()).open(request, timeout=60) as download:
        while chunk := download.read(1048576):
            digest.update(chunk)
    if digest.hexdigest() != hashlib.sha256(body).hexdigest():
        raise ArchiveError("PRIVATE_ARCHIVE_READBACK_SHA256")


def catch_up(github, access, parent, current_id, references):
    saved, incomplete = 0, 0
    for run in github.runs():
        run_id = run["id"]
        if not HISTORICAL_LAST < run_id < current_id or run["status"] != "completed":
            continue
        for number in range(1, run["run_attempt"] + 1):
            filename = archive_name(run_id, number)
            if find(parent, filename, access):
                continue
            body, report = archive_single(github, run, number, references, current=False)
            missing = len(report["unavailable_evidence"])
            name = filename if not missing else filename.replace(".zip", "_partial.zip")
            if not missing or not find(parent, name, access):
                confirmed_drive_zip(access, parent, name, body)
            saved += not missing
            incomplete += missing
    return saved, incomplete


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    try:
        github, access = GitHub(), drive_token()
        folder = private_drive_folder(access)
        references = recovery_index(access)
        if args.backfill:
            body, report = archive_historical(github, HISTORICAL_LAST, references)
            missing = report["unavailable_evidence"]
            suffix = "_partial" if missing else ""
            confirmed_drive_zip(access, folder, f"R11_BACKER_History_Through_{HISTORICAL_LAST}{suffix}.zip", body)
            print(f"backer_historical_archive=VERIFIED {json.dumps(report, sort_keys=True)}")
        else:
            run_id, attempt = (int(os.environ[key]) for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"))
            run = github.json(f"/actions/runs/{run_id}")
            if run.get("head_branch") != BRANCH or (run.get("head_repository") or {}).get("full_name") != REPO:
                raise ArchiveError("UNEXPECTED_TEST_BRANCH")
            body, report = archive_single(github, run, attempt, references, current=True)
            missing = len(report["unavailable_evidence"])
            filename = archive_name(run_id, attempt)
            confirmed_drive_zip(access, folder, filename if not missing else filename.replace(".zip", "_partial.zip"), body)
            print(f"backer_attempt_archive=VERIFIED run={run_id} attempt={attempt} "
                  f"jobs={report['job_records']} artifacts={report['artifact_records']} unavailable={missing}")
            if os.environ.get("GITHUB_EVENT_NAME") == "push":
                saved, pending = catch_up(github, access, folder, run_id, references)
                missing += pending
                print(f"backer_archive_catchup saved={saved} unavailable={pending}")
        return int(missing > 0)
    except Exception as error:
        status = str(error) if isinstance(error, ArchiveError) else type(error).__name__
        print(f"backer_drive_archive=HOLD {status}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
