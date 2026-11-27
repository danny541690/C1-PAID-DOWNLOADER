"""Synthetic Drive round trip, scoped to the project's recovery directory."""

import argparse
from datetime import date
import gzip
import hashlib
import json
import os
from http.client import IncompleteRead
from time import sleep
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener
from uuid import uuid4

from .archive import archive_plan, compress_original
from .drive_retry import ATTEMPTS, log_attempt, retry_read, transient
from .live_preflight import NoRedirect, read_json

BASE = "https://www.googleapis.com/drive/v3"
ROOT = "1FzqFPBOKQH2mk324dZAtG7D5z_hGdwoa"
FOLDER = "application/vnd.google-apps.folder"
FIELDS = "id,name,parents,mimeType,trashed"


def token():
    values = {
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],
        "grant_type": "refresh_token",
    }
    return read_json("https://oauth2.googleapis.com/token", data=urlencode(values).encode())["access_token"]


def find(parent, name, access):
    query = urlencode({
        "q": f"'{parent}' in parents and name = '{name}' and trashed = false",
        "fields": f"nextPageToken,incompleteSearch,files({FIELDS})",
        "pageSize": 100,
    })
    result = read_json(f"{BASE}/files?{query}", access)
    entries = result["files"]
    if result.get("nextPageToken") or result.get("incompleteSearch") or len(entries) > 1:
        raise RuntimeError("ambiguous Drive child")
    return entries[0] if entries else None


def read_drive_bytes(file_id, access, limit):
    req = Request(f"{BASE}/files/{file_id}?alt=media", headers={
        "Authorization": f"Bearer {access}", "Accept-Encoding": "identity",
    })

    def read():
        with build_opener(NoRedirect()).open(req, timeout=25) as response:
            return response.read(limit + 1)

    result = retry_read(read, "DRIVE_MEDIA")
    if len(result) > limit:
        raise ValueError("private readback exceeds bound")
    return result


def _resolve_create(parent, name, file_id, access, failure):
    # A POST with uncertain result is never repeated here. Read only the reserved object
    # or, for older callers, a single private child at the exact requested path.
    for attempt in range(ATTEMPTS):
        try:
            item = (read_json(f"{BASE}/files/{file_id}?fields={FIELDS}", access) if file_id is not None
                    else find(parent, name, access))
        except HTTPError as error:
            if error.code != 404:
                raise
            item = None
        if item is not None:
            return item
        if attempt + 1 < ATTEMPTS:
            sleep(2 ** attempt)
    raise failure


def ensure(parent, name, mime, access, content=None, *, create, file_id=None, lookup=True, read_winner=False):
    if not lookup and (not create or file_id is None):
        raise ValueError("a direct create needs a reserved Drive ID")
    if read_winner and (lookup or file_id is None):
        raise ValueError("content check can only be deferred for a reserved journal successor")
    existing = find(parent, name, access) if lookup else None
    uncertain = False
    if existing is None:
        if not create:
            raise RuntimeError("expected Drive child missing")
        fields = {"name": name, "parents": [parent], "mimeType": mime}
        if file_id is not None:
            fields["id"] = file_id
        metadata = json.dumps(fields).encode()
        endpoint = f"{BASE}/files"
        headers = {"Content-Type": "application/json"}
        body = metadata
        if content is not None:
            boundary = uuid4().hex
            endpoint = "https://www.googleapis.com/upload/drive/v3/files"
            headers = {"Content-Type": f"multipart/related; boundary={boundary}"}
            body = (
                f"--{boundary}\r\nContent-Type: application/json\r\n\r\n".encode() + metadata
                + f"\r\n--{boundary}\r\nContent-Type: {mime}\r\n\r\n".encode() + content
                + f"\r\n--{boundary}--\r\n".encode()
            )
        query = urlencode({"fields": FIELDS, **({"uploadType": "multipart"} if content is not None else {})})
        req = Request(f"{endpoint}?{query}", data=body, headers={
            "Authorization": f"Bearer {access}", **headers,
        })
        try:
            with build_opener(NoRedirect()).open(req, timeout=20) as response:
                existing = json.load(response)
        except (OSError, IncompleteRead) as error:
            collision = isinstance(error, HTTPError) and error.code == 409 and file_id is not None
            log_attempt("DRIVE_CREATE", error, 1)
            if not collision and not transient(error):
                raise
            uncertain = True
            existing = _resolve_create(parent, name, file_id, access, error)
    if (
        existing["name"] != name or existing["mimeType"] != mime
        or existing["parents"] != [parent] or existing["trashed"]
        or (file_id is not None and existing["id"] != file_id)
    ):
        raise RuntimeError("Drive child readback identity mismatch")
    if uncertain and content is not None and not read_winner:
        if read_drive_bytes(existing["id"], access, len(content)) != content:
            raise RuntimeError("Drive child readback content mismatch")
    return existing["id"]


def run(*, create):
    access = token()
    recovery = find(ROOT, "99_RECOVERY", access)
    if not recovery or recovery["mimeType"] != FOLDER or recovery["parents"] != [ROOT]:
        raise RuntimeError("recovery directory unavailable")
    run_id = int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    sandbox = f"R11_SYNTHETIC_PILOT_{run_id}_{attempt}"
    dates = [date(1982, 12, 1), date(1983, 1, 1)]
    plan = archive_plan(dates)
    if plan.months != ("1982-12", "1983-01"):
        raise RuntimeError("cross-year month index mismatch")
    original = json.dumps({"synthetic": True, "dates": [day.isoformat() for day in dates]}).encode()
    compressed, raw_sha, gzip_sha = compress_original(original)

    def archive(*, create):
        parent = recovery["id"]
        for name in [sandbox, *plan.anchor.split("/")]:
            parent = ensure(parent, name, FOLDER, access, create=create)
        file_id = ensure(
            parent, "synthetic.json.gz", "application/gzip", access, compressed, create=create
        )
        req = Request(f"{BASE}/files/{file_id}?alt=media", headers={"Authorization": f"Bearer {access}"})
        with build_opener(NoRedirect()).open(req, timeout=20) as response:
            saved = response.read(len(compressed) + 1)
        if hashlib.sha256(saved).hexdigest() != gzip_sha:
            raise RuntimeError("compressed checksum mismatch")
        if hashlib.sha256(gzip.decompress(saved)).hexdigest() != raw_sha:
            raise RuntimeError("raw checksum mismatch")
        return parent, file_id

    first = archive(create=create)
    if archive(create=False) != first:
        raise RuntimeError("replay created a different Drive object")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    try:
        run(create=not args.verify_only)
    except Exception as error:
        print(f"drive_synthetic_round_trip=FAIL ({type(error).__name__})")
        raise SystemExit(1) from None
    print("drive_synthetic_round_trip=PASS")
