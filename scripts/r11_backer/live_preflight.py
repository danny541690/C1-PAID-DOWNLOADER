"""Read-only runner checks. Public logs receive fixed status labels only."""

import json
import os
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_json(url, token=None, data=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, data=data, headers=headers)
    with build_opener(NoRedirect()).open(request, timeout=20) as response:
        body = response.read(65537)
    if len(body) > 65536:
        raise ValueError("oversized metadata response")
    return json.loads(body)


def drive_status():
    names = ("GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "GOOGLE_REFRESH_TOKEN")
    values = [os.environ.get(name) for name in names]
    if not all(values):
        return "MISSING_SECRETS"
    data = urlencode(dict(zip(("client_id", "client_secret", "refresh_token"), values)) | {
        "grant_type": "refresh_token",
    }).encode()
    token = read_json("https://oauth2.googleapis.com/token", data=data)["access_token"]
    base = "https://www.googleapis.com/drive/v3"
    quota = read_json(f"{base}/about?fields=storageQuota(limit%2Cusage)", token)["storageQuota"]
    if "limit" not in quota or "usage" not in quota:
        return "CAPACITY_UNVERIFIED"
    if int(quota["limit"]) <= int(quota["usage"]):
        return "NO_AVAILABLE_CAPACITY"
    root = os.environ.get("R11_EXPECTED_ROOT")
    bronze = os.environ.get("R11_EXPECTED_BRONZE")
    if not root or not bronze or not all(
        value.isascii() and value.replace("-", "").replace("_", "").isalnum()
        for value in (root, bronze)
    ):
        return "POSITIVE_CAPACITY_FOLDER_CONFIGURATION_MISSING_OR_INVALID"
    folder = read_json(
        f"{base}/files/{bronze}?fields=parents%2Cname%2CmimeType%2Ctrashed%2Ccapabilities(canAddChildren)",
        token,
    )
    if (
        folder.get("name") != "20_BRONZE"
        or root not in folder.get("parents", ())
        or folder.get("mimeType") != "application/vnd.google-apps.folder"
        or folder.get("trashed") is not False
    ):
        return "FOLDER_IDENTITY_UNVERIFIED"
    if folder.get("capabilities", {}).get("canAddChildren") is not True:
        return "BRONZE_ADD_CHILD_NOT_AVAILABLE"
    return "POSITIVE_CAPACITY_AND_BRONZE_ACCESS"


def finmind_status():
    token = os.environ.get("FINMIND_BACKER_TOKEN")
    if not token:
        return "MISSING_SECRET"
    info = read_json("https://api.web.finmindtrade.com/v2/user_info", token)
    limit = info.get("api_request_limit")
    used = info.get("user_count")
    if type(limit) is not int or type(used) is not int or not 0 <= used <= limit:
        return "USAGE_UNVERIFIED"
    if limit < 1600:
        return "LIMIT_BELOW_BACKER_REFERENCE"
    if used >= limit:
        return "NO_ACCOUNT_HEADROOM"
    return "ACCOUNT_HEADROOM_OBSERVED"


def main():
    for provider, check in (("drive", drive_status), ("finmind", finmind_status)):
        try:
            status = check()
        except Exception:
            status = "CHECK_FAILED"
        print(f"{provider}={status}")
    print("production=HOLD")


if __name__ == "__main__":
    main()
