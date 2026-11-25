"""Read-only runner checks. Public logs receive fixed status labels only."""

from datetime import date, datetime, timedelta
import json
import os
from urllib.error import HTTPError
from zoneinfo import ZoneInfo
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
    # Canonical IDs are already used by this repository's Drive preflight.
    root = "1FzqFPBOKQH2mk324dZAtG7D5z_hGdwoa"
    bronze = "13DY2S3TuC4KYr8JKiptzUeXG9xZyIzam"
    project = read_json(f"{base}/files/{root}?fields=name%2CmimeType%2Ctrashed", token)
    if (
        project.get("name") != "C1_0.98"
        or project.get("mimeType") != "application/vnd.google-apps.folder"
        or project.get("trashed") is not False
    ):
        return "PROJECT_IDENTITY_UNVERIFIED"
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
    if limit - used < 8:
        return "NO_ACCOUNT_HEADROOM"
    return "ACCOUNT_HEADROOM_OBSERVED"


def validate_month_sample(payload, start, end):
    if type(payload.get("status")) is not int or payload["status"] != 200:
        return "API_REJECTED"
    rows = payload.get("data")
    if not isinstance(rows, list):
        return "UNEXPECTED_RESPONSE"
    if not rows:
        return "EMPTY_WINDOW"
    required = {"date", "leading", "coincident", "lagging", "monitoring"}
    for row in rows:
        if not isinstance(row, dict) or not required <= row.keys():
            return "SCHEMA_UNVERIFIED"
        try:
            observed = date.fromisoformat(row["date"])
        except (ValueError, TypeError):
            return "DATE_UNVERIFIED"
        if not start <= observed <= end:
            return "OUTSIDE_REQUESTED_WINDOW"
    return "VALID_MONTH_SAMPLE"


def probe_business_indicator(start, end):
    params = urlencode({
        "dataset": "TaiwanBusinessIndicator",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    })
    try:
        payload = read_json(
            f"https://api.finmindtrade.com/api/v4/data?{params}",
            os.environ["FINMIND_BACKER_TOKEN"],
        )
        return validate_month_sample(payload, start, end)
    except HTTPError as error:
        if error.code == 429:
            return "RATE_LIMITED"
        if error.code in (401, 403):
            return "ACCESS_DENIED"
        return "HTTP_ERROR"
    except Exception:
        return "CHECK_FAILED"


REQUIRED_PILOT_RESULTS = {
    "drive": "POSITIVE_CAPACITY_AND_BRONZE_ACCESS",
    "finmind": "ACCOUNT_HEADROOM_OBSERVED",
    "backer_historical_probe": "VALID_MONTH_SAMPLE",
    "backer_recent_probe": "VALID_MONTH_SAMPLE",
}


def pilot_status(results):
    if all(results.get(check) == value for check, value in REQUIRED_PILOT_RESULTS.items()):
        return "PASS"
    return "HOLD"


def main():
    statuses = dict.fromkeys(REQUIRED_PILOT_RESULTS, "SKIPPED")
    for provider, check in (("drive", drive_status), ("finmind", finmind_status)):
        try:
            statuses[provider] = check()
        except Exception:
            statuses[provider] = "CHECK_FAILED"
    if statuses["finmind"] == REQUIRED_PILOT_RESULTS["finmind"]:
        historic = probe_business_indicator(date(1982, 1, 1), date(1982, 1, 31))
        statuses["backer_historical_probe"] = historic
        if historic in ("VALID_MONTH_SAMPLE", "EMPTY_WINDOW"):
            today = datetime.now(ZoneInfo("Asia/Taipei")).date()
            statuses["backer_recent_probe"] = probe_business_indicator(
                today - timedelta(days=119), today
            )

    result = pilot_status(statuses)
    for check, value in statuses.items():
        print(f"{check}={value}")
    print(f"pilot={result}")
    print("production=HOLD")
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as stream:
            for check, value in statuses.items():
                stream.write(f"{check}={value}\n")
            stream.write(f"pilot={result}\n")


if __name__ == "__main__":
    main()
