"""Read the complete TPEx delisting report using the exchange's paging contract."""

import json
from urllib.parse import urlencode


DELIST = "https://www.tpex.org.tw/www/zh-tw/company/deListed"
PAGE_SIZE = 100
REQUIRED_FIELDS = {"股票代號", "公司名稱", "終止上櫃日期", "終止上櫃原因"}


def delisting_url(year, offset=0):
    options = {"date": year, "response": "json", "paging-size": PAGE_SIZE, "paging-offset": offset}
    if offset:
        options.update({"paging-table": 0, "paging-order": 0, "paging-dir": "asc"})
    return f"{DELIST}?{urlencode(options)}"


def first_table(raw, year):
    payload = json.loads(raw)
    if payload.get("stat") != "ok" or str(payload.get("date")) != str(year) or len(payload["tables"]) != 1:
        raise ValueError("wrong TPEx delisting year or report type")
    table = payload["tables"][0]
    if not REQUIRED_FIELDS.issubset(table["fields"]) or type(table["totalCount"]) is not int:
        raise ValueError("TPEx delisting fields or total count are missing")
    return table


def complete_rows(pages, year):
    """Require the same total and valid, distinct rows across every requested offset."""
    table = first_table(pages[0], year)
    total, fields = table["totalCount"], table["fields"]
    offsets = tuple(range(0, max(total, 1), PAGE_SIZE))
    if total < 0 or set(pages) != set(offsets):
        raise ValueError("TPEx delisting pages do not cover the reported count")
    rows, seen = [], set()
    for offset in offsets:
        part = pages[offset]
        if offset:
            doc = json.loads(part)
            if doc.get("stat") != "ok" or (doc.get("date") is not None and str(doc["date"]) != str(year)):
                raise ValueError("invalid TPEx delisting continuation")
            part = doc
        else:
            part = table
        data = part["data"]
        if part["totalCount"] != total or len(data) != min(PAGE_SIZE, total - offset):
            raise ValueError("truncated TPEx delisting page or changing total")
        for row in data:
            if not isinstance(row, list) or len(row) != len(fields):
                raise ValueError("TPEx delisting row does not match the fields")
            signature = tuple(row)
            if signature in seen:
                raise ValueError("TPEx delisting rows repeat within the complete report")
            seen.add(signature)
            rows.append(row)
    return fields, rows
