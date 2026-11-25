"""Use dated exchange closing reports as a public benchmark for bulk row scope."""

from decimal import Decimal, InvalidOperation
import json
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen


EXCHANGES = {
    "TWSE": ("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX", {"type": "ALLBUT0999"}, "證券代號", "收盤價"),
    "TPEx": ("https://www.tpex.org.tw/www/zh-tw/afterTrading/otc", {"type": "EW"}, "代號", "收盤"),
}


def price_value(value):
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        return None
    try:
        amount = Decimal(str(value).replace(",", "").strip())
    except InvalidOperation:
        return None
    return amount if amount.is_finite() and amount > 0 else None


def parse_official_close(document, day, exchange):
    _, _, symbol_header, price_header = EXCHANGES[exchange]
    if document.get("date") != day.strftime("%Y%m%d") or document.get("stat", "").lower() != "ok":
        raise ValueError("official report not bound to requested trading day")
    selected = []
    for table in document["tables"]:
        headers = [field.strip() for field in table.get("fields", [])]
        if symbol_header in headers and price_header in headers:
            selected.append((headers.index(symbol_header), headers.index(price_header), table["data"]))
    if len(selected) != 1:
        raise ValueError("ambiguous market close table")
    symbol_index, price_index, rows = selected[0]
    result = {}
    for row in rows:
        code = row[symbol_index].strip()
        if not re.fullmatch(r"[0-9]{4}", code):
            continue
        if code in result:
            raise ValueError("duplicate official symbol on the same date and exchange")
        result[code] = price_value(row[price_index])
    if not any(value is not None for value in result.values()):
        raise ValueError("no official numeric close evidence")
    return result


def official_close(day, exchange):
    url, options, _, _ = EXCHANGES[exchange]
    params = options | {"date": day.strftime("%Y%m%d" if exchange == "TWSE" else "%Y/%m/%d"), "response": "json"}
    request = Request(f"{url}?{urlencode(params)}", headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urlopen(request, timeout=15) as response:
        body = response.read(4 * 1024**2 + 1)
    if len(body) > 4 * 1024**2:
        raise ValueError("exchange report exceeds bounded size")
    return parse_official_close(json.loads(body), day, exchange)


def reconcile_close(sample, official):
    """Return counts and exact source row positions for accepted evidence only."""
    if sample.status != "VALID_SAMPLE":
        return {"status": sample.status}, []
    if not official or any(not prices for prices in official.values()):
        return {"status": "HOLD_OFFICIAL_DATE_REFERENCE"}, []
    expected = {}
    for venue, closes in official.items():
        if expected.keys() & closes.keys():
            return {"status": "HOLD_AMBIGUOUS_OFFICIAL_IDENTITY"}, []
        expected.update({code: (venue, close) for code, close in closes.items()})

    counts = {venue: {"expected_numeric_closes": sum(close is not None for close in prices.values()),
                      "matched": 0, "missing": 0, "mismatches": 0}
              for venue, prices in official.items()}
    seen = set()
    matched_positions = []
    unmatched = duplicate = no_reference_close = 0
    for position, row in enumerate(sample.rows):
        code = row["stock_id"]
        if not isinstance(code, str) or code not in expected:
            unmatched += 1
            continue
        if code in seen:
            duplicate += 1
            continue
        seen.add(code)
        venue, price = expected[code]
        if price is None:
            no_reference_close += 1
        elif price_value(row["close"]) == price:
            counts[venue]["matched"] += 1
            matched_positions.append(position)
        else:
            counts[venue]["mismatches"] += 1
    for code in expected.keys() - seen:
        venue, price = expected[code]
        if price is not None:
            counts[venue]["missing"] += 1
    passed = not duplicate and all(entry["expected_numeric_closes"] > 0 and not (entry["missing"] or entry["mismatches"])
                                   for entry in counts.values())
    return {
        "status": "PASS_DATED_CLOSE_SAMPLE" if passed else "HOLD_DATED_CLOSE_RECONCILIATION",
        "markets": counts, "raw_rows": len(sample.rows), "rows_outside_dated_reference": unmatched,
        "duplicate_reference_keys": duplicate, "rows_without_official_numeric_close": no_reference_close,
        "matched_row_positions": len(matched_positions),
    }, matched_positions
