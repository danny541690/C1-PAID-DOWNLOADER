"""FinMind layouts checked against dataset-specific public tutorials."""

from dataclasses import dataclass
from datetime import date
from math import isfinite


@dataclass(frozen=True)
class NativeLayout:
    tutorial: str
    identity: str
    dimensions: tuple[str, ...]
    replaces: tuple[tuple[str, str], ...]
    extra_numbers: tuple[str, ...] = ()


CHIP = "https://finmind.github.io/tutor/TaiwanMarket/Chip/"
DERIVATIVE = "https://finmind.github.io/tutor/TaiwanMarket/Derivative/"
BOND = "https://finmind.github.io/tutor/TaiwanMarket/ConvertibleBond/"
INVESTORS = ("Foreign_Investor", "Investment_Trust", "Dealer_self")
BOND_INVESTORS = "TaiwanStockConvertibleBondInstitutionalInvestors"
LAYOUTS = {
    "TaiwanDailyShortSaleBalances": NativeLayout(
        CHIP, "stock_id", (), (
            ("MarginShortSalesPreviousBalance", "MarginShortSalesPreviousDayBalance"),
            ("MarginShortSalesStockReturns", "MarginShortSalesStockRedemption"),
            ("SBLShortSalesPreviousBalance", "SBLShortSalesPreviousDayBalance"),
        ),
    ),
    "TaiwanFuturesInstitutionalInvestors": NativeLayout(
        DERIVATIVE, "futures_id", ("institutional_investors",), (("name", "futures_id"),),
    ),
    "TaiwanOptionInstitutionalInvestors": NativeLayout(
        DERIVATIVE, "option_id", ("institutional_investors", "call_put"), (("name", "option_id"),),
    ),
    "TaiwanStockConvertibleBondDaily": NativeLayout(
        BOND, "cb_id", ("transaction_type",), (("volume", "unit"),), ("trading_value",),
    ),
    BOND_INVESTORS: NativeLayout(
        BOND, "cb_id", (), (
            ("Foreign_Investment_Buy", "Foreign_Investor_Buy"),
            ("Foreign_Investment_Sell", "Foreign_Investor_Sell"),
            ("Dealer_Buy", "Dealer_self_Buy"), ("Dealer_Sell", "Dealer_self_Sell"),
        ), tuple(f"{group}_Overbuy" for group in INVESTORS) + ("Total_Overbuy",),
    ),
}


def native_fields(layout, documented):
    replacements = dict(layout.replaces)
    return set(replacements.get(field, field) for field in documented) | set(replacements.values()) | {
        "date", layout.identity, *layout.dimensions, *layout.extra_numbers,
    }


def numeric(value):
    return type(value) is int or (type(value) is float and isfinite(value))


def native_result(task, contract, sample):
    if sample.status not in {"VALID_SAMPLE", "FIELD_DIFFERENCES"}:
        return sample.status
    if not sample.rows:
        return "HOLD_EMPTY_WINDOW"
    layout = LAYOUTS[task.dataset]
    required = native_fields(layout, contract.fields)
    text = ({"cb_name"} & required) | {layout.identity, "date", *layout.dimensions}
    numbers = required - text
    keys = set()
    for row in sample.rows:
        if not required <= row.keys():
            return "HOLD_MISSING_NATIVE_FIELDS"
        if any(not isinstance(row[field], str) or not row[field].strip() for field in text):
            return "HOLD_TEXT_TYPE"
        if row[layout.identity] != task.data_id:
            return "HOLD_ENTITY_MISMATCH"
        try:
            day = date.fromisoformat(row["date"]).isoformat()
        except ValueError:
            return "HOLD_DATE_FORMAT"
        if day != row["date"] or not task.start <= day <= task.end:
            return "HOLD_OUTSIDE_WINDOW"
        if any(not numeric(row[field]) for field in numbers):
            return "HOLD_NUMERIC_TYPE"
        key = (day, row[layout.identity], *(row[field] for field in layout.dimensions))
        if key in keys:
            return "HOLD_DUPLICATE_KEY"
        keys.add(key)
        if task.dataset == BOND_INVESTORS and not bond_investor_balance(row):
            return "HOLD_INVESTOR_RECONCILIATION"
    return "PASS_NATIVE_SAMPLE"


def bond_investor_balance(row):
    # Dealer_self is the documented category. It carries no broader dealer-total claim.
    net = [row[f"{group}_Overbuy"] for group in INVESTORS]
    return all(row[f"{group}_Buy"] - row[f"{group}_Sell"] == value
               for group, value in zip(INVESTORS, net)) and sum(net) == row["Total_Overbuy"]
