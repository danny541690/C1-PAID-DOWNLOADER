"""Reconcile public documentation with the historical review roster."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import re
from urllib.request import urlopen

from .preflight import read_catalog

REVISION = "56e9bbef66d237bd1526ac2b0e0ba59455e9a869"
SOURCE = f"https://raw.githubusercontent.com/FinMind/FinMind-MCP/{REVISION}/knowledge/datasets.md"
BACKER_COMPATIBLE = {"Free", "Free(w/ data_id)", "Backer"}
KNOWN_TIERS = BACKER_COMPATIBLE | {"Sponsor", "Sponsor Pro"}


def documented_tiers(markdown):
    catalog = {}
    for section in re.split(r"(?m)^### ", markdown)[1:]:
        lines = section.splitlines()
        dataset = lines[0].strip()
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]+", dataset):
            continue
        tiers = re.findall(r"(?m)^- \*\*Tier:\*\*[ \t]+([^\n]+)$", section)
        if len(tiers) != 1 or tiers[0].strip() not in KNOWN_TIERS or dataset in catalog:
            raise ValueError(f"ambiguous documented tier for {dataset}")
        catalog[dataset] = tiers[0].strip()
    if not catalog:
        raise ValueError("public catalog was empty")
    return catalog


def reconcile(candidates, catalog):
    historical = {row["dataset"]: row["routing"] for row in candidates}
    entries = []
    for dataset in sorted(historical.keys() | catalog.keys()):
        route = historical.get(dataset)
        tier = catalog.get(dataset)
        if route == "EXCLUDE":
            verdict = "EXCLUDED_BY_SCOPE"
        elif tier is None:
            verdict = "HOLD_MISSING_FROM_DOCUMENTATION"
        elif tier not in BACKER_COMPATIBLE:
            verdict = "HOLD_DOCUMENTED_HIGHER_TIER"
        elif route in (None, "REVIEW"):
            verdict = "HOLD_SCOPE_REVIEW"
        else:
            verdict = "HOLD_LIVE_ENTITLEMENT_AND_CONTRACT"
        entries.append({
            "dataset": dataset, "historical_route": route, "documented_tier": tier,
            "verdict": verdict, "live_entitlement": "NOT_TESTED",
        })
    return entries


def main():
    with urlopen(SOURCE, timeout=20) as response:
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000:
        raise ValueError("public catalog exceeded maximum size")
    candidates = read_catalog()
    catalog = documented_tiers(raw.decode("utf-8"))
    entries = reconcile(candidates, catalog)
    summary = {
        "historical": len(candidates),
        "documented": len(catalog),
        "documented_tiers": dict(Counter(catalog.values())),
        "historical_matched": sum(row["dataset"] in catalog for row in candidates),
        "public_only": len(entries) - len(candidates),
        "historical_verdicts": dict(Counter(
            row["verdict"] for row in entries if row["historical_route"] is not None
        )),
        "live_entitlements_tested": 0,
        "source": SOURCE,
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    Path("r11-public-catalog.json").write_text(
        json.dumps({"summary": summary, "entries": entries}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
