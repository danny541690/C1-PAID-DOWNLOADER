"""Complete native windows from separate checked source evidence."""

from collections import Counter
from datetime import date, timedelta
import json
import re

from .bulk_manifest import check_window
from .integration_pilot import digest, encode
from .release_rehearsal import require
from .v2_registry import read_rows


def split_queries(item):
    params = item["parameters"]
    first, last = date.fromisoformat(item["start"]), date.fromisoformat(item["end"])
    require(item["mode"].endswith("_RANGE") and first < last, "V26_WINDOW_HAS_NO_DISJOINT_SPLIT")
    mid = first + (last - first) // 2
    return (params | {"end_date": str(mid)}, params | {"start_date": str(mid + timedelta(days=1))})


def read_comparison(item, witness):
    """Bind a comparison to the immutable task and check all of the witness data."""
    require(witness["request_id"] == item["request_id"] and witness["contract_version"] == item["contract_version"],
            "V26_COMPARISON_WRONG_WINDOW")
    strategy = item["completeness"]["verifier"]
    kind = witness["kind"]
    if kind == "PROVIDER_SPLIT":
        require(strategy == kind and len(witness["replies"]) == 2, "V26_SPLIT_NOT_APPROVED")
        rule = check_window(item)
        records = []
        for params, part in zip(split_queries(item), witness["replies"], strict=True):
            raw = part["raw"]
            require(part["parameters"] == params and digest(raw) == part["raw_sha256"] and
                    part.get("request_id") != item["request_id"], "V26_SPLIT_INDEPENDENT_SOURCE")
            rows, reply = read_rows(rule, params, raw)
            require(not rule.page_field or reply.get(rule.page_field) is None, "V26_SPLIT_PAGINATION")
            records.extend(digest(encode(row)) for row in rows)
        require(records, "V26_EMPTY_REQUIRES_OFFICIAL_EVIDENCE")
        return {"method": "split_reconciliation", "row_hashes": records, "native_universe": "FULL_RESPONSE_SCOPE",
                "evidence": [part["raw_sha256"] for part in witness["replies"]]}
    require(kind in {"OFFICIAL_EXPECTED_KEYS", "OFFICIAL_ROW_MULTISET", "CONFIRMED_EMPTY"} and
            witness.get("source_scope") == "FULL_RESPONSE_SCOPE" and
            isinstance(witness.get("official_original"), str) and isinstance(witness.get("evidence_sha256"), str) and
            re.fullmatch(r"[a-f0-9]{64}", witness["evidence_sha256"]), "V26_INDEPENDENT_OFFICIAL_EVIDENCE")
    # The supplied original is the canonical extracted set, not the primary FinMind response.
    evidence = json.loads(witness["official_original"])
    require(digest(witness["official_original"].encode()) == witness["evidence_sha256"] and
            evidence["kind"] == kind and evidence["dataset"] == item["dataset"] and
            evidence["parameters"] == item["parameters"] and evidence.get("source_references"),
            "V26_OFFICIAL_EXPECTED_SCOPE")
    result = {"native_universe": "FULL_RESPONSE_SCOPE", "evidence": evidence["source_references"]}
    if kind == "CONFIRMED_EMPTY":
        require(evidence["disposition"] in {"OFFICIAL_MARKET_CLOSED", "OFFICIAL_NO_EVENT", "OFFICIAL_NO_TRADE"} and
                item["native_contract"]["grain"] in {"event", "event_interval"}, "V26_EMPTY_NOT_INDEPENDENT")
        return result | {"method": "confirmed_empty"}
    result |= ({"method": "expected_keys", "keys": evidence["keys"]} if kind == "OFFICIAL_EXPECTED_KEYS" else
               {"method": "expected_multiset", "row_hashes": evidence["row_hashes"]})
    return result


def stable_witness_ref(witness):
    if witness["kind"] == "PROVIDER_SPLIT":
        return digest(encode([(part["parameters"], part["raw_sha256"]) for part in witness["replies"]]))
    return witness["evidence_sha256"]


def coverage_inventory(item, rows):
    """Canonical native coverage independent from query shape and request ID."""
    rule = check_window(item)
    if rule.key_policy == "native_unique":
        tokens = [digest(encode([item["dataset"], [row[field] for field in rule.key_fields]])) for row in rows]
    else:
        occurrences = Counter()
        tokens = []
        for row in rows:
            body = digest(encode(row))
            occurrences[body] += 1
            tokens.append(digest(encode([item["dataset"], body, occurrences[body]])))
    return sorted(tokens)


def split_witness(item, transport, lookup, persist, capacity):
    """B09 compares two independent, durable responses through the existing B00 transport.

    The caller supplies its private content-addressed receipt store. Existing
    valid originals are reused. Failed or uncertain attempts stay in B00.
    """
    require(item["completeness"]["verifier"] == "PROVIDER_SPLIT", "V26_SPLIT_NOT_CONFIGURED")
    replies = []
    for params in split_queries(item):
        identity = digest(encode(["V26_B09_SPLIT", item["request_id"], params]))
        receipt = lookup(identity, params)
        if receipt is None:
            require(capacity.check(0, 2 * item["raw_limit"], reserve=True)["can_admit"], "V26_B09_CAPACITY_PAUSED")
            result, answer = transport.fetch(params, item["raw_limit"], identifier=identity, lane="B09")
            require(answer and result["status"] == "HTTP_RESPONSE_VERIFIED", "V26_B09_SOURCE_COMPARISON_UNAVAILABLE")
            raw, _ = answer
            receipt = {"request_id": identity, "parameters": params, "raw": raw, "raw_sha256": digest(raw)}
            persist(receipt)
            recovered = lookup(identity, params)
            require(recovered == receipt, "V26_B09_WITNESS_READBACK")
        require(receipt["parameters"] == params and receipt["raw_sha256"] == digest(receipt["raw"]), "V26_B09_WITNESS_BINDING")
        replies.append(receipt)
    return {"kind": "PROVIDER_SPLIT", "request_id": item["request_id"], "contract_version": item["contract_version"],
            "replies": replies}
