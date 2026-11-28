"""Admit new Drive work from actual headroom and conservatively reserved bytes."""

from collections import defaultdict
from math import ceil
from time import time

from .bulk_manifest import packed_limit
from .integration_pilot import digest, encode
from .release_rehearsal import require
from .v2_resources import observed_drive

WARNING = 40_000_000_000
FLOOR = 10_000_000_000
INTERVAL = 600
LEDGER_BUFFER = 16 * 1024 * 1024
UNIT = {"MARKET_DAY": 2_000_000, "ENTITY_RANGE": 250_000}
FALLBACK = 1_000_000
RETRY_SCENARIO = 1.10
RATE_UTILIZATION = 0.80


class CapacityGuard:
    """The active batch lease is responsible for its single set of admissions."""

    def __init__(self, observe, *, clock=time):
        self.observe = observe
        self.clock = clock
        self.snapshot = None
        self.pending_new_bytes = 0
        self.hold_observation = None

    def check(self, flight, next_bytes=0, *, new_batch=False, reserve=False):
        now = int(self.clock())
        require(type(flight) is int and flight >= 0 and type(next_bytes) is int and next_bytes >= 0, "V26_CAPACITY_BYTES")
        if new_batch or self.snapshot is None or now < self.snapshot[0] or now - self.snapshot[0] >= INTERVAL:
            try:
                values = self.observe()
                available = values.get("available_bytes")
            except (OSError, RuntimeError, ValueError, TypeError, KeyError):
                available = None
            self.snapshot = (now, available if type(available) is int and available >= 0 else None)
            self.pending_new_bytes = 0
        observed_at, available = self.snapshot
        required = FLOOR + LEDGER_BUFFER + flight + self.pending_new_bytes + next_bytes
        if available is None:
            status = "PAUSED_CAPACITY_READBACK_UNKNOWN"
        elif available < required:
            status = "PAUSED_CAPACITY_FLOOR"
        elif available < WARNING:
            status = "WARN"
        else:
            status = "OPEN"
        result = {"state": status, "drive_available_bytes": available, "checked_at_utc_epoch": observed_at,
                  "in_flight_bytes": flight, "new_reserved_bytes_since_check": self.pending_new_bytes,
                  "requested_bytes": next_bytes, "required_headroom_bytes": required, "warning_bytes": WARNING,
                  "floor_bytes": FLOOR, "next_check_from_utc_epoch": observed_at + INTERVAL,
                  "can_admit": status in {"WARN", "OPEN"}}
        if reserve and result["can_admit"]:
            self.pending_new_bytes += next_bytes
        if not result["can_admit"]:
            self.hold_observation = result
        return result


def flight_bytes(manifest, record, *, native):
    awaiting = 0
    if not record:
        return 0
    tasks = record["state"]["tasks"]
    for item in manifest["items"]:
        task = tasks[item["task_id"]]
        if task["status"] == "REQUESTING":
            awaiting += packed_limit(item) * (item["max_pages"] + 1) if native else 2 * (512 * 1024)
        elif task["status"] == "STAGED":
            awaiting += sum(page["bytes"] for page in task["pages"]) if native else (task.get("proof") or {}).get("bytes", 512 * 1024)
    return awaiting


def keep_readback(report, record, journal):
    current = record["state"].get("capacity_guard")
    fields = ("state", "checked_at_utc_epoch", "drive_available_bytes")
    if current is None or any(current.get(k) != report.get(k) for k in fields):
        from .live_bronze_admission import advance

        return advance(journal, record, capacity_guard=report)
    return record


def prep(ctx):
    previous = ctx.previous("RESOURCE_BUDGET.json")
    current = observed_drive(ctx.access)
    return ("PASS" if type(current.get("available_bytes")) is int else "HOLD"), {
        "CAPACITY_POLICY.json": {"periodic_check_seconds": INTERVAL, "check_each_batch": True,
            "warning_bytes": WARNING, "hard_floor_bytes": FLOOR, "ledger_buffer_bytes": LEDGER_BUFFER,
            "inflight_and_batch_reservation": True, "automatic_resume_on_observed_expansion": True,
            "automatic_purchase": False, "delete_formal_files": False,
            "previous_drive_quote": previous["drive"], "initial_observation": current}}


def estimate(ctx):
    from . import executable_manifest_v26 as manifest

    index, shards, axes = manifest.read_inputs(ctx.read(manifest.INDEX), ctx.read)
    previous = ctx.previous("RESOURCE_BUDGET.json")
    ledger = ctx.read("LOCK_INPUT.json")["quota"]
    verified_market_samples = ctx.previous("QUERY_MODE_BENCHMARK.json")["witnessed_market_raw_sizes"]
    historical = ctx.previous("REUSE_MAP.json")["archive_intervals"]
    prior = defaultdict(list)
    for row in historical:
        # Archive intervals are spread across unrelated ranges and are used only
        # for like-for-like dataset observations whose packed size is explicit.
        meta = row.get("proof", {})
        if type(meta.get("bytes")) is int and meta["bytes"] > 0:
            prior[row["dataset"]].append(meta["bytes"])
    existing = ctx.read("ACTIVE_BATCH_RECONCILIATION.json")["tasks"]
    existing_packed_samples = defaultdict(list)
    for row in existing:
        proof = (row.get("receipt") or {}).get("proof", {})
        if row["disposition"] == "VERIFIED_RECEIPT" and type(proof.get("bytes")) is int:
            existing_packed_samples[row["dataset"]].append(proof["bytes"])
    scenarios, total = [], 0
    expected_split_requests = 0
    for shard in shards["rows"]:
        name, groups = shard["dataset"], shard["normal_counts"]["by_mode"]
        dataset_bytes = 0
        quotes = {}
        for mode, count in groups.items():
            samples = prior[name]
            market = verified_market_samples.get(name, {}).get("variants", [])
            market_packed = [w.get("gzip_bytes") or w.get("packed_bytes") for w in market if w.get("market_status") == "VALID_SAMPLE"]
            market_packed = [value for value in market_packed if type(value) is int and value > 0]
            if mode == "MARKET_DAY" and market_packed:
                size, reason = max(market_packed), "SAME_DATASET_MARKET_GZIP_WITNESS"
            elif mode in {"ENTITY_RANGE", "SERIES_RANGE"} and samples:
                size, reason = max(max(samples), UNIT.get(mode, FALLBACK)), "SAME_DATASET_ARCHIVE_WITH_CATEGORY_FLOOR"
            else:
                size, reason = UNIT.get(mode, FALLBACK), "V26_CATEGORY_ASSUMPTION"
            quotes[mode] = {"requests": count, "gzip_bytes_per_request": size, "basis": reason}
            dataset_bytes += count * size
        if any(p["verification"] == "PROVIDER_SPLIT" for p in shard["normal"]):
            expected_split_requests += 2 * sum(groups.get(mode, 0) for mode in {p["mode"] for p in shard["normal"]
                                                                           if p["verification"] == "PROVIDER_SPLIT"})
        total += dataset_bytes
        scenarios.append({"dataset": name, "modes": quotes, "estimated_gzip_bytes": dataset_bytes})
    certification_bytes = sum(packed_limit(row) * row["max_pages"] for row in axes["certified_windows"])
    estimate_bytes = ceil((total + certification_bytes) * 1.30)
    launches = index["planned_request_count"]
    source_calls = launches + index["exploration_request_count"] + expected_split_requests
    hourly = ledger.get("current_effective_hourly_limit")
    require(type(hourly) is int and hourly > 0, "V26_QUOTA_LIMIT_MISSING")
    days = round(source_calls * RETRY_SCENARIO / (hourly * RATE_UTILIZATION * 24), 1)
    drive = observed_drive(ctx.access)
    baseline = CapacityGuard(lambda: drive).check(0, max((2 * packed_limit(row) for row in axes["certified_windows"]),
                                                        default=16 * 1024 * 1024), new_batch=True)
    space = drive.get("available_bytes")
    approx_fit = type(space) is int and space >= estimate_bytes
    result = {"rough_new_capacity_bytes": estimate_bytes, "rough_job_days": days,
              "planned_download_requests": launches, "bounded_exploration_requests": index["exploration_request_count"],
              "configured_split_comparison_request_allowance": expected_split_requests,
              "retry_request_multiplier": RETRY_SCENARIO, "hourly_target": hourly, "planning_utilization": RATE_UTILIZATION,
              "budget_driver": "PREPARED_SCOPE_WITH_30_PERCENT_DRIVE_ALLOWANCE",
              "full_history_denominator": index["full_history_denominator"], "retains_b09_pending_proof": True,
              "plan_storage_400_gib_fits": approx_fit, "drive": drive, "small_batch_admission": baseline,
              "capacity_notes": {"400GiB": "ROUGH_SCOPE_FITS" if approx_fit else "ROUGH_SCOPE_EXCEEDS_AVAILABLE_BYTES",
                                 "2TB": "USER_EXPANSION_WHEN_NEEDED_FOR_SCOPE_OR_DOWNSTREAM_LAYERS",
                                 "5TB": "NO_V26_EVIDENCE_REQUIRING_THIS_TIER"},
              "sources": {"archive_samples_reviewed": len(historical), "existing_v25_drive_limit": previous["drive"]["limit_bytes"],
                          "verified_month_window_gzip_bytes_by_dataset": {name: {
                              "samples": len(values), "bytes_mean": round(sum(values) / len(values)),
                              "scope": "ENTITY_MONTH_NOT_EXTRAPOLATED_TO_FULL_MARKET"} for name, values in sorted(existing_packed_samples.items())},
                          "new_sample_downloads": 0, "old_2026_scope_added": False},
              "per_dataset": scenarios, "membership_expiry_used": False, "automatic_purchase": False}
    approval = ctx.read("LOCK_INPUT.json")["backer_entitlement_verified"]
    return ("PASS" if approval and not ledger.get("auth_hold") and baseline["can_admit"] else "HOLD"), {"RESOURCE_ESTIMATE.json": result}
