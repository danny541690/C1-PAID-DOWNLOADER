"""Estimate only the evidenced scope at the new user-authorized planning rate."""

from math import ceil
from statistics import mean

from .bulk_manifest import planned_capacity, read_manifest
from .quota import TOTAL_LIMIT
from .v2_resources import campaign_forecast, observed_drive
from .v25_account import HORIZON_DAYS, usage


def run(ctx):
    index = ctx.read("MANIFEST_INDEX.json")
    witnesses = ctx.read("PROBE_RESULTS.json")
    ledger = usage(ctx)
    limit = ledger.get("current_effective_hourly_limit") or TOTAL_LIMIT
    native = planned_capacity(read_manifest(index, ctx.read))
    selectors = ctx.read("V25_HISTORY_SELECTORS.json")["rows"]
    drive = observed_drive(ctx.access)
    legacy = campaign_forecast(ctx.plan, ctx.supplement, ctx.master, drive["available_bytes"], {})
    measurements = [row for row in witnesses["rows"] if row.get("file_id")]
    raw = sum(row["raw_bytes"] for row in measurements)
    packed = sum(row["packed_bytes"] for row in measurements)
    drive_seconds = sum(row["drive_write_readback_seconds"] for row in measurements)
    measurement = {"count": len(measurements), "raw_bytes": raw, "gzip_bytes": packed,
                   "gzip_to_raw_ratio": packed / raw if raw else None,
                   "drive_write_readback_seconds": round(drive_seconds, 4),
                   "actual_small_probe_gzip_bytes_per_second": packed / drive_seconds if drive_seconds else None,
                   "mean_api_seconds": mean(r["elapsed_seconds"] for r in measurements) if measurements else None,
                   "small_probe_scope_only": True}
    current_rows = ctx.read("B09_QUEUE.json")
    modes = ctx.read("QUERY_MODE_BENCHMARK.json")
    market_sizes = {name: max((e["response_mib"] for e in observations["variants"] if e["market_status"] == "VALID_SAMPLE"), default=0)
                    for name, observations in modes["witnessed_market_raw_sizes"].items()}
    scoped = []
    for plan in selectors:
        count = plan["scheduled_requests"]
        name, mode = plan["dataset"], plan["mode"]
        size = market_sizes.get(name, 0) if mode == "MARKET_DAY" else 0
        scoped.append({"dataset": name, "mode": mode, "planned_http_requests": count,
                        "source_only_hours_at_planning_limit": count / limit,
                        "retry_envelope_max_http_requests": count * 4,
                        "observed_comparable_market_raw_mib": size or None,
                        "raw_bytes_scenario_single_observation_size": ceil(count * size * 1024**2) if size else None,
                        "deliverable_partition_storage_bytes": None})
    requests = native["requests"]
    capacity = native["stored_packed_bound"] + 4 * native["stage_peak_serial_bound"]
    fits = drive["available_bytes"] is not None and drive["available_bytes"] >= capacity
    estimate = {"known_prepared_windows": native["windows"], "base_http_requests": requests,
                "retry_upper_http_requests": requests * 4, "local_limit_requests_per_hour": limit,
                "low_source_time_hours": requests / limit, "conservative_source_time_hours": requests * 4 / limit,
                "stored_gzip_bound_bytes": native["stored_packed_bound"],
                "staging_peak_serial_bytes": native["stage_peak_serial_bound"],
                "combined_storage_bound_bytes": capacity, "storage_fits": fits}
    partial_ready = bool(requests and fits and witnesses.get("verified_backer_entitlement_response") and not ledger.get("auth_hold")
                         and not ledger.get("cooldown_until_ms"))
    scopes = {"source_actual_http": sum(row.get("elapsed_seconds") is not None for row in witnesses["rows"]),
              "end_to_end_seconds_for_bounded_live_job": witnesses.get("elapsed_seconds"),
              "github_full_volume_throughput_measured": False}
    blockers = []
    if not witnesses.get("verified_backer_entitlement_response"):
        blockers.append("BACKER_ONLY_ORIGINAL_ACCESS_UNVERIFIED")
    if not fits:
        blockers.append("BOUNDED_DRIVE_CAPACITY_UNVERIFIED")
    if index["full_history_denominator"] is None:
        blockers.append("FULL_HISTORY_CAPACITY_AND_COMPLETION_DURATION_UNVERIFIED")
    if ledger.get("auth_hold") or ledger.get("cooldown_until_ms"):
        blockers.append("OBSERVED_PROVIDER_RESTRICTION")
    report = {"local_planning_rate": TOTAL_LIMIT, "current_effective_rate": limit,
              "user_planning_horizon_days": HORIZON_DAYS, "theoretical_full_rate_requests_in_horizon": HORIZON_DAYS * 24 * TOTAL_LIMIT,
              "exact_membership_expiry_required": False, "official_quota_snapshot_required": False, "drive": drive,
              "api_and_drive_measurement": measurement, "available_full_resource_forecast": index["full_history_denominator"] is not None,
              "existing_limited_2026_campaign": legacy, "prepared_native_partition": estimate,
              "authorized_history_preview_per_dataset": scoped, "total_planned_selector_http_requests": index["planned_scope_http_requests"],
              "legacy_market_retry_assumptions_do_not_establish_full_history_throughput": True,
              "b09_official_unresolved_and_legacy_records": [len(current_rows["independent_source_gaps"]), len(current_rows["legacy_blocked"])],
              "witness_scope_telemetry": scopes, "resource_ready_for_bounded_review": partial_ready, "blockers": blockers}
    return ("HOLD" if blockers else "PASS"), {"RESOURCE_BUDGET.json": report}
