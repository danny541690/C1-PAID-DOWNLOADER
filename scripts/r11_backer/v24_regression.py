"""Exercise the production native-manifest reader and B01–B10 against an isolated Drive batch."""

import json
import os
from time import time
from unittest.mock import patch

from . import bulk_batch, production_download, release_batch
from .bulk_manifest import SCHEMA, compile_window, read_manifest
from .cross_run_ledger import DriveJournal
from .drive_pilot import FOLDER, ensure, find
from .integration_pilot import digest, encode, verify_private_folder
from .live_recovery_canary import month_folder, read_json_file, write_json
from .release_probes import Task
from .release_rehearsal import require
from .v2_registry import NativeContract
from .v2_recovery import metadata

CASE = "V24_NATIVE_CASES"
MANIFEST = "V24_TEST_SEGMENT.json"
SOURCE = "SYNTHETIC_V24_NATIVE_TEST"


def fixture():
    big_day = "2000-01-03"
    market_rows = [
        {"date": big_day, "stock_id": "1001", "name": "Foreign", "amount": 1, "evidence_padding": "S" * 270_000},
        {"date": big_day, "stock_id": "1001", "name": "Dealer", "amount": 0},
        {"date": big_day, "stock_id": "2002", "name": "Dealer", "amount": None},
        {"date": big_day, "stock_id": "3003", "name": "Foreign", "amount": 5},
    ]
    stock = NativeContract("TaiwanStockInstitutionalInvestorsBuySell", "daily_observation", "single_day",
                           ("date", "stock_id", "name", "amount"), ("date", "stock_id", "name"), identity_field="stock_id")
    # The market completeness set includes Emerging. Only listing and OTC keys feed research coverage.
    keys = [[r["date"], r["stock_id"], r["name"]] for r in market_rows]
    venues = {"1001": "TWSE", "2002": "TPEx"}
    stock_coverage = [{"subject": f"{venues[row['stock_id']]}:{row['stock_id']}", "native_key": key,
                       "period": big_day, "venue": venues[row["stock_id"]]} for row, key in zip(market_rows, keys, strict=True)
                      if row["stock_id"] in venues]
    market = compile_window(rule=stock, mode="MARKET_DAY", params={"dataset": stock.dataset, "start_date": big_day},
                            lane="B03", venue="TW_FULL_NATIVE", version=SOURCE,
                            completeness={"method": "expected_keys", "keys": keys, "native_universe": "FULL_RESPONSE_SCOPE"},
                            coverage=stock_coverage, evidence={"fixture": "native_full_market_mixed"}, raw_limit=512 * 1024)
    first, last = "1984-12-31", "1985-01-03"
    linear = NativeContract("TaiwanStockPrice", "daily_observation", "entity_range", ("date", "stock_id", "value"),
                            ("date", "stock_id"), identity_field="stock_id", allowed_parameters=("data_id",))
    wide = compile_window(rule=linear, mode="ENTITY_RANGE", params={"dataset": linear.dataset, "data_id": "1001",
                          "start_date": first, "end_date": last}, lane="B01", venue="TWSE", version=SOURCE,
                          completeness={"method": "expected_keys", "keys": [[first, "1001"], [last, "1001"]]},
                          coverage=[{"subject": "TWSE:1001:1984", "native_key": [day, "1001"], "period": day, "venue": "TWSE"}
                                    for day in (first, last)], evidence={"fixture": "cross_year"}, raw_limit=16_384)
    events = NativeContract("TaiwanStockParValueChange", "event", "event_range", ("date", "stock_id"), (),
                            key_policy="content_multiset")
    empty = compile_window(rule=events, mode="EVENT_RANGE", params={"dataset": events.dataset, "start_date": "2025-01-01",
                           "end_date": "2025-02-01"}, lane="B05", venue="TW", version=SOURCE,
                           completeness={"method": "confirmed_empty", "evidence": "verified_synthetic_event_calendar"},
                           coverage=[], evidence={"fixture": "confirmed_empty"}, raw_limit=4096)
    future = NativeContract("TaiwanFuturesFinalSettlementPrice", "event", "event_range", ("date", "futures_id", "value"),
                            (), key_policy="content_multiset", page_field="next", cursor_parameter="page")
    observation = {"date": "2025-01-15", "futures_id": "TX", "value": 12}
    paged = compile_window(rule=future, mode="EVENT_RANGE", params={"dataset": future.dataset, "start_date": "2025-01-01",
                           "end_date": "2025-01-31"}, lane="B08", venue="TAIFEX", version=SOURCE,
                           completeness={"method": "expected_multiset", "row_hashes": [digest(encode(observation))] * 2},
                           coverage=[{"subject": "TAIFEX:TX", "native_key": ["2025-01-15", "TX"],
                                      "period": "2025-01-15", "venue": "TAIFEX"}],
                           evidence={"fixture": "explicit_cursor_and_legitimate_duplicate_content"}, raw_limit=8192, max_pages=2)
    large = compile_window(rule=linear, mode="ENTITY_RANGE", params={"dataset": linear.dataset, "data_id": "2002",
                           "start_date": "2024-02-01", "end_date": "2024-02-01"}, lane="B01", venue="TPEx", version=SOURCE,
                           completeness={"method": "expected_keys", "keys": [["2024-02-01", "2002"]]},
                           coverage=[{"subject": "TPEx:2002:2024", "native_key": ["2024-02-01", "2002"],
                                      "period": "2024-02-01", "venue": "TPEx"}],
                           evidence={"fixture": "response_capacity_guard"}, raw_limit=2048)
    items = [market, wide, empty, paged, large]
    rows = {"schema_version": SCHEMA, "rows": items}
    index = {"schema_version": SCHEMA, "prepared_windows": len(items), "full_download_release": "HOLD",
             "production_dispatchable": 0, "dispatch_shards": [{"name": "V24_MANIFEST_FIXTURES.json", "windows": len(items),
                                                                  "sha256": digest(encode(rows))}]}
    responses = {stock.dataset: {"status": 200, "data": market_rows}, events.dataset: {"status": 200, "data": []}}
    return index, rows, {"responses": responses, "future_row": observation, "first": first, "last": last,
                         "market_request": market["request_id"]}


class FixturePermit:
    def __init__(self):
        self.reserved = set()
        self.completed = set()

    def __call__(self, identifier, parameters, item):
        require(identifier not in self.reserved, "DUPLICATE_FIXTURE_HTTP_RESERVATION")
        self.reserved.add(identifier)
        return "RESERVED"

    def complete(self, identifier, status, **kwargs):
        require(identifier in self.reserved and identifier not in self.completed, "FIXTURE_RESERVATION_RESULT")
        self.completed.add(identifier)


def fetch_fixture(source, *, allow_market):
    def fetch(params, cap):
        dataset = params["dataset"]
        if dataset == "TaiwanStockInstitutionalInvestorsBuySell":
            require(allow_market, "STAGED_MARKET_ORIGINAL_REQUESTED_TWICE")
        if dataset == "TaiwanStockPrice" and params.get("data_id") == "2002":
            require(len(b"x" * 2049) <= cap, "NATIVE_RESPONSE_LIMIT")
        if dataset == "TaiwanFuturesFinalSettlementPrice":
            payload = {"status": 200, "data": [source["future_row"]], "next": None if params.get("page") == "b" else "b"}
        elif dataset == "TaiwanStockPrice":
            payload = {"status": 200, "data": [{"date": stamp, "stock_id": "1001", "value": 2}
                                                for stamp in (source["first"], source["last"])]}
        else:
            payload = source["responses"][dataset]
        # A trailing newline is part of the preserved bytes.
        return encode(payload) + b"\n", "identity"
    return fetch


def path(root):
    def resolve(item, access, *, create):
        return month_folder(root, Task(item["dataset"], item["start"], item["end"]), access, create=create)
    return resolve


def private_child(parent, name, access, *, create):
    folder = ensure(parent, name, FOLDER, access, create=create)
    meta = metadata(folder, access)
    require(meta == {"id": folder, "name": name, "parents": [parent], "mimeType": FOLDER,
                     "shared": False, "trashed": False}, "ISOLATED_V24_FOLDER_IDENTITY")
    return folder


def sandbox(ctx, create, *, case=CASE):
    require("FINMIND_BACKER_TOKEN" not in os.environ, "V24_REGRESSION_SECRET_SCOPE")
    root = metadata(ctx.root, ctx.access)
    require(root and root["parents"] == [ctx.task_root] and root["shared"] is False, "V24_REGRESSION_ROOT")
    prepare = private_child(ctx.root, case, ctx.access, create=create)
    stage = private_child(prepare, "STAGING", ctx.access, create=create)
    bronze = private_child(prepare, "TEST_BRONZE", ctx.access, create=create)
    return prepare, stage, path(bronze)


def engine(ctx, unit_result):
    index = ctx.read("MANIFEST_INDEX.json")
    review, stage_id, destination = sandbox(ctx, True, case="G06_MANIFEST_SHARD_REVIEW")
    journal = DriveJournal(review, ctx.access)
    # Prepare the actual reviewed shard within a separate test prefix without dispatching.
    rows = production_download.prepare_native_segment(index, ctx.read, journal, ctx.access, stage_id,
                                                       resolve_destination=destination)["items"]
    require(len(rows) == index["prepared_windows"], "V24_LOADED_NATIVE_MANIFEST_COUNT")
    verified_total = {row["request_id"] for row in rows}
    require(len(verified_total) == len(rows) and index["prepared_windows"] > 0, "V24_NO_LIVE_MANIFEST_ROW_EXERCISED")
    index_data, fixture_rows, _ = fixture()
    require(len(read_manifest(index_data, lambda _: fixture_rows)) == 5, "V24_FIXTURE_SET")
    report = {"native_entry": "production_download.prepare_native_segment → release_batch.dispatch/accept",
              "unit": unit_result, "actual_prepared_shards": len(index["dispatch_shards"]),
              "loaded_prepared_windows": len(rows), "actual_prepared_index_sha256": digest(encode(index)),
              "first_prepared_request": rows[0]["request_id"], "full_download_release": "HOLD",
              "production_dispatchable": 0, "no_live_api_in_regression": True, "tests_failed": 0}
    return "PASS", {"ENGINE_TESTS.json": report}


def stage(ctx):
    prepare, stage_id, destination = sandbox(ctx, True)
    journal = DriveJournal(prepare, ctx.access)
    index, rows, source = fixture()
    if find(prepare, MANIFEST, ctx.access) is None:
        manifest = production_download.prepare_native_segment(index, lambda _: rows, journal, ctx.access, stage_id,
                                                              resolve_destination=destination)
        write_json(prepare, MANIFEST, json.loads(encode(manifest)), ctx.access)
    manifest = read_json_file(prepare, MANIFEST, ctx.access)
    require(manifest["index_sha256"] == digest(encode(index)) and manifest["stage"] == stage_id, "G07_V24_MANIFEST")
    if journal.version(0) is None:
        journal.initialize(bulk_batch.initial(manifest))
    initial = journal.head()
    permit = FixturePermit()
    original_write = bulk_batch.stage_blob
    market = source["market_request"]

    def lose_ack(active_manifest, row, pending, packed, access):
        original_write(active_manifest, row, pending, packed, access)
        if row["request_id"] == market:
            raise TimeoutError("synthetic staging acknowledgment lost")

    # Simulate expiry in the isolated journal without waiting for a real account lease.
    with patch.object(bulk_batch, "stage_blob", side_effect=lose_ack), patch.object(bulk_batch, "time", return_value=time() - 121):
        staged = release_batch.dispatch(manifest, initial, journal, ctx.access, ctx.run, permit=permit,
                                        native_fetch=fetch_fixture(source, allow_market=True))
    current = staged["state"]["tasks"][market]
    require(current["status"] == "REQUESTING" and current["reason"] == "STAGING_RESULT_UNCERTAIN", "G07_V24_UNKNOWN_WRITE_NOT_EXERCISED")
    report = {"origin_job": os.environ["GITHUB_JOB"], "origin_run": ctx.run, "origin_attempt": ctx.attempt,
              "journal_sequence": staged["sequence"], "journal_sha256": digest(encode(staged)),
              "manifest_sha256": digest(encode(manifest)), "market_request": market,
              "market_stage_id": current["pending"]["id"], "simulated_http_calls": len(permit.reserved),
              "actual_finmind_requests": 0, "test_only": True}
    return "PASS", {"G07_CHECKPOINT.json": report}


def resume(ctx):
    prepare, _, destination = sandbox(ctx, False)
    journal = DriveJournal(prepare, ctx.access)
    manifest = read_json_file(prepare, MANIFEST, ctx.access)
    checkpoint = ctx.read("G07_CHECKPOINT.json")
    initial = journal.version(checkpoint["journal_sequence"])
    require(initial and digest(encode(initial)) == checkpoint["journal_sha256"] and
            digest(encode(manifest)) == checkpoint["manifest_sha256"] and
            checkpoint["origin_job"] != os.environ["GITHUB_JOB"], "V24_CROSS_JOB_BINDING")
    recovered = bulk_batch.recover(manifest, journal.head(), journal, ctx.access)
    require(recovered["state"]["tasks"][checkpoint["market_request"]]["recovered"] and
            recovered["state"]["events"] == initial["state"]["events"], "V24_REDOWNLOAD_OR_RECOVERY_FAILED")
    require(not journal.append(initial, initial["state"] | {"late_stale_writer": True}), "V24_STALE_WRITER_ACCEPTED")
    accepted = release_batch.accept(manifest, recovered, journal, ctx.access, resolve_destination=destination)
    repeat = release_batch.accept(manifest, accepted, journal, ctx.access, resolve_destination=destination)
    require(accepted == repeat and journal.head() == repeat, "V24_B10_REPEAT_WRITE")
    result = bulk_batch.reconcile(accepted)
    require(result["states"] == {"VERIFIED": 4, "BLOCKED": 1} and
            list(result["B09"].values()) == ["NATIVE_RESPONSE_LIMIT"], "V24_B09_B10_COUNTS")
    market = result["B10"][checkpoint["market_request"]]
    require(market["proof"]["rows"] == 4 and len(market["coverage_ids"]) == 3 and market["files"][0]["raw_bytes"] > 256 * 1024,
            "V24_FULL_MARKET_ROWS_NOT_PRESERVED")
    require(any(p["anchor_month"] == "1984-12" and p["covered_months"] == ["1984-12", "1985-01"] and len(p["files"]) == 1
                for p in result["B10"].values()), "V24_CROSS_YEAR_ONE_ORIGINAL")
    require(any(p["proof"]["status"] == "EMPTY_CONFIRMED" and p["proof"]["rows"] == 0 for p in result["B10"].values()) and
            any(len(p["files"]) == p["proof"]["rows"] == 2 for p in result["B10"].values()), "V24_EMPTY_OR_PAGES")
    prior = ctx.prior("INTEGRATION_REPORT.json")["inherited_cross_run"]
    require(prior["status"] == "PASS" and prior["origin_run"] != prior["resume_run"], "VERIFIED_PRIOR_CROSS_RUN")
    return "PASS", {"INTEGRATION_REPORT.json": {"isolated_manifest_sha256": checkpoint["manifest_sha256"],
                     "stage_job": checkpoint["origin_job"], "resume_job": os.environ["GITHUB_JOB"],
                     "run_id": ctx.run, "attempt": ctx.attempt, "cross_job_same_native_checkpoint": True,
                     "durable_store_prior_cross_run": prior, "same_native_run_waiting_between_jobs": True,
                     "unacknowledged_stage_reused": checkpoint["market_stage_id"], "provider_requests_on_resume": 0,
                     "stale_writer_rejected": True, "duplicate_receipt_replay_unchanged": True, "test_states": result,
                     "production_mutations": 0, "actual_finmind_requests": 0, "terminal_sequence": accepted["sequence"]}}
