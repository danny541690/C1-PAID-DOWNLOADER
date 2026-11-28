"""Read the actual production manifest and exercise a paused, isolated native batch."""

from dataclasses import asdict
from time import time

from . import bulk_batch, executable_manifest_v26 as executable, production_download, release_batch
from .cross_run_ledger import DriveJournal
from .integration_pilot import digest, encode
from .release_rehearsal import require
from .v24_regression import private_child, sandbox
from .v26_acceptance import split_queries, split_witness
from .v26_capacity import CapacityGuard, FLOOR, WARNING, observed_drive
from .v2_registry import NativeContract

CASE = "V26_CHECKPOINT_CAPACITY"
DATASET = "TaiwanStockV26Fixture"
FIRST = "1984-12-31"
LAST = "1985-01-01"


def fixture(ctx):
    stamp = ctx.read("LOCK_INPUT.json")["as_of_taipei"]
    rule = NativeContract(DATASET, "daily", "entity_range", ("date", "stock_id", "close"), ("date", "stock_id"),
                          identity_field="stock_id", required_parameters=("dataset", "data_id", "start_date", "end_date"),
                          allowed_parameters=("dataset", "data_id", "start_date", "end_date"))
    work = {"dataset": DATASET, "name": "CROSS_YEAR", "mode": "ENTITY_RANGE", "grain": "daily",
            "contract": asdict(rule), "work": "NORMAL", "lane": "B01", "scope": "EXPLICIT",
            "ids_required": True, "identities": ["9001"], "interval": [FIRST, LAST], "maximum_days": 5,
            "source_unavailable_days": [], "source_unavailable_saturdays_through": None,
            "verification": "PROVIDER_SPLIT", "documentation": [{"type": "ISOLATED_FIXTURE", "no_live_source": True}],
            "raw_limit": 64 * 1024, "market_overlap_policy": None}
    axes = {"format": executable.FORMAT, "source_witnesses": {}, "exact_reuse_query_keys": {},
            "receipt_and_reservation_intervals": {}, "certified_windows": [],
            "existing_market_query_days": {}, "trading_dates": [], "issuer_intervals": []}
    body = {"format": executable.FORMAT, "as_of_taipei": stamp, "rows": [{"dataset": DATASET, "normal": [work],
            "exploration": [], "normal_counts": executable.enumerate_counts([work], axes, stamp),
            "exploration_counts": executable.enumerate_counts([], axes, stamp), "reuse": [], "saved_witness": None}]}
    files = {executable.INPUTS: axes, executable.SHARD: body}
    index = {"schema_version": executable.FORMAT, "native_schema_version": 24, "format": "FROZEN_EXECUTABLE_NATIVE_GENERATOR",
             "as_of_taipei": stamp, "dataset_count": 1, "scheduled_generator_requests": 1, "exploration_request_count": 0,
             "certified_native_windows": 0, "files": {key: digest(encode(value)) for key, value in files.items()},
             "full_download_release": "HOLD", "production_dispatchable": 0}
    return index, files.__getitem__


class FixtureTransport:
    def __init__(self):
        self.requests = []

    def fetch(self, params, limit, *, identifier, lane):
        self.requests.append({"identifier": identifier, "lane": lane, "params": params})
        if lane == "B09":
            require(params["dataset"] == DATASET and params["start_date"] == params["end_date"], "V26_FIXTURE_SPLIT_REQUEST")
            return {"status": "HTTP_RESPONSE_VERIFIED"}, (reply(params["start_date"]), "identity")
        require(params == {"dataset": DATASET, "data_id": "9001", "start_date": FIRST, "end_date": LAST}, "V26_FIXTURE_REQUEST")
        return {"status": "HTTP_RESPONSE_VERIFIED"}, (reply(FIRST, LAST), "identity")


def reply(*dates):
    return encode({"status": 200, "data": [{"date": day, "stock_id": "9001", "close": 10 + [FIRST, LAST].index(day)} for day in dates]})


def comparison(item):
    witnesses = []
    for params in split_queries(item):
        raw = reply(params["start_date"])
        witnesses.append({"parameters": params, "raw": raw, "raw_sha256": digest(raw),
                          "request_id": digest(encode(params))})
    return {"request_id": item["request_id"], "contract_version": item["contract_version"],
            "kind": "PROVIDER_SPLIT", "replies": witnesses}


def journals(ctx, create):
    root, stage, destination = sandbox(ctx, create, case=CASE)
    progress = DriveJournal(private_child(root, "CURSOR", ctx.access, create=create), ctx.access)
    batch = DriveJournal(private_child(root, "BATCH", ctx.access, create=create), ctx.access)
    return progress, batch, stage, destination


def engine(ctx, tests):
    index = ctx.read(executable.INDEX)
    checked = executable.audit(index, ctx.read)
    root, stage, destination = sandbox(ctx, True, case="V26_PRODUCTION_ENTRY")
    native = DriveJournal(root, ctx.access)
    first = production_download.prepare_native_segment(index, ctx.read, native, ctx.access, stage, resolve_destination=destination, limit=3)
    certified = production_download.prepare_native_segment(index, ctx.read, native, ctx.access, stage, resolve_destination=destination,
                                                           cursor=executable.origin(index, "certified"))
    exploration = production_download.prepare_native_segment(index, ctx.read, native, ctx.access, stage, resolve_destination=destination,
                                                           cursor=executable.origin(index, "exploration"), limit=2)
    require(first["items"] and len(certified["items"]) == index["certified_native_windows"] and exploration["items"] and
            all(row["lane"] == "B09" and row["destination"] == stage for row in exploration["items"]),
            "V26_PRODUCTION_READER_QUEUE_CONTRACT")
    repeat = production_download.prepare_native_segment(
        index, ctx.read, native, ctx.access, stage, resolve_destination=destination, limit=1,
        known_requests=[first["items"][0]["request_id"]])
    require(repeat["items"][0]["request_id"] != first["items"][0]["request_id"], "V26_RECEIPT_ID_REPLAYED")
    progress, prior_batch, fixture_stage, fixture_destination = journals(ctx, True)
    fixture_index, fixture_read = fixture(ctx)
    frozen, batch = production_download.resume_native_segment(fixture_index, fixture_read, progress, fixture_stage, fixture_destination)
    if prior_batch.head() is None:
        prior_batch.initialize(bulk_batch.initial(batch))
    transport = FixtureTransport()
    low = CapacityGuard(lambda: {"available_bytes": FLOOR - 1})
    paused = release_batch.dispatch(batch, prior_batch.head(), prior_batch, ctx.access, ctx.run, capacity=low, transport=transport)
    require(not transport.requests and not paused["state"]["events"] and
            paused["state"]["capacity_guard"]["state"] == "PAUSED_CAPACITY_FLOOR", "V26_SENT_WHILE_CAPACITY_PAUSED")
    inherited = ctx.previous("ENGINE_TESTS.json")
    return "PASS", {"ENGINE_TESTS.json": checked | {"production_reader": "production_download.prepare_native_segment",
        "loaded_normal": len(first["items"]), "loaded_certified": len(certified["items"]), "loaded_exploration": len(exploration["items"]),
        "new_tests": tests, "inherited_v24_tests": inherited["prior_tests"], "inherited_v25_tests": inherited["local_regression"]["tests"],
        "live_finmind_requests": 0},
        "CAPACITY_STAGE_CHECKPOINT.json": {"cursor_sha256": digest(encode(frozen)), "batch_sha256": digest(encode(batch)),
            "journal_sha256": digest(encode(paused)), "ids": [item["request_id"] for item in batch["items"]],
            "low_result": paused["state"]["capacity_guard"], "source": ctx.binding()}}


def resume(ctx):
    proof = ctx.read("CAPACITY_STAGE_CHECKPOINT.json")
    progress, store, stage, destination = journals(ctx, False)
    index, read = fixture(ctx)
    checkpoint, batch = production_download.resume_native_segment(index, read, progress, stage, destination)
    held = store.head()
    require(digest(encode(checkpoint)) == proof["cursor_sha256"] and digest(encode(batch)) == proof["batch_sha256"] and
            digest(encode(held)) == proof["journal_sha256"], "V26_RESUME_CHECKPOINT_READBACK")
    warning = CapacityGuard(lambda: {"available_bytes": WARNING - 1}).check(0, 65536, new_batch=True)
    require(warning["state"] == "WARN", "V26_WARNING_PATH")
    enlarged = observed_drive(ctx.access)
    guard = CapacityGuard(lambda: enlarged)
    transport = FixtureTransport()
    staged = release_batch.dispatch(batch, held, store, ctx.access, ctx.run, capacity=guard, transport=transport)
    active = batch["items"][0]
    key = active["request_id"]
    require(len(transport.requests) == 1 and staged["state"]["tasks"][key]["status"] == "STAGED",
            "V26_EXPANSION_DID_NOT_RESUME_ORIGINAL_REQUEST")
    incomplete = bulk_batch.accept(batch, staged, store, ctx.access, destination=destination)
    require(incomplete["state"]["tasks"][key]["status"] == "REPAIR", "V26_MISSING_EVIDENCE_PROMOTED")
    aux = FixtureTransport()
    evidence = {}
    def compare(item):
        read = lambda identity, params: evidence.get(identity)
        def persist(value):
            evidence[value["request_id"]] = value
        return split_witness(item, aux, read, persist, guard)

    result = bulk_batch.accept(batch, incomplete, store, ctx.access, destination=destination, comparison_reader=compare, capacity=guard)
    require(result["state"]["tasks"][key]["status"] == "VERIFIED", "V26_NATIVE_COMPARISON_REJECTED")
    resumed_dispatch = release_batch.dispatch(batch, result, store, ctx.access, ctx.run + 1, capacity=guard, transport=transport)
    require(len(transport.requests) == 1 and resumed_dispatch["state"]["tasks"][key]["receipt"] ==
            result["state"]["tasks"][key]["receipt"], "V26_VERIFIED_BLOB_REDOWNLOADED")
    done = executable.finish_active(progress, checkpoint, batch, resumed_dispatch)
    require(done["state"]["terminal_count"] == 1 and done["state"]["active"] is None, "V26_PROGRESS_NOT_COMMITTED")
    receipt = result["state"]["tasks"][key]["receipt"]
    compare(active)
    require(receipt["covered_months"] == ["1984-12", "1985-01"] and receipt["proof"]["native_keys"] == 2 and
            len(aux.requests) == 2 and all(item["lane"] == "B09" for item in aux.requests),
            "V26_CROSS_YEAR_LOSSLESS_ACCEPTANCE")
    return "PASS", {"CAPACITY_GUARD_TEST.json": {"before": proof["low_result"], "warning": warning,
             "after_capacity_quote": enlarged, "same_request_resumed": True, "capacity_test_use_isolated_fixture": True,
             "real_finmind_requests": 0, "fixture_requests": len(transport.requests), "fixture_b09_comparison_requests": len(aux.requests),
             "next_cursor": done["state"]["cursor"]},
            "CONTROLLER_RECOVERY.json": {"same_persisted_batch_across_independent_jobs": True,
                "duplicate_check_after_reopening_receipt": True, "receipt": receipt, "fixture_original_stage_kept_intact": True,
                "quota_checkpoint_prior_run": 46099758774, "live_finmind_requests": 0}}
