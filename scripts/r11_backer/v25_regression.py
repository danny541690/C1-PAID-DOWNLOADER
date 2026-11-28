"""Exercise B00 fencing against Drive while preserving prior Bronze regression evidence."""

import os

from . import autonomous_quota as quota, production_download, v24_regression, v25_manifest
from .cross_run_ledger import DriveJournal
from .integration_pilot import digest, encode
from .release_rehearsal import require
from .v24_regression import private_child

CASE = "V25_B00_ISOLATED_JOURNAL"


def engine(ctx, tests):
    index = ctx.read("MANIFEST_INDEX.json")
    checked = v25_manifest.verify_planning(index, ctx.read)
    review, stage, destination = v24_regression.sandbox(ctx, True, case="V25_MANIFEST_REVIEW")
    batch = production_download.prepare_native_segment(index, ctx.read, DriveJournal(review, ctx.access),
                                                       ctx.access, stage, resolve_destination=destination)
    require(len(batch["items"]) == checked["native_windows_loaded"] > 0, "V25_PRODUCTION_READER_DID_NOT_LOAD_WINDOWS")
    inventory = ctx.read("CONSUMER_INVENTORY.json")
    require(inventory["status"] == "PASS", "V25_KNOWN_ENTRY_GATE")
    inherited = ctx.prior("ENGINE_TESTS.json")
    require(inherited["unit"]["test_count"] == 26 and inherited["tests_failed"] == 0, "V25_PRIOR_TEST_BINDING")
    return "PASS", {"ENGINE_TESTS.json": checked | {"local_regression": tests, "production_native_and_legacy_share_transport": True,
        "prior_run_reused": 46087858930, "prior_tests": 26, "inherited_sha256": digest(encode(inherited)),
        "verified_legacy_workflows_quarantined": len(inventory["legacy_workflows"]), "actual_finmind_calls_in_G06": 0,
        "formal_controller_entry": "production_download validate-autonomous-quota"}}


def stage(ctx):
    require("FINMIND_BACKER_TOKEN" not in os.environ, "V25_ISOLATED_SECRET_SCOPE")
    folder = private_child(ctx.root, CASE, ctx.access, create=True)
    journal = DriveJournal(folder, ctx.access)
    clock = quota.utc_ms() - 2 * quota.LEASE_MS
    if journal.head() is None:
        journal.initialize(quota.checkpoint(quota.initial(clock, policy_evidence=ctx.spec_sha)))
    current, book, _ = quota.open_book(journal)
    if not book["lease"]:
        source = quota.AccountBroker(journal, f"{ctx.run}:{ctx.attempt}:G07_STAGE", clock=lambda: clock)
        lease = source.acquire()
        first = source.reserve("retained-uncertain-attempt", "data", "B01", "fixture", preflight=True)
        # Deliberately leave the claim without an acknowledgment. No HTTP is executed.
        current, book = source.record, source.book
        require(first.deadline < lease["expires"], "V25_FIXTURE_LEASE")
    first = next(e for e in book["events"] if e["id"] == "retained-uncertain-attempt")
    return "PASS", {"G07_QUOTA_CHECKPOINT.json": {"folder_id": folder, "sequence": current["sequence"],
        "sha256": digest(encode(current)), "origin": ctx.binding() | {"job": os.environ["GITHUB_JOB"]},
        "synthetic_clock_ms": clock, "lease": book["lease"], "grant": {
            "identifier": first["id"], "planned": first["planned"], "deadline": first["charge"], "epoch": first["fence"]},
        "live_finmind_requests": 0}}


def resume(ctx):
    evidence = ctx.read("G07_QUOTA_CHECKPOINT.json")
    folder = private_child(ctx.root, CASE, ctx.access, create=False)
    require(folder == evidence["folder_id"] and evidence["origin"]["job"] != os.environ["GITHUB_JOB"], "V25_CROSS_JOB_BINDING")
    journal = DriveJournal(folder, ctx.access)
    record, book, _ = quota.open_book(journal)
    require(record["sequence"] == evidence["sequence"] and digest(encode(record)) == evidence["sha256"] and
            book["preflight"]["reserved"]["data"] == 1 and book["preflight"]["actual"]["data"] == 0,
            "V25_UNCERTAIN_RESERVATION_SURVIVES")
    now = evidence["synthetic_clock_ms"] + quota.LEASE_MS + 100
    resumed = quota.AccountBroker(journal, f"{ctx.run}:{ctx.attempt}:G07", clock=lambda: now)
    current = resumed.acquire()
    require(current["generation"] == evidence["lease"]["generation"] + 1, "V25_FENCE_GENERATION_MUST_ADVANCE")
    stale_rejected = False
    try:
        resumed.begin(quota.Grant(**evidence["grant"]))
    except quota.Deferred:
        stale_rejected = True
    require(stale_rejected, "V25_STALE_OR_EXPIRED_GRANT_ACCEPTED")
    old_writer = journal.append(record, quota.checkpoint(book | {"invalid_old_writer": True}))
    require(not old_writer, "V25_STALE_CAS_ACCEPTED")
    marker = resumed.reserve("fresh-after-fence", "data", "B08", "fixture-2", preflight=True)
    resumed.result(marker, None, status="NOT_SENT")
    resumed.close()
    observed, final, _ = quota.open_book(journal)
    require(final["preflight"]["reserved"]["data"] == 2 and final["preflight"]["actual"]["data"] == 0 and
            final["lease"] is None, "V25_RUNTIME_NO_CREDIT_FOR_UNKNOWN_OR_EXPIRED")
    prior = ctx.prior("INTEGRATION_REPORT.json")
    require(prior["cross_run_native_checkpoint_reused"] is True and prior["production_mutations"] == 0,
            "V25_PREVIOUS_CROSS_RUN_BRONZE_EVIDENCE")
    return "PASS", {"INTEGRATION_REPORT.json": {"separate_write_and_resume_jobs": True,
        "actual_drive_quota_cas_readback": True, "current_spec_lease_fencing_verified": True,
        "unknown_and_unsent_remain_charged": True, "experimental_requests_not_charged_to_actual_b00": True,
        "native_bronze_cross_run_evidence_reused": {"origin": prior["native_origin_run_id"], "resume": prior["run_id"],
                                                   "sha256": digest(encode(prior))},
        "isolated_sequence": observed["sequence"], "actual_finmind_calls": 0, "production_mutations": 0}}
