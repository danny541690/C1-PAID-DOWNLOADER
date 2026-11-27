from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.r11_backer import release_batch, v2_engine


class EngineFixtureTests(unittest.TestCase):
    def test_native_matrix_exercises_all_lanes_and_page_evidence(self):
        matrix = v2_engine.engine_matrix(v2_engine.fixtures())
        self.assertEqual(len(matrix["lanes"]), 8)
        self.assertIn("duplicate_native_key", matrix["negative_cases"])
        self.assertEqual(matrix["fixture_page_chain"], "PASS")
        self.assertEqual(matrix["finmind_requests"], 0)

    def test_existing_controller_accepts_native_multirow_without_reencoding(self):
        item, packed = next(pair for pair in v2_engine.fixtures() if pair[0]["lane"] == "B02")
        task = {"status": "STAGED", "attempts": 1, "stage_id": "stage-id"}
        destination = "destination-id"
        item |= {"destination_id": destination, "name": "fixture.json.gz"}
        manifest = {"stage": "stage-folder"}
        with (
            patch.object(release_batch, "verify_private_folder"),
            patch.object(release_batch, "ensure", side_effect=(task["stage_id"], destination)) as ensure,
            patch.object(release_batch, "read_private_file", return_value=packed),
        ):
            result = release_batch.accept_item(manifest, item, task, "isolated-parent", "fixture-access",
                                               proof=v2_engine.native_proof)
        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(result["receipt"]["proof"]["rows"], 2)
        self.assertEqual(ensure.call_args.args[4], packed)

    def test_production_parent_is_rejected_before_creating_stage(self):
        ctx = SimpleNamespace(root="live-root", task_root="isolated-spec", access="fixture-access",
                              spec_sha="a" * 64, task_id=v2_engine.TASK_ID)
        with patch.object(v2_engine, "read_json", return_value={"parents": ["live-parent"]}), \
                patch.object(v2_engine, "private_child") as create:
            with self.assertRaisesRegex(ValueError, "G07_NOT_ISOLATED"):
                v2_engine.isolated_root(ctx, create=True)
        create.assert_not_called()
