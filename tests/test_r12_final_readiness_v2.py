import unittest

from scripts.r11_backer.production_preflight import FINAL_GATES, evaluate, final_readiness_verdict


class FinalReadinessTests(unittest.TestCase):
    def test_all_gates_pass_without_granting_release(self):
        result = evaluate(None, None, None, gate_results=dict.fromkeys(FINAL_GATES, "PASS"))
        self.assertEqual(result["result"], "READY_FOR_USER_RELEASE")
        self.assertEqual(result["full_download_release"], "HOLD")
        self.assertEqual(result["production_dispatchable"], 0)

    def test_missing_failed_and_held_gates_block_readiness(self):
        evidence = dict.fromkeys(FINAL_GATES, "PASS")
        del evidence["G01"]
        evidence.update(G04="HOLD", G07="FAIL")
        result = final_readiness_verdict(evidence)
        self.assertEqual(result["result"], "HOLD_WITH_EVIDENCE")
        self.assertEqual(result["unresolved_gates"], ["G01", "G04", "G07"])
