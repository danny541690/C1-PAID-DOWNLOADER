import unittest

from scripts.r11_backer.integration_pilot import encode
from scripts.r11_backer.release_rehearsal import RehearsalError
from scripts.r11_backer import v2_handover


class HandoverTests(unittest.TestCase):
    def test_fixed_event_replay_and_spec_binding(self):
        expected = v2_handover.ack_fields("a" * 64, "b" * 64, "c" * 64, "d" * 64)
        replay = v2_handover.ack_fields("a" * 64, "b" * 64, "c" * 64, "d" * 64)
        next_spec = v2_handover.ack_fields("f" * 64, "b" * 64, "c" * 64, "d" * 64)
        self.assertEqual(expected, replay)
        self.assertNotEqual(expected["event_id"], next_spec["event_id"])
        ack = expected | {"engineering_base_commit": "0" * 40}
        self.assertEqual(v2_handover.verify_ack(encode(ack), expected), ack)
        with self.assertRaisesRegex(RehearsalError, "H_DIRECT_ACK_BINDING"):
            v2_handover.verify_ack(encode(ack), next_spec)

    def test_runner_bridge_does_not_satisfy_direct_ack(self):
        expected = v2_handover.ack_fields("a" * 64, "b" * 64, "c" * 64, "d" * 64)
        bridge = expected | {"access_path": "GITHUB_BRIDGE", "engineering_base_commit": "0" * 40}
        with self.assertRaisesRegex(RehearsalError, "H_DIRECT_ACK_BINDING"):
            v2_handover.verify_ack(encode(bridge), expected)

    def test_agents_entrypoints_and_release_guards(self):
        proof = v2_handover.agents_test()
        self.assertEqual(proof["status"], "PASS")
        self.assertTrue(proof["checks"]["historical_archives"])


if __name__ == "__main__":
    unittest.main()
