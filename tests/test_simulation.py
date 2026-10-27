import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from recovery_under_interference.simulation import (
    Journal,
    LogError,
    create_run,
    recover,
)


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_case(self, scenario, strategy="conservative"):
        path = self.root / f"{scenario}-{strategy}"
        create_run(path, scenario, strategy)
        return recover(path)

    def test_second_fault_and_lost_ack_do_not_duplicate_effect(self):
        result = self.run_case("double_fault")
        self.assertTrue(result["oracle"]["successful_rollback"])
        self.assertEqual(result["attempts"]["shipping"], 3)
        self.assertEqual(result["oracle"]["effect_counts"]["shipping"], 1)
        naive = self.run_case("double_fault", "naive")
        self.assertFalse(naive["oracle"]["safe"])
        self.assertTrue(naive["oracle"]["target_matches"])

    def test_operator_inventory_update_is_preserved(self):
        conservative = self.run_case("human_stock")
        self.assertTrue(conservative["oracle"]["safe"])
        self.assertEqual(conservative["status"]["inventory"], "blocked")
        self.assertEqual(
            conservative["values"]["inventory"],
            25
            - Journal(self.root / "human_stock-conservative" / "journal.jsonl").state.spec[
                "quantity"
            ],
        )
        idempotent = self.run_case("human_stock", "idempotent")
        self.assertTrue(idempotent["oracle"]["successful_rollback"])
        self.assertFalse(self.run_case("human_stock", "naive")["oracle"]["safe"])

    def test_revocation_blocks_effect_and_dependency_chain(self):
        conservative = self.run_case("revoked_shipping")
        self.assertTrue(conservative["oracle"]["safe"])
        self.assertEqual(
            conservative["status"],
            {"shipping": "blocked", "billing": "pending", "inventory": "pending"},
        )
        self.assertEqual(conservative["oracle"]["effect_counts"], {})
        cas = self.run_case("revoked_shipping", "cas")
        self.assertEqual(cas["status"]["shipping"], "blocked")
        self.assertTrue(cas["oracle"]["safe"])
        self.assertEqual(cas["oracle"]["effect_counts"], {})

    def test_cross_service_change_after_success_invalidates_dependency(self):
        for scenario in ("reopened_shipping", "double_fault_and_dispatch"):
            with self.subTest(scenario=scenario):
                conservative = self.run_case(scenario)
                self.assertTrue(conservative["oracle"]["safe"])
                self.assertEqual(conservative["values"]["shipping"], "dispatched")
                self.assertEqual(conservative["status"]["billing"], "blocked")
                self.assertFalse(self.run_case(scenario, "cas")["oracle"]["safe"])

    def test_distinct_process_resume_reuses_durable_operation_id(self):
        run = self.root / "process"
        first = subprocess.run(
            [
                sys.executable,
                "-m",
                "recovery_under_interference.simulation",
                "demo",
                "--run-dir",
                str(run),
                "--scenario",
                "double_fault",
                "--checkpoint-after",
                "1",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(json.loads(first.stdout)["last_event"], "CHECKPOINT")
        second = subprocess.run(
            [sys.executable, "-m", "recovery_under_interference.simulation", "resume", str(run)],
            check=True,
            capture_output=True,
            text=True,
        )
        result = json.loads(second.stdout)
        self.assertTrue(result["oracle"]["successful_rollback"])
        self.assertEqual(result["oracle"]["effect_counts"]["shipping"], 1)
        before = len(Journal(run / "journal.jsonl").events)
        recover(run)
        self.assertEqual(len(Journal(run / "journal.jsonl").events), before + 1)

    def test_corrupt_and_partial_logs_fail_closed(self):
        for name, mutate in (
            ("checksum", lambda data: data.replace(b'"stage":"forward"', b'"stage":"forWard"', 1)),
            ("torn", lambda data: data[:-5]),
        ):
            with self.subTest(name=name):
                run = self.root / name
                create_run(run, "double_fault", "conservative", seed=0)
                log = run / "journal.jsonl"
                old = log.read_bytes()
                corrupt = mutate(old)
                self.assertNotEqual(corrupt, old)
                log.write_bytes(corrupt)
                with self.assertRaises(LogError):
                    recover(run)
                self.assertEqual(log.read_bytes(), corrupt)


if __name__ == "__main__":
    unittest.main()
