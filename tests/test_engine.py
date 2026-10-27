import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from recovery_under_interference import (
    AmbiguousOutcome,
    Compensation,
    Conflict,
    FileAdapter,
    JournalError,
    Reconciliation,
    RecoveryEngine,
    RetryableError,
)
from recovery_under_interference.cli import main, order_actions


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def order(self):
        adapter = FileAdapter.create(
            self.root / "service.json", {"shipping": "created", "billing": 75, "inventory": 9}
        )
        engine = RecoveryEngine.create(self.root / "workflow", order_actions())
        return engine, adapter

    def test_exact_budget_reports_complete_and_invalid_cli_creates_nothing(self):
        engine, adapter = self.order()
        report = engine.resume({"local": adapter}, max_attempts=3)
        self.assertTrue(report["complete"])
        self.assertEqual(report["last_event"]["outcome"], "complete")
        with contextlib.redirect_stderr(io.StringIO()):
            status = main(["demo", "--run-dir", str(self.root / "invalid"), "--max-attempts", "0"])
        self.assertEqual(status, 2)
        self.assertFalse((self.root / "invalid").exists())

    def test_lost_ack_checkpoint_resumes_in_another_process(self):
        run = self.root / "demo"
        first = subprocess.run(
            [
                sys.executable,
                "-m",
                "recovery_under_interference",
                "demo",
                "--run-dir",
                str(run),
                "--scenario",
                "lost-ack",
                "--checkpoint-after",
                "1",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(json.loads(first.stdout)["actions"]["cancel"]["status"], "uncertain")
        second = subprocess.run(
            [
                sys.executable,
                "-m",
                "recovery_under_interference",
                "resume",
                str(run / "workflow"),
                "--adapter",
                f"local={run / 'services.json'}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertTrue(json.loads(second.stdout)["complete"])
        adapter = FileAdapter(run / "services.json")
        self.assertEqual(len(adapter.inspect()["receipts"]), 3)
        self.assertEqual(len(adapter.inspect()["history"]), 3)

    def test_second_fault_during_reconciliation_never_reapplies(self):
        engine, adapter = self.order()

        class LostAck(FileAdapter):
            def apply(self, action):
                super().apply(action)
                raise AmbiguousOutcome("reply lost")

        first = engine.resume({"local": LostAck(adapter.path)})
        self.assertEqual(first["actions"]["cancel"]["status"], "uncertain")

        class ReadFault(FileAdapter):
            failed = False

            def reconcile(self, action):
                if not self.failed:
                    self.failed = True
                    raise RetryableError("lookup temporarily unavailable")
                return super().reconcile(action)

        final = engine.resume({"local": ReadFault(adapter.path)})
        self.assertTrue(final["complete"])
        self.assertEqual(final["actions"]["cancel"]["attempts"], 3)
        self.assertEqual(len(adapter.inspect()["history"]), 3)

    def test_operator_change_and_revocation_block_new_effects(self):
        engine, adapter = self.order()
        engine.resume({"local": adapter}, checkpoint_after=1)
        adapter.operator_update("shipping", "dispatched")
        report = engine.resume({"local": adapter})
        self.assertEqual(report["actions"]["refund"]["status"], "blocked")
        self.assertEqual(adapter.inspect()["resources"]["billing"]["value"], 75)
        adapter.operator_update("shipping", "none")
        adapter.set_permission("billing", False)
        denied = engine.resume({"local": adapter})
        self.assertIn("Permission revoked", denied["actions"]["refund"]["reason"])
        self.assertEqual(len(adapter.inspect()["receipts"]), 1)
        adapter.set_permission("billing", True)
        self.assertTrue(engine.resume({"local": adapter})["complete"])

    def test_corrupt_journal_and_adapter_state_fail_closed(self):
        engine, adapter = self.order()
        journal = engine.directory / "journal.jsonl"
        original = journal.read_bytes()
        journal.write_bytes(original[:-1])
        with self.assertRaises(JournalError):
            engine.resume({"local": adapter})
        self.assertEqual(adapter.inspect()["history"], [])
        self.assertEqual(journal.read_bytes(), original[:-1])
        journal.write_bytes(original)
        raw = adapter.path.read_bytes()
        adapter.path.write_bytes(raw.replace(b'"billing"', b'"billinG"', 1))
        corrupt = adapter.path.read_bytes()
        report = engine.resume({"local": adapter})
        self.assertEqual(report["actions"]["cancel"]["status"], "uncertain")
        self.assertEqual(adapter.path.read_bytes(), corrupt)

    def test_payload_collision_and_stale_version_are_rejected(self):
        engine, adapter = self.order()
        action = order_actions()[0]
        adapter.apply(action)
        different = Compensation(
            action.id,
            action.adapter,
            action.resource,
            action.operation,
            action.operation_id,
            {"value": "dispatched"},
        )
        with self.assertRaises(Conflict):
            adapter.apply(different)
        adapter.operator_update("inventory", 14)
        with self.assertRaises(Conflict):
            adapter.apply(order_actions()[2])
        self.assertEqual(adapter.inspect()["resources"]["inventory"]["value"], 14)

    def test_unknown_reconciliation_never_calls_apply(self):
        engine, adapter = self.order()

        class Unknown:
            def reconcile(self, action):
                return Reconciliation("unknown")

            def apply(self, action):
                raise AssertionError("Unknown outcome must not invoke apply")

        report = engine.resume({"local": Unknown()})
        self.assertIn("cannot establish", report["actions"]["cancel"]["reason"])
        self.assertEqual(adapter.inspect()["history"], [])

    def test_guard_types_do_not_treat_booleans_or_floats_as_integer_versions(self):
        adapter = FileAdapter.create(self.root / "guards.json", {"counter": 0, "gate": 1})
        adapter.operator_update("counter", 0)
        adapter.operator_update("gate", 1)
        invalid = [
            {"version": True},
            {"version": 1.0},
            {"guards": [{"resource": "gate", "version": True}]},
            {"guards": [{"resource": [], "value": 1}]},
            {"guards": [{"resource": "gate", "value": True}]},
            {"guards": [{"resource": "gate", "value": 1.0}]},
        ]
        for index, preconditions in enumerate(invalid):
            with self.subTest(preconditions=preconditions):
                action = Compensation(
                    "adjust",
                    "local",
                    "counter",
                    "increment",
                    f"bad-{index}",
                    {"amount": 1},
                    preconditions,
                )
                with self.assertRaises(Conflict):
                    adapter.apply(action)
        self.assertEqual(adapter.inspect()["receipts"], {})
        valid = Compensation(
            "adjust",
            "local",
            "counter",
            "increment",
            "valid",
            {"amount": 1},
            {"version": 1, "guards": [{"resource": "gate", "version": 1, "value": 1}]},
        )
        adapter.apply(valid)
        self.assertEqual(adapter.inspect()["resources"]["counter"]["value"], 1)

    def test_invalid_dependency_plan_leaves_no_directory(self):
        plan = [Compensation("x", "local", "x", "set", "op", {"value": 1}, requires=("x",))]
        with self.assertRaises(ValueError):
            RecoveryEngine.create(self.root / "invalid", plan)
        self.assertFalse((self.root / "invalid").exists())


if __name__ == "__main__":
    unittest.main()
