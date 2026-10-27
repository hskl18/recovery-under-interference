import sqlite3
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
    LockUnavailable,
    PermissionDenied,
    RecoveryEngine,
    SQLiteAdapter,
)
from recovery_under_interference.cli import order_actions


def action(
    operation_id="op-1",
    amount=1,
    preconditions=None,
    resource="stock",
    operation="increment",
    arguments=None,
):
    return Compensation(
        "a",
        "svc",
        resource,
        operation,
        operation_id,
        {"amount": amount} if arguments is None else arguments,
        {"version": 0} if preconditions is None else preconditions,
    )


class LostReply(SQLiteAdapter):
    def apply(self, action):
        super().apply(action)
        raise AmbiguousOutcome("reply lost after commit")


class SQLiteAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def create(self, name="service.sqlite", resources=None):
        return SQLiteAdapter.create(self.root / name, resources or {"stock": 9, "status": "open"})

    def tamper(self, adapter, *statements):
        connection = sqlite3.connect(adapter.path)
        try:
            for statement in statements:
                connection.execute(statement)
            connection.commit()
        finally:
            connection.close()

    def test_duplicate_apply_returns_one_receipt_and_payload_collision_conflicts(self):
        adapter = self.create()
        first = adapter.apply(action())
        self.assertEqual(adapter.apply(action()), first)
        state = adapter.inspect()
        self.assertEqual(state["resources"]["stock"], {"value": 10, "version": 1, "allowed": True})
        self.assertEqual(len(state["history"]), 1)
        with self.assertRaises(Conflict):
            adapter.apply(action(amount=2))
        self.assertEqual(adapter.reconcile(action()).state, "applied")
        self.assertEqual(adapter.reconcile(action("op-2")).state, "not_applied")

    def test_preconditions_and_results_match_the_file_adapter(self):
        resources = {"stock": 9, "status": "open"}
        sqlite = SQLiteAdapter.create(self.root / "service.sqlite", resources)
        local = FileAdapter.create(self.root / "service.json", resources)
        guard = [{"resource": "status", "value": "closed", "version": 1}]
        steps = [
            ("apply", action()),
            ("apply", action("stale", preconditions={"version": 0})),
            ("apply", action("guard", preconditions={"version": 1, "guards": guard})),
            ("operator", ("status", "closed")),
            ("apply", action("guard", preconditions={"version": 1, "guards": guard})),
            ("apply", action("missing", resource="missing")),
            ("apply", action("fields", preconditions={"version": 2, "extra": 1})),
            ("apply", action("op", operation="delete", arguments={"x": 1})),
            (
                "apply",
                action(
                    "set",
                    operation="set",
                    arguments={"value": [1, {"a": None}]},
                    preconditions={"version": 2},
                ),
            ),
            ("permission", ("stock", False)),
            ("apply", action("revoked", preconditions={"version": 3})),
            ("permission", ("stock", True)),
            (
                "apply",
                action(
                    "revoked",
                    preconditions={"version": 3},
                    operation="set",
                    arguments={"value": "x"},
                ),
            ),
        ]

        def run(adapter, kind, value):
            try:
                if kind == "apply":
                    return adapter.apply(value).to_dict()
                if kind == "operator":
                    return adapter.operator_update(*value)
                return adapter.set_permission(*value)
            except (Conflict, PermissionDenied) as error:
                return type(error).__name__

        for kind, value in steps:
            self.assertEqual(run(sqlite, kind, value), run(local, kind, value), (kind, value))
        for key in ("resources", "receipts", "history"):
            self.assertEqual(sqlite.inspect()[key], local.inspect()[key], key)

    def test_create_publishes_a_complete_wal_database_and_refuses_existing_paths(self):
        adapter = self.create()
        connection = sqlite3.connect(adapter.path)
        try:
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        finally:
            connection.close()
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), ["service.sqlite"])
        with self.assertRaises(FileExistsError):
            SQLiteAdapter.create(adapter.path, {"other": 1})
        (self.root / "evidence.txt").write_text("keep")
        with self.assertRaises(FileExistsError):
            SQLiteAdapter.create(self.root / "evidence.txt", {"other": 1})
        self.assertEqual((self.root / "evidence.txt").read_text(), "keep")
        self.assertEqual(adapter.inspect()["resources"]["stock"]["value"], 9)
        self.assertFalse([path for path in self.root.iterdir() if path.name.startswith(".")])

    def test_corrupt_or_foreign_databases_raise_journal_errors(self):
        cases = {
            "schema": ["PRAGMA user_version = 7"],
            "application": ["PRAGMA application_id = 1"],
            "value": ["UPDATE resources SET value = '{bad' WHERE name = 'stock'"],
            "permission": [
                "PRAGMA ignore_check_constraints = 1",
                "UPDATE resources SET allowed = 5",
            ],
            "encoding": ["UPDATE resources SET value = CAST(X'FF' AS TEXT) WHERE name = 'stock'"],
            "journal": ["PRAGMA journal_mode = DELETE"],
            "tables": ["DROP TABLE history"],
        }
        for name, statements in cases.items():
            with self.subTest(name):
                adapter = self.create(f"{name}.sqlite")
                self.tamper(adapter, *statements)
                with self.assertRaises(JournalError):
                    adapter.apply(action())
        foreign = self.root / "foreign.sqlite"
        sqlite3.connect(foreign).close()
        (self.root / "text.sqlite").write_text("not a database")
        for path in (foreign, self.root / "text.sqlite"):
            with self.subTest(path.name), self.assertRaises(JournalError):
                SQLiteAdapter(path).reconcile(action())

    def test_receipt_and_history_corruption_is_not_reported_as_a_conflict(self):
        adapter = self.create()
        adapter.apply(action())
        self.tamper(adapter, "UPDATE history SET entry = '[1]'")
        with self.assertRaises(JournalError):
            adapter.inspect()
        self.tamper(
            adapter,
            "UPDATE receipts SET receipt = json_set(receipt, '$.operation_id', 'other')",
        )
        with self.assertRaises(JournalError):
            adapter.reconcile(action())
        engine = RecoveryEngine.create(self.root / "workflow", [action()])
        report = engine.resume({"svc": adapter})
        self.assertEqual(report["last_event"]["outcome"], "uncertain")
        self.assertEqual(report["actions"]["a"]["error_code"], "adapter_state_corrupt")

    def test_writer_lock_is_lock_unavailable_and_readers_do_not_block(self):
        adapter = self.create()
        adapter.timeout = 0.05
        holder = sqlite3.connect(adapter.path, isolation_level=None)
        try:
            holder.execute("BEGIN IMMEDIATE")
            with self.assertRaises(LockUnavailable):
                adapter.apply(action())
            holder.execute("ROLLBACK")
            holder.execute("BEGIN")
            holder.execute("SELECT * FROM resources").fetchall()
            self.assertEqual(adapter.apply(action()).details["after"], 10)
            self.assertEqual(adapter.reconcile(action()).state, "applied")
        finally:
            holder.close()

    def test_oversized_effect_is_a_conflict_without_writes(self):
        large = "x" * (9 * 1024 * 1024)
        adapter = self.create(resources={"blob": large})
        replace = action(
            resource="blob", operation="set", arguments={"value": "y" * (9 * 1024 * 1024)}
        )
        with self.assertRaises(Conflict):
            adapter.apply(replace)
        state = adapter.inspect()
        self.assertEqual(state["resources"]["blob"]["version"], 0)
        self.assertEqual((state["receipts"], state["history"]), ({}, []))

    def test_lost_reply_resumes_without_repeating_effects(self):
        adapter = self.create(resources={"shipping": "pending", "billing": 75, "inventory": 4})
        engine = RecoveryEngine.create(self.root / "workflow", order_actions())
        self.assertFalse(engine.resume({"local": LostReply(adapter.path)})["complete"])
        report = RecoveryEngine(self.root / "workflow").resume(
            {"local": SQLiteAdapter(adapter.path)}
        )
        self.assertTrue(report["complete"])
        self.assertEqual(
            [entry["operation_id"] for entry in adapter.inspect()["history"]],
            ["order-1:cancel", "order-1:refund", "order-1:release"],
        )

    def test_concurrent_processes_commit_one_effect(self):
        adapter = self.create(resources={"stock": 0})
        script = (
            "import sys, time\n"
            "from recovery_under_interference import Compensation, LockUnavailable, SQLiteAdapter\n"
            "adapter = SQLiteAdapter(sys.argv[1], timeout=10)\n"
            "action = Compensation('a', 'svc', 'stock', 'increment', 'race', {'amount': 1},"
            " {'version': 0})\n"
            "for _ in range(100):\n"
            "    try:\n"
            "        print(adapter.apply(action).effect_id); break\n"
            "    except LockUnavailable:\n"
            "        time.sleep(0.01)\n"
        )
        workers = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(adapter.path)],
                stdout=subprocess.PIPE,
                text=True,
            )
            for _ in range(8)
        ]
        effects = {worker.communicate(timeout=60)[0].strip() for worker in workers}
        self.assertEqual(len(effects), 1)
        self.assertNotIn("", effects)
        self.assertEqual(
            adapter.inspect()["resources"]["stock"], {"value": 1, "version": 1, "allowed": True}
        )


if __name__ == "__main__":
    unittest.main()
