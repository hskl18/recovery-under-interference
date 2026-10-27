import contextlib
import hashlib
import io
import json
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from recovery_under_interference import (
    Compensation,
    Conflict,
    FileAdapter,
    JournalError,
    JournalIOError,
    LockUnavailable,
    Receipt,
    Reconciliation,
    RecoveryEngine,
)
from recovery_under_interference import engine as engine_module
from recovery_under_interference.cli import main
from recovery_under_interference.journal import checksum, encoded
from recovery_under_interference.jsondata import MAX_DOCUMENT_BYTES, loads


class DurabilityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def workflow(self):
        service = FileAdapter.create(self.root / "service.json", {"counter": 0})
        action = Compensation(
            "increment", "local", "counter", "increment", "operation-1", {"amount": 1}
        )
        engine = RecoveryEngine.create(self.root / "workflow", [action])
        return engine, service, action

    def test_receipt_fsync_failure_stops_journal_writes_and_restart_preserves_one_effect(self):
        engine, service, _ = self.workflow()
        path = engine.directory / "journal.jsonl"
        original_fsync = os.fsync
        failed = False

        def fail_receipt_sync(descriptor):
            nonlocal failed
            if (
                not failed
                and os.fstat(descriptor).st_ino == path.stat().st_ino
                and b'"type":"RECEIPT"' in path.read_bytes()
            ):
                failed = True
                raise OSError("injected receipt fsync failure")
            original_fsync(descriptor)

        with (
            patch("recovery_under_interference.journal.os.fsync", fail_receipt_sync),
            self.assertRaises(JournalIOError) as failure,
        ):
            engine.resume({"local": service})
        self.assertEqual(failure.exception.stage, "fsync")
        self.assertNotIn(b"UNCERTAIN", path.read_bytes())
        self.assertTrue(engine.resume({"local": service})["complete"])
        self.assertEqual(len(service.inspect()["history"]), 1)
        self.assertEqual(service.inspect()["resources"]["counter"]["value"], 1)

    def test_reconcile_resynchronizes_receipt_after_uncertain_directory_commit(self):
        engine, service, _ = self.workflow()
        with patch(
            "recovery_under_interference.adapters.sync_directory",
            side_effect=OSError("directory sync failed"),
        ):
            report = engine.resume({"local": service})
        self.assertEqual(report["actions"]["increment"]["status"], "uncertain")
        self.assertEqual(len(service.inspect()["history"]), 1)
        with patch(
            "recovery_under_interference.adapters.sync_directory",
            side_effect=OSError("still unavailable"),
        ):
            report = engine.resume({"local": service})
        self.assertEqual(report["actions"]["increment"]["phase"], "reconcile")
        self.assertEqual(report["actions"]["increment"]["status"], "uncertain")
        self.assertTrue(engine.resume({"local": service})["complete"])
        self.assertEqual(len(service.inspect()["history"]), 1)

    def test_mismatched_receipt_after_committed_effect_is_uncertain_not_a_precondition_rejection(
        self,
    ):
        engine, service, _ = self.workflow()

        class WrongReceipt(FileAdapter):
            def apply(self, action):
                super().apply(action)
                return Receipt("wrong-operation", action.fingerprint, "wrong-receipt")

        report = engine.resume({"local": WrongReceipt(service.path)})
        status = report["actions"]["increment"]
        self.assertEqual(status["status"], "uncertain")
        self.assertEqual(status["error_code"], "invalid_receipt")
        self.assertEqual(status["phase"], "receipt_validation")
        self.assertTrue(engine.resume({"local": service})["complete"])
        self.assertEqual(len(service.inspect()["history"]), 1)

    def test_failed_attempt_sync_never_contacts_adapter(self):
        engine, service, _ = self.workflow()
        path = engine.directory / "journal.jsonl"
        original_fsync = os.fsync

        def fail_journal_sync(descriptor):
            if os.fstat(descriptor).st_ino == path.stat().st_ino:
                raise OSError("attempt durability unavailable")
            original_fsync(descriptor)

        with (
            patch("recovery_under_interference.journal.os.fsync", fail_journal_sync),
            patch.object(service, "reconcile") as reconcile,
        ):
            with self.assertRaises(JournalIOError):
                engine.resume({"local": service})
            reconcile.assert_not_called()
        self.assertTrue(engine.resume({"local": service})["complete"])
        self.assertEqual(len(service.inspect()["history"]), 1)

    def test_partial_receipt_has_guarded_backup_repair_and_reconciles_existing_effect(self):
        engine, service, _ = self.workflow()
        path = engine.directory / "journal.jsonl"
        original_open = Path.open

        class PartialWriter:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def write(self, raw):
                if b'"type":"RECEIPT"' in raw:
                    self.stream.write(raw[: len(raw) // 2])
                    self.stream.flush()
                    raise OSError("partial receipt write")
                return self.stream.write(raw)

        def opened(file, mode="r", *args, **kwargs):
            stream = original_open(file, mode, *args, **kwargs)
            return PartialWriter(stream) if file == path and mode == "ab" else stream

        with patch.object(Path, "open", opened), self.assertRaises(JournalIOError):
            engine.resume({"local": service})
        broken = path.read_bytes()
        diagnostic = engine.diagnose()
        self.assertTrue(diagnostic["recoverable_tail"])
        self.assertEqual(diagnostic["actions"]["increment"]["status"], "inflight")
        with self.assertRaises(JournalError):
            engine.repair_tail(expected_sha256="0" * 64)
        self.assertEqual(path.read_bytes(), broken)
        repaired = engine.repair_tail(expected_sha256=diagnostic["journal_sha256"])
        self.assertEqual(Path(repaired["backup"]).read_bytes(), broken)
        self.assertTrue(engine.resume({"local": service})["complete"])
        self.assertEqual(len(service.inspect()["history"]), 1)

    def test_real_sigkill_before_and_after_atomic_service_commit(self):
        for phase in ("before", "after"):
            with self.subTest(phase=phase):
                root = self.root / phase
                service = FileAdapter.create(root / "service.json", {"counter": 0})
                action = Compensation("x", "custom", "counter", "increment", "op", {"amount": 1})
                engine = RecoveryEngine.create(root / "workflow", [action])
                script = """
import os,signal,sys
from recovery_under_interference import FileAdapter,RecoveryEngine
import recovery_under_interference.adapters as module
original=module.os.replace
def interrupted(source,target):
    if sys.argv[3]=='before': os.kill(os.getpid(),signal.SIGKILL)
    original(source,target)
    os.kill(os.getpid(),signal.SIGKILL)
module.os.replace=interrupted
RecoveryEngine(sys.argv[1]).resume({'custom':FileAdapter(sys.argv[2])})
"""
                child = subprocess.run(
                    [sys.executable, "-c", script, str(engine.directory), str(service.path), phase],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(child.returncode, -signal.SIGKILL, child.stderr)
                self.assertEqual(engine.inspect()["actions"]["x"]["status"], "inflight")
                self.assertTrue(engine.resume({"custom": FileAdapter(service.path)})["complete"])
                self.assertEqual(service.inspect()["resources"]["counter"]["value"], 1)
                self.assertEqual(len(service.inspect()["receipts"]), 1)
                self.assertEqual(len(service.inspect()["history"]), 1)

    @contextlib.contextmanager
    def held_lock(self, path):
        script = "from recovery_under_interference.journal import exclusive; import sys,time\nwith exclusive(sys.argv[1]):\n print('locked',flush=True)\n time.sleep(30)\n"
        child = subprocess.Popen(
            [sys.executable, "-c", script, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(5), "child did not acquire its lock")
            self.assertEqual(child.stdout.readline().strip(), "locked")
            yield
        finally:
            child.kill()
            child.communicate(timeout=5)

    def test_process_lock_contention_has_actionable_distinct_classification(self):
        engine, service, _ = self.workflow()
        before = (engine.directory / "journal.jsonl").read_bytes()
        with self.held_lock(engine.directory / ".lock"):
            with self.assertRaises(LockUnavailable):
                engine.resume({"local": service})
            with contextlib.redirect_stderr(io.StringIO()) as stream:
                self.assertEqual(main(["inspect", str(engine.directory)]), 3)
            self.assertEqual(json.loads(stream.getvalue())["error_code"], "lock_busy")
        self.assertEqual((engine.directory / "journal.jsonl").read_bytes(), before)
        with self.held_lock(service.lock_path):
            report = engine.resume({"local": service}, max_attempts=100)
            self.assertEqual(report["last_event"]["outcome"], "lock_busy")
            self.assertEqual(report["actions"]["increment"]["attempts"], 1)
            self.assertEqual(report["actions"]["increment"]["phase"], "reconcile")
        self.assertTrue(engine.resume({"local": service})["complete"])
        self.assertEqual(len(service.inspect()["history"]), 1)

    def test_public_inputs_are_frozen_and_invalid_json_is_rejected_before_writing(self):
        values = {"value": {"nested": [1]}}
        action = Compensation("x", "custom", "x", "set", "op", values)
        fingerprint = action.fingerprint
        values["value"]["nested"].append(2)
        self.assertEqual(action.fingerprint, fingerprint)
        self.assertEqual(action.to_dict()["arguments"]["value"]["nested"], [1])
        with self.assertRaises(TypeError):
            action.arguments["value"]["nested"] = ()
        details = {"nested": [1]}
        receipt = Receipt("op", fingerprint, "effect", details)
        details["nested"].append(2)
        self.assertEqual(receipt.to_dict()["details"], {"nested": [1]})
        for value in ({1: "coerced"}, {"n": float("nan")}, {"n": 10**2000}, {"bad": "\ud800"}):
            with self.subTest(value_type=type(value)), self.assertRaises(ValueError):
                FileAdapter.create(self.root / "invalid" / "state.json", {"x": value})
        self.assertFalse((self.root / "invalid").exists())
        for raw in ('{"x":1,"x":2}', "1e400", "NaN"):
            with self.assertRaises(ValueError):
                loads(raw)
        with self.assertRaises(ValueError):
            Reconciliation("unknown", "not-a-receipt")

    def test_oversized_state_and_overflow_are_classified_without_effect(self):
        oversized = self.root / "oversized.json"
        with oversized.open("wb") as stream:
            stream.truncate(MAX_DOCUMENT_BYTES + 1)
        with self.assertRaisesRegex(JournalError, "exceeds"):
            FileAdapter(oversized).inspect()
        service = FileAdapter.create(self.root / "huge-number.json", {"counter": 1e308})
        action = Compensation("x", "local", "counter", "increment", "op", {"amount": 1e308})
        with self.assertRaisesRegex(Conflict, "numeric range"):
            service.apply(action)
        self.assertEqual(service.inspect()["history"], [])

    def test_exact_byte_limit_state_creation_and_replacement_remain_readable(self):
        template = {
            "schema": 1,
            "resources": {"counter": {"value": "", "version": 0, "allowed": True}},
            "receipts": {},
            "history": [],
        }
        overhead = len(encoded({"state": template, "sha256": checksum(template)}))
        value = "x" * (MAX_DOCUMENT_BYTES - overhead)
        created = FileAdapter.create(self.root / "exact-create.json", {"counter": value})
        self.assertEqual(created.path.stat().st_size, MAX_DOCUMENT_BYTES)
        self.assertEqual(created.inspect()["resources"]["counter"]["value"], value)

        template["resources"]["counter"]["allowed"] = False
        template["history"].append({"actor": "operator", "resource": "counter", "allowed": False})
        overhead = len(encoded({"state": template, "sha256": checksum(template)}))
        value = "x" * (MAX_DOCUMENT_BYTES - overhead)
        changed = FileAdapter.create(self.root / "exact-replace.json", {"counter": value})
        changed.set_permission("counter", False)
        self.assertEqual(changed.path.stat().st_size, MAX_DOCUMENT_BYTES)
        state = changed.inspect()
        self.assertEqual(state["resources"]["counter"]["value"], value)
        self.assertFalse(state["resources"]["counter"]["allowed"])
        before = changed.path.read_bytes()
        with self.assertRaisesRegex(Conflict, "exceeds JSON limits"):
            changed.set_permission("counter", True)
        self.assertEqual(changed.path.read_bytes(), before)

    def test_receipt_growth_over_limit_preserves_readable_state_and_no_effect(self):
        template = {
            "schema": 1,
            "resources": {"counter": {"value": 0, "version": 1, "allowed": True}},
            "receipts": {},
            "history": [{"actor": "operator", "resource": "counter", "before": "", "after": 0}],
        }
        overhead = len(encoded({"state": template, "sha256": checksum(template)}))
        value = "x" * (MAX_DOCUMENT_BYTES - overhead)
        service = FileAdapter.create(self.root / "receipt-overflow.json", {"counter": value})
        service.operator_update("counter", 0)
        self.assertEqual(service.path.stat().st_size, MAX_DOCUMENT_BYTES)
        before = service.path.read_bytes()
        action = Compensation("x", "local", "counter", "increment", "op", {"amount": 1})
        with self.assertRaisesRegex(Conflict, "exceeds JSON limits"):
            service.apply(action)
        self.assertEqual(service.path.read_bytes(), before)
        state = service.inspect()
        self.assertEqual(state["resources"]["counter"]["value"], 0)
        self.assertEqual(state["receipts"], {})
        self.assertEqual(len(state["history"]), 1)

    def test_legacy_state_with_newline_remains_readable(self):
        _, service, _ = self.workflow()
        service.path.write_bytes(service.path.read_bytes() + b"\n")
        self.assertEqual(service.inspect()["resources"]["counter"]["value"], 0)

    def test_invalid_historical_receipts_block_an_unrelated_effect(self):
        _, service, action = self.workflow()
        service.apply(action)
        original = service.inspect()
        receipt = original["receipts"][action.operation_id]
        variants = [None, {**receipt, "unexpected": 1}]
        for key in ("operation_id", "action_fingerprint", "effect_id"):
            variants.extend(({k: v for k, v in receipt.items() if k != key}, {**receipt, key: " "}))
        variants.extend(
            {**receipt, **change}
            for change in (
                {"operation_id": "another-operation"},
                {"action_fingerprint": "A" * 64},
                {"action_fingerprint": "a" * 63},
                {"details": None},
                {"details": []},
            )
        )
        unrelated = Compensation("other", "local", "counter", "increment", "other", {"amount": 1})
        for raw in variants:
            with self.subTest(receipt=raw):
                data = json.loads(json.dumps(original))
                data["receipts"][action.operation_id] = raw
                before = encoded({"state": data, "sha256": checksum(data)})
                service.path.write_bytes(before)
                with self.assertRaises(JournalError):
                    service.inspect()
                with self.assertRaises(JournalError):
                    service.apply(unrelated)
                self.assertEqual(service.path.read_bytes(), before)

    def test_nested_receipt_json_limits_still_apply(self):
        _, service, action = self.workflow()
        service.apply(action)
        original = service.inspect()
        nested = None
        for _ in range(70):
            nested = [nested]
        for value in (float("nan"), 1 << 4096, "\ud800", nested):
            with self.subTest(value_type=type(value)):
                data = json.loads(json.dumps(original))
                data["receipts"][action.operation_id]["details"] = {"nested": value}
                raw_data = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
                digest = hashlib.sha256(raw_data).hexdigest()
                service.path.write_text(json.dumps({"state": data, "sha256": digest}))
                before = service.path.read_bytes()
                with self.assertRaises(JournalError):
                    service.inspect()
                self.assertEqual(service.path.read_bytes(), before)

    def test_legacy_missing_receipt_details_and_detached_return_values(self):
        _, service, action = self.workflow()
        expected = service.apply(action)
        data = service.inspect()
        del data["receipts"][action.operation_id]["details"]
        service.path.write_bytes(encoded({"state": data, "sha256": checksum(data)}))
        before = service.path.read_bytes()
        inspected = service.inspect()
        inspected["receipts"][action.operation_id]["effect_id"] = "caller-edited"
        recovered = service.reconcile(action).receipt
        self.assertEqual(recovered.effect_id, expected.effect_id)
        self.assertEqual(recovered.to_dict()["details"], {})
        with self.assertRaises(TypeError):
            recovered.details["new"] = "caller-edit"
        self.assertEqual(service.path.read_bytes(), before)

    def test_initial_files_and_directories_are_synchronized(self):
        from recovery_under_interference import journal

        with patch.object(journal, "sync_directory", wraps=journal.sync_directory) as synced:
            engine, _, _ = self.workflow()
            synced_paths = [Path(call.args[0]) for call in synced.call_args_list]
            self.assertIn(engine.directory, synced_paths)
            self.assertIn(engine.directory.parent, synced_paths)
        with patch("recovery_under_interference.adapters.sync_directory") as synced:
            service = FileAdapter.create(self.root / "separate.json", {"counter": 0})
            synced.assert_called_with(service.path.parent)

    def test_resume_replays_once_for_many_business_operations(self):
        class Provider:
            def reconcile(self, action):
                return Reconciliation("not_applied")

            def apply(self, action):
                return Receipt(action.operation_id, action.fingerprint, action.id)

        count = 300
        actions = [
            Compensation(
                str(i),
                "provider",
                "x",
                "business_operation",
                f"op-{i}",
                requires=(str(i - 1),) if i else (),
            )
            for i in range(count)
        ]
        engine = RecoveryEngine.create(self.root / "bulk", reversed(actions))
        with patch.object(engine_module, "replay", wraps=engine_module.replay) as replay:
            report = engine.resume({"provider": Provider()}, max_attempts=count)
        self.assertTrue(report["complete"])
        self.assertEqual(replay.call_count, 1)
        self.assertEqual(report["event_count"], 2 * count + 2)

    def test_custom_business_adapter_recovers_killed_payment_and_completes_warehouse(self):
        import runpy

        example = Path(__file__).resolve().parents[1] / "examples" / "business_refund.py"
        business = runpy.run_path(str(example))["BusinessAdapter"]
        payment = FileAdapter.create(self.root / "payments.json", {"order-42": 7500})
        warehouse = FileAdapter.create(self.root / "warehouse.json", {"sku-17": 9})
        actions = [
            Compensation(
                "refund",
                "payments",
                "order-42",
                "refund_payment",
                "order-42:refund",
                {"amount": 2500},
                {"version": 0},
            ),
            Compensation(
                "stock",
                "warehouse",
                "sku-17",
                "restore_stock",
                "order-42:stock",
                {"amount": 1},
                {"version": 0},
                ("refund",),
            ),
        ]
        engine = RecoveryEngine.create(self.root / "business-workflow", actions)
        script = """
import os,runpy,signal,sys
from recovery_under_interference import FileAdapter,RecoveryEngine
BusinessAdapter=runpy.run_path(sys.argv[1])['BusinessAdapter']
class InterruptedProvider(FileAdapter):
    def apply(self,action):
        super().apply(action)
        os.kill(os.getpid(),signal.SIGKILL)
RecoveryEngine(sys.argv[2]).resume({
    'payments':BusinessAdapter(InterruptedProvider(sys.argv[3]),'refund_payment'),
    'warehouse':BusinessAdapter(FileAdapter(sys.argv[4]),'restore_stock'),
})
"""
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                str(example),
                str(engine.directory),
                str(payment.path),
                str(warehouse.path),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(child.returncode, -signal.SIGKILL, child.stderr)
        self.assertEqual(len(payment.inspect()["history"]), 1)
        self.assertEqual(len(warehouse.inspect()["history"]), 0)
        adapters = {
            "payments": business(payment, "refund_payment"),
            "warehouse": business(warehouse, "restore_stock"),
        }
        report = RecoveryEngine(engine.directory).resume(adapters)
        self.assertTrue(report["complete"])
        self.assertEqual(
            report["actions"]["refund"]["receipt"]["action_fingerprint"], actions[0].fingerprint
        )
        self.assertEqual(payment.inspect()["resources"]["order-42"]["value"], 10000)
        self.assertEqual(warehouse.inspect()["resources"]["sku-17"]["value"], 10)
        self.assertEqual(len(payment.inspect()["history"]), 1)
        self.assertEqual(len(warehouse.inspect()["history"]), 1)
        self.assertTrue(RecoveryEngine(engine.directory).resume(adapters)["complete"])
        self.assertEqual(len(payment.inspect()["history"]), 1)

    def test_invalid_adapter_configuration_does_not_append_attempt(self):
        engine, _, _ = self.workflow()
        path = engine.directory / "journal.jsonl"
        before = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "implement reconcile and apply"):
            engine.resume({"local": object()})
        self.assertEqual(path.read_bytes(), before)

    def test_checksummed_invalid_history_is_not_repaired(self):
        engine, _, _ = self.workflow()
        path = engine.directory / "journal.jsonl"
        first = json.loads(path.read_text())
        event = {"type": "RECEIPT", "action": "increment", "receipt": {}}
        body = {"seq": 1, "previous": first["sha256"], "event": event}
        with path.open("ab") as stream:
            stream.write(encoded({**body, "sha256": checksum(body)}) + b"\n")
        before = path.read_bytes()
        diagnostic = engine.diagnose()
        self.assertFalse(diagnostic["recoverable_tail"])
        with self.assertRaises(JournalError):
            engine.repair_tail(expected_sha256=diagnostic["journal_sha256"])
        self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
