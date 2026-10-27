import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from recovery_under_interference import (
    AmbiguousOutcome,
    Compensation,
    FileAdapter,
    JournalError,
    JournalIOError,
    RecoveryEngine,
)
from recovery_under_interference.cli import main
from recovery_under_interference.journal import Journal


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.service = FileAdapter.create(self.root / "service.json", {"counter": 0})
        self.action = Compensation(
            "credit", "local", "counter", "increment", "credit-1", {"amount": 12}
        )
        self.engine = RecoveryEngine.create(self.root / "workflow", [self.action])
        self.path = self.engine.directory / "journal.jsonl"
        self.reason = "Operator reviewed replacement case-42; stop retrying this plan"

    def test_blocked_workflow_retires_without_effect_or_action_state_changes(self):
        self.service.set_permission("counter", False)
        before = self.engine.resume({"local": self.service})
        service_before = self.service.inspect()
        self.assertFalse(before["retired"])
        self.assertIsNone(before["retirement"])
        report = self.engine.retire(reason=self.reason)
        self.assertTrue(report["retired"])
        self.assertFalse(report["complete"])
        self.assertEqual(report["retirement"], {"type": "RETIRED", "reason": self.reason})
        self.assertEqual(report["event_count"], before["event_count"] + 1)
        for field in ("actions", "plan"):
            self.assertEqual(report[field], before[field])
        self.assertEqual(self.service.inspect(), service_before)
        self.service.set_permission("counter", True)
        with (
            patch.object(self.service, "reconcile") as reconcile,
            patch.object(self.service, "apply") as apply,
        ):
            self.assertEqual(self.engine.resume({"local": self.service}), report)
            reconcile.assert_not_called()
            apply.assert_not_called()
        self.assertEqual(RecoveryEngine(self.engine.directory).resume({}), report)

    def test_identical_retry_is_durable_noop_and_conflicting_reason_rejects(self):
        report = self.engine.retire(reason=self.reason)
        before = self.path.read_bytes()
        with patch("recovery_under_interference.journal.os.fsync", wraps=os.fsync) as fsync:
            self.assertEqual(self.engine.retire(reason=self.reason), report)
            self.assertEqual(fsync.call_count, 2)
        with self.assertRaisesRegex(ValueError, "different reason"):
            self.engine.retire(reason="Changed explanation")
        self.assertEqual(self.path.read_bytes(), before)
        report["retirement"]["reason"] = "Mutated response"
        self.assertEqual(self.engine.inspect()["retirement"]["reason"], self.reason)

    def test_invalid_reason_and_complete_workflow_leave_bytes_unchanged(self):
        before = self.path.read_bytes()
        for reason in (None, 1, False, "", "  ", "x" * 4097, "bad\ud800"):
            with self.subTest(reason=repr(reason)[:30]), self.assertRaises(ValueError):
                self.engine.retire(reason=reason)
            self.assertEqual(self.path.read_bytes(), before)
        self.assertTrue(self.engine.resume({"local": self.service})["complete"])
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, "complete"):
            self.engine.retire(reason=self.reason)
        self.assertEqual(self.path.read_bytes(), before)

    def test_unknown_committed_effect_remains_unknown_and_is_not_reapplied(self):
        class LostReply(FileAdapter):
            def apply(self, action):
                super().apply(action)
                raise AmbiguousOutcome("Provider committed; reply lost")

        before = self.engine.resume({"local": LostReply(self.service.path)})
        self.assertEqual(before["actions"]["credit"]["status"], "uncertain")
        self.assertEqual(self.service.inspect()["resources"]["counter"]["value"], 12)
        report = self.engine.retire(reason=self.reason)
        self.assertEqual(report["actions"], before["actions"])
        self.assertFalse(report["complete"])
        self.assertEqual(self.engine.resume({"local": self.service}), report)
        self.assertEqual(len(self.service.inspect()["history"]), 1)

    def test_inflight_retirement_preserves_unresolved_attempt(self):
        Journal(self.path).append({"type": "ATTEMPT", "action": "credit", "number": 1})
        before = self.engine.inspect()
        report = self.engine.retire(reason=self.reason)
        self.assertEqual(report["actions"], before["actions"])
        self.assertEqual(report["actions"]["credit"]["status"], "inflight")
        diagnostic = self.engine.diagnose()
        self.assertTrue(diagnostic["retired"])
        self.assertIn("Investigate unfinished effects", diagnostic["next_step"])

    def test_retirement_fsync_failure_can_be_reconfirmed_without_second_event(self):
        with patch(
            "recovery_under_interference.journal.os.fsync", side_effect=OSError("disk sync failed")
        ):
            with self.assertRaises(JournalIOError) as failure:
                self.engine.retire(reason=self.reason)
        self.assertEqual(failure.exception.stage, "fsync")
        before = self.path.read_bytes()
        self.assertTrue(self.engine.inspect()["retired"])
        with patch(
            "recovery_under_interference.journal.os.fsync", side_effect=OSError("still unavailable")
        ):
            with self.assertRaises(JournalIOError):
                self.engine.retire(reason=self.reason)
        self.assertEqual(self.path.read_bytes(), before)
        report = self.engine.retire(reason=self.reason)
        self.assertTrue(report["retired"])
        self.assertEqual(report["event_count"], 2)
        self.assertEqual(self.path.read_bytes(), before)

    def test_directory_fsync_failure_on_retirement_retry_is_not_acknowledged(self):
        with patch(
            "recovery_under_interference.journal.sync_directory",
            side_effect=OSError("directory sync failed"),
        ):
            for _ in range(2):
                with self.assertRaises(JournalIOError) as failure:
                    self.engine.retire(reason=self.reason)
                self.assertEqual(failure.exception.stage, "directory fsync")
        self.assertEqual(self.engine.retire(reason=self.reason)["event_count"], 2)

    def test_partial_retirement_requires_repair_and_explicit_retry(self):
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
                self.stream.write(raw[: len(raw) // 2])
                self.stream.flush()
                raise OSError("Interrupted retirement write")

        def opened(path, mode="r", *args, **kwargs):
            stream = original_open(path, mode, *args, **kwargs)
            return PartialWriter(stream) if path == self.path and mode == "ab" else stream

        with patch.object(Path, "open", opened), self.assertRaises(JournalIOError):
            self.engine.retire(reason=self.reason)
        broken = self.path.read_bytes()
        with self.assertRaises(JournalError):
            self.engine.resume({"local": self.service})
        diagnostic = self.engine.diagnose()
        self.assertFalse(diagnostic["retired"])
        self.assertTrue(diagnostic["recoverable_tail"])
        self.assertIn("retry any interrupted retirement first", diagnostic["next_step"])
        repaired = self.engine.repair_tail(expected_sha256=diagnostic["journal_sha256"])
        self.assertEqual(Path(repaired["backup"]).read_bytes(), broken)
        self.assertFalse(repaired["retired"])
        self.assertTrue(self.engine.retire(reason=self.reason)["retired"])
        self.assertEqual(self.engine.resume({})["event_count"], 2)

    def test_complete_event_after_retirement_is_rejected_before_adapters(self):
        self.engine.retire(reason=self.reason)
        Journal(self.path).append(
            {"type": "SESSION_END", "outcome": "blocked", "attempts_this_session": 0}
        )
        before = self.path.read_bytes()
        with patch.object(self.service, "reconcile") as reconcile:
            with self.assertRaisesRegex(JournalError, "terminal retirement"):
                self.engine.resume({"local": self.service})
            reconcile.assert_not_called()
        self.assertFalse(self.engine.diagnose()["recoverable_tail"])
        with self.assertRaises(JournalError):
            self.engine.repair_tail(expected_sha256=self.engine.diagnose()["journal_sha256"])
        self.assertEqual(self.path.read_bytes(), before)

    def test_torn_tail_after_retirement_repair_remains_retired(self):
        retired = self.engine.retire(reason=self.reason)
        with self.path.open("ab") as stream:
            stream.write(b'{"unexpected":')
        diagnostic = self.engine.diagnose()
        self.assertTrue(diagnostic["retired"])
        self.assertIn("valid prefix remains retired", diagnostic["next_step"])
        self.engine.repair_tail(expected_sha256=diagnostic["journal_sha256"])
        self.assertEqual(self.engine.resume({}), retired)

    def test_cli_retired_resume_reports_incomplete_without_requiring_adapter(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(
                main(["retire", str(self.engine.directory), "--reason", self.reason]), 0
            )
        retired = json.loads(output.getvalue())
        before = self.path.read_bytes()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(main(["resume", str(self.engine.directory)]), 0)
        self.assertEqual(json.loads(output.getvalue()), retired)
        self.assertTrue(retired["retired"])
        self.assertFalse(retired["complete"])
        self.assertEqual(self.path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
