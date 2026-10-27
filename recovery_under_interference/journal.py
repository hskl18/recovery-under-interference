"""Durable single-writer JSONL journal with explicit storage failure boundaries."""

import fcntl
import hashlib
import os
import shutil
from contextlib import contextmanager
from pathlib import Path

from .jsondata import MAX_DOCUMENT_BYTES, encoded, loads, plain


class JournalError(ValueError):
    """A journal is corrupt, incomplete, or incompatible; no service was called."""


class JournalIOError(OSError):
    """Persistence may be incomplete; stop writing and reopen before recovery."""

    def __init__(self, path, stage, cause):
        self.path = str(path)
        self.stage = stage
        self.durability_unknown = True
        super().__init__(
            f"Journal persistence failed during {stage} at {path}; stop and diagnose before resuming: {cause}"
        )


class LockUnavailable(RuntimeError):
    """A cooperating process holds the requested local lock; retry later."""

    def __init__(self, path):
        self.path = str(path)
        super().__init__(
            f"Another process holds {path}; retry after that process releases its lock"
        )


def checksum(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_mkdir(path, *, exist_ok=True):
    """Persist each newly created directory entry, including missing ancestors."""
    path = Path(path)
    if path.exists():
        if not exist_ok or not path.is_dir():
            raise FileExistsError(str(path))
        return
    durable_mkdir(path.parent)
    try:
        path.mkdir()
    except FileExistsError:
        if not exist_ok or not path.is_dir():
            raise
    sync_directory(path)
    sync_directory(path.parent)


@contextmanager
def exclusive(path):
    """Fail immediately when a cooperating process already owns the lock."""
    with Path(path).open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise LockUnavailable(path) from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


class Journal:
    """Caller holds the workflow lock; a failed append poisons this instance."""

    def __init__(self, path, *, allow_incomplete_tail=False):
        self.path = Path(path)
        self.events = []
        self.head = "0" * 64
        self.valid_bytes = 0
        self.incomplete_tail_bytes = 0
        self._failed = False
        if not self.path.exists():
            return
        with self.path.open("rb") as stream:
            for number in range(1, 2**63):
                line = stream.readline(MAX_DOCUMENT_BYTES + 2)
                if not line:
                    break
                if len(line) > MAX_DOCUMENT_BYTES + 1:
                    raise JournalError(f"Journal record {number} exceeds the byte limit")
                if not line.endswith(b"\n"):
                    self.incomplete_tail_bytes = len(line)
                    if allow_incomplete_tail:
                        break
                    raise JournalError(
                        f"Incomplete journal tail at byte {self.valid_bytes}; run diagnose, then guarded repair-tail"
                    )
                try:
                    record = loads(line[:-1])
                    if not isinstance(record, dict) or set(record) != {
                        "seq",
                        "previous",
                        "event",
                        "sha256",
                    }:
                        raise ValueError("invalid record fields")
                    body = {key: record[key] for key in ("seq", "previous", "event")}
                    if (
                        type(record["seq"]) is not int
                        or record["seq"] != len(self.events)
                        or record["previous"] != self.head
                    ):
                        raise ValueError("broken sequence")
                    if record["sha256"] != checksum(body) or not isinstance(record["event"], dict):
                        raise ValueError("checksum or event mismatch")
                except (ValueError, TypeError, KeyError) as error:
                    raise JournalError(
                        f"Invalid journal line {number} at byte {self.valid_bytes}: {error}"
                    ) from error
                self.events.append(record["event"])
                self.head = record["sha256"]
                self.valid_bytes += len(line)

    def append(self, event):
        if self._failed:
            raise JournalError("This journal had a persistence failure; reopen it before appending")
        if self.incomplete_tail_bytes:
            raise JournalError("Incomplete journal tail cannot be appended")
        # Copy before serialization so caller mutation cannot rewrite the in-memory history.
        event = plain(event)
        body = {"seq": len(self.events), "previous": self.head, "event": event}
        record = {**body, "sha256": checksum(body)}
        raw = encoded(record) + b"\n"
        stage = "open"
        try:
            with self.path.open("ab") as stream:
                stage = "write"
                written = stream.write(raw)
                if written != len(raw):
                    raise OSError("Short journal write")
                stage = "flush"
                stream.flush()
                stage = "fsync"
                os.fsync(stream.fileno())
            # Also retry this sync after a prior process died during first-file creation.
            stage = "directory fsync"
            sync_directory(self.path.parent)
        except OSError as error:
            self._failed = True
            raise JournalIOError(self.path, stage, error) from error
        self.events.append(event)
        self.head = record["sha256"]
        self.valid_bytes += len(raw)

    def confirm_durable(self):
        """Reconfirm a loaded terminal decision before acknowledging an identical retry."""
        if self._failed or self.incomplete_tail_bytes:
            raise JournalError("Reopen a complete journal before confirming durability")
        stage = "open"
        try:
            with self.path.open("rb") as stream:
                stage = "fsync"
                os.fsync(stream.fileno())
            stage = "directory fsync"
            sync_directory(self.path.parent)
        except OSError as error:
            self._failed = True
            raise JournalIOError(self.path, stage, error) from error


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def repair_incomplete_tail(journal, expected_sha256):
    """Caller holds the lock and has validated the complete prefix's workflow state."""
    if not journal.incomplete_tail_bytes:
        raise JournalError("No incomplete final record to repair")
    if file_sha256(journal.path) != expected_sha256:
        raise JournalError("Journal changed since diagnosis; expected SHA-256 does not match")
    backup = journal.path.with_name(f"{journal.path.name}.before-repair-{expected_sha256}")
    if backup.exists():
        if file_sha256(backup) != expected_sha256:
            raise JournalError(
                "An incomplete or different repair backup already exists; preserve it for investigation"
            )
        with backup.open("rb") as stream:
            os.fsync(stream.fileno())
    else:
        with journal.path.open("rb") as source, backup.open("xb") as destination:
            shutil.copyfileobj(source, destination, 1024 * 1024)
            destination.flush()
            os.fsync(destination.fileno())
    sync_directory(backup.parent)
    with journal.path.open("r+b") as stream:
        stream.truncate(journal.valid_bytes)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(journal.path.parent)
    return backup
