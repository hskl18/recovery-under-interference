"""Durable JSON-file and SQLite adapters for local applications and integration examples."""

import os
import sqlite3
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

from .engine import Compensation, Conflict, PermissionDenied, Receipt, Reconciliation
from .journal import (
    JournalError,
    LockUnavailable,
    checksum,
    durable_mkdir,
    exclusive,
    sync_directory,
)
from .jsondata import encoded, load, loads, plain


def _check_resources(resources):
    if (
        not isinstance(resources, dict)
        or not resources
        or any(type(key) is not str or not key.strip() for key in resources)
    ):
        raise ValueError("resources must be a nonempty object with string keys")


def _effect(action, resource, lookup):
    """Enforce permission and preconditions at commit time; return (before, after)."""
    if resource is None:
        raise Conflict(f"Unknown resource: {action.resource}")
    if not resource["allowed"]:
        raise PermissionDenied(f"Permission revoked for {action.resource}")
    if set(action.preconditions) - {"version", "guards"}:
        raise Conflict("Unsupported precondition fields")
    if "version" in action.preconditions:
        expected = action.preconditions["version"]
        if type(expected) is not int or expected < 0:
            raise Conflict("version must be a nonnegative integer, excluding booleans")
        if resource["version"] != expected:
            raise Conflict(f"Resource version changed: {action.resource}")
    guards = action.preconditions.get("guards", [])
    if not isinstance(guards, (list, tuple)):
        raise Conflict("guards must be an array")
    for guard in guards:
        if (
            not isinstance(guard, Mapping)
            or "resource" not in guard
            or set(guard) - {"resource", "version", "value"}
        ):
            raise Conflict("Invalid guard")
        if not isinstance(guard["resource"], str) or not guard["resource"].strip():
            raise Conflict("Guard resource must be a nonempty string")
        if "version" in guard and (type(guard["version"]) is not int or guard["version"] < 0):
            raise Conflict("Guard version must be a nonnegative integer, excluding booleans")
        current = lookup(guard["resource"])
        if (
            current is None
            or ("version" in guard and current["version"] != guard["version"])
            or ("value" in guard and encoded(current["value"]) != encoded(guard["value"]))
        ):
            raise Conflict(f"Dependency changed: {guard['resource']}")
    before = resource["value"]
    if action.operation == "set" and set(action.arguments) == {"value"}:
        return before, plain(action.arguments["value"])
    if action.operation == "increment" and set(action.arguments) == {"amount"}:
        amount = action.arguments["amount"]
        if (
            isinstance(before, bool)
            or not isinstance(before, (int, float))
            or isinstance(amount, bool)
            or not isinstance(amount, (int, float))
        ):
            raise Conflict("increment requires numeric state and amount")
        try:
            after = before + amount
            encoded(after)
        except (ValueError, OverflowError) as error:
            raise Conflict(
                "increment result exceeds the supported finite JSON numeric range"
            ) from error
        return before, after
    raise Conflict("Unsupported operation or arguments")


def _new_receipt(action, before, after, version):
    try:
        receipt = Receipt(
            action.operation_id,
            action.fingerprint,
            checksum({"operation": action.operation_id, "fingerprint": action.fingerprint}),
            {
                "resource": action.resource,
                "before": before,
                "after": after,
                "version_before": version,
                "version_after": version + 1,
            },
        )
    except ValueError as error:
        raise Conflict(f"Effect exceeds JSON limits; no change was written: {error}") from error
    receipt.validate(action)
    return receipt


class FileAdapter:
    """Commit resource changes and their deduplication receipt in one atomic replace.

    This adapter is for modest local state on a local POSIX filesystem.
    All precondition resources must be owned by this same adapter.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @classmethod
    def create(cls, path: str | Path, resources: dict[str, Any]) -> "FileAdapter":
        _check_resources(resources)
        data = {
            "schema": 1,
            "resources": {
                key: {"value": value, "version": 0, "allowed": True}
                for key, value in resources.items()
            },
            "receipts": {},
            "history": [],
        }
        raw = encoded({"state": data, "sha256": checksum(data)})
        path = Path(path)
        durable_mkdir(path.parent)
        adapter = cls(path)
        with exclusive(adapter.lock_path):
            with path.open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            sync_directory(path.parent)
        return adapter

    @property
    def lock_path(self):
        return self.path.with_name(self.path.name + ".lock")

    def _read(self):
        try:
            envelope = load(self.path)
            if not isinstance(envelope, dict) or set(envelope) != {"state", "sha256"}:
                raise ValueError("Invalid state envelope")
            data = envelope["state"]
            if not isinstance(data, dict) or set(data) != {
                "schema",
                "resources",
                "receipts",
                "history",
            }:
                raise ValueError("Invalid state fields")
            if (
                checksum(data) != envelope["sha256"]
                or type(data["schema"]) is not int
                or data["schema"] != 1
            ):
                raise ValueError("State checksum or schema mismatch")
            if (
                not isinstance(data["resources"], dict)
                or not isinstance(data["receipts"], dict)
                or not isinstance(data["history"], list)
            ):
                raise ValueError("Invalid state collections")
            if not data["resources"] or any(not key.strip() for key in data["resources"]):
                raise ValueError("Invalid resource names")
            for item in data["resources"].values():
                if (
                    not isinstance(item, dict)
                    or set(item) != {"value", "version", "allowed"}
                    or type(item["version"]) is not int
                    or item["version"] < 0
                    or type(item["allowed"]) is not bool
                ):
                    raise ValueError("Invalid resource state")
            for operation_id, raw in data["receipts"].items():
                if not isinstance(raw, dict):
                    raise ValueError("Invalid stored receipt")
                # Bounded JSON and checksum checks already validated all nested values.
                Receipt._validate_fields(**{"details": {}, **raw})
                if operation_id != raw["operation_id"]:
                    raise ValueError("Receipt operation key mismatch")
            for item in data["history"]:
                if (
                    not isinstance(item, dict)
                    or item.get("actor") not in ("operator", "recovery")
                    or item.get("resource") not in data["resources"]
                ):
                    raise ValueError("Invalid effect history")
            return data
        except (ValueError, KeyError, TypeError) as error:
            raise JournalError(f"Invalid adapter state: {error}") from error

    def _write(self, data):
        try:
            raw = encoded({"state": data, "sha256": checksum(data)})
        except ValueError as error:
            raise Conflict(
                f"Local state exceeds JSON limits; no change was written: {error}"
            ) from error
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.path.parent, prefix=".recovery-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            temporary = None
            sync_directory(self.path.parent)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @staticmethod
    def _receipt(data, action):
        raw = data["receipts"].get(action.operation_id)
        if raw is None:
            return None
        receipt = Receipt(**raw)
        receipt.validate(action)
        return receipt

    def _confirm_durable(self):
        # A prior worker may have died after rename but before syncing its directory entry.
        with self.path.open("rb") as stream:
            os.fsync(stream.fileno())
        sync_directory(self.path.parent)

    def inspect(self) -> dict[str, Any]:
        with exclusive(self.lock_path):
            return self._read()

    def reconcile(self, action: Compensation) -> Reconciliation:
        with exclusive(self.lock_path):
            receipt = self._receipt(self._read(), action)
            if receipt:
                self._confirm_durable()
            return Reconciliation("applied", receipt) if receipt else Reconciliation("not_applied")

    def apply(self, action: Compensation) -> Receipt:
        with exclusive(self.lock_path):
            data = self._read()
            existing = self._receipt(data, action)
            if existing:
                self._confirm_durable()
                return existing
            resource = data["resources"].get(action.resource)
            before, after = _effect(action, resource, data["resources"].get)
            receipt = _new_receipt(action, before, after, resource["version"])
            resource.update(value=after, version=resource["version"] + 1)
            data["receipts"][action.operation_id] = receipt.to_dict()
            data["history"].append(
                {"actor": "recovery", "operation_id": action.operation_id, **plain(receipt.details)}
            )
            self._write(data)
            return receipt

    def operator_update(self, resource: str, value: Any) -> None:
        """Apply an explicit local operator edit, preserving a separate history entry."""
        value = plain(value)
        encoded(value)
        with exclusive(self.lock_path):
            data = self._read()
            item = data["resources"][resource]
            data["history"].append(
                {"actor": "operator", "resource": resource, "before": item["value"], "after": value}
            )
            item.update(value=value, version=item["version"] + 1)
            self._write(data)

    def set_permission(self, resource: str, allowed: bool) -> None:
        """Change permission in the local example store, not a remote account."""
        if type(allowed) is not bool:
            raise ValueError("allowed must be a boolean")
        with exclusive(self.lock_path):
            data = self._read()
            data["resources"][resource]["allowed"] = allowed
            data["history"].append({"actor": "operator", "resource": resource, "allowed": allowed})
            self._write(data)


class SQLiteAdapter:
    """Commit each effect and its deduplication receipt in one SQLite transaction.

    Receipts are keyed by operation ID, so reconciliation and apply read only the rows they
    need instead of the complete state that FileAdapter rewrites on every effect.
    The database must live on a local filesystem with working SQLite locks.
    All precondition resources must be owned by this same database.
    """

    SCHEMA = 1
    # Marks databases created here, so an unrelated SQLite file is never adopted.
    APPLICATION_ID = 0x52554931

    def __init__(self, path: str | Path, *, timeout: float = 5.0) -> None:
        self.path = Path(path)
        self.timeout = timeout

    @classmethod
    def create(cls, path: str | Path, resources: dict[str, Any]) -> "SQLiteAdapter":
        _check_resources(resources)
        rows = [(name, encoded(value).decode()) for name, value in resources.items()]
        path = Path(path)
        durable_mkdir(path.parent)
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".recovery-", suffix=".sqlite", delete=False
        ) as stream:
            temporary = Path(stream.name)
        try:
            db = cls(temporary)._connect()
            try:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "CREATE TABLE resources (name TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL,"
                    " version INTEGER NOT NULL CHECK (version >= 0),"
                    " allowed INTEGER NOT NULL CHECK (allowed IN (0, 1)))"
                )
                db.execute(
                    "CREATE TABLE receipts (operation_id TEXT PRIMARY KEY NOT NULL,"
                    " receipt TEXT NOT NULL)"
                )
                db.execute(
                    "CREATE TABLE history (seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                    " entry TEXT NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO resources (name, value, version, allowed) VALUES (?, ?, 0, 1)",
                    rows,
                )
                db.execute(f"PRAGMA application_id = {cls.APPLICATION_ID}")
                db.execute(f"PRAGMA user_version = {cls.SCHEMA}")
                db.execute("COMMIT")
                # Rollback-journal commits are not durable across power loss with FULL sync.
                if db.execute("PRAGMA journal_mode = WAL").fetchone()[0] != "wal":
                    raise OSError("SQLite could not enable WAL journal mode")
            finally:
                db.close()
            # Linking the finished file publishes it atomically and refuses an existing path.
            os.link(temporary, path)
            sync_directory(path.parent)
        finally:
            for leftover in ("", "-wal", "-shm", "-journal"):
                Path(f"{temporary}{leftover}").unlink(missing_ok=True)
        return cls(path)

    def _connect(self):
        uri = self.path.absolute().as_uri() + "?mode=rw"
        db = sqlite3.connect(uri, uri=True, timeout=self.timeout, isolation_level=None)
        try:
            db.execute("PRAGMA synchronous = FULL")
        except BaseException:
            db.close()
            raise
        return db

    @contextmanager
    def _transaction(self, *, write):
        db = None
        try:
            db = self._connect()
            header = (
                db.execute("PRAGMA application_id").fetchone()[0],
                db.execute("PRAGMA user_version").fetchone()[0],
                db.execute("PRAGMA journal_mode").fetchone()[0],
            )
            tables = {
                name
                for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            if header != (self.APPLICATION_ID, self.SCHEMA, "wal") or not tables >= {
                "resources",
                "receipts",
                "history",
            }:
                raise JournalError(
                    "Invalid adapter state: not a recovery SQLite database"
                    f" at schema {self.SCHEMA} in WAL mode"
                )
            # The write lock serializes the receipt check, preconditions, and commit.
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.execute("COMMIT")
        except sqlite3.DatabaseError as error:
            code = getattr(error, "sqlite_errorcode", None)
            if code is not None and code & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                raise LockUnavailable(self.path) from error
            # Python-level conversion errors carry no SQLite code, such as undecodable text.
            if code is None or code & 0xFF in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB):
                raise JournalError(f"Invalid adapter state: {error}") from error
            raise
        finally:
            if db is not None:
                if db.in_transaction:
                    with suppress(sqlite3.Error):
                        db.execute("ROLLBACK")
                db.close()

    @staticmethod
    def _resource(db, name):
        row = db.execute(
            "SELECT value, version, allowed FROM resources WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            return None
        value, version, allowed = row
        try:
            if type(value) is not str or type(version) is not int or version < 0:
                raise ValueError("Invalid resource row")
            if allowed not in (0, 1):
                raise ValueError("Invalid resource permission")
            return {"value": loads(value), "version": version, "allowed": bool(allowed)}
        except ValueError as error:
            raise JournalError(f"Invalid adapter state: {error}") from error

    @staticmethod
    def _receipt(db, action):
        row = db.execute(
            "SELECT receipt FROM receipts WHERE operation_id = ?", (action.operation_id,)
        ).fetchone()
        if row is None:
            return None
        try:
            raw = loads(row[0])
            if not isinstance(raw, dict):
                raise ValueError("Invalid stored receipt")
            receipt = Receipt(**raw)
            if receipt.operation_id != action.operation_id:
                raise ValueError("Receipt operation key mismatch")
        except (ValueError, TypeError) as error:
            raise JournalError(f"Invalid adapter state: {error}") from error
        # Only a same-ID, different-payload request is a Conflict.
        receipt.validate(action)
        return receipt

    @staticmethod
    def _record(db, entry):
        db.execute("INSERT INTO history (entry) VALUES (?)", (encoded(entry).decode(),))

    def inspect(self) -> dict[str, Any]:
        with self._transaction(write=False) as db:
            resources = {
                name: self._resource(db, name)
                for (name,) in db.execute("SELECT name FROM resources ORDER BY name")
            }
            try:
                receipts = {}
                for operation_id, raw in db.execute(
                    "SELECT operation_id, receipt FROM receipts ORDER BY operation_id"
                ):
                    receipt = Receipt(**loads(raw))
                    if receipt.operation_id != operation_id:
                        raise ValueError("Receipt operation key mismatch")
                    receipts[operation_id] = receipt.to_dict()
                history = [
                    loads(entry)
                    for (entry,) in db.execute("SELECT entry FROM history ORDER BY seq")
                ]
                for item in history:
                    if (
                        not isinstance(item, dict)
                        or item.get("actor") not in ("operator", "recovery")
                        or item.get("resource") not in resources
                    ):
                        raise ValueError("Invalid effect history")
            except (ValueError, TypeError) as error:
                raise JournalError(f"Invalid adapter state: {error}") from error
        return {
            "schema": self.SCHEMA,
            "resources": resources,
            "receipts": receipts,
            "history": history,
        }

    def reconcile(self, action: Compensation) -> Reconciliation:
        with self._transaction(write=False) as db:
            receipt = self._receipt(db, action)
        return Reconciliation("applied", receipt) if receipt else Reconciliation("not_applied")

    def apply(self, action: Compensation) -> Receipt:
        with self._transaction(write=True) as db:
            existing = self._receipt(db, action)
            if existing:
                return existing
            resource = self._resource(db, action.resource)
            before, after = _effect(action, resource, lambda name: self._resource(db, name))
            receipt = _new_receipt(action, before, after, resource["version"])
            try:
                value = encoded(after).decode()
                stored = encoded(receipt.to_dict()).decode()
                history = encoded(
                    {
                        "actor": "recovery",
                        "operation_id": action.operation_id,
                        **plain(receipt.details),
                    }
                ).decode()
            except ValueError as error:
                raise Conflict(
                    f"Effect exceeds JSON limits; no change was written: {error}"
                ) from error
            db.execute(
                "UPDATE resources SET value = ?, version = version + 1 WHERE name = ?",
                (value, action.resource),
            )
            db.execute(
                "INSERT INTO receipts (operation_id, receipt) VALUES (?, ?)",
                (action.operation_id, stored),
            )
            db.execute("INSERT INTO history (entry) VALUES (?)", (history,))
        return receipt

    def operator_update(self, resource: str, value: Any) -> None:
        """Apply an explicit local operator edit, preserving a separate history entry."""
        value = plain(value)
        text = encoded(value).decode()
        with self._transaction(write=True) as db:
            current = self._resource(db, resource)
            if current is None:
                raise KeyError(resource)
            db.execute(
                "UPDATE resources SET value = ?, version = version + 1 WHERE name = ?",
                (text, resource),
            )
            self._record(
                db,
                {
                    "actor": "operator",
                    "resource": resource,
                    "before": current["value"],
                    "after": value,
                },
            )

    def set_permission(self, resource: str, allowed: bool) -> None:
        """Change permission in the local example store, not a remote account."""
        if type(allowed) is not bool:
            raise ValueError("allowed must be a boolean")
        with self._transaction(write=True) as db:
            if self._resource(db, resource) is None:
                raise KeyError(resource)
            db.execute("UPDATE resources SET allowed = ? WHERE name = ?", (int(allowed), resource))
            self._record(db, {"actor": "operator", "resource": resource, "allowed": allowed})
