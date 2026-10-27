"""Durable compensation for applications with explicit service adapters."""

from .adapters import FileAdapter, SQLiteAdapter
from .engine import (
    AmbiguousOutcome,
    Compensation,
    Conflict,
    PermissionDenied,
    Receipt,
    Reconciliation,
    RecoveryEngine,
    RetryableError,
    ServiceAdapter,
)
from .journal import JournalError, JournalIOError, LockUnavailable

__version__ = "0.5.0"
__all__ = [
    "AmbiguousOutcome",
    "Compensation",
    "Conflict",
    "FileAdapter",
    "JournalError",
    "JournalIOError",
    "LockUnavailable",
    "PermissionDenied",
    "Receipt",
    "Reconciliation",
    "RecoveryEngine",
    "RetryableError",
    "SQLiteAdapter",
    "ServiceAdapter",
]
