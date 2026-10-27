"""Adapter-driven durable compensation with explicit uncertain outcomes."""

import heapq
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .journal import (
    Journal,
    JournalError,
    LockUnavailable,
    checksum,
    durable_mkdir,
    exclusive,
    file_sha256,
    repair_incomplete_tail,
)
from .jsondata import encoded, frozen_object, plain


class RetryableError(RuntimeError):
    """This call may be retried using the same operation identity."""


class PermissionDenied(RuntimeError):
    """The adapter refused an effect under current authorization."""


class Conflict(RuntimeError):
    """A version, dependency, or operation-payload precondition failed."""


class AmbiguousOutcome(RuntimeError):
    """The adapter cannot yet establish whether an effect committed."""


def nonempty(value, name):
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")


def retirement_reason(value):
    nonempty(value, "retirement reason")
    if len(value) > 4096:
        raise ValueError("retirement reason must not exceed 4096 characters")
    encoded(value)


@dataclass(frozen=True)
class Compensation:
    """An immutable inverse operation; its adapter enforces preconditions."""

    id: str
    adapter: str
    resource: str
    operation: str
    operation_id: str
    arguments: Mapping[str, Any] = field(default_factory=dict)
    preconditions: Mapping[str, Any] = field(default_factory=dict)
    requires: tuple[str, ...] = ()
    _fingerprint: str = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        for name in ("id", "adapter", "resource", "operation", "operation_id"):
            nonempty(getattr(self, name), name)
        object.__setattr__(self, "arguments", frozen_object(self.arguments, "arguments"))
        object.__setattr__(
            self, "preconditions", frozen_object(self.preconditions, "preconditions")
        )
        if not isinstance(self.requires, (list, tuple)):
            raise ValueError("requires must be an array of action IDs")
        for required in self.requires:
            nonempty(required, "required action")
        if len(set(self.requires)) != len(self.requires):
            raise ValueError("requires contains duplicates")
        object.__setattr__(self, "requires", tuple(self.requires))
        object.__setattr__(self, "_fingerprint", checksum(self.to_dict()))

    def to_dict(self):
        return {
            "id": self.id,
            "adapter": self.adapter,
            "resource": self.resource,
            "operation": self.operation,
            "operation_id": self.operation_id,
            "arguments": plain(self.arguments),
            "preconditions": plain(self.preconditions),
            "requires": list(self.requires),
        }

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict):
            raise ValueError("action must be an object")
        try:
            return cls(**value)
        except TypeError as error:
            raise ValueError(f"invalid action fields: {error}") from error

    @property
    def fingerprint(self):
        return self._fingerprint


@dataclass(frozen=True)
class Receipt:
    operation_id: str
    action_fingerprint: str
    effect_id: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self._validate_fields(
            self.operation_id, self.action_fingerprint, self.effect_id, self.details
        )
        object.__setattr__(self, "details", frozen_object(self.details, "receipt details"))
        encoded(self.to_dict())

    @staticmethod
    def _validate_fields(operation_id, action_fingerprint, effect_id, details):
        for name, value in (
            ("operation_id", operation_id),
            ("action_fingerprint", action_fingerprint),
            ("effect_id", effect_id),
        ):
            nonempty(value, name)
        if len(action_fingerprint) != 64 or any(
            c not in "0123456789abcdef" for c in action_fingerprint
        ):
            raise ValueError("action_fingerprint must be a lowercase SHA-256 digest")
        if not isinstance(details, Mapping):
            raise ValueError("receipt details must be a JSON object")

    def to_dict(self):
        return {
            "operation_id": self.operation_id,
            "action_fingerprint": self.action_fingerprint,
            "effect_id": self.effect_id,
            "details": plain(self.details),
        }

    def validate(self, action):
        if (
            self.operation_id != action.operation_id
            or self.action_fingerprint != action.fingerprint
        ):
            raise Conflict("Receipt does not match this operation and payload")


@dataclass(frozen=True)
class Reconciliation:
    """unknown must never be interpreted as evidence that no effect occurred."""

    state: str
    receipt: Receipt | None = None

    def __post_init__(self):
        if self.state not in ("applied", "not_applied", "unknown"):
            raise ValueError("Invalid reconciliation state")
        if (self.state == "applied" and not isinstance(self.receipt, Receipt)) or (
            self.state != "applied" and self.receipt is not None
        ):
            raise ValueError("Only applied reconciliation carries a Receipt")


class ServiceAdapter(Protocol):
    """Adapters own durable effect identity and commit-time preconditions."""

    def reconcile(self, action: Compensation) -> Reconciliation: ...

    def apply(self, action: Compensation) -> Receipt: ...


def validate_limits(max_attempts, checkpoint_after):
    for name, value in (("max_attempts", max_attempts), ("checkpoint_after", checkpoint_after)):
        if name == "checkpoint_after" and value is None:
            continue
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")


def ordered_actions(actions):
    try:
        actions = list(actions)
    except TypeError as error:
        raise ValueError("actions must be an iterable of Compensation values") from error
    if not actions or any(not isinstance(action, Compensation) for action in actions):
        raise ValueError("At least one Compensation value is required")
    positions = {action.id: index for index, action in enumerate(actions)}
    operations = {(action.adapter, action.operation_id) for action in actions}
    if len(positions) != len(actions) or len(operations) != len(actions):
        raise ValueError("Action and adapter-scoped operation IDs must be unique")
    followers = {action.id: [] for action in actions}
    indegree = {}
    for action in actions:
        indegree[action.id] = len(action.requires)
        for dependency in action.requires:
            if dependency not in positions:
                raise ValueError(f"Unknown dependency for {action.id}")
            followers[dependency].append(action.id)
    ready = [positions[name] for name, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    result = []
    while ready:
        action = actions[heapq.heappop(ready)]
        result.append(action)
        for name in followers[action.id]:
            indegree[name] -= 1
            if indegree[name] == 0:
                heapq.heappush(ready, positions[name])
    if len(result) != len(actions):
        raise ValueError("Compensation dependency cycle")
    return result


def _transition(state, lookup, event):
    kind = event.get("type")
    if kind == "RETIRED":
        if set(event) != {"type", "reason"}:
            raise ValueError("Invalid retirement fields")
        retirement_reason(event["reason"])
        if all(item["status"] == "done" for item in state.values()):
            raise ValueError("A complete workflow cannot be retired")
        return
    if kind in ("CHECKPOINT", "SESSION_END"):
        if (
            type(event.get("attempts_this_session")) is not int
            or event["attempts_this_session"] < 0
        ):
            raise ValueError("Invalid session attempt count")
        if kind == "SESSION_END" and event.get("outcome") not in (
            "complete",
            "blocked",
            "uncertain",
            "attempt_limit",
            "lock_busy",
        ):
            raise ValueError("Invalid session outcome")
        if (
            kind == "SESSION_END"
            and event["outcome"] == "complete"
            and any(item["status"] != "done" for item in state.values())
        ):
            raise ValueError("Incomplete workflow marked complete")
        return
    action = lookup[event["action"]]
    current = state[action.id]
    if kind == "ATTEMPT":
        if (
            current["status"] == "done"
            or type(event.get("number")) is not int
            or event["number"] != current["attempts"] + 1
            or any(state[name]["status"] != "done" for name in action.requires)
        ):
            raise ValueError("Invalid attempt transition or unsatisfied dependency")
        current.update(
            status="inflight", attempts=event["number"], reason=None, error_code=None, phase=None
        )
    elif kind == "RECEIPT":
        if current["status"] != "inflight":
            raise ValueError("Receipt without an attempt")
        receipt = Receipt(**event["receipt"])
        receipt.validate(action)
        current.update(
            status="done", receipt=receipt.to_dict(), reason=None, error_code=None, phase=None
        )
    elif kind in ("RETRYABLE", "BLOCKED", "UNCERTAIN"):
        if current["status"] != "inflight" or type(event.get("reason")) is not str:
            raise ValueError("Invalid failure outcome without an attempt")
        current.update(
            status={"RETRYABLE": "pending", "BLOCKED": "blocked", "UNCERTAIN": "uncertain"}[kind],
            reason=event["reason"],
            error_code=event.get("error_code"),
            phase=event.get("phase"),
        )
    else:
        raise ValueError(f"Unknown journal event: {kind}")


def replay(journal):
    if not journal.events:
        raise JournalError("Missing workflow plan")
    first = journal.events[0]
    try:
        if (
            first.get("type") != "INIT"
            or type(first.get("schema")) is not int
            or first["schema"] != 1
        ):
            raise ValueError("Unsupported workflow schema")
        actions = ordered_actions([Compensation.from_dict(row) for row in first["actions"]])
        state = {
            action.id: {
                "status": "pending",
                "attempts": 0,
                "reason": None,
                "receipt": None,
                "error_code": None,
                "phase": None,
            }
            for action in actions
        }
        lookup = {action.id: action for action in actions}
        retired = False
        for event in journal.events[1:]:
            if retired:
                raise ValueError("Journal event follows terminal retirement")
            _transition(state, lookup, event)
            retired = event.get("type") == "RETIRED"
        return actions, state
    except (ValueError, TypeError, KeyError, Conflict) as error:
        raise JournalError(f"Invalid workflow history: {error}") from error


def _failure(error, kind, action, phase, code):
    return {
        "type": kind,
        "action": action.id,
        "reason": f"{type(error).__name__}: {error}"[:4096],
        "error_code": code,
        "phase": phase,
    }


class RecoveryEngine:
    """Persist a recovery plan and resume it with caller-supplied adapters."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    @classmethod
    def create(cls, directory: str | Path, actions: Iterable[Compensation]) -> "RecoveryEngine":
        actions = ordered_actions(actions)
        event = {"type": "INIT", "schema": 1, "actions": [action.to_dict() for action in actions]}
        # Validate the complete envelope before creating any workflow files.
        encoded({"seq": 0, "previous": "0" * 64, "event": event, "sha256": "0" * 64})
        directory = Path(directory)
        durable_mkdir(directory, exist_ok=False)
        engine = cls(directory)
        with exclusive(directory / ".lock"):
            Journal(directory / "journal.jsonl").append(event)
        return engine

    @staticmethod
    def _report(journal, actions=None, state=None):
        if actions is None or state is None:
            actions, state = replay(journal)
        complete = all(item["status"] == "done" for item in state.values())
        retired = journal.events[-1].get("type") == "RETIRED"
        return {
            "schema": 1,
            "complete": complete,
            "retired": retired,
            "retirement": plain(journal.events[-1]) if retired else None,
            "actions": plain(state),
            "plan": [action.to_dict() for action in actions],
            "journal_head": journal.head,
            "event_count": len(journal.events),
            "last_event": plain(journal.events[-1]),
        }

    def inspect(self) -> dict[str, Any]:
        with exclusive(self.directory / ".lock"):
            return self._report(Journal(self.directory / "journal.jsonl"))

    def retire(self, *, reason: str) -> dict[str, Any]:
        """Permanently stop local retries without changing any recorded effect outcome.

        Retirement cannot cancel remote work or authorize a replacement plan.
        Uncertain and inflight outcomes still require provider investigation.
        """
        retirement_reason(reason)
        with exclusive(self.directory / ".lock"):
            journal = Journal(self.directory / "journal.jsonl")
            actions, state = replay(journal)
            event = {"type": "RETIRED", "reason": reason}
            if journal.events[-1].get("type") == "RETIRED":
                if journal.events[-1] != event:
                    raise ValueError("Workflow already retired with a different reason")
                # A previous response may have been lost after write but before fsync.
                journal.confirm_durable()
            else:
                _transition(state, {action.id: action for action in actions}, event)
                journal.append(event)
            return self._report(journal, actions, state)

    def diagnose(self) -> dict[str, Any]:
        """Read diagnostics without calling an adapter or changing journal bytes."""
        with exclusive(self.directory / ".lock"):
            path = self.directory / "journal.jsonl"
            digest = file_sha256(path)
            try:
                journal = Journal(path, allow_incomplete_tail=True)
                report = self._report(journal)
            except JournalError as error:
                return {
                    "recoverable_tail": False,
                    "journal_sha256": digest,
                    "error": str(error),
                    "next_step": "Preserve the files and investigate corruption; automatic truncation is refused.",
                }
            if report["retired"]:
                next_step = "Workflow is retired; resume will not call adapters. Investigate unfinished effects with the provider."
                if journal.incomplete_tail_bytes:
                    next_step = "Repair the incomplete tail with this exact SHA-256; the valid prefix remains retired. Investigate unfinished effects with the provider."
            elif journal.incomplete_tail_bytes:
                next_step = "Repair the incomplete tail with this exact SHA-256. Confirm operator intent before resume; retry any interrupted retirement first."
            else:
                next_step = (
                    "Inspect action status, error_code, phase, and reason before retrying. "
                    "Journal integrity does not establish provider state. "
                    "For blocked actions, verify and resolve the reported condition with the "
                    "provider before retrying. Resume reconciles unfinished operations using "
                    "their original operation IDs."
                )
            return {
                **report,
                "journal_sha256": digest,
                "recoverable_tail": bool(journal.incomplete_tail_bytes),
                "incomplete_tail_bytes": journal.incomplete_tail_bytes,
                "valid_bytes": journal.valid_bytes,
                "next_step": next_step,
            }

    def repair_tail(self, *, expected_sha256: str) -> dict[str, Any]:
        """Explicitly remove only a torn final record after making a durable backup."""
        with exclusive(self.directory / ".lock"):
            journal = Journal(self.directory / "journal.jsonl", allow_incomplete_tail=True)
            replay(journal)
            backup = repair_incomplete_tail(journal, expected_sha256)
            return {**self._report(Journal(journal.path)), "backup": str(backup)}

    def resume(
        self,
        adapters: Mapping[str, ServiceAdapter],
        *,
        max_attempts: int = 20,
        checkpoint_after: int | None = None,
    ) -> dict[str, Any]:
        validate_limits(max_attempts, checkpoint_after)
        with exclusive(self.directory / ".lock"):
            journal = Journal(self.directory / "journal.jsonl")
            actions, state = replay(journal)
            if journal.events[-1].get("type") == "RETIRED":
                return self._report(journal, actions, state)
            adapters = dict(adapters)
            lookup = {action.id: action for action in actions}
            missing = {
                action.adapter for action in actions if state[action.id]["status"] != "done"
            } - set(adapters)
            if missing:
                raise ValueError(f"Missing adapters: {', '.join(sorted(missing))}")
            for action in actions:
                if state[action.id]["status"] != "done" and any(
                    not callable(getattr(adapters[action.adapter], method, None))
                    for method in ("reconcile", "apply")
                ):
                    raise ValueError(f"Adapter {action.adapter} must implement reconcile and apply")

            def record(event):
                # Persistence errors propagate, never becoming another event on a possibly torn log.
                journal.append(event)
                _transition(state, lookup, event)

            def finish(outcome, used):
                record({"type": "SESSION_END", "outcome": outcome, "attempts_this_session": used})
                return self._report(journal, actions, state)

            used = 0
            for action in actions:
                if state[action.id]["status"] == "done":
                    continue
                while used < max_attempts:
                    record(
                        {
                            "type": "ATTEMPT",
                            "action": action.id,
                            "number": state[action.id]["attempts"] + 1,
                        }
                    )
                    used += 1
                    terminal = None
                    phase = "reconcile"
                    try:
                        adapter = adapters[action.adapter]
                        reconciled = adapter.reconcile(action)
                        if not isinstance(reconciled, Reconciliation):
                            raise AmbiguousOutcome("Adapter returned an invalid reconciliation")
                        if reconciled.state == "unknown":
                            raise AmbiguousOutcome("Adapter cannot establish the operation outcome")
                        if reconciled.state == "applied":
                            receipt = reconciled.receipt
                        else:
                            phase = "apply"
                            receipt = adapter.apply(action)
                        phase = "receipt_validation"
                        if not isinstance(receipt, Receipt):
                            raise AmbiguousOutcome("Adapter returned an invalid receipt")
                        try:
                            receipt.validate(action)
                        except Conflict as error:
                            raise AmbiguousOutcome(
                                "Adapter receipt identity or payload does not match the operation"
                            ) from error
                        event = {
                            "type": "RECEIPT",
                            "action": action.id,
                            "receipt": receipt.to_dict(),
                        }
                    except LockUnavailable as error:
                        terminal = "lock_busy"
                        event = _failure(error, "RETRYABLE", action, phase, "lock_busy")
                    except (PermissionDenied, Conflict) as error:
                        terminal = "blocked"
                        event = _failure(
                            error,
                            "BLOCKED",
                            action,
                            phase,
                            "permission_denied"
                            if isinstance(error, PermissionDenied)
                            else "conflict",
                        )
                    except RetryableError as error:
                        event = _failure(error, "RETRYABLE", action, phase, "retryable")
                    except Exception as error:
                        terminal = "uncertain"
                        event = _failure(
                            error,
                            "UNCERTAIN",
                            action,
                            phase,
                            "invalid_receipt"
                            if phase == "receipt_validation"
                            else "adapter_state_corrupt"
                            if isinstance(error, JournalError)
                            else "outcome_unknown",
                        )
                    record(event)
                    if checkpoint_after is not None and used >= checkpoint_after:
                        record({"type": "CHECKPOINT", "attempts_this_session": used})
                        return self._report(journal, actions, state)
                    if terminal:
                        return finish(terminal, used)
                    if state[action.id]["status"] == "done":
                        break
                if used >= max_attempts and any(
                    item["status"] != "done" for item in state.values()
                ):
                    return finish("attempt_limit", used)
            return finish("complete", used)
