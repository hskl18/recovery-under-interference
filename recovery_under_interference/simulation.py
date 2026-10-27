#!/usr/bin/env python3
"""Durable, local compensation experiments with deliberately bounded services."""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import platform
import random
import sys
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STRATEGIES = ("naive", "idempotent", "cas", "conservative")
SCENARIOS = (
    "clean",
    "double_fault",
    "human_stock",
    "human_dispatch",
    "revoked_shipping",
    "reopened_shipping",
    "double_fault_and_dispatch",
)
ORDER = ("shipping", "billing", "inventory")
ZERO_HASH = "0" * 64


class LogError(ValueError):
    pass


class Blocked(RuntimeError):
    pass


class TransientFailure(RuntimeError):
    pass


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def now() -> str:
    return datetime.now(UTC).isoformat()


def scenario_spec(name: str, seed: int) -> dict:
    if name not in SCENARIOS:
        raise ValueError(f"Unknown scenario: {name}")
    rng = random.Random(seed)
    return {
        "scenario": name,
        "seed": seed,
        "quantity": rng.randint(1, 3),
        "charge": rng.randint(10, 40),
        "initial": {"inventory": 20, "billing": 200, "shipping": "none"},
    }


class State:
    def __init__(self) -> None:
        self.spec: dict = {}
        self.strategy = ""
        self.values: dict = {}
        self.versions: dict = {}
        self.permissions = dict.fromkeys(ORDER, True)
        self.forward: dict = {}
        self.operations: dict = {}
        self.attempts: Counter = Counter()
        self.status = dict.fromkeys(ORDER, "pending")
        self.fired: set[str] = set()

    def replay(self, event: dict) -> None:
        kind = event.get("type")
        if kind == "INIT":
            if self.spec or event.get("schema") != 1 or event["strategy"] not in STRATEGIES:
                raise LogError("Invalid or repeated INIT")
            self.spec = event["spec"]
            if self.spec != scenario_spec(self.spec["scenario"], self.spec["seed"]):
                raise LogError("Invalid scenario specification")
            self.strategy = event["strategy"]
            self.values = dict(self.spec["initial"])
            self.versions = dict.fromkeys(ORDER, 0)
            return
        if not self.spec:
            raise LogError("Missing initial event")
        if kind == "EFFECT":
            resource = event["resource"]
            if (
                resource not in ORDER
                or event["before"] != self.values[resource]
                or event["version_before"] != self.versions[resource]
                or event["version_after"] != self.versions[resource] + 1
                or event["operation_id"] in self.operations
            ):
                raise LogError("Inconsistent effect history")
            value = event["after"]
            if resource == "shipping":
                if value not in ("none", "created", "dispatched"):
                    raise LogError("Invalid shipment state")
            elif not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise LogError("Invalid numeric service state")
            if event["stage"] not in ("forward", "compensate", "human"):
                raise LogError("Unknown effect stage")
            self.values[resource] = value
            self.versions[resource] = event["version_after"]
            self.operations[event["operation_id"]] = event
            if event["stage"] == "forward":
                if resource in self.forward:
                    raise LogError("Repeated forward action")
                self.forward[resource] = event
        elif kind == "ATTEMPT":
            resource = event["resource"]
            if resource not in ORDER or event["number"] != self.attempts[resource] + 1:
                raise LogError("Invalid attempt sequence")
            self.attempts[resource] += 1
        elif kind in ("DONE", "BLOCKED", "FAILURE"):
            resource = event["resource"]
            if resource not in ORDER:
                raise LogError("Unknown action")
            self.status[resource] = {"DONE": "done", "BLOCKED": "blocked", "FAILURE": "pending"}[
                kind
            ]
        elif kind == "REVOKE":
            self.permissions[event["resource"]] = False
        elif kind == "TRIGGER":
            if event["key"] in self.fired:
                raise LogError("Repeated disturbance")
            self.fired.add(event["key"])
        elif kind not in ("ABORT", "CHECKPOINT", "SESSION_END", "FAULT", "DEDUPLICATED"):
            raise LogError(f"Unknown event type: {kind}")


class Journal:
    """Append-only source of truth for the simulated services and workflow worker."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.events: list[dict] = []
        self.state = State()
        self.head = ZERO_HASH
        if path.exists():
            raw = path.read_bytes()
            if raw and not raw.endswith(b"\n"):
                raise LogError("Incomplete final log line; refusing automatic repair")
            for line in raw.splitlines():
                try:
                    record = json.loads(line)
                    expected = {
                        "seq": len(self.events),
                        "prev_hash": self.head,
                        "event": record["event"],
                    }
                    if set(record) != {"seq", "prev_hash", "event", "hash"}:
                        raise LogError("Unexpected envelope fields")
                    if record["seq"] != expected["seq"] or record["prev_hash"] != self.head:
                        raise LogError("Broken log ordering")
                    if record["hash"] != digest(expected):
                        raise LogError("Log checksum mismatch")
                    self.state.replay(record["event"])
                    self.events.append(record["event"])
                    self.head = record["hash"]
                except (ValueError, KeyError, TypeError) as error:
                    raise LogError(
                        f"Invalid log at line {len(self.events) + 1}: {error}"
                    ) from error

    def append(self, kind: str, **fields: Any) -> None:
        event = {"type": kind, **fields}
        # Validate on an isolated state before committing an invalid event.
        import copy

        next_state = copy.deepcopy(self.state)
        next_state.replay(event)
        envelope = {"seq": len(self.events), "prev_hash": self.head, "event": event}
        record = {**envelope, "hash": digest(envelope)}
        with self.path.open("ab") as stream:
            stream.write(canonical(record) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        self.state = next_state
        self.events.append(event)
        self.head = record["hash"]


@contextmanager
def locked(run_dir: Path):
    with (run_dir / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another process owns this simulated run") from error
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def effect(journal: Journal, resource: str, after: Any, stage: str, operation_id: str) -> None:
    state = journal.state
    journal.append(
        "EFFECT",
        resource=resource,
        before=state.values[resource],
        after=after,
        version_before=state.versions[resource],
        version_after=state.versions[resource] + 1,
        stage=stage,
        operation_id=operation_id,
        action_id=f"undo:{resource}" if stage == "compensate" else operation_id,
    )


def create_run(run_dir: Path, scenario: str, strategy: str, seed: int = 0) -> None:
    if strategy not in STRATEGIES:
        raise ValueError("Unknown strategy")
    spec = scenario_spec(scenario, seed)
    run_dir.mkdir(parents=True, exist_ok=False)
    with locked(run_dir):
        journal = Journal(run_dir / "journal.jsonl")
        journal.append("INIT", schema=1, strategy=strategy, spec=spec)
        effect(
            journal,
            "inventory",
            spec["initial"]["inventory"] - spec["quantity"],
            "forward",
            "reserve",
        )
        effect(journal, "billing", spec["initial"]["billing"] - spec["charge"], "forward", "charge")
        effect(journal, "shipping", "created", "forward", "shipment")
        journal.append(
            "ABORT", reason="Downstream order acceptance failed after three committed steps"
        )


def disturbances(journal: Journal, resource: str, number: int) -> None:
    scenario = journal.state.spec["scenario"]
    key = f"{scenario}:{resource}:{number}"
    if key in journal.state.fired:
        return
    event = None
    if scenario == "human_stock" and resource == "inventory" and number == 1:
        event = ("inventory", journal.state.values["inventory"] + 5)
    elif scenario == "human_dispatch" and resource == "shipping" and number == 1:
        event = ("shipping", "dispatched")
    elif scenario == "reopened_shipping" and resource == "billing" and number == 1:
        event = ("shipping", "dispatched")
    elif scenario == "double_fault_and_dispatch" and resource == "shipping" and number == 2:
        event = ("shipping", "dispatched")
    elif scenario == "revoked_shipping" and resource == "shipping" and number == 1:
        journal.append("REVOKE", resource="shipping", actor="operator")
    else:
        return
    if event:
        effect(journal, event[0], event[1], "human", f"human:{key}")
    journal.append("TRIGGER", key=key)


def inject_fault(journal: Journal, resource: str, number: int, phase: str) -> None:
    if (
        journal.state.spec["scenario"] in ("double_fault", "double_fault_and_dispatch")
        and resource == "shipping"
        and ((number == 1 and phase == "after") or (number == 2 and phase == "before"))
    ):
        journal.append("FAULT", resource=resource, number=number, phase=phase)
        raise TransientFailure(
            "Lost acknowledgement after durable effect"
            if phase == "after"
            else "Second fault before service call"
        )


def compensate(journal: Journal, resource: str, operation_id: str) -> None:
    state = journal.state
    if operation_id in state.operations:
        journal.append("DEDUPLICATED", resource=resource, operation_id=operation_id)
        return
    if not state.permissions[resource]:
        raise Blocked("permission_revoked")
    if state.strategy == "conservative":
        if resource in ("billing", "inventory") and state.values["shipping"] != "none":
            raise Blocked("shipment_not_cancelled")
        if resource == "inventory" and "undo:billing" not in state.operations:
            raise Blocked("refund_not_committed")
    forward = state.forward[resource]
    if state.strategy in ("cas", "conservative"):
        if (
            state.versions[resource] != forward["version_after"]
            or state.values[resource] != forward["after"]
        ):
            raise Blocked("forward_version_changed")
    if resource == "shipping" or state.strategy == "naive":
        after = forward["before"]
    else:
        amount = state.spec["quantity"] if resource == "inventory" else state.spec["charge"]
        after = state.values[resource] + amount
    effect(journal, resource, after, "compensate", operation_id)


def recover(run_dir: Path, max_attempts: int = 20, checkpoint_after: int | None = None) -> dict:
    if max_attempts < 1 or (checkpoint_after is not None and checkpoint_after < 1):
        raise ValueError("Attempt limits must be positive")
    with locked(run_dir):
        journal = Journal(run_dir / "journal.jsonl")
        if not journal.state.spec or set(journal.state.forward) != set(ORDER):
            raise LogError("Run does not contain a complete forward checkpoint")
        used = 0
        outcome = "finished"
        for resource in ORDER:
            if journal.state.status[resource] == "done":
                continue
            while used < max_attempts:
                number = journal.state.attempts[resource] + 1
                operation_id = f"undo:{resource}" + (
                    f":attempt:{number}" if journal.state.strategy == "naive" else ""
                )
                journal.append(
                    "ATTEMPT", resource=resource, number=number, operation_id=operation_id
                )
                disturbances(journal, resource, number)
                used += 1
                terminal = False
                blocked = False
                try:
                    inject_fault(journal, resource, number, "before")
                    compensate(journal, resource, operation_id)
                    inject_fault(journal, resource, number, "after")
                    journal.append("DONE", resource=resource, operation_id=operation_id)
                    terminal = True
                except Blocked as error:
                    journal.append("BLOCKED", resource=resource, reason=str(error))
                    terminal = True
                    blocked = True
                except TransientFailure as error:
                    journal.append("FAILURE", resource=resource, reason=str(error))
                if checkpoint_after is not None and used >= checkpoint_after:
                    journal.append(
                        "CHECKPOINT", reason="explicit_attempt_boundary", attempts_this_session=used
                    )
                    return inspect_run(run_dir, journal)
                if blocked:
                    journal.append("SESSION_END", outcome="blocked", attempts_this_session=used)
                    return inspect_run(run_dir, journal)
                if terminal:
                    break
            if used >= max_attempts and any(
                status != "done" for status in journal.state.status.values()
            ):
                outcome = "attempt_limit"
                break
        journal.append("SESSION_END", outcome=outcome, attempts_this_session=used)
        return inspect_run(run_dir, journal)


def oracle(events: list[dict]) -> dict:
    """Evaluate raw effects without using worker statuses or compensation helpers."""
    spec = events[0]["spec"]
    values = dict(spec["initial"])
    allowed = dict.fromkeys(ORDER, True)
    undo_counts: Counter = Counter()
    human_delta = {"inventory": 0, "billing": 0}
    human_shipment = None
    violations = []
    for index, event in enumerate(events):
        if event["type"] == "REVOKE":
            allowed[event["resource"]] = False
        if event["type"] != "EFFECT":
            continue
        resource, stage = event["resource"], event["stage"]
        before, after = event["before"], event["after"]
        if before != values[resource]:
            violations.append({"event": index, "reason": "broken_effect_history"})
        if stage == "human":
            if resource == "shipping":
                human_shipment = after
            else:
                human_delta[resource] += after - before
        if stage == "compensate":
            reasons = []
            undo_counts[resource] += 1
            if undo_counts[resource] > 1:
                reasons.append("duplicate_compensation_effect")
            if not allowed[resource]:
                reasons.append("effect_after_revocation")
            if resource == "shipping" and before == "dispatched":
                reasons.append("erased_human_dispatch")
            if resource in ("billing", "inventory") and values["shipping"] != "none":
                reasons.append("compensated_while_shipment_active")
            if resource == "inventory" and undo_counts["billing"] == 0:
                reasons.append("released_before_refund")
            if resource != "shipping":
                amount = spec["quantity"] if resource == "inventory" else spec["charge"]
                if after - before != amount:
                    reasons.append("incorrect_inverse_or_human_update_lost")
            violations.extend({"event": index, "reason": reason} for reason in reasons)
        values[resource] = after
    target = dict(spec["initial"])
    for resource, delta in human_delta.items():
        target[resource] += delta
    if human_shipment is not None:
        target["shipping"] = human_shipment
    complete = all(undo_counts[resource] >= 1 for resource in ORDER)
    return {
        "safe": not violations,
        "effects_complete": complete,
        "target_matches": values == target,
        "successful_rollback": not violations and complete and values == target,
        "violations": violations,
        "effect_counts": dict(undo_counts),
        "observed_final_state": values,
        "expected_state_preserving_human_changes": target,
    }


def inspect_run(run_dir: Path, journal: Journal | None = None) -> dict:
    journal = journal or Journal(run_dir / "journal.jsonl")
    if not journal.state.spec:
        raise LogError("Empty run")
    state = journal.state
    return {
        "scenario": state.spec["scenario"],
        "strategy": state.strategy,
        "seed": state.spec["seed"],
        "status": state.status,
        "values": state.values,
        "versions": state.versions,
        "attempts": dict(state.attempts),
        "events": len(journal.events),
        "journal_head": journal.head,
        "last_event": journal.events[-1]["type"],
        "oracle": oracle(journal.events),
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def benchmark(output: Path, seeds: int) -> dict:
    if seeds < 1:
        raise ValueError("Seeds must be positive")
    output.mkdir(parents=True, exist_ok=False)
    started = now()
    results = []
    for scenario in SCENARIOS:
        for strategy in STRATEGIES:
            for seed in range(seeds):
                run_dir = output / "runs" / scenario / strategy / str(seed)
                create_run(run_dir, scenario, strategy, seed)
                result = recover(run_dir)
                write_json(run_dir / "result.json", result)
                results.append(result)
    summary = {}
    for strategy in STRATEGIES:
        group = [row for row in results if row["strategy"] == strategy]
        summary[strategy] = {
            "runs": len(group),
            "safe": sum(row["oracle"]["safe"] for row in group),
            "successful_rollbacks": sum(row["oracle"]["successful_rollback"] for row in group),
            "blocked_runs": sum("blocked" in row["status"].values() for row in group),
            "unsafe_runs": sum(not row["oracle"]["safe"] for row in group),
        }
    write_json(output / "results.json", {"summary": summary, "runs": results})
    with (output / "results.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=(
                "scenario",
                "strategy",
                "seed",
                "safe",
                "successful_rollback",
                "attempts",
                "violations",
            ),
        )
        writer.writeheader()
        for row in results:
            writer.writerow(
                {key: row[key] for key in ("scenario", "strategy", "seed")}
                | {
                    "safe": row["oracle"]["safe"],
                    "successful_rollback": row["oracle"]["successful_rollback"],
                    "attempts": sum(row["attempts"].values()),
                    "violations": ";".join(item["reason"] for item in row["oracle"]["violations"]),
                }
            )
    files = sorted(path for path in output.rglob("*") if path.is_file() and path.name != ".lock")
    manifest = {
        "schema": 1,
        "started_utc": started,
        "finished_utc": now(),
        "python": sys.version,
        "platform": platform.platform(),
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scenarios": list(SCENARIOS),
        "strategies": list(STRATEGIES),
        "seeds": list(range(seeds)),
        "run_count": len(results),
        "scope": "deterministic local service simulation; no models or remote services",
        "source_files": {
            "recovery_under_interference/" + path.name: hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sorted(Path(__file__).parent.glob("*.py"))
        },
        "scenario_specs_sha256": digest(
            [scenario_spec(scenario, seed) for scenario in SCENARIOS for seed in range(seeds)]
        ),
        "files": {
            str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in files
        },
    }
    write_json(output / "manifest.json", manifest)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    demo = sub.add_parser("demo")
    demo.add_argument("--run-dir", type=Path, required=True)
    demo.add_argument("--scenario", choices=SCENARIOS, default="double_fault_and_dispatch")
    demo.add_argument("--strategy", choices=STRATEGIES, default="conservative")
    demo.add_argument("--seed", type=int, default=0)
    demo.add_argument("--checkpoint-after", type=int)
    demo.add_argument("--max-attempts", type=int, default=20)
    resume = sub.add_parser("resume")
    resume.add_argument("run_dir", type=Path)
    resume.add_argument("--max-attempts", type=int, default=20)
    resume.add_argument("--checkpoint-after", type=int)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("run_dir", type=Path)
    bench = sub.add_parser("benchmark")
    bench.add_argument("--output", type=Path, required=True)
    bench.add_argument("--seeds", type=int, default=3)
    args = parser.parse_args()
    try:
        if args.command == "demo":
            from .engine import validate_limits

            validate_limits(args.max_attempts, args.checkpoint_after)
            create_run(args.run_dir, args.scenario, args.strategy, args.seed)
            result = recover(args.run_dir, args.max_attempts, args.checkpoint_after)
        elif args.command == "resume":
            result = recover(args.run_dir, args.max_attempts, args.checkpoint_after)
        elif args.command == "inspect":
            with locked(args.run_dir):
                result = inspect_run(args.run_dir)
        else:
            result = benchmark(args.output, args.seeds)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(
            json.dumps({"error": str(error), "error_type": type(error).__name__}), file=sys.stderr
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
