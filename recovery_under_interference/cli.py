"""CLI for local recovery journals and the separate policy simulator."""

import argparse
import json
import sys
from pathlib import Path

from . import AmbiguousOutcome, Compensation, FileAdapter, RecoveryEngine
from .engine import validate_limits
from .journal import JournalIOError, LockUnavailable
from .jsondata import load


def order_actions():
    return [
        Compensation(
            "cancel",
            "local",
            "shipping",
            "set",
            "order-1:cancel",
            {"value": "none"},
            {"version": 0},
        ),
        Compensation(
            "refund",
            "local",
            "billing",
            "increment",
            "order-1:refund",
            {"amount": 25},
            {"version": 0, "guards": [{"resource": "shipping", "value": "none"}]},
            ("cancel",),
        ),
        Compensation(
            "release",
            "local",
            "inventory",
            "increment",
            "order-1:release",
            {"amount": 1},
            {
                "version": 0,
                "guards": [
                    {"resource": "shipping", "value": "none"},
                    {"resource": "billing", "value": 100},
                ],
            },
            ("refund",),
        ),
    ]


def demo(directory, scenario, max_attempts, checkpoint_after):
    validate_limits(max_attempts, checkpoint_after)
    directory = Path(directory)
    if directory.exists():
        raise ValueError("Demo directory already exists; choose a new path")
    directory.mkdir(parents=True)
    adapter = FileAdapter.create(
        directory / "services.json", {"shipping": "created", "billing": 75, "inventory": 9}
    )
    engine = RecoveryEngine.create(directory / "workflow", order_actions())
    if scenario == "revoked":
        adapter.set_permission("shipping", False)
    if scenario == "interference":
        adapter.operator_update("shipping", "dispatched")
    if scenario == "lost-ack":

        class LostAck(FileAdapter):
            def apply(self, action):
                receipt = super().apply(action)
                if action.id == "cancel":
                    raise AmbiguousOutcome(
                        "Demonstration: durable effect committed but reply was lost"
                    )
                return receipt

        adapter = LostAck(adapter.path)
    report = engine.resume(
        {"local": adapter}, max_attempts=max_attempts, checkpoint_after=checkpoint_after
    )
    return {
        **report,
        "demo_service_file": str(adapter.path),
        "workflow_directory": str(engine.directory),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    make = commands.add_parser(
        "init", help="Persist a JSON compensation plan without calling a service"
    )
    make.add_argument("--run-dir", type=Path, required=True)
    make.add_argument("--plan", type=Path, required=True)
    resume = commands.add_parser("resume")
    resume.add_argument("run_dir", type=Path)
    resume.add_argument("--adapter", action="append", default=[], metavar="NAME=JSON_FILE")
    resume.add_argument("--max-attempts", type=int, default=20)
    resume.add_argument("--checkpoint-after", type=int)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("run_dir", type=Path)
    retire = commands.add_parser(
        "retire", help="Permanently stop this workflow's retries without changing effect outcomes"
    )
    retire.add_argument("run_dir", type=Path)
    retire.add_argument("--reason", required=True)
    diagnose = commands.add_parser(
        "diagnose",
        help="Inspect corruption and a possible torn final record without calling a service",
    )
    diagnose.add_argument("run_dir", type=Path)
    repair = commands.add_parser(
        "repair-tail",
        help="Back up and remove only a torn final record, guarded by the diagnosed SHA-256",
    )
    repair.add_argument("run_dir", type=Path)
    repair.add_argument("--expected-sha256", required=True)
    example = commands.add_parser("demo")
    example.add_argument("--run-dir", type=Path, required=True)
    example.add_argument(
        "--scenario", choices=("clean", "lost-ack", "revoked", "interference"), default="lost-ack"
    )
    example.add_argument("--max-attempts", type=int, default=20)
    example.add_argument("--checkpoint-after", type=int)
    bench = commands.add_parser("benchmark", help="Run the separate deterministic policy simulator")
    bench.add_argument("--output", type=Path, required=True)
    bench.add_argument("--seeds", type=int, default=3)
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            raw = load(args.plan)
            if not isinstance(raw, list):
                raise ValueError("Plan must be a JSON array")
            engine = RecoveryEngine.create(
                args.run_dir, [Compensation.from_dict(row) for row in raw]
            )
            result = engine.inspect()
        elif args.command == "resume":
            validate_limits(args.max_attempts, args.checkpoint_after)
            adapters = {}
            for spec in args.adapter:
                name, separator, path = spec.partition("=")
                if not separator or not name or not path or name in adapters:
                    raise ValueError("Adapters require unique NAME=JSON_FILE entries")
                adapters[name] = FileAdapter(path)
            result = RecoveryEngine(args.run_dir).resume(
                adapters, max_attempts=args.max_attempts, checkpoint_after=args.checkpoint_after
            )
        elif args.command == "diagnose":
            result = RecoveryEngine(args.run_dir).diagnose()
        elif args.command == "repair-tail":
            result = RecoveryEngine(args.run_dir).repair_tail(expected_sha256=args.expected_sha256)
        elif args.command == "inspect":
            result = RecoveryEngine(args.run_dir).inspect()
        elif args.command == "retire":
            result = RecoveryEngine(args.run_dir).retire(reason=args.reason)
        elif args.command == "demo":
            result = demo(args.run_dir, args.scenario, args.max_attempts, args.checkpoint_after)
        else:
            from .simulation import benchmark

            result = benchmark(args.output, args.seeds)
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except LockUnavailable as error:
        print(
            json.dumps(
                {
                    "error": str(error),
                    "type": type(error).__name__,
                    "error_code": "lock_busy",
                    "path": error.path,
                }
            ),
            file=sys.stderr,
        )
        return 3
    except JournalIOError as error:
        print(
            json.dumps(
                {
                    "error": str(error),
                    "type": type(error).__name__,
                    "error_code": "journal_persistence_unknown",
                    "path": error.path,
                    "stage": error.stage,
                    "durability_unknown": True,
                }
            ),
            file=sys.stderr,
        )
        return 4
    except (OSError, ValueError, RuntimeError, TypeError, KeyError) as error:
        print(json.dumps({"error": str(error), "type": type(error).__name__}), file=sys.stderr)
        return 2
