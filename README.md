# Recovery Under Interference

A Python library for durably resuming compensation after a workflow has already produced effects.
Applications provide service adapters with explicit effect identities and preconditions.
The engine persists recovery plans, reconciles uncertain outcomes, records receipts, and stops when permissions or prerequisites no longer permit an inverse.
Operators can permanently retire an abandoned plan so stale workers cannot retry it after a replacement has been reviewed.
It is a focused recovery library, not a forward workflow orchestrator or a generic agent framework.

The package also includes a separate deterministic policy simulator for studying recovery-time interference.
Simulator scores do not establish the reliability of an external adapter or a production deployment.

## Install and run

Python 3.11 or newer on a local POSIX filesystem is required.
Local checks run on macOS, and the CI workflow runs on Ubuntu.
The runtime has no third-party dependencies.

```sh
python3 -m venv .venv && . .venv/bin/activate
python3 -m pip install .
recovery-under-interference demo --run-dir runs/order --scenario lost-ack --checkpoint-after 1
recovery-under-interference inspect runs/order/workflow
recovery-under-interference resume runs/order/workflow --adapter local=runs/order/services.json
```

From a source checkout, `python3 -m recovery_under_interference` runs the same commands without installation.
Demo and initialization output paths must be new; they refuse to replace existing evidence or service files.
The demo changes only its own local JSON service file.
It models inventory, account balance, and shipment state for an already-failed order.
The first process persists a cancellation effect but loses the reply; the second process reconciles its receipt and finishes the remaining compensations without a second cancellation effect.

Other demo scenarios are `clean`, `revoked`, and `interference`.
`clean --max-attempts 3` completes all three steps and reports `complete`, rather than misreporting an exhausted attempt budget.
Invalid attempt limits are rejected before creating a demo directory or service state.
CLI exit code 0 means the command ran; check the JSON `complete`, `retired`, action statuses, and session outcome when present to determine the recovery result.
Invalid input, inaccessible journals, and corruption rejected by `inspect` or `resume` return exit code 2.
`diagnose` returns 0 when it successfully produces a diagnostic report, including reports of corruption; inspect `error` and `recoverable_tail` before choosing a recovery action.
Workflow lock contention returns 3, and uncertain journal persistence returns 4 with structured diagnostic fields.

## Application API

```python
from recovery_under_interference import Compensation, FileAdapter, RecoveryEngine

service = FileAdapter.create("example/service.json", {"stock": 9})
action = Compensation(
    id="release-stock", adapter="inventory", resource="stock",
    operation="increment", operation_id="order-42:release-stock",
    arguments={"amount": 1}, preconditions={"version": 0},
)
engine = RecoveryEngine.create("example/workflow", [action])
report = engine.resume({"inventory": service})
assert report["complete"]
```

`RecoveryEngine.create` records a plan without invoking an adapter.
`RecoveryEngine(path).inspect()` reconstructs the plan and status from its durable journal.
`resume` accepts an adapter mapping, a per-call attempt budget, and an optional checkpoint boundary.
Adapters can wrap databases, APIs, or other services.
The package ships `FileAdapter` for small local state and `SQLiteAdapter`, which commits each effect and its receipt in one SQLite transaction and keeps per-effect cost flat as receipts accumulate.
[examples/sqlite_order.py](examples/sqlite_order.py) loses a reply after a commit and finishes in a second process without repeating the effect.
See the [API reference](docs/API.md), [adapter contract](docs/ADAPTERS.md), and [failure recovery guide](docs/RECOVERY.md).
A runnable library example is [examples/compensate_order.py](examples/compensate_order.py).
For abandoned or replaced plans, run [examples/retire_replaced_plan.py](examples/retire_replaced_plan.py) with a new directory.
`retire RUN_DIR --reason TEXT` preserves unfinished outcomes and permanently prevents further adapter calls by that workflow.
It does not cancel remote work or establish that new replacement effects are safe.

## Guarantees and required adapter behavior

The engine gives each inverse a stable operation identity and validates every receipt against its full action fingerprint.
It journals attempts before calling an adapter and records completed effects separately from uncertain outcomes.
Every retry first asks the adapter to reconcile the operation.
An `unknown` result stops recovery; it is never treated as evidence that no effect occurred.
Compensation dependencies are checked in topological order, while current service preconditions remain the adapter's responsibility.

The included `FileAdapter` commits the effect, receipt, authorization check, and all local resource guards under one advisory lock and atomic file replacement.
An external adapter must provide a durable and compatible idempotency contract and enforce its own preconditions at the authoritative commit point.
The engine cannot create remote exactly-once execution or atomic checks across unrelated services.
A preflight read followed by an unconditional remote write does not satisfy the documented conditional-commit contract.

The JSONL journal rejects checksum failures, unsupported schemas, inconsistent transitions, and torn final lines.
A failed append stops all further journal writes; `diagnose` and explicit hash-guarded `repair-tail` support recovery from an incomplete final record without repeating an unverified effect.
It is not a cryptographic authentication mechanism, and valid-prefix truncation is not always detectable.
File locking is single-host POSIX coordination, not a distributed lease or network-filesystem guarantee.
See [SECURITY.md](SECURITY.md) for local data and trust boundaries.

## Failure handling

Persistence failures are isolated from adapter failures: the engine stops before another journal append, and a fresh process either recovers a complete receipt or reconciles an interrupted operation after guarded tail repair.
Actions and receipts deeply freeze copied JSON values, so caller mutations cannot change a fingerprint.
Run [the custom business adapter](examples/business_refund.py) to recover a payment refund and inventory restoration across a deliberate worker kill.
The tests exercise process interruption and storage failures against local provider fixtures; power loss and commercial providers are not covered.
The [operator guide](docs/RECOVERY.md) explains diagnosis, lock contention, and safe tail repair.

## Separate policy experiment

```sh
recovery-under-interference benchmark --output runs/benchmark --seeds 3
python3 -m recovery_under_interference.simulation demo --run-dir runs/policy-example --scenario double_fault_and_dispatch --strategy conservative
```

The simulator lives in `recovery_under_interference/simulation.py`.
The production-facing library API is `RecoveryEngine`, not that simulator's fixed three-action interpreter.
The simulator compares naive snapshots, additive idempotent inverses, same-resource CAS, and CAS with cross-resource dependency checks.
All use identical schedules, permission enforcement, budgets, and fail-stop behavior.
Its seven controlled schedules and numeric seed variants measure safety, completion, and deferral separately.

With `--seeds 3`, conservative checks avoid unsafe effects in selected interference cases but do not increase successful rollback counts over CAS.
The additive idempotent baseline completes the independent stock-adjustment case where both CAS policies defer.
These cases illustrate mechanisms and tradeoffs; they do not establish research novelty or broad superiority.
The [method](METHOD.md) defines the oracles and cites UndoBench, DART, and REVISE.

The simulator is deterministic, so the benchmark command above regenerates these results.
Each run writes `manifest.json`, `results.json`, `results.csv`, and per-run journals; manifest source hashes identify the implementation.

## Development

```sh
python3 -m unittest discover -s tests -v
```

CI runs the tests, lint and format checks, a package build, and wheel-installed CLI smoke checks on Python 3.11 through 3.14.
The full local check sequence is in [CONTRIBUTING.md](CONTRIBUTING.md).
This project is licensed under [MIT](LICENSE).
