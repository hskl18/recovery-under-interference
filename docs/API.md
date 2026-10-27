# Public API

Import public names from `recovery_under_interference`.
The library requires Python 3.11 or newer and a local POSIX filesystem for workflow journals.
The `simulation` module is a separate research example and is not a stable application API.

## Compensation

`Compensation(id, adapter, resource, operation, operation_id, arguments={}, preconditions={}, requires=())` describes one inverse.
`id` identifies the node within a workflow; `adapter` selects the caller-supplied service adapter.
`operation_id` must uniquely identify this logical inverse within the adapter's durable scope.
Do not recycle an operation ID for another order or another payload.
The engine fingerprints all action fields, including preconditions and dependencies, and validates adapter receipts against that fingerprint.
Arguments and preconditions must be finite JSON values inside objects.
They are recorded in the journal, so they must not contain credentials or unnecessary personal data.

`requires` contains action IDs whose compensation receipts must exist before this action is attempted.
Plans reject unknown dependencies, duplicate action IDs, duplicate adapter-scoped operation IDs, and cycles.
The engine uses a stable topological order and stops the entire session at a blocked or uncertain action.
It does not execute independent branches concurrently.
`requires` establishes historical completion, not current validity of a resource; use adapter-enforced preconditions for the latter.

## RecoveryEngine

`RecoveryEngine.create(directory, actions)` validates and writes a new plan without contacting services.
It refuses an existing directory.
`RecoveryEngine(directory)` opens a handle without changing the journal.
`inspect()` validates and replays the journal, returning plan, action statuses, attempts, receipts, journal hash, lifecycle fields, and the last event.
`resume(adapters, *, max_attempts=20, checkpoint_after=None)` resolves adapter names, acquires the workflow lock, and executes recovery.

The attempt budget applies to this resume call, including reconciliation calls.
A checkpoint records a deliberate boundary after an attempt and releases the process lock.
Reaching the budget on the last successful action reports completion.
Completed actions are not called again on resume.
Blocked or uncertain actions can be retried after an operator or the service resolves the underlying condition.
Calling resume again is not itself evidence that the condition was resolved.
`retire(reason=...)` permanently disables further recovery attempts for this workflow without contacting adapters or changing action outcomes.
Use it when an operator has decided to stop retrying an incomplete plan.

The return value contains `complete`, `retired`, `retirement`, and an `actions` mapping.
`complete` means all actions have receipts, while `retired` means further attempts are disabled.
An active or complete workflow has `retired=false` and `retirement=null`.
Retirement is accepted only for an incomplete workflow, so it never changes `complete` to true.
Action status is `pending`, `inflight`, `done`, `blocked`, or `uncertain`.
An `inflight` action found during startup may have committed remotely, so it is reconciled before any apply call.
Session outcome is `complete`, `attempt_limit`, `blocked`, or `uncertain`; an explicit checkpoint appears as its own last event.
There is no automatic background retry loop or worker daemon.

## Workflow retirement

`engine.retire(reason="Reviewed replacement record incident-42; stop this plan")` appends one terminal `RETIRED` event under the workflow lock and returns the usual inspection report.
The retirement metadata is `{ "type": "RETIRED", "reason": "..." }`.
The reason must be a nonblank string of at most 4096 characters and valid JSON Unicode.
It may reference the application's replacement record, but the engine does not interpret that reference or create a replacement plan.
All original plan fields, action statuses, attempt counts, errors, and receipts remain unchanged.
Pending, inflight, blocked, and uncertain statuses are not converted into evidence that no effect occurred.

After retirement, `resume({})` returns the same report without adapter resolution, adapter calls, a new attempt, or a session event.
Retirement is permanent; the API has no unretire operation.
Repeating retirement with exactly the same reason returns the same journal head and event count after resynchronizing the journal and directory.
This confirms durability after a previous response was lost or synchronization failed.
A different reason, an already complete plan, invalid input, or corrupt history is rejected.
An active worker holding the workflow lock causes `LockUnavailable`; retirement does not interrupt a running adapter.

Unknown remote effects remain unknown after retirement, and a remote request may finish independently of the retired local workflow.
Investigate provider records and current conditions before authorizing any replacement effects.
Retirement does not authorize replacing unresolved operations with new IDs, release provider resources, cancel a remote request, or reconcile unfinished effects.

Existing schema-1 histories remain readable without rewriting their prefix.
The terminal event extends the history vocabulary.
Any complete journal event after `RETIRED` is invalid, including a second retirement or a checkpoint.
Upgrade all consumers that need to inspect a retired history; downgrade of that history is unsupported.
Restoring a backup from before retirement can lose the stop decision, as with any valid-prefix rollback.
Retain the retirement head independently when rollback detection is required.

## Adapter result types

`Reconciliation("applied", receipt)` establishes that this exact logical inverse committed.
`Reconciliation("not_applied")` allows an idempotent apply call under the adapter contract.
`Reconciliation("unknown")` stops recovery without applying anything.
`Receipt(operation_id, action_fingerprint, effect_id, details={})` is durable service evidence for a committed inverse.
Receipt details must be JSON-safe and sanitized.

`PermissionDenied` and `Conflict` produce an explicit blocked status.
`RetryableError` uses another attempt within the same session, beginning with reconciliation.
`AmbiguousOutcome` and unexpected adapter exceptions produce an uncertain status and stop the session.
Journal corruption raises `JournalError` without invoking adapters.
Malformed plans and options raise `ValueError`; filesystem and lock errors remain visible.

## FileAdapter

`FileAdapter.create(path, resources)` creates a local JSON service state with version-zero resources.
`FileAdapter(path)` reopens existing state.
`inspect()` returns verified resource state, committed receipts, and effect history.
`operator_update(resource, value)` records a separate local operator edit and increments the resource version.
`set_permission(resource, allowed)` changes the local example's current authorization state.

The adapter supports `set` with `{ "value": ... }` and `increment` with `{ "amount": number }`.
Its preconditions accept a target `version` and a `guards` array.
Each guard has a `resource` and optional `version` and `value`; every mentioned resource belongs to the same JSON service file.
Versions must be nonnegative Python integers; booleans and floating-point values are rejected.
Value guards use canonical JSON representation equality, preserving the distinction between `true`, `1`, and `1.0` while ignoring object-key order.
All guards and permission checks run in the same locked commit as the effect and receipt.
Its monotonic versions detect intervening edits even if a value changes back.

The file adapter rewrites its complete state and retains all deduplication receipts.
It is intended for modest local state, examples, and adapter conformance work.
Journal replay is linear in event count; receipt garbage collection and checkpoint compaction are not implemented.

## SQLiteAdapter

`SQLiteAdapter.create(path, resources)` creates a new SQLite database with version-zero resources and refuses an existing path.
`SQLiteAdapter(path, timeout=5.0)` reopens an existing database; `timeout` is the SQLite busy wait in seconds.
It accepts the same operations, preconditions, and guards as `FileAdapter`, and offers the same `inspect`, `operator_update`, and `set_permission` methods.

The database uses WAL journal mode with `synchronous=FULL`, because rollback-journal commits at that level are not durable across power loss.
`create` builds the database in a temporary file and links it into place, so a partial database is never visible at the final path.
Each `apply` runs in one `BEGIN IMMEDIATE` transaction: the receipt lookup, permission and guard checks, resource update, receipt insert, and history insert commit or roll back together.
`reconcile` and `inspect` use read transactions and do not wait for other readers.
Receipts use the operation ID as primary key, so reconciliation and apply read only the rows they need.
A busy database raises `LockUnavailable` after the timeout with no change written.
An effect whose receipt would exceed the JSON limits raises `Conflict` before anything is written, in both adapters.
A file that is not a database created by this adapter, a schema other than 1, a journal mode other than WAL, or a malformed stored row raises `JournalError`.
The database must stay on a local filesystem with working SQLite locks; network filesystems are not supported.
Like the workflow journal, SQLite uses ordinary `fsync`; on macOS that does not force data through the drive cache, so power-loss durability there is not established.

## Persistence and diagnostics

`Compensation` and `Receipt` defensively copy and deeply freeze their JSON input.
Nested arrays become tuples and nested objects become read-only mappings.
Use `to_dict()` to obtain an independent JSON-compatible copy for a provider SDK.
`dataclasses.asdict()` is not the serialization API for these immutable objects.
Action fingerprints are cached only after the complete immutable action is validated.

`JournalIOError` means a journal write, flush, or synchronization failed and the final record may or may not be durable.
It carries `path`, `stage`, and `durability_unknown=True`.
The engine stops immediately; it does not append another outcome to a possibly torn journal.
No adapter is called after a failed attempt append.
A receipt append failure may follow an already committed service effect, so use diagnosis and reconciliation instead of changing operation identities.

`LockUnavailable` identifies a busy local advisory lock and exposes its path.
A workflow lock conflict raises it before changing the journal.
A service lock conflict records `error_code="lock_busy"`, leaves the action pending, and ends the session with `outcome="lock_busy"`.
The next call reconciles again, so pending does not establish that the earlier effect was absent.
Other adapter failures include structured `error_code` and `phase` values in action reports.
The phase is `reconcile`, `apply`, or `receipt_validation` and distinguishes a failed lookup, an uncertain write, and an invalid returned receipt.
A mismatched receipt after an apply is uncertain with `error_code="invalid_receipt"`, not a claim that a precondition safely rejected the effect.

`diagnose()` validates the complete journal prefix without calling adapters.
It reports the full file SHA-256, byte offset of the valid prefix, incomplete-tail size, and an operator next step.
For an intact active journal, the next step directs callers to inspect action statuses and error fields and resolve blocked conditions with the provider before retrying with the original operation IDs.
Journal integrity does not establish provider state.
`repair_tail(expected_sha256=...)` requires the exact diagnosed file hash, validates the complete prefix, persists an original-file backup, and removes only an incomplete final line.
It refuses checksum failures, invalid complete records, unsupported workflow histories, and an incomplete initial plan.
A subsequent `resume` on an active workflow reconciles an operation whose receipt was interrupted.
For an interrupted retirement, follow [the operator guide](RECOVERY.md) and establish the stop decision before allowing retries.
The repair command does not apply, refund, delete, or otherwise mutate service resources.

## Input and scaling limits

JSON documents, service files, and individual journal records are limited to 16 MiB.
Service-file byte limits include all whitespace; writers do not add a trailing newline.
JSONL journal records have a separate single-byte newline delimiter outside the encoded record limit.
JSON nesting is limited to 64 levels and integer values to 4,096 bits.
Duplicate object keys, non-string keys, invalid Unicode, non-finite numbers, and numeric exponent overflow are rejected.
The JSON loader reads at most one document limit plus one byte; journal loading reads bounded records instead of copying the entire raw file first.
Aggregate journal memory still grows with the number of events, and the host must bound total history and process resources.

The engine replays history once per resume and then updates only the affected action after each durable event.
Dependency ordering uses a stable priority queue, with complexity proportional to dependencies plus actions times the logarithm of action count.
The file adapter still reads and replaces its entire bounded store for each effect.
It is not a high-throughput database, and its cumulative I/O can grow quadratically with stored receipt count.
Use `SQLiteAdapter` or a service-native transactional adapter for large or long-lived workloads rather than deleting deduplication records or claiming the JSON fixture scales indefinitely.
In one local measurement on a macOS machine with 3,000 retained receipts, `FileAdapter` took about 90 to 100 ms per effect, while `SQLiteAdapter` stayed under 1 ms.
