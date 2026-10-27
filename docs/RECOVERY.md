# Operating recovery safely

Persist a plan only after the application knows which forward effects require compensation.
This library does not execute the forward workflow or infer valid inverse operations.
Amounts, ownership, versions, and business prerequisites come from the application and its authoritative service records.

## Restart

Each attempt is journaled before contacting its adapter.
If the process stops before a receipt is journaled, an active workflow's next process reconciles the same operation ID.
An explicitly retired workflow never starts another reconciliation or apply call.
A stored receipt may complete the action without another effect.
A service-confirmed absent operation allows an idempotent apply attempt.
An unknown outcome stops recovery and remains visible in `inspect`.

```sh
recovery-under-interference inspect runs/order/workflow
recovery-under-interference resume runs/order/workflow --adapter local=runs/order/services.json --max-attempts 20
```

Resume requires the same durable adapter state and operation identity scope.
Do not delete service receipts or recreate an empty service file to make recovery appear successful.
The engine treats journal corruption as an error and never truncates it automatically.
An explicit hash-guarded repair command is available only for an incomplete final record.

## Conflicts and permission changes

`blocked` means an adapter rejected the current inverse or a required condition.
Inspect the action reason and service state before changing permissions or resolving an operator edit.
`diagnose` validates the local journal; an intact journal does not establish the provider's current state.
Check each action's `status`, `error_code`, `phase`, and `reason`, then verify and resolve a blocked condition with the provider before retrying with the same operation IDs.
The library does not rewrite an operator's conflicting work or silently update a plan's version precondition.
An application may explicitly resolve the state and retry the existing plan, or create a separately reviewed recovery plan with new operation identities.
Reusing an old operation ID with new arguments must be rejected by the adapter.
Before enabling a reviewed replacement, retire an abandoned original plan so a stale scheduler cannot resume it later.
Investigate all unresolved effects before authorizing a replacement; retirement itself makes no claim about their absence.

The CLI exposes file adapters only.
An application using another service must supply its adapter through the Python API.
There is no plugin loader that imports arbitrary code from an untrusted journal.

## Stop retrying an abandoned plan

Use the permanent retirement operation after an operator decides to stop the incomplete workflow:

```sh
recovery-under-interference inspect runs/order/workflow
recovery-under-interference retire runs/order/workflow \
  --reason 'Reviewed replacement incident-42; stop retries of the original plan'
recovery-under-interference resume runs/order/workflow
```

The last command returns `retired: true` and `complete: false` without requiring an adapter or invoking a service.
The original action statuses and receipts stay visible, including uncertain and inflight operations.
Retirement does not cancel remote work, restore resources, or authorize a replacement plan.
Keep provider evidence and the reviewed replacement decision in the application's operational records.
The [retirement example](../examples/retire_replaced_plan.py) shows a local permission-blocked plan retired before a separately reviewed replacement is enabled.

The workflow lock prevents retirement from overtaking an active resume call.
If retirement returns lock contention, wait for the worker to stop or finish, inspect the outcome, and retry retirement.
Do not remove the lock file or assume a failed retirement stopped the worker.
A successful retirement cannot be undone through this API.
An identical reason retry reconfirms journal and directory durability without adding another event; a different reason is rejected.

If retirement returns journal persistence uncertainty, inspect and diagnose before doing anything else.
A complete terminal record can be reconfirmed by repeating the same retirement.
A torn retirement record establishes no committed stop decision and blocks resume until reviewed repair.
After repairing such a tail, explicitly repeat retirement before re-enabling any scheduler.
Do not interpret repair alone as permission to resume an abandoned plan.
If the valid prefix was already retired, repair preserves retirement and resume remains disabled.

## Persistence limits

Journal writes flush and `fsync` each event and synchronize the containing directory.
Creation also synchronizes newly created parent directories and initial service-file entries.
The file adapter flushes its replacement file and parent directory after an atomic rename.
The file adapter writes service JSON without a trailing newline, keeping an exact 16 MiB encoded document within the reader's 16 MiB file limit.
Earlier ordinary service files ending in a newline remain readable when their total file size is within that limit.
Reconciliation synchronizes an existing receipt file and its directory again before reporting it applied, covering an earlier interruption between rename and directory synchronization.
These mechanisms support ordinary process restart on a local POSIX filesystem.
The checked tests cover distinct-process restart, deliberately corrupted records, torn final journal lines, and effect/acknowledgement separation.
They do not exhaustively test arbitrary kill points, hardware power loss, filesystem bugs, or network filesystems.

Hash chains and adapter checksums detect many accidental changes but do not authenticate trusted writers.
A valid-prefix rollback can escape checksum detection without an independently retained expected head.
Keep workflow and service files in a directory owned by the application, restrict access, and back them up consistently.
Another process attempting the same workflow receives a lock error; there is no distributed lease or failover coordinator.

## A journal write failed after the service committed

A `JournalIOError` is a persistence incident, not an ordinary adapter failure.
The engine stops without writing an additional uncertain outcome.
Preserve the same workflow directory and service-side operation records.
Do not create a replacement operation ID or repeat an unconditional inverse.

```sh
recovery-under-interference diagnose runs/order/workflow
```

If the final record is complete and valid, `inspect` can recover it and an active workflow can continue with `resume`.
The commands in this section concern interrupted effect recording; for an interrupted retirement, follow the retirement recovery steps above.
If only the final line is incomplete, diagnosis reports `recoverable_tail: true` and `journal_sha256`.
Use that exact value to request the guarded repair:

```sh
recovery-under-interference repair-tail runs/order/workflow --expected-sha256 HASH_FROM_DIAGNOSIS
recovery-under-interference resume runs/order/workflow --adapter local=runs/order/services.json
```

Repair first writes and synchronizes a backup named `journal.jsonl.before-repair-SHA256` in the workflow directory.
It then truncates only the incomplete final line and synchronizes the journal again.
The backup is retained for investigation.
A valid backup from an interrupted repair can be reused; an incomplete or conflicting backup requires investigation.
Any complete corrupt record or invalid transition is refused instead of silently discarding history.
A pending or inflight action then reconciles using its original operation identity before any repeat is considered.

## Machine-readable failures

CLI exit code 3 indicates workflow lock contention and includes `error_code="lock_busy"` and a path.
Exit code 4 indicates journal persistence uncertainty and includes the path and failing write stage.
Exit code 2 remains invalid input, corrupt state, or another filesystem error.
Successful inspection, diagnosis, repair, retirement, and resume commands return 0; callers must inspect `complete`, `retired`, action statuses, and diagnostic fields.
`retired=true` is a stopped incomplete workflow, not successful compensation.
A service lock conflict is a successful session report with `outcome="lock_busy"`, not evidence of completion.
Release the competing worker or use application-controlled backoff before resuming.
Do not delete a lock file to bypass its holder; replacing it would create a different lock inode and allow concurrent writers.

## Verified process-failure windows

The regression suite sends real SIGKILL signals to child workers immediately before and after the provider fixture's atomic replacement.
It also kills a custom business-adapter worker after a payment refund commits and before its receipt reaches the workflow journal.
A new worker reconciles the payment and restores inventory with one provider effect per logical operation.
These are local durable provider fixtures, not tests of a commercial payment service.
Fault injection additionally covers journal fsync failure, partial receipt writes, and held locks in independent processes.
No test establishes hardware power-loss behavior or remote provider guarantees.
