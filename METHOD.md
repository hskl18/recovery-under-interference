# Method and interpretation

This document describes the separate controlled simulator in `recovery_under_interference/simulation.py`.
Its four-policy experiment is distinct from the adapter-driven `RecoveryEngine` library and its integration tests.
The simulator retains a specialized interpreter so each baseline's inverse and retry semantics stay explicit.
These simulation results are not evidence of external-service adapter reliability or deployment.

The research question is whether rechecking cross-service prerequisites during compensation prevents unsafe effects that same-resource CAS alone cannot catch after an earlier undo has succeeded.
The implementation is an executable local counterexample study.
It does not claim a new recovery algorithm, benchmark-wide model result, or superiority over mature workflow systems.

## Workflow and effect contract

A forward workflow reserves a quantity of stock, charges an account, and creates a shipment.
A synthetic downstream rejection then requests compensation in reverse order.
The model requires shipment cancellation before refund, and refund before stock release.
Human stock adjustments belong to a separate operation and must survive recovery.
A human-dispatched shipment must not be silently cancelled by a stale snapshot.
Revoked service permission must prevent a new workflow effect, while reading an already committed operation's acknowledgement is allowed.

Each service effect is durable before the worker records completion.
An injected lost acknowledgement therefore leaves a real committed journal effect without a corresponding worker `DONE` event.
Stable operation IDs resolve this ambiguity on retry.
The conservative strategy also requires the compensated resource to retain its forward version and checks the current shipment state before refund or release.
Stock release additionally requires a recorded refund operation.

The simulator holds one advisory file lock for the worker session, and all controlled human interleavings are journal events within that session.
Dependency checks and local commits are consequently atomic with respect to this simulator's other actors.
This assumption is stronger than independent remote services and must not be presented as distributed transaction safety.
A real integration would need a compatible conditional commit, a coordinated transaction, or an explicitly weaker guarantee.

## Controls

All four policies run on identical initial data, schedules, permission enforcement, inverse ordering, and retry budgets.
All stop on a permanent failure.
The naive policy is intentionally a weak snapshot/retry reference.
The idempotent policy uses additive inverses and therefore preserves an independent stock addition without requiring a version match.
CAS checks only the resource being compensated.
The conservative policy adds checks of related resources; it does not change the workload or oracle.

| Schedule | Intervention | Relevant comparison |
| --- | --- | --- |
| `clean` | No interference. | All methods should complete safely. |
| `double_fault` | First undo commits but loses its reply; the second attempt fails before the call. | Stable operation IDs versus duplicate effects. |
| `human_stock` | Operator adds five stock units just before stock compensation. | Independent additive inverse versus stale snapshot and conservative CAS deferral. |
| `human_dispatch` | Operator dispatches before shipment compensation. | Same-resource CAS should tie the conservative policy. |
| `revoked_shipping` | Shipment permission is revoked before cancellation. | Every policy should stop without a denied effect. |
| `reopened_shipping` | Operator dispatches after cancellation, before refund. | Cross-service dependency checks versus isolated resource CAS. |
| `double_fault_and_dispatch` | Lost reply, operator dispatch, then a second fault. | A deduplicated cancellation receipt does not establish current dependency validity. |

These are hand-authored distinguishing cases, including a case where conservative deferral loses progress to a safe additive inverse.
They are not a held-out test set and do not estimate the prevalence of any failure in real deployments.
The seeds vary quantity and charge only.
Repeated deterministic seed variants must not be treated as independent evidence for statistical significance.

## Independent checks

The oracle reads raw journal events and reconstructs state without consulting worker `DONE` or `BLOCKED` decisions or invoking compensation policy helpers.
It separately checks final-state restoration and the history of committed effects.
History checks cover repeated logical compensation effects, permission violations, erasure of human dispatch, refunds while shipping is active, release before refund, and incorrect numeric inverses.
This catches the naive lost-ack case even when its final state happens to match the desired target.
Expected final numeric values preserve the operator's signed adjustments; shipment state preserves the most recent operator override.

`safe` means no modeled history invariant was violated.
`successful_rollback` requires safe history, all three inverse effects, and a final state matching the independently computed target.
`blocked_runs` reports work left for explicit resolution and is not included as successful rollback.
The test suite includes negative controls and cross-process replay, but it is not a formal proof of the oracle or runtime.

## Related work and novelty boundary

[UndoBench: Separating Task Competence from Recovery Capability in Tool-Using AI Agents](https://arxiv.org/html/2610.05622v1) motivates separating task completion, recovery, ambiguous effect outcomes, and effect-history evidence.
This project borrows that evaluation distinction and examines a small set of compensation-time interleavings.
It does not reproduce UndoBench's tasks, model experiments, or aggregate claims.

[DART: Semantic Recoverability for Structured Tool Agents](https://arxiv.org/html/2605.23311v1) studies recoverable boundaries and semantically admissible rollback.
Its scope makes semantic preconditions an existing research theme, so the checks here are not presented as a newly invented concept.

[REVISE: Validity-Guided Recovery for Online Revisions in Agent Workflows](https://arxiv.org/html/2609.00643v1) studies validity-aware reuse and commit-time revalidation after workflow revisions.
The related question here is whether a previous successful cancellation still supports a later refund after an operator change.
The local code is not an implementation or comparative evaluation of REVISE.

Idempotency, compare-and-set, fail-stop recovery, and saga compensation are established mechanisms.
Where CAS matches the conservative policy in a scenario, the results show a tie.
Where the additive idempotent baseline safely completes work that conservative CAS blocks, that is a cost of conservatism.
Demonstrating the selected counterexamples does not establish that no existing workflow system handles them.

## Evidence to add before stronger claims

Useful next evidence would include held-out dependency graphs, independent fault schedules, a real external service adapter with documented idempotency, and check-to-commit races outside the simulator's global lock.
Permission changes after a precheck, process termination between every append boundary, partial log writes, and more complex operator edits require separate coverage.
No real-service deployment, user adoption, model capability, throughput improvement, or broad distributed-systems guarantee follows from these results.
