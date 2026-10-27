# Service adapter contract

The engine is reusable through the structural `ServiceAdapter` protocol.
No inheritance, registration decorator, global registry, or model SDK is required.
An application passes its adapter objects directly to `RecoveryEngine.resume`.

```python
from recovery_under_interference import Reconciliation, RecoveryEngine

class InventoryAdapter:
    def __init__(self, client):
        self.client = client

    def reconcile(self, action):
        return self.client.lookup_compensation(action.operation_id, action.fingerprint)

    def apply(self, action):
        return self.client.conditional_compensate(action)

# The client methods must implement the contract below and return library result types.
# engine = RecoveryEngine("workflow")
# report = engine.resume({"inventory": InventoryAdapter(client)})
```

This is an integration shape, not a claim that an arbitrary client already has safe compensation semantics.
The fully runnable local example uses `FileAdapter` in [../examples/compensate_order.py](../examples/compensate_order.py).

## Durable operation identity

The service must bind the operation ID to the full action fingerprint.
Repeating the same ID and fingerprint must return the same committed effect receipt without applying the inverse again.
Reusing the ID for another payload must fail with `Conflict`.
Deduplication state must survive worker and service restart for the full supported recovery horizon.
If the provider expires idempotency keys, document the expiry and stop automatic recovery after that boundary.
An in-memory Python set or a journal marker written before an external API request does not establish remote idempotency.

Effect commit and durable deduplication must have a service-supported atomic relationship.
For the included file adapter, one locked atomic replacement contains both.
For a database adapter this normally requires a suitable transaction and unique operation key.
For an HTTP service it requires a documented provider contract, not merely a custom request header.
The library does not manufacture exactly-once effects at a remote endpoint.

## Reconciliation and uncertainty

`applied` requires a matching durable receipt.
`not_applied` must not be inferred from a timeout, an eventually consistent empty read, or a denied lookup.
The subsequent apply call still requires durable idempotency to cover races and delayed original requests.
Return `unknown` when the outcome cannot be established safely.
Raise `AmbiguousOutcome` when a write may have committed but its result is unavailable.
The engine will stop and require a later reconciliation attempt.

`RetryableError` permits an immediate bounded retry, always beginning with reconciliation.
It is appropriate for a failed read or a request whose repeat is safe under the declared durable operation contract.
Unexpected exceptions are treated as uncertain, not as proof that no effect occurred.
Do not mark an action applied merely because an HTTP request returned a success-shaped payload; validate the provider's operation identity and effect result.

## Authorization and preconditions

Enforce current permission and target-version conditions at the authoritative commit point.
The engine's dependency ordering only establishes that prerequisite receipts were seen.
It does not establish that prerequisite resources still have their required state.
The adapter must enforce `preconditions` according to its documented operation schema, or reject unsupported conditions.
Silently ignoring an unknown guard is a contract violation.

Guards spanning several resources require a compatible atomic service boundary or a documented coordinated protocol.
A collection of remote reads followed by unconditional writes has a race window.
Do not claim it satisfies this library's conditional-commit semantics.
When a provider cannot support the necessary condition, return an explicit conflict or unknown result and require application review.
This conservative choice may reduce completion; it is not itself a successful compensation.

## Conformance evidence

Before integrating a new adapter, exercise duplicate operation IDs, payload collisions, effect commit followed by lost acknowledgement, service restart, and stale versions.
Also exercise revoked authorization, failed reconciliation, unsupported conditions, and dependency changes between earlier compensation and the next commit.
Record final state and effect history separately; matching final state alone can hide duplicate effects.
The repository tests these behaviors against the local file adapter and controlled faults.
It does not certify any remote database, payment processor, or remote API adapter.

## SQLite adapter

`SQLiteAdapter` is the database-shaped reference for this contract.
The operation ID is the receipts primary key, and one immediate WAL transaction holds the receipt check, preconditions, effect, and receipt.
A lost reply after `COMMIT` is therefore resolved by the next reconciliation, and a concurrent duplicate waits for the write lock and then returns the stored receipt.
Port the same shape to a server database by using a unique operation key and performing the precondition check and effect in one transaction.
[examples/sqlite_order.py](../examples/sqlite_order.py) runs a lost reply followed by a second process that completes without repeating the effect.

## Local file adapter capacity

`FileAdapter` is intended for modest local state.
Every inspection or reconciliation reads and validates the complete state, including every stored receipt.
Every new effect rewrites that state, so operation cost grows with retained history and receipts.
The adapter retains those receipts to preserve idempotency; deleting them can permit an old operation to apply again.

The complete JSON document is limited to 16 MiB.
A write that would exceed the limit raises `Conflict` before replacing the existing file.
Use `SQLiteAdapter` or another adapter with durable indexed operation identities when sustained history growth makes full-state reads and writes unsuitable.
Changing adapters or retiring a workflow does not itself establish that old effects can be safely forgotten.

## Runnable business integration

[The business refund example](../examples/business_refund.py) implements an application adapter for `refund_payment` and `restore_stock`.
It translates domain requests into provider calls and rebinds validated provider receipts to the original immutable action fingerprint.
Payments and inventory use separate durable provider fixtures, and the workflow persists a dependency between their compensations.

```sh
python3 examples/business_refund.py runs/business --kill-after-payment
python3 examples/business_refund.py runs/business --resume
```

The first command deliberately terminates its own worker with SIGKILL after the payment effect commits.
Its nonzero signal exit is expected and should not be chained with `&&` to the resume command.
The second process reconciles that effect and completes the inventory adjustment.
A normal uninterrupted example omits both flags.
The example asserts final balances and the number of committed effects separately.
A real provider still needs the same durable identity, conditional commit, and reconciliation guarantees; replacing the fixture with an arbitrary HTTP endpoint does not supply them.
