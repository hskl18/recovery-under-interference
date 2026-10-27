"""Run a custom business adapter against a local durable provider fixture.

This demonstrates application integration, not a certified remote payments adapter.
The provider fixture has durable operation IDs and atomic version checks.
"""

import argparse
import json
import os
import signal
from pathlib import Path

from recovery_under_interference import (
    Compensation,
    Conflict,
    FileAdapter,
    Receipt,
    Reconciliation,
    RecoveryEngine,
)


class BusinessAdapter:
    """Translate domain requests and provider receipts at the application boundary."""

    def __init__(self, provider: FileAdapter, operation: str):
        self.provider = provider
        self.operation = operation

    def _request(self, action: Compensation) -> Compensation:
        if action.operation != self.operation or set(action.arguments) != {"amount"}:
            raise Conflict("Unsupported business operation or arguments")
        amount = action.arguments["amount"]
        if type(amount) is not int or amount <= 0:
            raise Conflict("Business adjustments require positive integer minor units")
        return Compensation(
            action.id,
            action.adapter,
            action.resource,
            "increment",
            action.operation_id,
            {"amount": amount},
            action.preconditions,
            action.requires,
        )

    @staticmethod
    def _receipt(action: Compensation, provider_receipt: Receipt) -> Receipt:
        return Receipt(
            action.operation_id,
            action.fingerprint,
            provider_receipt.effect_id,
            {
                "provider_fingerprint": provider_receipt.action_fingerprint,
                "provider_result": provider_receipt.to_dict()["details"],
            },
        )

    def reconcile(self, action: Compensation) -> Reconciliation:
        result = self.provider.reconcile(self._request(action))
        if result.state == "applied":
            assert result.receipt is not None
            return Reconciliation("applied", self._receipt(action, result.receipt))
        return Reconciliation(result.state)

    def apply(self, action: Compensation) -> Receipt:
        return self._receipt(action, self.provider.apply(self._request(action)))


def run(directory: Path, *, kill_after_payment=False) -> dict:
    payment = FileAdapter.create(directory / "payments.json", {"order-42": 7500})
    warehouse = FileAdapter.create(directory / "warehouse.json", {"sku-17": 9})
    plan = [
        Compensation(
            "refund",
            "payments",
            "order-42",
            "refund_payment",
            "order-42:refund",
            {"amount": 2500},
            {"version": 0},
        ),
        Compensation(
            "stock",
            "warehouse",
            "sku-17",
            "restore_stock",
            "order-42:stock",
            {"amount": 1},
            {"version": 0},
            ("refund",),
        ),
    ]
    engine = RecoveryEngine.create(directory / "workflow", plan)
    if kill_after_payment:

        class InterruptedPayment(FileAdapter):
            def apply(self, action):
                super().apply(action)
                os.kill(os.getpid(), signal.SIGKILL)

        payment = InterruptedPayment(payment.path)
    adapters = {
        "payments": BusinessAdapter(payment, "refund_payment"),
        "warehouse": BusinessAdapter(warehouse, "restore_stock"),
    }
    checkpoint = engine.resume(adapters, checkpoint_after=1)
    assert not checkpoint["complete"]
    return resume(directory)


def resume(directory: Path) -> dict:
    payment = FileAdapter(directory / "payments.json")
    warehouse = FileAdapter(directory / "warehouse.json")
    engine = RecoveryEngine(directory / "workflow")
    adapters = {
        "payments": BusinessAdapter(payment, "refund_payment"),
        "warehouse": BusinessAdapter(warehouse, "restore_stock"),
    }
    report = engine.resume(adapters)
    assert report["complete"]
    assert payment.inspect()["resources"]["order-42"]["value"] == 10000
    assert warehouse.inspect()["resources"]["sku-17"]["value"] == 10
    assert len(payment.inspect()["history"]) == len(warehouse.inspect()["history"]) == 1
    return {
        "complete": report["complete"],
        "payment_effects": len(payment.inspect()["history"]),
        "warehouse_effects": len(warehouse.inspect()["history"]),
        "workflow": str(engine.directory),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resume", action="store_true")
    mode.add_argument("--kill-after-payment", action="store_true")
    args = parser.parse_args()
    report = (
        resume(args.directory)
        if args.resume
        else run(args.directory, kill_after_payment=args.kill_after_payment)
    )
    print(json.dumps(report, indent=2))
