"""Lose a reply after a SQLite commit, then finish recovery in a second process.

Run with a new directory after installing the package.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

from recovery_under_interference import (
    AmbiguousOutcome,
    Compensation,
    RecoveryEngine,
    SQLiteAdapter,
)

# Cancel shipping, then refund, then restock; each step re-checks what the previous one changed.
ACTIONS = [
    Compensation(
        "cancel", "local", "shipping", "set", "order-1:cancel", {"value": "none"}, {"version": 0}
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


class LostReply(SQLiteAdapter):
    """Commit the effect and receipt, then behave as if the reply never arrived."""

    def apply(self, action):
        super().apply(action)
        raise AmbiguousOutcome("reply lost after commit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--resume", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    database = args.directory / "service.sqlite"
    workflow = args.directory / "workflow"
    if args.resume:
        print(json.dumps(RecoveryEngine(workflow).resume({"local": SQLiteAdapter(database)})))
        return
    if args.directory.exists():
        parser.error("directory must be new")
    SQLiteAdapter.create(database, {"shipping": "pending", "billing": 75, "inventory": 4})
    first = RecoveryEngine.create(workflow, ACTIONS).resume({"local": LostReply(database)})
    assert not first["complete"]
    subprocess.run(
        [sys.executable, __file__, str(args.directory), "--resume"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    state = SQLiteAdapter(database).inspect()
    effects = [entry["operation_id"] for entry in state["history"]]
    assert RecoveryEngine(workflow).inspect()["complete"]
    assert effects == ["order-1:cancel", "order-1:refund", "order-1:release"], effects
    print(json.dumps({"effects": effects, "resources": state["resources"]}, indent=2))


if __name__ == "__main__":
    main()
