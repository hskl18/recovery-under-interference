"""Run with a new directory after installing the package."""

import argparse
import json
from pathlib import Path

from recovery_under_interference import Compensation, FileAdapter, RecoveryEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.directory.exists():
        parser.error("directory must be new")
    service = FileAdapter.create(args.directory / "inventory.json", {"stock": 9})
    action = Compensation(
        "release",
        "warehouse",
        "stock",
        "increment",
        "order-42:release",
        {"amount": 1},
        {"version": 0},
    )
    engine = RecoveryEngine.create(args.directory / "workflow", [action])
    result = engine.resume({"warehouse": service})
    assert result["complete"]
    assert service.inspect()["resources"]["stock"]["value"] == 10
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
