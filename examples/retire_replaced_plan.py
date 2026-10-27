"""Stop stale retries after reviewing a replacement against a local provider fixture."""

import argparse
import json
from pathlib import Path

from recovery_under_interference import Compensation, FileAdapter, RecoveryEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="New local fixture directory")
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=False)
    service = FileAdapter.create(args.directory / "provider.json", {"credits": 3})
    original = RecoveryEngine.create(
        args.directory / "original",
        [
            Compensation(
                "credit", "provider", "credits", "increment", "original:credit", {"amount": 12}
            )
        ],
    )
    service.set_permission("credits", False)
    blocked = original.resume({"provider": service})
    assert blocked["actions"]["credit"]["status"] == "blocked"
    retired = original.retire(
        reason="Operator reviewed replacement:credit for 5 units; stop original retries"
    )
    assert retired["actions"] == blocked["actions"]
    # This fixture's permission rejection proves no first effect; arbitrary unknown outcomes do not.
    assert service.inspect()["receipts"] == {}
    replacement = RecoveryEngine.create(
        args.directory / "replacement",
        [
            Compensation(
                "credit", "provider", "credits", "increment", "replacement:credit", {"amount": 5}
            )
        ],
    )
    service.set_permission("credits", True)
    completed = replacement.resume({"provider": service})
    stale_retry = RecoveryEngine(original.directory).resume({})
    assert stale_retry == retired and not stale_retry["complete"]
    state = service.inspect()
    assert completed["complete"] and state["resources"]["credits"]["value"] == 8
    assert len(state["receipts"]) == 1
    print(
        json.dumps({"original": stale_retry, "replacement": completed, "provider": state}, indent=2)
    )


if __name__ == "__main__":
    main()
