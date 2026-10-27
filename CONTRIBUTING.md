# Contributing

Keep this library focused on durable compensation and explicit service contracts.
Forward workflow orchestration, model prompting, UI integrations, and production credentials do not belong in the core.

## Local checks

```sh
python3 -m venv .venv && . .venv/bin/activate
python3 -m unittest discover -s tests -v
python3 -m pip install build ruff==0.17.2
ruff check recovery_under_interference tests examples
ruff format --check recovery_under_interference tests examples
python3 -m build
```

The runtime uses the standard library; `build` and setuptools are packaging tools only.
Use a fresh virtual environment to install the built wheel and run the CLI outside the checkout.
The CI workflow demonstrates the installed-package smoke sequence.
CI runs Python 3.11, 3.12, 3.13, and 3.14.

For an adapter change, include a small case proving the relevant effect identity, restart, permission, or precondition behavior.
Use temporary local state, deterministic failures, and separate effect-history assertions.
Do not add real account calls or network dependencies to tests.
Keep blocked and uncertain outcomes distinct from successful recovery in tests.

## Evidence and compatibility

Write benchmark output to a new directory under `runs/`; never edit generated metrics or journals.
An improvement claim requires an explicit baseline, equal conditions, and both completion and safety evidence.
The controlled simulator is separate from the public engine; changes to one do not automatically validate the other.
Document any public API, journal schema, or adapter-contract change before release.
Do not silently migrate journals whose semantic compatibility has not been established.

## Release checks

Release checks exercise process interruption and persistence errors, not only successful resumes.
The durability tests create their own child processes, send SIGKILL only to those children, and verify provider effect history after another worker resumes.
They also cover partial journal writes, failed synchronization, immutable inputs, bounded JSON loading, and cross-process lock contention.
The custom business adapter test verifies a payment and inventory workflow through provider-specific operation mapping.
Hardware power loss and commercial provider contracts are outside this local suite.

## License

Contributions are provided under the project's MIT license.
