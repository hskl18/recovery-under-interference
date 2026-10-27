# Security policy

Report security concerns privately to `hi@hanbintan.com` before publishing exploit details or private logs.
Include the affected package version, a minimal local reproduction, and the relevant adapter contract.
Do not send credentials, payment data, customer records, or production journal contents.
You can also use GitHub private vulnerability reporting on the repository's Security tab.
This project has no promised response-time or support-service agreement.

## Supported versions

Security fixes target the latest 0.x release.

## Trust boundaries

Journals contain compensation arguments, resource identifiers, and receipt details in plaintext.
Use application-owned directories and sanitized inputs; credentials belong in the adapter's external configuration, not in a plan.
Adapter error messages are journaled and must not contain secrets.
The engine accepts adapter objects from trusted application code and does not dynamically import code named by a journal.

Checksums provide corruption detection, not authentication or tamper-proof audit evidence.
Local advisory locks coordinate cooperating processes only.
Untrusted users with filesystem write access can alter state, replace files, or invalidate recovery assumptions.
Remote exactly-once execution, distributed locking, and provider authorization cannot be supplied by this local library.

Corrupt journals fail closed without service calls.
Uncertain service outcomes remain explicit until an adapter can reconcile them.
Production adapters and their credentials require separate review and conformance evidence.
