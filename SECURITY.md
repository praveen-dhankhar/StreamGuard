# Security Policy

## Supported Versions

StreamGuard is currently maintained as a single active line on `main`.

| Version | Supported |
|---|---|
| `0.1.x` | Yes |
| `< 0.1.0` | No |

## Reporting a Vulnerability

Please report security issues privately by opening a GitHub security advisory or by emailing `praveendhankhar0@gmail.com`.

Include:

- affected version or commit,
- impact summary,
- reproduction steps,
- any proposed mitigation if available.

Do not open public issues for unpatched vulnerabilities involving authentication, budget bypass, request replay semantics, or upstream credential handling.

## Scope

Security reports are especially relevant for:

- API key validation and redaction,
- authorization boundaries on `/usage/{key}` and `/healthz`,
- budget enforcement,
- circuit-breaker behavior under concurrent load,
- request replay and failover handling.
