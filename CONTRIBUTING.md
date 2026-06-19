# Contributing to StreamGuard

Thanks for contributing to StreamGuard.

## Development Setup

StreamGuard targets Go `1.22+`.

Build and test locally:

```sh
go build ./...
go test ./...
go test -race ./...
```

Run the proxy locally:

```sh
OPERATOR_TOKEN=dev-operator-token go run ./cmd/streamguard
```

Run the deterministic demo upstreams used for screenshots and failover validation:

```sh
go run ./cmd/demo-upstreams
```

## Contribution Guidelines

- Keep changes tightly scoped.
- Preserve the documented SSE wire protocol and closed enum values.
- Add or update tests when behavior changes.
- Keep documentation aligned with actual implementation.
- Do not introduce placeholder badges, fake install commands, or speculative production claims.

## Commit Style

Use conventional commit prefixes where practical:

- `feat:`
- `fix:`
- `docs:`
- `test:`
- `refactor:`
- `chore:`

Keep subject lines short and imperative.

## Pull Requests

Each pull request should include:

- a concise problem statement,
- a summary of the user-visible or operator-visible change,
- test coverage or verification notes,
- follow-up work or known limitations, if any.

Prefer one logical change per pull request. If the change affects protocol behavior, usage accounting, or breaker logic, call that out explicitly in the description.
