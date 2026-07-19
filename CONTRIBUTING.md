# Contributing

Thank you for helping improve LangGraph Go.

## Before opening a change

- Search existing issues and pull requests.
- Use a focused issue for compatibility gaps or behavioral changes.
- For upstream compatibility, include the LangGraph `1.2.9` API or test that
  defines the expected behavior.
- Discuss broad API redesigns before investing in an implementation.

## Development setup

Requirements:

- Go 1.25 or newer
- Docker for PostgreSQL and Temporal integration tests

Run the standard checks:

```bash
go test ./...
go test -race ./...
go vet ./...
```

PostgreSQL tests use `LANGGRAPH_POSTGRES_DSN`. Temporal integration uses
`LANGGRAPH_TEMPORAL_ADDRESS`.

## Pull requests

- Keep changes small and behavior-focused.
- Add tests for success, error, cancellation, and recovery paths as applicable.
- Preserve deterministic ordering in concurrent execution.
- Do not add provider SDK dependencies to core packages.
- Update public documentation when behavior or compatibility changes.
- Run `gofmt`, tests, race checks, and `go vet` before requesting review.

By contributing, you agree that your contribution is licensed under the MIT
License of this repository.
