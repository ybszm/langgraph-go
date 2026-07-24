# Changelog

All notable changes to LangGraph Go are documented in this file. The project
uses Semantic Versioning while it remains pre-1.0: minor releases may introduce
intentional API changes, and patch releases preserve public API compatibility.

## [Unreleased]

### Added

- Docs: `COMPARISON.md`, `LANGCHAINGO.md`, `VERSIONING.md`; README QuickAgent front door.
- `eval` golden trajectory helpers for tool-order and final-content assertions.
- `a2a` minimal Agent-to-Agent HTTP card + message:send protocol.
- `httpware` bearer auth, session header, and local rate-limit middleware.
- `examples/mcp-secure` GuardTools pattern for MCP tool binding.
- `graph.RunConfig.Durability` with sync/async/exit support.
- `graph.WithDeferred` node marker + `CompiledGraph.IsDeferred`.
- Deferred edge scheduling after ordinary work drains, including cross-step
  trigger collapse and checkpoint-safe interrupt/resume.
- Exit durability with run-local intermediate checkpoints, final-only history,
  and recovery-boundary flushes for interrupts and failed super-steps.
- Async durability with an ordered run-scoped writer, in-memory draft visibility,
  background-error cancellation, and mandatory flush before return.
- Graph micro-benchmarks (`BenchmarkLinearInvoke`, `BenchmarkFanOutThree`).

### Changed

### Fixed

- Reject line-breaking SSE event IDs and modes before writing protocol frames.
- Isolate async/exit checkpoint drafts by thread and namespace, retain stateful
  child history within a run, and flush nested exit boundaries before parents.

## [0.1.0] - 2026-07-23

First supported development release of LangGraph Go. Usable, tested, and
documented for GitHub consumers via `go get github.com/ybszm/langgraph-go@v0.1.0`.

Compatibility targets LangGraph `1.2.9` behavioral reference
(`95af6a00718588e7b7ce17310e8006d267896a77`). This is **not** a drop-in Python
API. See [COMPATIBILITY.md](COMPATIBILITY.md) and
[docs/DURABILITY.md](docs/DURABILITY.md). Consumer-safety rules:
[docs/PUBLISHING.md](docs/PUBLISHING.md).

### Added

- Typed `StateGraph` runtime with BSP super-steps, Send, Command, subgraphs,
  interrupts, time travel, streaming, and Functional API.
- Checkpoint savers (memory, SQLite, PostgreSQL) plus optional Redis module.
- Prebuilt agents: ChatModelAgent, DeepAgent, Router/Supervisor/Handoff,
  `NewQuickAgent`, `NewAgentRunner`, tool-guard middleware, model fallback.
- Optional providers, MCP client, OpenTelemetry callbacks.
- Optional remote HTTP/SSE (including development EventLog helpers and
  reconnecting clients), distributed PostgreSQL primitives, Temporal adapter.
- `graph.WithAllowUnreachableNodes` / `WithAllowUnreachableEND` (opt-in).
- `checkpoint/retention` helpers; memory and SQLite `PruneThread`.
- `backend/distributed.Metrics` process-local counters.
- `compat` behavioral harness with core StateGraph scenarios.
- Canonical examples for agents, persistence, streaming, interrupt, and more.
- Docs: architecture, durability, publishing, agents, streaming, callbacks.

### Notes for consumers

- Pin a version in `go.mod`; pre-1.0 minors may refine APIs with changelog notes.
- Defaults preserve strict compile and prior semantics; new behaviors are opt-in.
- Nested modules require path-prefixed tags (see RELEASING.md).

[Unreleased]: https://github.com/ybszm/langgraph-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ybszm/langgraph-go/releases/tag/v0.1.0
