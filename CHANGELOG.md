# Changelog

All notable changes to LangGraph Go are documented in this file. The project
uses Semantic Versioning while it remains pre-1.0: minor releases may introduce
intentional API changes, and patch releases preserve public API compatibility.

## [Unreleased]

### Added

- `prebuilt.NewQuickAgent` for the smallest model+tools+system-prompt path.
- `prebuilt.NewAgentRunner` high-level Run/Query/Resume event facade (with docs
  and examples).
- `prebuilt` tool guard middleware (`ToolGuardPolicy`, `GuardTools`) for allowlist,
  deny list, and human-approval gates.
- `graph.WithAllowUnreachableNodes` / `WithAllowUnreachableEND` compile options.
- `checkpoint/retention` helpers; memory and SQLite `PruneThread` support.
- `backend/distributed.Metrics` process-local counters for ops export.
- `remote.DevelopmentServerOptions` wiring an in-memory durable EventLog.
- `compat` behavioral harness with core StateGraph scenarios.
- `docs/DURABILITY.md` mapping Python durability modes to Go persistence.
- `docs/PUBLISHING.md` consumer-safety and GitHub release guidance.

### Changed

- Documentation clarifies tutorial vs core runtime (`python学习文档/`) and dual
  language hero assets.
- `.gitignore` covers Python teaching-tree caches and local virtualenvs.

## [0.1.0] - 2026-07-22

First supported development release. It establishes the package boundaries
described in the repository documentation:

- Typed `StateGraph` runtime with BSP super-steps, Send, Command, subgraphs,
  interrupts, time travel, streaming, and Functional API.
- Checkpoint savers (memory, SQLite, PostgreSQL) plus optional Redis module.
- Prebuilt agents, providers, MCP client, OpenTelemetry callbacks.
- Optional remote HTTP/SSE, distributed PostgreSQL primitives, and Temporal
  adapter modules.

Compatibility targets LangGraph `1.2.9` behavioral reference
(`95af6a00718588e7b7ce17310e8006d267896a77`). This is **not** a drop-in Python
API. See [COMPATIBILITY.md](COMPATIBILITY.md) and
[docs/DURABILITY.md](docs/DURABILITY.md).

### Included highlights (0.1.0 surface)

- High-level `AgentRunner` for ChatModelAgent, DeepAgent, Router, Supervisor,
  and Handoff harnesses.
- Canonical examples for core graph, persistence, streaming, interrupt,
  time-travel, subgraph, and functional workflows.
- Native SSE model streaming for OpenAI-compatible, Anthropic, Gemini, and
  Vertex AI adapters.
- Retrieval (BM25/hybrid), memory middleware, release multi-module tooling.

[Unreleased]: https://github.com/ybszm/langgraph-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ybszm/langgraph-go/releases/tag/v0.1.0
