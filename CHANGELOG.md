# Changelog

All notable changes to LangGraph Go are documented in this file. The project
uses Semantic Versioning while it remains pre-1.0: minor releases may introduce
intentional API changes, and patch releases preserve public API compatibility.

## [Unreleased]

### Added

- Canonical, executable examples for core graph, persistence, streaming,
  interrupt, time-travel, subgraph, and functional workflows.
- Optional Redis module implementing checkpoint, store, and task-cache
  contracts.
- Native SSE model streaming for OpenAI-compatible, Anthropic, Gemini, and
  Vertex AI adapters, including normalized tool-call and usage chunks.
- Lightweight retrieval with Unicode text splitting, BM25, vector and hybrid
  indexes, ingestion, retriever tools, and RAG context middleware.
- Sliding-window and summarization model middleware that preserve durable graph
  history and complete assistant/tool exchanges.
- Release checklist and coordinated multi-module tag tooling.

### Changed

- Documentation consistently requires Go 1.25.
- Provider examples use the generic constructors exposed by the provider
  module.

## [0.1.0] - Unreleased

The first supported development release. It establishes the typed graph,
checkpoint, functional, prebuilt-agent, remote, provider, observability, MCP,
distributed PostgreSQL, and Temporal package boundaries described in the
repository documentation.

[Unreleased]: https://github.com/wahanbo/langgraph-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/wahanbo/langgraph-go/releases/tag/v0.1.0
