<div align="center">

<img src="docs/assets/langgraph-go-hero-v2.png" alt="LangGraph Go execution, checkpoints, and retrieval graph" width="100%" />

# LangGraph Go

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md) | [한국어](README.ko.md)

**A typed, durable graph runtime for Go, inspired by LangGraph.**

[![CI](https://github.com/wahanbo/langgraph-go/actions/workflows/ci.yml/badge.svg)](https://github.com/wahanbo/langgraph-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/wahanbo/langgraph-go.svg)](https://pkg.go.dev/github.com/wahanbo/langgraph-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/wahanbo/langgraph-go)](https://goreportcard.com/report/github.com/wahanbo/langgraph-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

LangGraph Go is an independent, community-maintained Go implementation of the
core graph execution ideas popularized by
[`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph). It is
designed around Go generics, explicit interfaces, deterministic concurrent
execution, and durable state.

The behavioral reference for compatibility work is LangGraph `1.2.9` at commit
`95af6a00718588e7b7ce17310e8006d267896a77`. This project is not an official
LangChain product and is not a source-to-source port of the Python package.

> [!IMPORTANT]
> The project is under active development. Core workflows are usable and
> extensively tested, but the complete Python API is not yet reproduced. See
> [Compatibility](COMPATIBILITY.md) before adopting it as a drop-in replacement.

## Highlights

- Generic `StateGraph[S, D]` API with typed state and updates
- BSP-style super-steps with deterministic reduction
- Static, conditional, waiting, and dynamic `Send` edges
- `Command` updates, routing, parent targeting, and durable resume
- Concurrent nodes with limits, retries, caching, timeouts, and cancellation
- Checkpointing with memory, SQLite, PostgreSQL, and optional Redis implementations
- Interrupt/resume, replay, branching, time travel, and nested subgraphs
- Values, updates, messages, custom, debug, and subgraph streaming, including native provider chunks
- Typed Functional API with tasks, futures, persistence, and recovery
- Provider-neutral ToolNode and ReAct-style agent building blocks
- Ten optional model adapters, MCP tools, and agent-as-tool composition
- OpenTelemetry callbacks and a dependency-free local trace viewer
- Long-term stores, TTL, semantic indexing, vector backends, BM25/hybrid retrieval, and context memory middleware
- Optional HTTP/SSE, Redis, distributed PostgreSQL, and Temporal integrations

## Install

```bash
go get github.com/wahanbo/langgraph-go@latest
```

Go 1.25 or newer is required.

## Quick start

```go
package main

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
)

type State struct {
	Count int
}

type Delta struct {
	Increment int
}

func main() {
	builder := graph.NewStateGraph(func(
		_ context.Context,
		state State,
		updates []Delta,
	) (State, error) {
		for _, update := range updates {
			state.Count += update.Increment
		}
		return state, nil
	})

	if err := builder.AddNode("increment", func(
		_ context.Context,
		_ State,
		_ graph.Runtime,
	) (graph.Command[Delta], error) {
		return graph.Update(Delta{Increment: 1}), nil
	}); err != nil {
		panic(err)
	}
	if err := builder.AddEdge(graph.START, "increment"); err != nil {
		panic(err)
	}
	if err := builder.AddEdge("increment", graph.END); err != nil {
		panic(err)
	}

	compiled, err := builder.Compile()
	if err != nil {
		panic(err)
	}

	result, err := compiled.Invoke(context.Background(), State{}, graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Count) // 1
}
```

The runnable example is available at [`examples/basic`](examples/basic).

## Packages

| Package | Purpose |
|---|---|
| `graph` | Typed graph builder, compiler, runtime, streaming, interrupts, and state inspection |
| `channel` | Pregel-style typed channel primitives |
| `checkpoint/*` | Memory, SQLite, and PostgreSQL checkpoint savers and codecs |
| `functional` | Durable tasks, futures, and typed entrypoints |
| `prebuilt` | Messages, ToolNode, `NewAgent`, ReAct, and agent-as-tool components |
| `retrieval` | Text splitting, BM25/vector/hybrid retrieval, ingestion, and retriever-as-tool |
| `memory` | Sliding-window, summarization, and retrieval-context model middleware |
| `store/*` | Long-term key/value, TTL, embedding, and vector stores |
| `redis/*` | Optional Redis checkpoint, long-term store, and task cache module |
| `providers/*` | Ten optional model-provider adapters (separate module) |
| `mcpclient` | Official-SDK MCP tool client (separate module) |
| `remote` | Typed HTTP/SSE server, client, and local trace UI (separate module) |
| `observability/otel` | OpenTelemetry graph callbacks (separate module) |
| `backend/distributed` | Leased queues, event logs, interrupts, and transactional outboxes |
| `backend/temporal` | Temporal adapter and official SDK binding (separate module) |

## Retrieval and context memory

The lightweight `retrieval` package can be used as a ReAct tool or as model
middleware. `memory.NewWindow` and `memory.NewSummary` project bounded context
without deleting durable graph history; `memory.NewRetrieval` injects ranked
documents only for the current model call. BM25 works without credentials, and
vector retrieval composes with the existing `store.Embedder` and
`store.VectorIndex` contracts.

See the credential-free [`examples/retrieval-memory`](examples/retrieval-memory)
program for an end-to-end composition.

## Compatibility boundaries

LangGraph Go aims for behavioral compatibility where the concepts translate
cleanly to Go. It intentionally uses Go APIs rather than mirroring Python syntax.
Known gaps include:

- low-level Python `Pregel` / `NodeBuilder` construction
- `defer=True` nodes and per-run `sync` / `async` / `exit` durability modes
- the Python v3 stream-transformer and graph-UI helper APIs
- several convenience and legacy Prebuilt exports
- full hosted LangGraph Platform and Python SDK protocol coverage

The detailed, evidence-based status is maintained in
[`COMPATIBILITY.md`](COMPATIBILITY.md).

## Development

```bash
go test ./...
go vet ./...
```

This repository is a Go workspace. Run the same commands from `providers`,
`mcpclient`, `remote`, `observability/otel`, and `backend/temporal` when changing
an optional module. Go 1.25 or newer is required across the workspace.

Database integration tests are enabled with `LANGGRAPH_POSTGRES_DSN`. Temporal
integration is enabled with `LANGGRAPH_TEMPORAL_ADDRESS`. The CI workflow runs
unit, race, PostgreSQL, Redis, and Temporal checks on Linux.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the contribution workflow and
[`ARCHITECTURE.md`](ARCHITECTURE.md) for design details.

Eleven credential-free, executable workflows are indexed in
[`examples/README.md`](examples/README.md). Release coordination and the
pre-1.0 compatibility policy are documented in [`RELEASING.md`](RELEASING.md).

## Project status and support

This repository is experimental and currently maintained on a best-effort
basis. Please use GitHub Discussions for design questions and Issues for
reproducible defects or compatibility gaps.

## License

LangGraph Go is available under the [MIT License](LICENSE). Upstream attribution
and dependency notices are listed in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
