<div align="center">

<img src="docs/assets/langgraph-go-hero-v2.png" alt="LangGraph Go execution, checkpoints, and retrieval graph" width="100%" />

# LangGraph Go

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md) | [한국어](README.ko.md)

**A typed, durable graph runtime for Go, inspired by LangGraph.**

[![CI](https://github.com/ybszm/langgraph-go/actions/workflows/ci.yml/badge.svg)](https://github.com/ybszm/langgraph-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ybszm/langgraph-go.svg)](https://pkg.go.dev/github.com/ybszm/langgraph-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/ybszm/langgraph-go)](https://goreportcard.com/report/github.com/ybszm/langgraph-go)
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
> [Compatibility](COMPATIBILITY.md) and [Durability](docs/DURABILITY.md) before
> adopting it as a drop-in replacement.

## Repository layout notes

- **Core Go runtime**: packages at the repo root (`graph`, `checkpoint`,
  `prebuilt`, …) and optional modules listed in `go.work`.
- **`python学习文档/`**: a **standalone teaching project** (Python MiniGraph + a
  small Go runtime rewrite). It is not imported by the published modules and is
  not a substitute for the main library API. Start with the root README examples
  or `examples/` for production-oriented usage.

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
go get github.com/ybszm/langgraph-go@v0.1.0
```

Go 1.25 or newer is required. See [versioning](docs/VERSIONING.md) for pre-1.0
compatibility promises.

## Quick start (agent)

Most applications should start with **QuickAgent**, not a hand-built graph:

```go
package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type echo struct{}

func (echo) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{Content: "hello from quick-agent"}, nil
}

func main() {
	agent, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{
		Model:        echo{},
		SystemPrompt: "Be helpful.",
	})
	if err != nil {
		panic(err)
	}
	state, err := agent.Run(context.Background(), "hi", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(state.FinalResponse())
}
```

Runnable demos:

| Example | Concept |
|---|---|
| [`examples/quick-agent`](examples/quick-agent) | Smallest agent entry |
| [`examples/multi-agent`](examples/multi-agent) | Supervisor + parallel sub-agents |
| [`examples/basic`](examples/basic) | Typed StateGraph without LLM |

Multi-agent, handoff, and deep-agent patterns: [docs/agents.md](docs/agents.md).
Interop with langchaingo: [docs/LANGCHAINGO.md](docs/LANGCHAINGO.md).
How we compare to other Go stacks: [docs/COMPARISON.md](docs/COMPARISON.md).

## Packages

| Package | Purpose |
|---|---|
| `graph` | Typed graph builder, compiler, runtime, streaming, interrupts, and state inspection |
| `channel` | Pregel-style typed channel primitives |
| `checkpoint/*` | Memory, SQLite, and PostgreSQL checkpoint savers and codecs |
| `functional` | Durable tasks, futures, and typed entrypoints |
| `prebuilt` | Messages, ToolNode, `ChatModelAgent`, `DeepAgent`, multi-agent coordination, and ReAct components |
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

## Agent harnesses

`prebuilt.NewChatModelAgent` is the shortest path from a provider-neutral chat
model to a runnable agent: it supplies checkpoint-safe message state, prompt
injection, ReAct tool execution, and a text-oriented `Run` method.

`prebuilt.NewAgentRunner` is the application-facing execution facade shared by
ChatModelAgent, DeepAgent, Router, Supervisor, and Handoff harnesses. Its
`Query` method emits high-level message, custom, interrupt, done, and error
events without requiring applications to understand graph stream modes;
`Resume` continues durable human-in-the-loop sessions through the same event
contract.

For complex work, `prebuilt.NewDeepAgent` adds a `write_todos` planning tool and
a `task` delegation tool. It creates a general-purpose isolated worker by
default, accepts specialized `SubAgent` values, and executes multiple task calls
concurrently through ToolNode. `NewMultiAgentCoordinator` exposes the same
supervisor pattern without the planning harness. In both cases the supervisor
receives only each worker's final response, keeping intermediate context out of
the main conversation.

`NewRouterAgent` provides a selective one-pass fan-out/synthesis pattern, while
`NewHandoffAgent` persists the active persona as models transfer direct control.
`FallbackChatModel` composes ordered model/provider failover. Agent-level
streaming forwards subagent chunks with worker metadata, and agent callbacks
cover model, individual tool, and delegation boundaries.

Start with the credential-free [`examples/agent-runner`](examples/agent-runner),
then see [`examples/multi-agent`](examples/multi-agent) for the complete
ChatModelAgent + DeepAgent + parallel coordination flow.
The [documentation index](docs/README.md) links dedicated agent, streaming, and
callback guides.

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

Eighteen credential-free, executable workflows are indexed in
[`examples/README.md`](examples/README.md). Release coordination and the
pre-1.0 compatibility policy are documented in [`RELEASING.md`](RELEASING.md).

## Project status and support

This repository is experimental and currently maintained on a best-effort
basis. Please use GitHub Discussions for design questions and Issues for
reproducible defects or compatibility gaps.

## License

LangGraph Go is available under the [MIT License](LICENSE). Upstream attribution
and dependency notices are listed in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
