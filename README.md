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

LangGraph Go is a low-level runtime for long-running, stateful workflows and
agents. You define typed state transitions; the runtime schedules them as a
graph, reduces concurrent updates deterministically, and can persist execution
at super-step boundaries.

The design is inspired by
[`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph), Pregel,
and durable workflow systems, but the API is intentionally Go-native: generics,
explicit interfaces, `context.Context`, and errors.

> [!WARNING]
> **v0.1.0 is experimental.** The core is being stabilized for production use,
> but public APIs and checkpoint schemas may still change before v1.0. Pin the
> module version and read [Versioning](docs/VERSIONING.md),
> [Compatibility](COMPATIBILITY.md), and [Durability](docs/DURABILITY.md).

## Why use it?

Use LangGraph Go when an operation is not just one request/response:

- **Durable execution**: checkpoint state and resume after interruption or
  failure.
- **Human-in-the-loop**: pause a task, inspect state, and continue with an
  explicit answer.
- **Deterministic concurrency**: run independent nodes together, then reduce
  their typed updates in a stable order.
- **Time travel**: inspect history, replay from a checkpoint, or fork an older
  state without mutating it.
- **Streaming and observability**: observe values, updates, messages, custom
  events, interrupts, and node lifecycle callbacks.

If you only need a short stateless tool loop, a graph runtime is probably more
complex than necessary.

## Install

```bash
go get github.com/ybszm/langgraph-go@v0.1.0
```

Go 1.25 or newer is required. See [versioning](docs/VERSIONING.md) for pre-1.0
compatibility promises.

## Core mental model

| Concept | Meaning |
|---|---|
| `State` (`S`) | The complete typed snapshot visible to nodes |
| `Delta` (`D`) | A typed update returned by a node |
| Reducer | The only function allowed to merge deltas into state |
| Node | A unit of work: `State -> Command[Delta]` |
| Edge | Declares which node may run next |
| Super-step | One deterministic scheduling boundary; ready nodes may run concurrently |
| Thread | One durable execution history, selected by `RunConfig.ThreadID` |
| Checkpoint | A persisted state and task snapshot at a super-step boundary |

Nodes do not mutate shared state. They return deltas, and the reducer applies
those deltas after the super-step. This separation is the basis for replay,
parallel execution, and recovery.

## Tutorial 1: build a typed graph

The smallest useful graph increments a counter:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ybszm/langgraph-go/graph"
)

type State struct {
	Count int
}

type Delta struct {
	Add int
}

func main() {
	builder := graph.NewStateGraph(func(
		_ context.Context,
		state State,
		updates []Delta,
	) (State, error) {
		for _, update := range updates {
			state.Count += update.Add
		}
		return state, nil
	})

	err := builder.AddNode("increment", func(
		_ context.Context,
		_ State,
		_ graph.Runtime,
	) (graph.Command[Delta], error) {
		return graph.Update(Delta{Add: 1}), nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "increment"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("increment", graph.END); err != nil {
		log.Fatal(err)
	}

	compiled, err := builder.Compile()
	if err != nil {
		log.Fatal(err)
	}
	result, err := compiled.Invoke(
		context.Background(),
		State{},
		graph.RunConfig{},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Count) // 1
}
```

Execution is:

```text
START -> increment -> END
            |
            +-- returns Delta{Add: 1}
                       |
Reducer(State{}, [Delta{Add: 1}]) -> State{Count: 1}
```

Run the complete example:

```bash
go run ./examples/basic
```

## Tutorial 2: make execution durable

A graph becomes durable when it is compiled with a `checkpoint.Saver` and
invoked with a stable thread ID:

```go
import (
	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
)

compiled, err := builder.Compile(graph.WithPersistence(
	graph.PersistenceConfig[State, Delta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[State]("example.state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[Delta]("example.delta", 1),
	},
))

config := graph.RunConfig{
	ThreadID:  "counter-demo",
	Durability: graph.DurabilitySync,
}
result, err := compiled.Invoke(context.Background(), State{}, config)
```

The codec name and version are part of your persisted data contract. Do not
silently reuse a codec name for an incompatible Go type. The memory saver is
for tests and examples; use SQLite for local durable workflows and evaluate
PostgreSQL or Redis for deployed systems.

See [`examples/checkpoint-resume`](examples/checkpoint-resume) for history
inspection and [`examples/interrupt-resume`](examples/interrupt-resume) for a
complete pause/resume cycle.

## How recovery works

For every durable thread:

1. The runtime loads the latest checkpoint.
2. Ready nodes execute for one super-step.
3. Successful task writes are recorded before the full step commits.
4. The reducer creates the next state.
5. The saver commits the next checkpoint and scheduled tasks.

If one parallel node fails after a peer succeeds, the successful pending write
can be reused during recovery instead of running that peer again. Side effects
inside a node still require application-level idempotency.

Durability modes trade latency for crash exposure:

| Mode | Behavior |
|---|---|
| `sync` | Confirm each checkpoint before the next super-step |
| `async` | Preserve write order while overlapping checkpoint I/O with execution |
| `exit` | Keep intermediate checkpoints in memory and publish final/recovery boundaries |

Start with `sync` until measurements justify another mode.

## Learning path

| Step | Read or run | What to learn |
|---|---|---|
| 1 | [`examples/basic`](examples/basic) | State, Delta, reducer, node, edge |
| 2 | [`examples/conditional-routing`](examples/conditional-routing) and [`examples/fanout`](examples/fanout) | Routing and deterministic parallel reduction |
| 3 | [`examples/checkpoint-resume`](examples/checkpoint-resume) | Threads, codecs, checkpoints, history |
| 4 | [`examples/interrupt-resume`](examples/interrupt-resume) | Human-in-the-loop and durable resume |
| 5 | [`examples/time-travel`](examples/time-travel) | Replay and immutable forks |
| 6 | [`examples/streaming`](examples/streaming) | Values, updates, and terminal events |
| 7 | [`examples/subgraph`](examples/subgraph) | Typed composition and nested persistence |
| 8 | [Architecture](ARCHITECTURE.md), [Durability](docs/DURABILITY.md), and [Versioning](docs/VERSIONING.md) | Production and compatibility boundaries |

## Core and optional modules

The project deliberately keeps model and infrastructure integrations outside
the core module.

| Scope | Packages | Status |
|---|---|---|
| Core runtime | `graph`, `channel`, `checkpoint/*` | **Stabilizing** |
| Core support | `functional`, `cache`, `store/*` | **Experimental** |
| Agent conveniences | `prebuilt`, `memory`, `retrieval` | **Experimental** |
| Integrations | `providers`, `mcpclient`, `remote`, `redis`, `observability/otel`, `backend/temporal` | **Optional / Experimental** |

Start with the core runtime. Add an integration only when the application needs
it. Agent-oriented examples and guides remain available through
[docs/agents.md](docs/agents.md) and [examples/README.md](examples/README.md).

## Before production

- Pin an exact module version.
- Use explicit, versioned codecs for every persisted type.
- Keep state small; store large documents or artifacts outside checkpoints.
- Make node side effects idempotent.
- Set node timeouts, retry predicates, and concurrency limits deliberately.
- Test recovery against the same saver used in deployment.
- Plan checkpoint retention, database backup, and schema migration.
- Propagate `run_id`, `thread_id`, and checkpoint coordinates into logs/traces.

The project does not yet claim a stable v1 API or universal production
readiness. See the [documentation index](docs/README.md) and open an issue for
any recovery or compatibility behavior that cannot be reproduced from tests.

## Development

```bash
go test ./...
go vet ./...
```

This repository is a Go workspace. Run the same commands from `providers`,
`mcpclient`, `remote`, `redis`, `observability/otel`, and `backend/temporal`
when changing an optional module. Go 1.25 or newer is required across the
workspace.

Database integration tests are enabled with `LANGGRAPH_POSTGRES_DSN`. Temporal
integration is enabled with `LANGGRAPH_TEMPORAL_ADDRESS`. The CI workflow runs
unit, race, PostgreSQL, Redis, and Temporal checks on Linux.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the contribution workflow and
[`ARCHITECTURE.md`](ARCHITECTURE.md) for design details.

Credential-free executable workflows are indexed in
[`examples/README.md`](examples/README.md). Release coordination and the
pre-1.0 compatibility policy are documented in [`RELEASING.md`](RELEASING.md).

## Project status and support

This repository is experimental and currently maintained on a best-effort
basis. Please use GitHub Discussions for design questions and Issues for
reproducible defects or compatibility gaps.

## License

LangGraph Go is available under the [MIT License](LICENSE). Upstream attribution
and dependency notices are listed in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
