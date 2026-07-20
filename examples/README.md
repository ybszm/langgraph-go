# Example catalog

Every directory below is a standalone program compiled by `go test ./...`.
The examples use deterministic local models and in-memory persistence so CI
does not require credentials or external services.

## Agent harnesses

| Example | Concept |
|---|---|
| `chat-model-agent` | Minimal model + tool agent with default state |
| `deep-agent` | Structured planning and automatic general-purpose delegation |
| `multi-agent` | Specialized workers and parallel supervisor coordination |
| `router-agent` | Selective parallel routes followed by synthesis |
| `handoff-agent` | Stateful persona transfer in one conversation |
| `model-fallback` | Ordered provider/model failover |
| `agent-streaming` | Agent-level messages, custom, and terminal streams |
| `agent-callbacks` | Model and tool lifecycle observation |

## Graph runtime

| Example | Concept |
|---|---|
| `basic` | Typed state, delta, reducer, node, and edge |
| `conditional-routing` | Conditional loop over reduced state |
| `fanout` | Concurrent fan-out with deterministic reduction |
| `streaming` | Values, updates, custom, and terminal events |
| `subgraph` | Typed parent/child graph composition |
| `functional-api` | Concurrent durable-task facade |

## Durable and context-aware workflows

| Example | Concept |
|---|---|
| `checkpoint-resume` | Durable checkpoints and state history |
| `interrupt-resume` | Human-in-the-loop pause and resume |
| `time-travel` | Historical checkpoint fork and manual update |
| `retrieval-memory` | BM25 retrieval, RAG context injection, and sliding-window memory |

Run one example from the repository root:

```bash
go run ./examples/conditional-routing
```

Start with `chat-model-agent`, then choose `deep-agent`, `router-agent`, or
`handoff-agent` according to the coordination semantics your application needs.
