# Canonical examples

Every directory below is a standalone program compiled by `go test ./...`.
The examples use deterministic local models and in-memory persistence so CI
does not require credentials or external services.

| Example | Concept |
|---|---|
| `basic` | Typed state, delta, reducer, node, and edge |
| `minimal-agent` | Provider-neutral ReAct agent and tool |
| `conditional-routing` | Conditional loop over reduced state |
| `fanout` | Concurrent fan-out with deterministic reduction |
| `streaming` | Values, updates, custom, and terminal events |
| `checkpoint-resume` | Durable checkpoints and state history |
| `interrupt-resume` | Human-in-the-loop pause and resume |
| `time-travel` | Historical checkpoint fork and manual update |
| `subgraph` | Typed parent/child graph composition |
| `functional-api` | Concurrent durable-task facade |
| `retrieval-memory` | BM25 retrieval, RAG context injection, and sliding-window memory |

Run one example from the repository root:

```bash
go run ./examples/conditional-routing
```
