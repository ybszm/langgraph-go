# Architecture

LangGraph Go is a typed graph runtime built for deterministic concurrency and
durable execution. It borrows behavioral concepts from LangGraph while keeping
the core independent of Python, LangChain, model-provider SDKs, and workflow
orchestrators.

## Design principles

1. State and updates are generic Go types: `StateGraph[S, D]`.
2. Nodes treat state as immutable and return serializable `Command[D]` values.
3. Concurrent work is reduced in deterministic task order.
4. Checkpoint heads, channel values, and pending writes are committed atomically.
5. Blocking and streaming operations propagate `context.Context`.
6. Provider and storage integrations live behind explicit interfaces.
7. Durable identifiers, clocks, codecs, and key resolvers are injectable.

## Runtime model

Execution follows bulk-synchronous parallel super-steps:

```text
input
  -> schedule ready tasks
  -> run one super-step concurrently
  -> order task results deterministically
  -> reduce updates
  -> persist checkpoint and pending writes
  -> route the next super-step
  -> output / interrupt / continue
```

`CompiledGraph` is immutable after compilation. A run consists of canonical
state, scheduled tasks, versioned checkpoint channels, retry/cache policy,
interrupt controls, and optional subgraph coordinates.

## Package boundaries

```text
graph ──────────────── core builder, compiler, scheduler, streams
  ├── channel ──────── typed Pregel channel primitives
  ├── checkpoint ───── saver and codec contracts
  ├── managed ──────── non-persisted runtime projections
  ├── cache ────────── task-result cache contract
  └── store ────────── long-term memory contract

functional ─────────── durable task/entrypoint facade
prebuilt ───────────── messages, tools, and agent components
retrieval ──────────── provider-neutral ingestion and document retrieval
memory ─────────────── model-context window, summary, and RAG projection
remote ─────────────── HTTP/SSE transport and control plane
backend/distributed ── leased PostgreSQL execution primitives
backend/temporal ───── optional workflow-orchestrator adapter
```

Core packages do not import model-provider, MCP, OpenTelemetry, remote-server,
Redis, or Temporal SDK dependencies. The `providers`, `mcpclient`, `remote`,
`observability/otel`, `redis`, and `backend/temporal` integrations are separate Go
modules in one development workspace, so applications pay only for modules
they import. The provider-neutral agent contracts remain in `prebuilt`.

## Graph compilation

Compilation validates node identifiers, edge endpoints, declared routing
destinations, interrupts, channel reads, typed context compatibility, persistence
requirements, and nested subgraph dependencies. The builder is copied into an
immutable execution plan so later builder mutations cannot affect active runs.

Go does not inspect function bodies to infer routing or interrupt behavior.
Dynamic destinations and interrupt-capable nodes must be declared explicitly.

## Persistence

The `checkpoint.Saver` interface stores:

- canonical graph state
- channel versions and per-node versions seen
- next tasks and waiting barriers
- parent checkpoint coordinates
- task-level pending results, errors, sends, and interrupts

Memory, SQLite, and PostgreSQL savers share a reusable conformance suite.
Serialization is separated through typed codecs; JSON, MessagePack-oriented,
Protobuf-oriented, and encrypted wrappers are available.

The native Go schema and the Python physical-compatibility schema are separate
adapters. They must use separate PostgreSQL schemas or databases because their
migration ledgers and scheduler envelopes differ.

## Interrupts and time travel

Interrupts are durable task controls identified by stable IDs. A resume command
is validated and persisted before interrupted tasks are replayed. Exact
checkpoint coordinates support history inspection, manual updates, forks, and
nested subgraph continuation without mutating earlier history.

## Streaming

The runtime exposes typed values, updates, messages, custom events, debug events,
interrupts, and terminal events. Streams use bounded channels, propagate
cancellation, and include subgraph namespaces when requested. Provider adapters
normalize native SSE text, tool-call, finish, and usage fragments into
`AssistantMessageChunk` message events before merging the final model response.

## Optional production backends

The distributed backend provides leased queues, fencing tokens, event logs,
interrupt stores, and result/checkpoint outboxes. PostgreSQL implementations use
transactions and `FOR UPDATE SKIP LOCKED` for multi-worker coordination.

The Temporal adapter maps runs to workflows, side-effecting nodes to activities,
resume operations to signals/updates, state inspection to queries, nested graphs
to child workflows, and long histories to checkpoint-verified Continue-As-New.

## Compatibility policy

Compatibility targets observable behavior, not identical language syntax.
Claims require executable tests against the pinned upstream reference. A feature
is not considered conformant merely because a similarly named type exists.

See [`COMPATIBILITY.md`](COMPATIBILITY.md) for current coverage and known gaps.
