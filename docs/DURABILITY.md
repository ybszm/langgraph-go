# Durability semantics

LangGraph Python exposes run-level durability modes such as `sync`, `async`, and
`exit`. LangGraph Go mirrors those names through `RunConfig.Durability` and
implements all three. This document maps the Go behavior without implying a
drop-in Python API.

## What is durable today

When a graph is compiled with `graph.WithPersistence(...)`:

1. Each completed super-step writes a checkpoint head, channel values, and any
   pending task writes through `checkpoint.Saver`.
2. Static and dynamic interrupts are only accepted when persistence is
   configured; resumes validate and persist before replaying interrupted tasks.
3. Functional entrypoints with task recovery rewrite completed side effects from
   durable pending writes.
4. Remote servers may expose durable run streams when `ServerOptions.EventLog`
   is set; clients reconnect with `StreamRunWithOptions` using `Last-Event-ID`.

Without persistence, runs are process-local: cancellation and process exit drop
in-flight state.

## Mapping from Python durability modes

`graph.RunConfig.Durability` accepts:

| Value | Behavior |
|---|---|
| `""` / `DurabilityUnspecified` | Same as Sync when a checkpointer is configured |
| `DurabilitySync` (`"sync"`) | Supported: commit at super-step boundaries |
| `DurabilityAsync` (`"async"`) | Supported: ordered checkpoint IO overlaps subsequent node execution and flushes before return |
| `DurabilityExit` (`"exit"`) | Supported: publish only the final checkpoint per active namespace; interrupts and failed super-steps flush recovery boundaries |

| Python concept | Closest Go behavior |
|---|---|
| `sync` | `DurabilitySync` / default with `WithPersistence` |
| `async` | `DurabilityAsync`; one run-scoped writer preserves Put/PutWrites order |
| `exit` | `DurabilityExit`; intermediate checkpoints stay run-local |

## Practical guidance

- Local demos: memory saver is enough.
- Single-process production: SQLite or PostgreSQL savers with codecs and
  optional encryption.
- Multi-worker production: `backend/distributed` leased queues + event log +
  checkpoint/result outboxes, or the Temporal adapter.
- Long-running threads: apply checkpoint retention
  (`checkpoint/retention`) so history does not grow without bound.

## Implementation notes

`RunConfig.Durability` exposes the upstream names. Unknown modes fail before the
saver is read or written. Exit uses a run-scoped buffering saver and publishes
only the latest boundary in each active namespace, flushing nested namespaces
before their parent. Async uses a run-scoped single-writer queue:

1. Checkpoint drafts remain readable in memory while the writer catches up.
2. Checkpoint and pending-write commands are serialized in their original order.
3. Node execution may overlap the prior commit; run return and lock release wait
   for a complete flush.
4. A background persistence error cancels execution and is returned to the caller.
5. Parent IDs, channel versions, task IDs, and pending-write ordering remain stable.
6. Draft reads and history queries are isolated by thread and checkpoint namespace.

Async improves latency hiding, not crash guarantees: Sync remains the strongest
choice when every super-step must be confirmed durable before the next begins.
