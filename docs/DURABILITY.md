# Durability semantics

LangGraph Python exposes run-level durability modes such as `sync`, `async`, and
`exit`. LangGraph Go does not currently mirror those names as a public
`RunConfig` enum. This document maps the Go behavior so operators can reason
about crash safety without assuming a drop-in Python API.

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
| `DurabilityAsync` (`"async"`) | Returns `ErrUnsupportedDurability` |
| `DurabilityExit` (`"exit"`) | Returns `ErrUnsupportedDurability` |

| Python concept | Closest Go behavior |
|---|---|
| `sync` | `DurabilitySync` / default with `WithPersistence` |
| `async` | Not implemented (use `backend/distributed` for multi-worker async IO) |
| `exit` | Not implemented as a mode |

## Practical guidance

- Local demos: memory saver is enough.
- Single-process production: SQLite or PostgreSQL savers with codecs and
  optional encryption.
- Multi-worker production: `backend/distributed` leased queues + event log +
  checkpoint/result outboxes, or the Temporal adapter.
- Long-running threads: apply checkpoint retention
  (`checkpoint/retention`) so history does not grow without bound.

## Roadmap note

A future `graph.Durability` field on `RunConfig` may expose explicit modes. Until
then, treat “persistence enabled” as the durable path and document application
expectations in deploy guides rather than assuming Python mode names.
