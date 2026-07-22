# Compatibility with LangGraph 1.2.9

The behavioral reference is `langchain-ai/langgraph` tag `1.2.9`, commit
`95af6a00718588e7b7ce17310e8006d267896a77`.

Status meanings:

- **Supported**: implemented and covered by Go behavior tests.
- **Partial**: useful coverage exists, but known upstream behavior is missing.
- **Extension**: Go-specific production capability without a direct core-Python equivalent.

No row below should be interpreted as a drop-in Python API guarantee.

| Area | Status | Notes |
|---|---|---|
| Typed StateGraph | Partial | Typed nodes, reducers, schemas, static/conditional/waiting edges, Send, Command, compilation and inspection. Default compile still requires every node (and END) reachable from START—stricter than Python—but `WithAllowUnreachableNodes` / `WithAllowUnreachableEND` can relax that. Full Python **deferred node** scheduling is not implemented yet. |
| Pregel runtime | Partial | Deterministic BSP execution, concurrency, retries, caching, timeouts, pending-write recovery. No public low-level `Pregel`/`NodeBuilder` facade or configurable `sync`/`async`/`exit` durability. |
| Channels | Supported | Last/Any/Ephemeral/Untracked values, aggregate, topic, named barrier, delta, checkpoint versions, and freshness scheduling. |
| Streaming | Partial | Values, updates, messages, custom, debug, interrupts, subgraphs, backpressure, run-event v3, and native SSE provider chunks. Python stream-transformer and graph-UI APIs are absent. |
| Checkpoints | Supported | Memory, native SQLite/PostgreSQL, optional Redis, history, pending writes, replay, forks, encryption, and typed codecs. Python physical adapters require isolated schemas/databases. |
| Interrupts and time travel | Supported | Static and dynamic interrupts, multiple/parallel resumes, nested subgraphs, exact history forks, and durable task replay. |
| Subgraphs | Supported | Typed adapters, inherited persistence/runtime services, stable namespaces, retained state, nested inspection, resume, and forks. |
| Functional API | Supported | Typed tasks/futures/entrypoints, previous/final, retry/cache, recovery, interrupts, streams, timeouts, and graph nesting. |
| Store and memory | Supported | Memory/SQLite/PostgreSQL stores, optional Redis store/cache, structured filters, TTL, embeddings, vector search, BM25/hybrid retrieval, retriever tools, context windows, summaries, RAG injection, and durable vector outbox. |
| Prebuilt tools and agents | Partial | Messages, reducer semantics, ToolNode, ToolRuntime, hooks, structured responses, middleware, ReAct-style routing, QuickAgent, and tool-guard policies. Several Python convenience/legacy exports are absent. |
| Remote graph | Partial | Go HTTP/SSE invoke, batch, state, run control, reconnect, durable streams (requires `EventLog`; `DevelopmentServerOptions` for local demos), auth, and tracing. Only a subset of the Python SDK resource protocol is implemented. |
| Distributed PostgreSQL | Extension | Leased queues, fencing, event log, interrupt store, atomic result/checkpoint outboxes, recovery, process-local Metrics, and checkpoint retention helpers. |
| Temporal | Extension | Provider-neutral adapter plus optional official SDK binding for workflows, activities, controls, child workflows, Continue-As-New, and Worker Deployments. |

## Important differences

- Go uses explicit generic state/update types rather than Python annotation-driven
  per-field channels.
- Dynamic routing and interrupt behavior must be declared; Go does not inspect
  function bodies.
- Go currently requires every declared node and `END` to be reachable at compile
  time, which is stricter than the Python reference.
- Default error classification for retries is Go-specific.
- Hosted assistants, cron jobs, deployment administration, and the complete
  LangGraph Platform API are outside the current scope.

Compatibility bugs and missing behaviors are welcome as focused issues with a
minimal Python 1.2.9 reproduction and the expected Go behavior.
