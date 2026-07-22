# Remote server

The remote module contains the typed HTTP/SSE server and client, durable run
control endpoints, authentication hooks, request trace sinks, and a minimal
development trace viewer.

```go
traces := remote.NewMemoryTraceStore(512)
// DevelopmentServerOptions enables an in-memory EventLog so
// GET /v1/threads/{thread}/runs/{run}/stream works locally.
opts, err := remote.DevelopmentServerOptions()
if err != nil {
    panic(err)
}
opts.TraceSink = traces
server, err := remote.NewServer(invoker, opts)

// Use the same value in graph.RunConfig.Callbacks to include graph/node spans.
mux.Handle("/api/", server)
mux.Handle("/debug/traces/", http.StripPrefix("/debug/traces", remote.NewTraceUI(traces)))
```

## Durable run streams and reconnect

- Server: set `ServerOptions.EventLog` (memory or Postgres). Without it, durable
  run stream endpoints return `501` with a clear protocol error.
- Client: `StreamRun` tails one connection; `StreamRunWithOptions` reconnects on
  protocol drops using `Last-Event-ID` / the last delivered event ID.
- Production: pair `EventLog` with a shared `ControlStore` and do not use the
  in-memory development helpers.

The trace UI is not mounted automatically. Put it behind authentication outside
local development; trace names, identifiers, and errors may contain operational
information. For production export, use the separate `observability/otel`
callback module with an OpenTelemetry Collector.
