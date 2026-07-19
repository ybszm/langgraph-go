# Remote server

The remote module contains the typed HTTP/SSE server and client, durable run
control endpoints, authentication hooks, request trace sinks, and a minimal
development trace viewer.

```go
traces := remote.NewMemoryTraceStore(512)
server, err := remote.NewServer(invoker, remote.ServerOptions{
    TraceSink: traces,
})

// Use the same value in graph.RunConfig.Callbacks to include graph/node spans.
mux.Handle("/api/", server)
mux.Handle("/debug/traces/", http.StripPrefix("/debug/traces", remote.NewTraceUI(traces)))
```

The trace UI is not mounted automatically. Put it behind authentication outside
local development; trace names, identifiers, and errors may contain operational
information. For production export, use the separate `observability/otel`
callback module with an OpenTelemetry Collector.
