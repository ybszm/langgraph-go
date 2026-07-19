# OpenTelemetry callbacks

This optional module exports graph, subgraph, and node attempts as standard
OpenTelemetry spans. Interrupt and resume transitions are span events. Provide
the callback through `graph.RunConfig.Callbacks`:

```go
callback := langotel.New(otel.GetTracerProvider())
result, err := compiled.Invoke(ctx, input, graph.RunConfig{
    Callbacks: []graph.GraphCallback{callback},
})
```

Exporter, sampler, resource, and propagation setup stay under application
control. The module does not install or replace the global tracer provider.
