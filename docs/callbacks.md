# Callbacks

Callbacks are synchronous observers. Implementations shared by concurrent
graphs, tools, or subagents must be concurrency-safe and should return quickly.

## Graph callbacks

Add `graph.GraphCallbackFuncs` to `graph.RunConfig.Callbacks` to observe:

- graph start, end, and terminal error;
- node attempt start, end, error, and retry attempts;
- durable interrupt and resume boundaries.

Graph callbacks carry run IDs, parent IDs, tags, metadata, thread/checkpoint
coordinates, and typed input/output values.

## Agent callbacks

Add `prebuilt.AgentCallbackFuncs` to an Agent config's `Callbacks` field to
observe:

- model start, end, and error;
- individual tool start, end, and error;
- supervisor delegation start, end, and error.

```go
callback := prebuilt.AgentCallbackFuncs{
    ModelStart: func(ctx context.Context, event prebuilt.AgentModelStartEvent) {
        log.Printf("model agent=%s step=%d", event.Agent, event.Runtime.Step)
    },
    ToolEnd: func(ctx context.Context, event prebuilt.AgentToolEndEvent) {
        log.Printf("tool agent=%s name=%s", event.Agent, event.Call.Name)
    },
}

agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{
    Model: model,
    Tools: tools,
    Callbacks: []prebuilt.AgentCallback{callback},
})
```

Agent callbacks are construction-scoped, while graph callbacks are run-scoped.
Use both when a trace needs orchestration semantics and scheduler details.

See [`examples/agent-callbacks`](../examples/agent-callbacks).
