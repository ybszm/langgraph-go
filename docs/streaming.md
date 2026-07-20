# Streaming

Graph streams and agent streams use the same typed `graph.StreamEvent[S, D]`
envelope. `Mode` determines which payload is populated.

| Mode | Payload | Typical use |
|---|---|---|
| `values` | `State` | Render a complete snapshot after each super-step |
| `updates` | `Updates` | Apply incremental node deltas |
| `messages` | `Message` | Display model chunks and completed messages |
| `custom` | `Custom` | Show application progress emitted with `Runtime.WriteCustom` |
| `debug` | `Debug` | Inspect task and checkpoint scheduler boundaries |
| `interrupt` | `Interrupts` | Present human-in-the-loop requests |
| `done` | `State` | Read the successful final state |
| `error` | `Err` | Handle terminal failure |

## Agent stream

```go
events := agent.Stream(ctx, "Explain checkpoints", graph.RunConfig{}, graph.StreamOptions{
    Modes: []graph.StreamMode{
        graph.StreamMessages,
        graph.StreamCustom,
        graph.StreamUpdates,
        graph.StreamDone,
    },
})
for event := range events {
    if event.Mode == graph.StreamError {
        return event.Err
    }
}
```

Use `StreamState` for an explicit `AgentState`. `StreamEvents` exposes the v3
run tree (`on_chain_start`, node boundaries, message chunks, custom data, and
terminal events) for tracing integrations.

## Subagent streams

DeepAgent, RouterAgent, and MultiAgentCoordinator stream child
`messages`/content-block events through the parent stream. Forwarded message
metadata includes `langgraph_subagent`, so UIs can group tokens by worker.
Custom child data is wrapped in `prebuilt.SubAgentStreamEvent` with the worker
name and child namespace.

Independent child tasks can produce interleaved chunks. The final ToolMessages
remain deterministic in the original model call order.

Consumers that stop early must cancel the context so the graph and any active
subagents release their goroutines.

See [`examples/agent-streaming`](../examples/agent-streaming) and the lower-level
[`examples/streaming`](../examples/streaming).
