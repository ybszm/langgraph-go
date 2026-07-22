# Documentation

| Guide | Use it when |
|---|---|
| [Agent harnesses](agents.md) | Choosing ChatModelAgent, DeepAgent, Router, Supervisor, Handoff, or fallback models |
| [Streaming](streaming.md) | Building token, state, progress, subagent, or run-event consumers |
| [Callbacks](callbacks.md) | Adding graph, node, model, tool, and delegation observability |
| [Examples](../examples/README.md) | Finding a credential-free executable workflow |
| [Architecture](../ARCHITECTURE.md) | Understanding graph execution and package boundaries |
| [Durability](DURABILITY.md) | Mapping Python durability modes to Go persistence |
| [Compatibility](../COMPATIBILITY.md) | Checking behavior against upstream LangGraph |
| [Remote server](../remote/README.md) | HTTP/SSE, durable streams, reconnect |
| [Publishing](PUBLISHING.md) | Safe GitHub release / consumer compatibility |

All examples compile as part of `go test ./...`; none require provider
credentials or external services.
