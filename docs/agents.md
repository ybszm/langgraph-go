# Agent harnesses

The `prebuilt` package provides three layers over the typed graph runtime. They
share `AgentState`, remain provider-neutral, and expose the compiled graph when
streaming, interrupts, persistence, or inspection are needed.

## AgentRunner: the default application entry point

`AgentRunner` is the high-level facade for every built-in harness. It provides
one `Run` API for request/response use and one `Query` event stream for chat
applications, while hiding graph stream modes by default:

```go
agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{
    Name:         "assistant",
    SystemPrompt: "Answer with evidence.",
    Model:        model,
    Tools:        tools,
})
runner, err := prebuilt.NewAgentRunner(prebuilt.AgentRunnerConfig{
    Agent: agent,
})

for event := range runner.Query(ctx, "What changed?") {
    switch event.Kind {
    case prebuilt.AgentEventMessage:
        // Render a model/tool/subagent message or content-block chunk.
    case prebuilt.AgentEventInterrupt:
        // Collect human input, then call runner.Resume.
    case prebuilt.AgentEventDone:
        fmt.Println(event.State.FinalResponse())
    case prebuilt.AgentEventError:
        return event.Err
    }
}
```

Configure `AgentRunnerConfig.RunConfig.ThreadID` and compile the harness with a
checkpointer to use `runner.Resume`. The runner copies tags, metadata,
callbacks, stream modes, and other slices/maps at construction and invocation
boundaries. If a consumer stops reading `Query` or `Resume` early, it must
cancel the context so model, tool, and subagent writers can exit.

The default event set contains messages, custom data, interrupts, completion,
and errors. Request values/updates/debug modes explicitly when building the
runner; these arrive as `AgentEventGraph`, preserving access to the graph
runtime without making it part of the common path.

## QuickAgent

`NewQuickAgent` is the smallest facade when you only need a model, optional
tools, and a system prompt. It builds on `ChatModelAgent` without introducing a
second state type:

```go
agent, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{
    Model:        model,
    Tools:        tools,
    SystemPrompt: "Be concise.",
})
result, err := agent.Run(ctx, "What changed?", graph.RunConfig{})
```

Use tool guards when tools are untrusted or environment-sensitive:

```go
tools, err = prebuilt.GuardTools(tools, prebuilt.ToolGuardPolicy{
    Allowed:      []string{"search", "lookup"},
    RequireHuman: []string{"restart"},
})
```

## ChatModelAgent

Use `NewChatModelAgent` for a single model with optional tools:

```go
agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{
    Name:         "assistant",
    SystemPrompt: "Answer with evidence.",
    Model:        model,
    Tools:        tools,
})
result, err := agent.Run(ctx, "What changed?", graph.RunConfig{})
fmt.Println(result.FinalResponse())
```

Provider adapters should use `prebuilt.AgentModelMessages` as their message
reader. `agent.Graph()` provides the normal compiled-graph streaming and resume
APIs.

## DeepAgent

`NewDeepAgent` installs two model-visible harness tools:

- `write_todos` replaces the structured `AgentState.Todos` plan and records the
  same validated plan as a checkpoint-safe tool message.
- `task` delegates a self-contained request to a named, context-isolated worker.

A general-purpose worker using the parent model and application tools is added
unless `DisableGeneralPurpose` is true. Specialized workers can use different
models, prompts, and tool sets:

```go
agent, err := prebuilt.NewDeepAgent(prebuilt.DeepAgentConfig{
    Model: model,
    Tools: tools,
    SubAgents: []prebuilt.SubAgent{
        {Name: "researcher", Description: "Finds verified facts.", Agent: researcher},
        {Name: "writer", Description: "Produces concise prose.", Agent: writer},
    },
})
```

If a model emits multiple `task` calls in one response, ToolNode runs them in
parallel up to `ToolNodeConfig.MaxConcurrency`. Results are appended in model
call order, so synthesis is deterministic.

## MultiAgentCoordinator

Use `NewMultiAgentCoordinator` when only supervisor/worker coordination is
needed. It adds `task` but not `write_todos` or the automatic general-purpose
worker.

Workers receive only their delegated task and their own configured system
prompt. They return only the final assistant response to the supervisor. This
provides context isolation without hiding graph runtime controls.

Persistent child agents can set `SubAgent.RunConfig` to derive a child
`ThreadID` and `CheckpointNamespace` from the parent runtime and tool call. The
default is intentionally non-persistent so a coordinator works without a
checkpointer.

## RouterAgent

`NewRouterAgent` is intended for one classify/fan-out/synthesize pass. Its
prompt asks the model to select only relevant routes, emit selected `task`
calls together, and synthesize without multi-hop delegation. Use the
coordinator instead when the supervisor must delegate repeatedly as the
conversation evolves.

## HandoffAgent

`NewHandoffAgent` keeps one conversation state while switching the active
persona, system prompt, model, and bound tool schemas. Generated
`transfer_to_<name>` tools update `AgentState.ActiveAgent` and include the
required ToolMessage acknowledgement. This pattern is useful when the active
agent should speak directly to the user across multiple turns.

## Model fallback

`FallbackChatModel` tries models in order and preserves tool binding for every
candidate. `ShouldFallback` can prevent retries for permanent validation or
authorization errors.

## Streaming and callbacks

Every ChatModelAgent-based harness exposes `Stream`, `StreamState`, and
`StreamEvents`. See [Streaming](streaming.md) for subagent event forwarding and
[Callbacks](callbacks.md) for model/tool/delegation lifecycle hooks.

Prefer `AgentRunner.Query` in applications. Use the direct graph streaming APIs
when implementing low-level schedulers, debugging state updates, or integrating
with a protocol that already understands graph stream modes.
