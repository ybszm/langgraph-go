# Agent harnesses

The `prebuilt` package provides three layers over the typed graph runtime. They
share `AgentState`, remain provider-neutral, and expose the compiled graph when
streaming, interrupts, persistence, or inspection are needed.

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
