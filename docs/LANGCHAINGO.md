# Using langgraph-go with langchaingo

[langchaingo](https://github.com/tmc/langchaingo) is a popular Go toolkit for
models, embeddings, and document pipelines. **langgraph-go does not replace it.**
Use langchaingo (or any provider SDK) for model I/O, and langgraph-go for durable
graph execution, interrupts, and multi-agent orchestration.

## Recommended split

| Concern | Prefer |
|---|---|
| Chat completions, embeddings, loaders, vector stores | langchaingo |
| Typed graph, checkpoint, resume, fan-out, supervisors | langgraph-go |
| Tool allowlists / human approval | `prebuilt.GuardTools` |
| Long-term memory in the graph | langgraph-go `store` + optional retrieval |

## Message bridge pattern

langgraph-go agents speak `prebuilt.Message` / `AgentState`. Adapters map
provider messages into that shape. A typical langchaingo bridge:

```go
// Pseudo-code: adapt langchaingo llms.MessageContent into prebuilt.AgentState.
// Keep this adapter in your application module so core langgraph-go stays free
// of a hard langchaingo dependency.

import (
    "context"

    "github.com/tmc/langchaingo/llms"
    "github.com/ybszm/langgraph-go/graph"
    "github.com/ybszm/langgraph-go/prebuilt"
)

type LangChainGoModel struct {
    Model llms.Model
}

func (m LangChainGoModel) Invoke(
    ctx context.Context,
    state prebuilt.AgentState,
    _ graph.Runtime,
) (prebuilt.AssistantMessage, error) {
    messages, err := state.ProviderMessages()
    if err != nil {
        return prebuilt.AssistantMessage{}, err
    }
    // Convert prebuilt.Message → []llms.MessageContent in your app.
    content, err := toLangChainMessages(messages)
    if err != nil {
        return prebuilt.AssistantMessage{}, err
    }
    resp, err := m.Model.GenerateContent(ctx, content)
    if err != nil {
        return prebuilt.AssistantMessage{}, err
    }
    return fromLangChainResponse(resp)
}
```

Then:

```go
agent, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{
    Model: LangChainGoModel{Model: openaiModel},
    Tools: tools,
})
```

## Tools

- Prefer `prebuilt.Tool` / `ToolFunc` for graph-native tools.
- Wrap external HTTP tools with timeouts in the `Run` closure.
- Apply `prebuilt.GuardTools` before binding tools to the agent.

## What not to do

- Do not import langchaingo from langgraph-go core packages (keeps the module lean).
- Do not expect Python LangGraph + Python LangChain APIs to map 1:1.

## Minimal checklist

1. Implement `prebuilt.ChatModel[prebuilt.AgentState]` over your langchaingo model.
2. Build with `NewQuickAgent` or `NewChatModelAgent`.
3. Add `graph.WithPersistence` when you need resume.
4. Stream with `AgentRunner.Query` for application UIs.
