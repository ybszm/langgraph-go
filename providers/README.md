# Model providers

The provider module contains ten HTTP-based `prebuilt.ChatModel` adapters. It
keeps vendor clients and credentials outside the graph runtime while preserving
tool definitions, tool calls, cancellation, and typed non-2xx errors.

| Adapter | Protocol | Native model streaming |
|---|---|---|
| `openai` | OpenAI Chat Completions | SSE |
| `azureopenai` | Azure OpenAI Chat Completions | SSE |
| `anthropic` | Anthropic Messages | SSE |
| `gemini` | Gemini `generateContent` | SSE |
| `vertexai` | Vertex AI `generateContent` | SSE |
| `bedrock` | Amazon Bedrock Converse | Invoke only |
| `ollama` | OpenAI-compatible chat API | SSE |
| `deepseek` | OpenAI-compatible chat API | SSE |
| `dashscope` | DashScope compatible-mode API | SSE |
| `mistral` | Mistral Chat Completions | SSE |

Adapters accept an injected `http.Client`; cloud authentication can therefore
use the caller's standard credential transport. Vertex AI also accepts a
request editor, while Bedrock requires a SigV4 `RequestSigner`. The module does
not read environment variables or refresh credentials implicitly.

```go
model, err := openai.New(openai.Config[prebuilt.AgentState]{
    APIKey:   os.Getenv("OPENAI_API_KEY"),
    Model:    "gpt-4.1-mini",
    Streaming: true,
    Messages: prebuilt.AgentModelMessages,
})
if err != nil {
    return err
}

agent, err := prebuilt.NewAgent(model, tools, prebuilt.ReactAgentConfig{})
```

With `Streaming: true`, `Invoke` consumes the provider stream, publishes
normalized `AssistantMessageChunk` values through graph message streams, and
returns the merged final assistant message. Native models also expose `Stream`
for callers that need a direct per-chunk callback. Text, tool-call argument
fragments, finish reasons, usage, cancellation, and bounded response sizes are
handled without exposing provider SDK types.

Bedrock Converse remains invoke-only because `ConverseStream` uses AWS binary
EventStream framing rather than SSE. Applications can still use graph-level
values, updates, custom, debug, and subgraph streaming with that adapter.
