# Comparison with other Go agent frameworks

This document positions **langgraph-go** relative to common Go LLM/agent stacks.
It is a product and architecture guide, not a competitive attack.

## One-line positions

| Project | Position |
|---|---|
| **langgraph-go** | Typed, durable **graph runtime** inspired by LangGraph |
| **LangChain Go** ([langchaingo](https://github.com/tmc/langchaingo)) | Broad **LLM application toolkit** (chains, RAG, loaders, many integrations) |
| **Google ADK-Go** | Google **Agent Development Kit** — agent product skeleton and multi-agent patterns |
| **tRPC-Agent-Go** | Production agent platform in the **tRPC** ecosystem (tools, MCP/A2A, eval, observability) |
| **go-agent** (lightweight libraries) | Minimal ReAct / tool loops with few dependencies |

## Capability matrix

| Dimension | langgraph-go | LangChain Go | ADK-Go | tRPC-Agent-Go | light go-agent |
|---|---|---|---|---|---|
| Graph / state machine | Strong (BSP, Send, Command, subgraphs) | Medium | Medium | Strong | Weak |
| Typed state | Strong (`StateGraph[S,D]`) | Medium | Medium | Medium | Medium–weak |
| Checkpoint / resume / time travel | Strong | Limited | Session-focused | Present | Usually weak |
| Streaming modes | Strong | Medium | Medium | Medium–strong | Varies |
| Multi-agent | Present (supervisor/router/handoff/deep) | Composable | Design focus | Design focus | Rare |
| Model / RAG ecosystem | Focused adapters | Very broad | Google-leaning | Broad (cloud + OSS) | Varies |
| MCP / A2A | MCP module + A2A subset package | Community | Ecosystem | First-class | Rare |
| Evaluation | `eval` golden trajectories | Community | Medium | First-class | Rare |
| RPC binding | Optional remote / Temporal | None | Medium | tRPC-native | None |
| Dependency coupling | Core stays lean | App-level | Vendor ecosystem | tRPC ecosystem | Minimal |

## When to choose langgraph-go

- Long-running workflows that must **interrupt, resume, and fork history**
- Deterministic concurrent super-steps with typed reducers
- Multi-sub-agent orchestration that shares the same durable runtime
- You want **LangGraph-class semantics in Go** without binding to one cloud or RPC stack

## When another stack fits better

- Fast RAG / document pipelines / widest model catalog → **langchaingo**
- Google-centric agent product templates → **ADK-Go**
- tRPC microservices + A2A/MCP platform + evaluation suite → **tRPC-Agent-Go**
- Ten-line ReAct demo → lightweight **go-agent**

## Interop strategy

langgraph-go is designed as the **execution core**. Models and data loaders can
come from other libraries:

- Bridge messages with [LANGCHAINGO.md](LANGCHAINGO.md)
- Secure MCP tools with `prebuilt.GuardTools` + [mcpclient](../mcpclient/README.md)
- Expose agents over HTTP with [remote](../remote/README.md) or [a2a](../a2a/README.md)

## Honest gaps (tracked)

See [COMPATIBILITY.md](../COMPATIBILITY.md), [DURABILITY.md](DURABILITY.md), and
the roadmap notes in [CHANGELOG.md](../CHANGELOG.md) Unreleased. Priority themes:

1. Broader ecosystem examples (langchaingo / enterprise auth)
2. Evaluation and multi-agent protocols
3. Deeper deferred-node scheduling and remote SDK surface
