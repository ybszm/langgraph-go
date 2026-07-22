<div align="center">

<img src="docs/assets/langgraph-go-hero-v2.png" alt="LangGraph Go 执行、checkpoint 与检索图" width="100%" />

# LangGraph Go

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md) | [한국어](README.ko.md)

**受 LangGraph 启发，为 Go 构建的强类型、可持久化图运行时。**

[![CI](https://github.com/ybszm/langgraph-go/actions/workflows/ci.yml/badge.svg)](https://github.com/ybszm/langgraph-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ybszm/langgraph-go.svg)](https://pkg.go.dev/github.com/ybszm/langgraph-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/ybszm/langgraph-go)](https://goreportcard.com/report/github.com/ybszm/langgraph-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

LangGraph Go 是由社区独立维护的 Go 实现，借鉴了
[`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph) 所推广的核心图执行理念。
项目围绕 Go 泛型、显式接口、确定性并发执行和持久化状态设计。

兼容性工作的行为基准是 LangGraph `1.2.9`，对应提交
`95af6a00718588e7b7ce17310e8006d267896a77`。本项目不是 LangChain 官方产品，
也不是 Python 包的逐行或源码级移植。

> [!IMPORTANT]
> 项目仍在积极开发中。核心工作流已经可用并经过大量测试，但尚未完整复现全部
> Python API。在将其视为直接替代品之前，请先阅读[兼容性说明](COMPATIBILITY.md)
> 与[持久化语义](docs/DURABILITY.md)。

## 仓库目录说明

- **核心 Go 运行时**：仓库根目录包（`graph`、`checkpoint`、`prebuilt` 等）以及
  `go.work` 中的可选 module。
- **`python学习文档/`**：**独立教学项目**（Python MiniGraph + 小规模 Go 对照实现），
  不被已发布 module 引用，也**不能**当作主库 API 文档。生产向用法请从根 README
  与 `examples/` 入手。

## 主要能力

- 基于泛型的 `StateGraph[S, D]`，提供强类型状态与更新
- BSP 风格 super-step 与确定性归并
- 静态边、条件边、等待边和动态 `Send` 边
- `Command` 更新、动态路由、父图定向与持久化恢复
- 带并发限制、重试、缓存、超时和取消的节点执行
- 内存、SQLite、PostgreSQL 和可选 Redis checkpoint 实现
- 中断与恢复、重放、分支、时间旅行和嵌套子图
- Values、Updates、Messages、Custom、Debug 和子图流式输出，包括模型原生 SSE 分片
- 带任务、Future、持久化和恢复能力的强类型 Functional API
- 与模型供应商无关的 ToolNode 和 ReAct 风格 Agent 组件
- 长期 Store、TTL、语义索引、向量后端、BM25/混合检索与上下文记忆中间件
- 可选 HTTP/SSE、Redis、分布式 PostgreSQL 与 Temporal 集成

## 安装

```bash
go get github.com/ybszm/langgraph-go@latest
```

需要 Go 1.25 或更高版本。

## 快速开始

```go
package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
)

type State struct {
	Count int
}

type Delta struct {
	Increment int
}

func main() {
	builder := graph.NewStateGraph(func(
		_ context.Context,
		state State,
		updates []Delta,
	) (State, error) {
		for _, update := range updates {
			state.Count += update.Increment
		}
		return state, nil
	})

	if err := builder.AddNode("increment", func(
		_ context.Context,
		_ State,
		_ graph.Runtime,
	) (graph.Command[Delta], error) {
		return graph.Update(Delta{Increment: 1}), nil
	}); err != nil {
		panic(err)
	}
	if err := builder.AddEdge(graph.START, "increment"); err != nil {
		panic(err)
	}
	if err := builder.AddEdge("increment", graph.END); err != nil {
		panic(err)
	}

	compiled, err := builder.Compile()
	if err != nil {
		panic(err)
	}

	result, err := compiled.Invoke(context.Background(), State{}, graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Count) // 1
}
```

可运行示例位于 [`examples/basic`](examples/basic)。

## 包结构

| 包 | 用途 |
|---|---|
| `graph` | 强类型图构建器、编译器、运行时、流式输出、中断和状态检查 |
| `channel` | Pregel 风格强类型 Channel 原语 |
| `checkpoint/*` | 内存、SQLite、PostgreSQL checkpoint saver 与 codec |
| `functional` | 持久化任务、Future 和强类型入口 |
| `prebuilt` | 消息、ToolNode、`ChatModelAgent`、`DeepAgent`、多 Agent 协调和 ReAct 组件 |
| `retrieval` | 文本分块、BM25/向量/混合检索、摄取和 Retriever Tool |
| `memory` | 滑动窗口、摘要和 RAG 上下文模型中间件 |
| `store/*` | 长期键值存储、TTL、Embedding 与向量存储 |
| `redis/*` | 可选 Redis checkpoint、长期 Store 和任务缓存模块 |
| `remote` | 强类型 HTTP/SSE 服务端与客户端 |
| `backend/distributed` | 租约队列、事件日志、中断和事务 Outbox |
| `backend/temporal` | 与供应商无关的 Temporal Adapter 和可选官方 SDK Binding |

## 开箱即用的 Agent

`prebuilt.NewChatModelAgent` 为单模型 Agent 提供默认的可持久化消息状态、系统提示词注入、
ReAct 工具循环和面向文本输入的 `Run` 方法，无需自行声明 State、Delta、Reducer 与 Adapter。

`prebuilt.NewAgentRunner` 是 ChatModelAgent、DeepAgent、Router、Supervisor 和 Handoff
共用的应用层执行入口。`Query` 默认只暴露消息、自定义数据、中断、完成和错误事件，应用无需理解
Graph StreamMode；`Resume` 使用同一事件模型继续可持久化的人机协作会话。

复杂任务可以使用 `prebuilt.NewDeepAgent`。它默认加入 `write_todos` 计划工具和 `task`
委派工具，并自动提供一个上下文隔离的通用子 Agent；也可以注册使用不同模型、提示词和工具集的
专用 `SubAgent`。模型在同一轮发出多个 `task` 调用时，现有 ToolNode 会并行执行这些任务，
再按调用顺序把最终结果交给主 Agent 汇总。只需要 Supervisor/Worker 模式时，可以直接使用
`NewMultiAgentCoordinator`。

此外，`NewRouterAgent` 提供单轮分类、并行分发与汇总，`NewHandoffAgent` 将当前 Agent
作为可 checkpoint 的状态并支持直接交接，`FallbackChatModel` 提供模型/供应商顺序降级。
Agent streaming 会透传带子 Agent 标识的消息块，自定义 callbacks 可以观察模型、单个工具和
委派生命周期。

先运行无凭据的 [`examples/agent-runner`](examples/agent-runner)，再通过
[`examples/multi-agent`](examples/multi-agent) 查看完整的 ChatModelAgent、DeepAgent 与并行协调；
设计与持久化子 Agent 配置见 [`docs/agents.md`](docs/agents.md)。
完整文档入口见 [`docs/README.md`](docs/README.md)，其中包含 streaming 与 callbacks 专题。

## 兼容性边界

LangGraph Go 会在适合 Go 的概念上追求行为兼容，并有意使用 Go 风格 API，
而不是照搬 Python 语法。目前已知差异包括：

- Python 底层 `Pregel` / `NodeBuilder` 构建接口
- `defer=True` 节点和单次运行的 `sync` / `async` / `exit` durability 模式
- Python v3 stream transformer 与图形 UI 辅助 API
- 部分便利性及旧版 Prebuilt 导出
- 完整的托管 LangGraph Platform 与 Python SDK 协议覆盖

详细、基于测试证据的状态记录在 [`COMPATIBILITY.md`](COMPATIBILITY.md)。

## 开发

```bash
go test ./...
go vet ./...
```

设置 `LANGGRAPH_POSTGRES_DSN` 可启用数据库集成测试；设置
`LANGGRAPH_TEMPORAL_ADDRESS` 可启用 Temporal 集成测试。CI 会在 Linux 上运行单元测试、
race detector、PostgreSQL、Redis 和 Temporal 检查。

贡献流程请阅读 [`CONTRIBUTING.md`](CONTRIBUTING.md)，设计细节请阅读
[`ARCHITECTURE.md`](ARCHITECTURE.md)。

## 项目状态与支持

本仓库目前处于实验阶段，由维护者尽力维护。设计问题请使用 GitHub Discussions；
可复现缺陷或兼容性差异请通过 Issues 提交。

## 许可证

LangGraph Go 使用 [MIT License](LICENSE)。上游归属和依赖声明位于
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)。
