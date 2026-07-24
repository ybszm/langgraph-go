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

LangGraph Go 是一个面向长时间运行、有状态工作流与 Agent 的底层运行时。开发者负责定义
强类型状态转换；运行时负责编排图、确定性归并并发更新，并在 super-step 边界持久化执行状态。

项目借鉴了
[`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph)、Pregel
以及持久化工作流系统，但 API 坚持 Go 风格：泛型、显式接口、`context.Context` 和错误返回。

> [!WARNING]
> **v0.1.0 仍处于实验阶段。** 核心运行时正在向生产稳定性收敛，但 v1.0 前公开 API
> 与 checkpoint schema 仍可能调整。请固定依赖版本，并阅读
> [版本策略](docs/VERSIONING.md)、[兼容性说明](COMPATIBILITY.md)和
> [持久化语义](docs/DURABILITY.md)。

## 为什么使用它？

当一次操作不再只是简单的请求—响应时，可以考虑 LangGraph Go：

- **持久化执行**：保存状态，并在中断或故障后继续运行。
- **人机协作**：暂停任务、检查状态，再用明确的人类输入继续。
- **确定性并发**：并行运行互不依赖的节点，再以稳定顺序归并强类型更新。
- **时间旅行**：检查历史、从 checkpoint 重放，或从旧状态创建不可变分支。
- **流式与可观测**：观察状态、更新、消息、自定义事件、中断和节点生命周期。

如果需求只是一个短小、无状态的工具调用循环，引入图运行时通常会增加不必要的复杂度。

## 安装

```bash
go get github.com/ybszm/langgraph-go@v0.1.0
```

需要 Go 1.25 或更高版本。版本承诺见 [docs/VERSIONING.md](docs/VERSIONING.md)。

## 核心心智模型

| 概念 | 含义 |
|---|---|
| `State`（`S`） | 节点可见的完整强类型状态快照 |
| `Delta`（`D`） | 节点返回的强类型更新 |
| Reducer | 唯一允许把多个 Delta 归并进 State 的函数 |
| Node | 一个工作单元：`State -> Command[Delta]` |
| Edge | 声明接下来允许运行哪些节点 |
| Super-step | 一个确定性调度边界；就绪节点可以并发运行 |
| Thread | 由 `RunConfig.ThreadID` 选择的一条持久化执行历史 |
| Checkpoint | 在 super-step 边界保存的状态与任务快照 |

节点不直接修改共享状态，而是返回 Delta；Reducer 在 super-step 结束后统一归并。
正是这一层分离，使并发执行、重放与故障恢复具有清晰语义。

## 教程一：构建强类型图

最小可用图只做一件事：把计数器加一。

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ybszm/langgraph-go/graph"
)

type State struct {
	Count int
}

type Delta struct {
	Add int
}

func main() {
	builder := graph.NewStateGraph(func(
		_ context.Context,
		state State,
		updates []Delta,
	) (State, error) {
		for _, update := range updates {
			state.Count += update.Add
		}
		return state, nil
	})

	err := builder.AddNode("increment", func(
		_ context.Context,
		_ State,
		_ graph.Runtime,
	) (graph.Command[Delta], error) {
		return graph.Update(Delta{Add: 1}), nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "increment"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("increment", graph.END); err != nil {
		log.Fatal(err)
	}

	compiled, err := builder.Compile()
	if err != nil {
		log.Fatal(err)
	}
	result, err := compiled.Invoke(
		context.Background(),
		State{},
		graph.RunConfig{},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Count) // 1
}
```

执行过程可以理解为：

```text
START -> increment -> END
            |
            +-- 返回 Delta{Add: 1}
                       |
Reducer(State{}, [Delta{Add: 1}]) -> State{Count: 1}
```

运行仓库中的完整示例：

```bash
go run ./examples/basic
```

## 教程二：让执行可恢复

编译时配置 `checkpoint.Saver`，运行时提供稳定的 Thread ID，图就会进入持久化执行路径：

```go
import (
	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
)

compiled, err := builder.Compile(graph.WithPersistence(
	graph.PersistenceConfig[State, Delta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[State]("example.state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[Delta]("example.delta", 1),
	},
))

config := graph.RunConfig{
	ThreadID:  "counter-demo",
	Durability: graph.DurabilitySync,
}
result, err := compiled.Invoke(context.Background(), State{}, config)
```

Codec 名称与版本属于持久化数据协议。不要在 Go 类型不兼容时悄悄复用原有 Codec 名称。
Memory Saver 只适合测试和示例；本地持久化可使用 SQLite，部署场景应根据运维条件评估
PostgreSQL 或 Redis。

继续运行 [`examples/checkpoint-resume`](examples/checkpoint-resume) 学习历史检查，
再运行 [`examples/interrupt-resume`](examples/interrupt-resume) 完成人机暂停与恢复。

## 恢复为什么可靠

对每个持久化 Thread，运行时会：

1. 读取最新 Checkpoint。
2. 执行当前 super-step 中的就绪节点。
3. 在完整 super-step 提交前记录成功任务的 pending writes。
4. 通过 Reducer 生成下一状态。
5. 提交新的 Checkpoint 与后续任务。

如果一个并行节点失败、另一个节点已经成功，恢复时可以重用成功节点的 pending write，
避免再次执行它。节点内部的外部副作用仍必须由应用保证幂等。

三种 Durability 模式在延迟与崩溃暴露面之间取舍：

| 模式 | 行为 |
|---|---|
| `sync` | 每个 Checkpoint 落盘成功后才进入下一 super-step |
| `async` | 保持写入顺序，同时让 Checkpoint I/O 与后续执行重叠 |
| `exit` | 中间 Checkpoint 留在内存，只发布最终或恢复边界 |

在性能数据证明有必要之前，应优先使用 `sync`。

## 建议学习顺序

| 步骤 | 阅读或运行 | 目标 |
|---|---|---|
| 1 | [`examples/basic`](examples/basic) | State、Delta、Reducer、Node、Edge |
| 2 | [`examples/conditional-routing`](examples/conditional-routing) 与 [`examples/fanout`](examples/fanout) | 路由和确定性并发归并 |
| 3 | [`examples/checkpoint-resume`](examples/checkpoint-resume) | Thread、Codec、Checkpoint、历史 |
| 4 | [`examples/interrupt-resume`](examples/interrupt-resume) | 人机协作与持久化恢复 |
| 5 | [`examples/time-travel`](examples/time-travel) | 重放与不可变分支 |
| 6 | [`examples/streaming`](examples/streaming) | Values、Updates 与终态事件 |
| 7 | [`examples/subgraph`](examples/subgraph) | 强类型组合与嵌套持久化 |
| 8 | [架构](ARCHITECTURE.md)、[持久化](docs/DURABILITY.md)和[版本策略](docs/VERSIONING.md) | 生产与兼容性边界 |

## 核心与可选模块

项目有意让模型和基础设施集成与核心模块保持隔离。

| 范围 | 包 | 状态 |
|---|---|---|
| 核心运行时 | `graph`、`channel`、`checkpoint/*` | **稳定化中** |
| 核心辅助 | `functional`、`cache`、`store/*` | **Experimental** |
| Agent 便利层 | `prebuilt`、`memory`、`retrieval` | **Experimental** |
| 集成 | `providers`、`mcpclient`、`remote`、`redis`、`observability/otel`、`backend/temporal` | **可选 / Experimental** |

建议先掌握核心运行时，只在应用确实需要时加入集成模块。Agent 示例与说明仍可通过
[docs/agents.md](docs/agents.md)和[examples/README.md](examples/README.md)查阅。

## 投入生产前

- 固定精确的 Module 版本。
- 为每一种持久化类型使用显式、带版本的 Codec。
- 控制 State 体积；大文档和制品放在 Checkpoint 之外。
- 保证节点外部副作用幂等。
- 明确设置节点超时、重试条件和并发上限。
- 使用与生产相同的 Saver 演练故障恢复。
- 规划 Checkpoint 清理、数据库备份和 Schema 迁移。
- 在日志与 Trace 中传播 `run_id`、`thread_id` 和 Checkpoint 坐标。

项目尚未承诺稳定的 v1 API，也不宣称适用于所有生产场景。完整入口见
[文档索引](docs/README.md)；任何无法通过测试复现的恢复或兼容问题都应提交 Issue。

## 开发

```bash
go test ./...
go vet ./...
```

本仓库使用 Go workspace；修改可选模块时，还应分别在 `providers`、`mcpclient`、
`remote`、`redis`、`observability/otel` 与 `backend/temporal` 目录运行上述命令。
整个 workspace 要求 Go 1.25 或更高版本。

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
