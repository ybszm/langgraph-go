<div align="center">

<img src="docs/assets/langgraph-go-hero.png" alt="LangGraph Go 실행 그래프" width="100%" />

# LangGraph Go

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md) | [한국어](README.ko.md)

**LangGraph에서 영감을 받은 Go용 타입 안전 영속 그래프 런타임.**

[![CI](https://github.com/wahanbo/langgraph-go/actions/workflows/ci.yml/badge.svg)](https://github.com/wahanbo/langgraph-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/wahanbo/langgraph-go.svg)](https://pkg.go.dev/github.com/wahanbo/langgraph-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/wahanbo/langgraph-go)](https://goreportcard.com/report/github.com/wahanbo/langgraph-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

LangGraph Go는 [`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph)가
널리 알린 핵심 그래프 실행 개념을 Go로 구현한 독립적인 커뮤니티 프로젝트입니다.
Go 제네릭, 명시적 인터페이스, 결정론적 동시 실행과 영속 상태를 중심으로 설계되었습니다.

호환성 검증의 동작 기준은 LangGraph `1.2.9`, 커밋
`95af6a00718588e7b7ce17310e8006d267896a77`입니다. 이 프로젝트는 LangChain의 공식 제품이 아니며,
Python 패키지를 소스 수준에서 그대로 옮긴 포트도 아닙니다.

> [!IMPORTANT]
> 프로젝트는 현재 활발히 개발 중입니다. 핵심 워크플로는 사용할 수 있고 폭넓게 테스트되었지만,
> 전체 Python API를 아직 재현하지는 않습니다. 완전한 대체재로 사용하기 전에
> [호환성 문서](COMPATIBILITY.md)를 확인하세요.

## 주요 기능

- 타입이 지정된 상태와 업데이트를 위한 제네릭 `StateGraph[S, D]` API
- BSP 방식 super-step과 결정론적 reducer
- 정적, 조건부, 대기 및 동적 `Send` edge
- `Command` 업데이트, 라우팅, 부모 그래프 지정과 영속 재개
- 동시 실행 제한, 재시도, 캐시, 타임아웃과 취소
- 메모리, SQLite, PostgreSQL checkpoint 구현
- interrupt/resume, replay, branch, time travel과 중첩 subgraph
- Values, Updates, Messages, Custom, Debug 및 subgraph 스트리밍
- task, future, 영속성과 복구를 지원하는 타입 기반 Functional API
- 공급자 중립적인 ToolNode와 ReAct 방식 Agent 구성 요소
- 장기 Store, TTL, 의미 기반 인덱싱과 벡터 백엔드
- 선택적 HTTP/SSE, 분산 PostgreSQL 및 Temporal 통합

## 설치

```bash
go get github.com/wahanbo/langgraph-go@latest
```

Go 1.24 이상이 필요합니다.

## 빠른 시작

```go
package main

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
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

실행 가능한 예제는 [`examples/basic`](examples/basic)에 있습니다.

## 패키지

| 패키지 | 용도 |
|---|---|
| `graph` | 타입 기반 그래프 빌더, 컴파일러, 런타임, 스트리밍, 인터럽트와 상태 조회 |
| `channel` | Pregel 방식 타입 기반 Channel 프리미티브 |
| `checkpoint/*` | 메모리, SQLite, PostgreSQL checkpoint saver와 codec |
| `functional` | 영속 task, future와 타입 기반 entrypoint |
| `prebuilt` | Message, ToolNode와 ReAct 방식 Agent 구성 요소 |
| `store/*` | 장기 key/value, TTL, embedding과 vector store |
| `remote` | 타입 기반 HTTP/SSE 서버 및 클라이언트 |
| `backend/distributed` | lease queue, event log, interrupt와 transactional outbox |
| `backend/temporal` | 공급자 중립 Temporal adapter와 선택적 공식 SDK binding |

## 호환성 범위

LangGraph Go는 Go에 자연스럽게 적용되는 개념에 대해 동작 호환성을 목표로 합니다.
Python 문법을 그대로 복제하지 않고 Go다운 API를 사용합니다. 알려진 차이는 다음과 같습니다.

- Python 저수준 `Pregel` / `NodeBuilder` 구성 API
- `defer=True` 노드와 실행별 `sync` / `async` / `exit` durability mode
- Python v3 stream transformer와 graph UI helper API
- 일부 편의 기능 및 레거시 Prebuilt export
- 호스팅 LangGraph Platform과 Python SDK 프로토콜의 완전한 지원

테스트 근거를 포함한 상세 상태는 [`COMPATIBILITY.md`](COMPATIBILITY.md)에 기록되어 있습니다.

## 개발

```bash
go test ./...
go vet ./...
```

`LANGGRAPH_POSTGRES_DSN`을 설정하면 데이터베이스 통합 테스트가,
`LANGGRAPH_TEMPORAL_ADDRESS`를 설정하면 Temporal 통합 테스트가 활성화됩니다.
CI는 Linux에서 unit, race, PostgreSQL, Temporal 검사를 실행합니다.

기여 절차는 [`CONTRIBUTING.md`](CONTRIBUTING.md), 설계 세부 사항은
[`ARCHITECTURE.md`](ARCHITECTURE.md)를 참고하세요.

## 프로젝트 상태 및 지원

이 저장소는 실험 단계이며 best-effort 방식으로 유지 관리됩니다. 설계 관련 질문은 GitHub Discussions,
재현 가능한 결함이나 호환성 차이는 Issues를 사용하세요.

## 라이선스

LangGraph Go는 [MIT License](LICENSE)로 제공됩니다. 업스트림 귀속과 의존성 고지는
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)에 있습니다.
