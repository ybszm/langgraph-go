<div align="center">

<img src="docs/assets/langgraph-go-hero.png" alt="LangGraph Go 実行グラフ" width="100%" />

# LangGraph Go

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md) | [한국어](README.ko.md)

**LangGraph に着想を得た、Go 向けの型安全で永続化可能なグラフランタイム。**

[![CI](https://github.com/wahanbo/langgraph-go/actions/workflows/ci.yml/badge.svg)](https://github.com/wahanbo/langgraph-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/wahanbo/langgraph-go.svg)](https://pkg.go.dev/github.com/wahanbo/langgraph-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/wahanbo/langgraph-go)](https://goreportcard.com/report/github.com/wahanbo/langgraph-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

LangGraph Go は、[`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph)
によって広まったグラフ実行の中核概念を Go で実装する、独立したコミュニティプロジェクトです。
Go のジェネリクス、明示的なインターフェース、決定論的な並行実行、永続化可能な状態を中心に設計されています。

互換性検証の基準は LangGraph `1.2.9`、コミット
`95af6a00718588e7b7ce17310e8006d267896a77` です。本プロジェクトは LangChain の公式製品ではなく、
Python パッケージのソースコードをそのまま移植したものでもありません。

> [!IMPORTANT]
> 本プロジェクトは活発に開発中です。主要なワークフローは利用可能で広範にテストされていますが、
> Python API 全体を再現しているわけではありません。完全な代替として採用する前に
> [互換性情報](COMPATIBILITY.md)を確認してください。

## 主な機能

- 型付き状態と更新を扱うジェネリック `StateGraph[S, D]` API
- BSP 形式の super-step と決定論的な reducer
- 静的、条件付き、待機、動的 `Send` エッジ
- `Command` 更新、ルーティング、親グラフ指定、永続的な再開
- 同時実行数制限、リトライ、キャッシュ、タイムアウト、キャンセル
- メモリ、SQLite、PostgreSQL、オプションの Redis checkpoint 実装
- interrupt/resume、replay、branch、time travel、ネストした subgraph
- Values、Updates、Messages、Custom、Debug、subgraph ストリーミング
- task、future、永続化、復旧に対応する型付き Functional API
- プロバイダー非依存の ToolNode と ReAct 形式の Agent 部品
- 長期 Store、TTL、セマンティックインデックス、ベクトルバックエンド
- オプションの HTTP/SSE、Redis、分散 PostgreSQL、Temporal 連携

## インストール

```bash
go get github.com/wahanbo/langgraph-go@latest
```

Go 1.25 以降が必要です。

## クイックスタート

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

実行可能な例は [`examples/basic`](examples/basic) にあります。

## パッケージ

| パッケージ | 用途 |
|---|---|
| `graph` | 型付きグラフビルダー、コンパイラー、ランタイム、ストリーミング、割り込み、状態参照 |
| `channel` | Pregel 形式の型付き Channel プリミティブ |
| `checkpoint/*` | メモリ、SQLite、PostgreSQL の checkpoint saver と codec |
| `functional` | 永続化 task、future、型付き entrypoint |
| `prebuilt` | Message、ToolNode、ReAct 形式の Agent 部品 |
| `retrieval` | テキスト分割、BM25/vector/hybrid 検索、取り込み、Retriever Tool |
| `memory` | sliding window、要約、RAG context の model middleware |
| `store/*` | 長期 key/value、TTL、embedding、vector store |
| `redis/*` | オプションの Redis checkpoint、長期 Store、task cache |
| `remote` | 型付き HTTP/SSE サーバーおよびクライアント |
| `backend/distributed` | lease queue、event log、interrupt、transactional outbox |
| `backend/temporal` | プロバイダー非依存の Temporal adapter と任意の公式 SDK binding |

## 互換性の範囲

LangGraph Go は、Go に自然に適用できる概念について動作互換性を目指します。
Python の構文をそのまま模倣せず、Go らしい API を採用しています。既知の差分は次のとおりです。

- Python の低レベル `Pregel` / `NodeBuilder` 構築 API
- `defer=True` ノードと実行単位の `sync` / `async` / `exit` durability mode
- Python v3 stream transformer と graph UI helper API
- 一部の便利機能および旧 Prebuilt export
- ホスト型 LangGraph Platform と Python SDK プロトコルの完全な対応

テスト根拠を含む詳細は [`COMPATIBILITY.md`](COMPATIBILITY.md) に記載しています。

## 開発

```bash
go test ./...
go vet ./...
```

`LANGGRAPH_POSTGRES_DSN` を設定するとデータベース統合テスト、
`LANGGRAPH_TEMPORAL_ADDRESS` を設定すると Temporal 統合テストが有効になります。
CI は Linux 上で unit、race、PostgreSQL、Redis、Temporal の各テストを実行します。

コントリビューション手順は [`CONTRIBUTING.md`](CONTRIBUTING.md)、設計の詳細は
[`ARCHITECTURE.md`](ARCHITECTURE.md) を参照してください。

## プロジェクトの状態とサポート

本リポジトリは実験段階であり、best-effort で保守されています。設計上の質問には GitHub Discussions、
再現可能な不具合や互換性の差分には Issues を利用してください。

## ライセンス

LangGraph Go は [MIT License](LICENSE) で提供されます。上流プロジェクトの帰属と依存関係の通知は
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) に記載しています。
