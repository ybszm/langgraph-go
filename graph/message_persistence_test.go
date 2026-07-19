package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type durableMessageState struct {
	Messages []prebuilt.AssistantMessage `json:"messages,omitempty"`
}

type durableMessageDelta struct {
	Messages []prebuilt.AssistantMessage `json:"messages,omitempty"`
}

func durableMessageReducer(_ context.Context, state durableMessageState, updates []durableMessageDelta) (durableMessageState, error) {
	for _, update := range updates {
		state.Messages = append(state.Messages, update.Messages...)
	}
	return state, nil
}

func TestDeltaNormalizerStabilizesMessageIDAcrossPendingWriteRecovery(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(durableMessageReducer)
	var generated atomic.Int32
	builder.SetDeltaNormalizer(func(_ context.Context, delta durableMessageDelta) (durableMessageDelta, error) {
		messages := make([]prebuilt.Message, len(delta.Messages))
		for index, message := range delta.Messages {
			messages[index] = message
		}
		normalized, err := prebuilt.NormalizeMessages(messages, func() (string, error) {
			return fmt.Sprintf("message-%d", generated.Add(1)), nil
		})
		if err != nil {
			return durableMessageDelta{}, err
		}
		for index, message := range normalized {
			delta.Messages[index] = message.(prebuilt.AssistantMessage)
		}
		return delta, nil
	})
	successFinished := make(chan struct{})
	var successOnce sync.Once
	var messageCalls atomic.Int32
	var flakyCalls atomic.Int32
	_ = builder.AddNode("message", func(context.Context, durableMessageState, graph.Runtime) (graph.Command[durableMessageDelta], error) {
		messageCalls.Add(1)
		successOnce.Do(func() { close(successFinished) })
		return graph.Update(durableMessageDelta{Messages: []prebuilt.AssistantMessage{{Content: "stable"}}}), nil
	})
	_ = builder.AddNode("flaky", func(ctx context.Context, _ durableMessageState, _ graph.Runtime) (graph.Command[durableMessageDelta], error) {
		select {
		case <-successFinished:
		case <-ctx.Done():
			return graph.NoCommand[durableMessageDelta](), ctx.Err()
		}
		if flakyCalls.Add(1) == 1 {
			return graph.NoCommand[durableMessageDelta](), errors.New("transient")
		}
		return graph.NoCommand[durableMessageDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "message")
	_ = builder.AddEdge(graph.START, "flaky")
	_ = builder.AddEdge("message", graph.END)
	_ = builder.AddEdge("flaky", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[durableMessageState, durableMessageDelta]{
		Saver:       saver,
		StateCodec:  checkpoint.MustJSONCodec[durableMessageState]("tests/durable-message-state", 1),
		DeltaCodec:  checkpoint.MustJSONCodec[durableMessageDelta]("tests/durable-message-delta", 1),
		Clock:       checkpoint.ClockFunc(func() time.Time { return time.Unix(1_700_000_000, 0) }),
		IDGenerator: &sequenceIDGenerator{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "durable-message-id"}
	if _, err := compiled.Invoke(context.Background(), durableMessageState{}, config); err == nil {
		t.Fatal("first Invoke succeeded")
	}
	result, err := compiled.Invoke(context.Background(), durableMessageState{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 1 || result.Messages[0].ID != "message-1" {
		t.Fatalf("messages=%#v", result.Messages)
	}
	if messageCalls.Load() != 1 || flakyCalls.Load() != 2 || generated.Load() != 1 {
		t.Fatalf("message calls=%d flaky calls=%d generated=%d", messageCalls.Load(), flakyCalls.Load(), generated.Load())
	}
}

type durableRawMessageState struct {
	Messages []map[string]any `json:"messages,omitempty"`
}

type durableRawMessageDelta struct {
	Messages []map[string]any `json:"messages,omitempty"`
}

func durableRawMessageReducer(_ context.Context, state durableRawMessageState, updates []durableRawMessageDelta) (durableRawMessageState, error) {
	for _, update := range updates {
		state.Messages = append(state.Messages, update.Messages...)
	}
	return state, nil
}

func TestRawMessageNormalizerStabilizesPendingWriteID(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(durableRawMessageReducer)
	var generated atomic.Int32
	builder.SetDeltaNormalizer(func(_ context.Context, delta durableRawMessageDelta) (durableRawMessageDelta, error) {
		normalized, err := graph.NormalizeMessageValue(delta.Messages, func() (string, error) {
			return fmt.Sprintf("raw-%d", generated.Add(1)), nil
		})
		if err != nil {
			return durableRawMessageDelta{}, err
		}
		delta.Messages = normalized.([]map[string]any)
		return delta, nil
	})
	messageFinished := make(chan struct{})
	var finishedOnce sync.Once
	var messageCalls atomic.Int32
	var flakyCalls atomic.Int32
	_ = builder.AddNode("message", func(context.Context, durableRawMessageState, graph.Runtime) (graph.Command[durableRawMessageDelta], error) {
		messageCalls.Add(1)
		finishedOnce.Do(func() { close(messageFinished) })
		return graph.Update(durableRawMessageDelta{Messages: []map[string]any{
			{"role": "assistant", "content": "durable"},
			{"role": "unknown", "content": "ordinary"},
		}}), nil
	})
	_ = builder.AddNode("flaky", func(ctx context.Context, _ durableRawMessageState, _ graph.Runtime) (graph.Command[durableRawMessageDelta], error) {
		select {
		case <-messageFinished:
		case <-ctx.Done():
			return graph.NoCommand[durableRawMessageDelta](), ctx.Err()
		}
		if flakyCalls.Add(1) == 1 {
			return graph.NoCommand[durableRawMessageDelta](), errors.New("transient")
		}
		return graph.NoCommand[durableRawMessageDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "message")
	_ = builder.AddEdge(graph.START, "flaky")
	_ = builder.AddEdge("message", graph.END)
	_ = builder.AddEdge("flaky", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[durableRawMessageState, durableRawMessageDelta]{
		Saver:       saver,
		StateCodec:  checkpoint.MustJSONCodec[durableRawMessageState]("tests/durable-raw-message-state", 1),
		DeltaCodec:  checkpoint.MustJSONCodec[durableRawMessageDelta]("tests/durable-raw-message-delta", 1),
		Clock:       checkpoint.ClockFunc(func() time.Time { return time.Unix(1_700_000_000, 0) }),
		IDGenerator: &sequenceIDGenerator{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "durable-raw-message-id"}
	if _, err := compiled.Invoke(context.Background(), durableRawMessageState{}, config); err == nil {
		t.Fatal("first Invoke succeeded")
	}
	result, err := compiled.Invoke(context.Background(), durableRawMessageState{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 || result.Messages[0]["id"] != "raw-1" {
		t.Fatalf("messages=%v", result.Messages)
	}
	if _, exists := result.Messages[1]["id"]; exists {
		t.Fatalf("ordinary map normalized: %v", result.Messages[1])
	}
	if generated.Load() != 1 || messageCalls.Load() != 1 || flakyCalls.Load() != 2 {
		t.Fatalf("generated=%d message=%d flaky=%d", generated.Load(), messageCalls.Load(), flakyCalls.Load())
	}
}
