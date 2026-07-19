package prebuilt_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type sdkChunk struct {
	ID        string
	Text      string
	Arguments string
}

func TestStreamingChatModelAdaptsStreamsAndMergesToolCallChunks(t *testing.T) {
	adapter := prebuilt.MessageChunkAdapterFunc[sdkChunk](func(_ context.Context, chunk sdkChunk) (prebuilt.AssistantMessageChunk, error) {
		return prebuilt.AssistantMessageChunk{
			ID: chunk.ID, Content: chunk.Text,
			ToolCallChunks:   []prebuilt.ToolCallChunk{{Index: 0, ID: "call-1", Name: "weather", Arguments: chunk.Arguments}},
			ResponseMetadata: map[string]any{"provider": "fixture"},
		}, nil
	})
	model := prebuilt.StreamingChatModel[reactState, sdkChunk]{
		Adapter: adapter, Metadata: map[string]any{"model": "fixture-v1"},
		Stream: func(_ context.Context, _ reactState, _ graph.Runtime, emit func(sdkChunk) error) error {
			if err := emit(sdkChunk{ID: "message-1", Text: "hel", Arguments: `{"city":`}); err != nil {
				return err
			}
			return emit(sdkChunk{Text: "lo", Arguments: `"Paris"}`})
		},
	}
	agent, err := prebuilt.CreateReactAgent[reactState, reactDelta](model, nil, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil || len(result.Assistant) != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	message := result.Assistant[0]
	if message.ID != "message-1" || message.Content != "hello" || len(message.ToolCalls) != 1 ||
		string(message.ToolCalls[0].Arguments) != `{"city":"Paris"}` {
		t.Fatalf("merged message=%+v", message)
	}

	var chunks []prebuilt.AssistantMessageChunk
	events := agent.StreamWithOptions(context.Background(), reactState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages}, Buffer: 4,
	})
	for event := range events {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Mode == graph.StreamMessages && event.Message != nil {
			chunk, ok := event.Message.Message.(prebuilt.AssistantMessageChunk)
			if ok {
				chunks = append(chunks, chunk)
				if event.Message.Metadata["model"] != "fixture-v1" {
					t.Fatalf("metadata=%+v", event.Message.Metadata)
				}
			}
		}
	}
	if len(chunks) != 2 || chunks[0].ID != "message-1" || chunks[1].ID != "message-1" ||
		!reflect.DeepEqual([]string{chunks[0].Content, chunks[1].Content}, []string{"hel", "lo"}) {
		t.Fatalf("stream chunks=%+v", chunks)
	}
}

func TestMergeAssistantMessageChunksRejectsProtocolDrift(t *testing.T) {
	_, err := prebuilt.MergeAssistantMessageChunks([]prebuilt.AssistantMessageChunk{
		{ID: "one", ToolCallChunks: []prebuilt.ToolCallChunk{{Index: 0, ID: "call", Name: "tool", Arguments: "{"}}},
		{ID: "two", ToolCallChunks: []prebuilt.ToolCallChunk{{Index: 0, Arguments: "}"}}},
	})
	if err == nil {
		t.Fatal("conflicting message IDs accepted")
	}
	_, err = prebuilt.MergeAssistantMessageChunks([]prebuilt.AssistantMessageChunk{{
		ID: "one", ToolCallChunks: []prebuilt.ToolCallChunk{{Index: 0, ID: "call", Name: "tool", Arguments: "{"}},
	}})
	if err == nil {
		t.Fatal("invalid final tool JSON accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	model := prebuilt.StreamingChatModel[int, int]{
		Adapter: prebuilt.MessageChunkAdapterFunc[int](func(ctx context.Context, _ int) (prebuilt.AssistantMessageChunk, error) {
			return prebuilt.AssistantMessageChunk{}, ctx.Err()
		}),
		Stream: func(_ context.Context, _ int, _ graph.Runtime, emit func(int) error) error { return emit(1) },
	}
	_, err = model.Invoke(canceled, 0, graph.Runtime{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err=%v", err)
	}
}

func TestStreamingChatModelObserverReceivesStableGeneratedID(t *testing.T) {
	var observed []prebuilt.AssistantMessageChunk
	model := prebuilt.StreamingChatModel[int, string]{
		Adapter: prebuilt.MessageChunkAdapterFunc[string](func(_ context.Context, text string) (prebuilt.AssistantMessageChunk, error) {
			return prebuilt.AssistantMessageChunk{Content: text}, nil
		}),
		Emit: func(chunk prebuilt.AssistantMessageChunk) error {
			observed = append(observed, chunk)
			return nil
		},
		Stream: func(_ context.Context, _ int, _ graph.Runtime, emit func(string) error) error {
			if err := emit("one"); err != nil {
				return err
			}
			return emit(" two")
		},
	}
	message, err := model.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	if len(observed) != 2 || observed[0].ID == "" || observed[0].ID != observed[1].ID || message.ID != observed[0].ID || message.Content != "one two" {
		t.Fatalf("observed=%+v message=%+v", observed, message)
	}
}
