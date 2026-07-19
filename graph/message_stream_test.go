package graph_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
)

type testMessage struct {
	Role    string
	Content string
}

func TestRuntimeWriteContentBlockV2Lifecycle(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		events := []graph.ContentBlockStreamEvent{
			{Event: graph.ContentMessageStart, Role: "ai", MessageID: "message-1"},
			{Event: graph.ContentBlockStart, Index: 0, ContentBlock: map[string]any{"type": "text", "text": "", "provider": map[string]any{"trace": "one"}}},
			{Event: graph.ContentBlockDelta, Index: 0, ContentBlock: map[string]any{"type": "text", "text": "hello "}},
			{Event: graph.ContentBlockDelta, Index: 0, ContentBlock: map[string]any{"type": "text", "text": "world"}},
			{Event: graph.ContentBlockFinish, Index: 0, ContentBlock: map[string]any{"type": "text", "text": "hello world"}},
			{Event: graph.ContentMessageFinish, Reason: "stop"},
		}
		for _, event := range events {
			if err := runtime.WriteContentBlock(event, map[string]any{"provider": "fixture"}); err != nil {
				return graph.NoCommand[customDelta](), err
			}
		}
		return graph.NoCommand[customDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var events []*graph.MessageStreamEvent
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			events = append(events, event.Message)
		}
	}
	wantKinds := []graph.ContentBlockEventKind{
		graph.ContentMessageStart, graph.ContentBlockStart, graph.ContentBlockDelta,
		graph.ContentBlockDelta, graph.ContentBlockFinish, graph.ContentMessageFinish,
	}
	gotKinds := make([]graph.ContentBlockEventKind, len(events))
	for index, event := range events {
		if event.ContentBlock == nil || event.Metadata["langgraph_node"] != "model" || event.Metadata["provider"] != "fixture" {
			t.Fatalf("event[%d]=%+v", index, event)
		}
		gotKinds[index] = event.ContentBlock.Event
	}
	if !reflect.DeepEqual(gotKinds, wantKinds) || events[4].ContentBlock.ContentBlock["text"] != "hello world" {
		t.Fatalf("events=%+v", events)
	}
	provider := events[1].ContentBlock.ContentBlock["provider"].(map[string]any)
	if provider["trace"] != "one" {
		t.Fatalf("provider extension=%v", provider)
	}
}

func TestRuntimeWriteContentBlockRejectsMalformedEvent(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteContentBlock(graph.ContentBlockStreamEvent{
			Event: graph.ContentBlockDelta, Index: 0, ContentBlock: map[string]any{"text": "missing type"},
		})
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, _ := builder.Compile()
	var got error
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Err != nil {
			got = event.Err
		}
	}
	if !errors.Is(got, graph.ErrInvalidContentBlockEvent) {
		t.Fatalf("stream err=%v", got)
	}
}

type identifiedMessage struct {
	ID      string
	Content string
}

func (m identifiedMessage) MessageID() string { return m.ID }

type messageHistoryState struct {
	Messages []prebuilt.AssistantMessage
}

type messageHistoryDelta struct {
	Messages []prebuilt.AssistantMessage
}

func messageHistoryReducer(_ context.Context, state messageHistoryState, updates []messageHistoryDelta) (messageHistoryState, error) {
	for _, update := range updates {
		state.Messages = append(state.Messages, update.Messages...)
	}
	return state, nil
}

func TestMessageStreamSeedsSeenIDsFromNodeInput(t *testing.T) {
	existing := prebuilt.AssistantMessage{ID: "history-1", Content: "history"}
	builder := graph.NewStateGraph(messageHistoryReducer)
	builder.SetInputMessageExtractor(func(_ context.Context, state messageHistoryState) ([]any, error) {
		messages := make([]any, len(state.Messages))
		for index, message := range state.Messages {
			messages[index] = message
		}
		return messages, nil
	})
	builder.SetMessageExtractor(func(_ context.Context, delta messageHistoryDelta) ([]graph.MessageEmission, error) {
		emissions := make([]graph.MessageEmission, len(delta.Messages))
		for index, message := range delta.Messages {
			emissions[index] = graph.MessageEmission{Message: message}
		}
		return emissions, nil
	})
	_ = builder.AddNode("model", func(_ context.Context, _ messageHistoryState, runtime graph.Runtime) (graph.Command[messageHistoryDelta], error) {
		if err := runtime.WriteMessage(prebuilt.AssistantMessage{ID: existing.ID, Content: "chunk"}); err != nil {
			return graph.Command[messageHistoryDelta]{}, err
		}
		return graph.Update(messageHistoryDelta{Messages: []prebuilt.AssistantMessage{existing}}), nil
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var contents []string
	for event := range compiled.StreamWithOptions(context.Background(), messageHistoryState{Messages: []prebuilt.AssistantMessage{existing}}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			contents = append(contents, event.Message.Message.(prebuilt.AssistantMessage).Content)
		}
	}
	if !reflect.DeepEqual(contents, []string{"chunk"}) {
		t.Fatalf("contents=%v", contents)
	}
}

func TestMessageStreamNormalizesAndDedupesRawMessageMaps(t *testing.T) {
	chunk := map[string]any{"role": "assistant", "id": "raw-1", "content": "chunk"}
	missing := map[string]any{"type": "ai", "content": "missing", "nested": map[string]any{"value": 1}}
	unrelated := map[string]any{"role": "unknown", "content": "ordinary"}
	final := map[string]any{"role": "assistant", "id": "raw-1", "content": "final"}
	builder := graph.NewStateGraph(customReducer)
	builder.SetMessageExtractor(func(context.Context, customDelta) ([]graph.MessageEmission, error) {
		return []graph.MessageEmission{{Message: final}}, nil
	})
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		for _, message := range []map[string]any{chunk, missing, unrelated} {
			if err := runtime.WriteMessage(message); err != nil {
				return graph.Command[customDelta]{}, err
			}
		}
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	generated := 0
	var messages []map[string]any
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
		MessageIDGenerator: func() (string, error) {
			generated++
			return "raw-generated", nil
		},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			messages = append(messages, event.Message.Message.(map[string]any))
		}
	}
	if len(messages) != 3 || messages[0]["id"] != "raw-1" || messages[1]["id"] != "raw-generated" {
		t.Fatalf("messages=%v", messages)
	}
	if _, exists := messages[2]["id"]; exists || generated != 1 {
		t.Fatalf("unrelated=%v generated=%d", messages[2], generated)
	}
	if _, exists := missing["id"]; exists {
		t.Fatalf("source missing message mutated: %v", missing)
	}
	messages[1]["nested"].(map[string]any)["value"] = 2
	if missing["nested"].(map[string]any)["value"] != 1 {
		t.Fatal("raw message nested map was aliased")
	}
}

func TestRawMessageInputIDSeedsFinalDedupe(t *testing.T) {
	type rawState struct{ Messages []map[string]any }
	type rawDelta struct{ Messages []map[string]any }
	reducer := func(_ context.Context, state rawState, updates []rawDelta) (rawState, error) {
		for _, update := range updates {
			state.Messages = append(state.Messages, update.Messages...)
		}
		return state, nil
	}
	history := map[string]any{"role": "user", "id": "input-1", "content": "history"}
	builder := graph.NewStateGraph(reducer)
	builder.SetInputMessageExtractor(func(_ context.Context, state rawState) ([]any, error) {
		result := make([]any, len(state.Messages))
		for index, message := range state.Messages {
			result[index] = message
		}
		return result, nil
	})
	builder.SetMessageExtractor(func(_ context.Context, delta rawDelta) ([]graph.MessageEmission, error) {
		return []graph.MessageEmission{{Message: delta.Messages[0]}}, nil
	})
	_ = builder.AddNode("node", func(context.Context, rawState, graph.Runtime) (graph.Command[rawDelta], error) {
		return graph.Update(rawDelta{Messages: []map[string]any{history}}), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for event := range compiled.StreamWithOptions(context.Background(), rawState{Messages: []map[string]any{history}}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			count++
		}
	}
	if count != 0 {
		t.Fatalf("message events=%d", count)
	}
}

func TestRawMessageRejectsNonStringID(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteMessage(map[string]any{"role": "assistant", "id": 42})
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var got error
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Err != nil {
			got = event.Err
		}
	}
	if got == nil || !strings.Contains(got.Error(), "want string") {
		t.Fatalf("error=%v", got)
	}
}

func TestNormalizeMessageValueMixedListPreservesShapeAndSources(t *testing.T) {
	typed := prebuilt.UserMessage{Content: "typed"}
	raw := map[string]any{"role": "assistant", "content": "raw", "nested": map[string]any{"value": 1}}
	ordinary := map[string]any{"role": "unknown", "content": "ordinary"}
	next := 0
	normalized, err := graph.NormalizeMessageValue([]any{typed, raw, ordinary}, func() (string, error) {
		next++
		return fmt.Sprintf("mixed-%d", next), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := normalized.([]any)
	if result[0].(prebuilt.UserMessage).ID != "mixed-1" || result[1].(map[string]any)["id"] != "mixed-2" || next != 2 {
		t.Fatalf("result=%v generated=%d", result, next)
	}
	if typed.ID != "" {
		t.Fatalf("typed source ID=%q", typed.ID)
	}
	if _, exists := raw["id"]; exists {
		t.Fatalf("raw source=%v", raw)
	}
	result[1].(map[string]any)["nested"].(map[string]any)["value"] = 2
	if raw["nested"].(map[string]any)["value"] != 1 || !reflect.DeepEqual(result[2], ordinary) {
		t.Fatalf("raw=%v ordinary=%v", raw, result[2])
	}
}

func TestNormalizeMessageValueReturnsNoPartialSliceOnError(t *testing.T) {
	value, err := graph.NormalizeMessageValue([]map[string]any{
		{"role": "assistant", "content": "first"},
		{"role": "assistant", "id": 42},
	}, func() (string, error) { return "generated", nil })
	if err == nil || value != nil || !strings.Contains(err.Error(), "message 1") {
		t.Fatalf("value=%v error=%v", value, err)
	}
}

func TestInputMessageExtractorIsSkippedOutsideMessagesMode(t *testing.T) {
	builder := graph.NewStateGraph(messageHistoryReducer)
	var calls atomic.Int32
	builder.SetInputMessageExtractor(func(context.Context, messageHistoryState) ([]any, error) {
		calls.Add(1)
		return nil, nil
	})
	_ = builder.AddNode("node", func(context.Context, messageHistoryState, graph.Runtime) (graph.Command[messageHistoryDelta], error) {
		return graph.NoCommand[messageHistoryDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), messageHistoryState{}, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	for event := range compiled.StreamWithOptions(context.Background(), messageHistoryState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamValues},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("extractor calls=%d", calls.Load())
	}
}

func TestInputMessageExtractorFailureStopsBeforeNode(t *testing.T) {
	want := errors.New("cannot inspect input")
	builder := graph.NewStateGraph(messageHistoryReducer)
	builder.SetInputMessageExtractor(func(context.Context, messageHistoryState) ([]any, error) {
		return nil, want
	})
	var nodeCalls atomic.Int32
	_ = builder.AddNode("node", func(context.Context, messageHistoryState, graph.Runtime) (graph.Command[messageHistoryDelta], error) {
		nodeCalls.Add(1)
		return graph.NoCommand[messageHistoryDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var got error
	for event := range compiled.StreamWithOptions(context.Background(), messageHistoryState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
	}) {
		if event.Err != nil {
			got = event.Err
		}
	}
	if !errors.Is(got, want) || nodeCalls.Load() != 0 {
		t.Fatalf("error=%v node calls=%d", got, nodeCalls.Load())
	}
}

func TestMessageStreamAssignsMissingIDsWithoutMutatingSource(t *testing.T) {
	chunk := prebuilt.AssistantMessage{Content: "chunk"}
	final := prebuilt.AssistantMessage{Content: "final"}
	builder := graph.NewStateGraph(customReducer)
	builder.SetMessageExtractor(func(context.Context, customDelta) ([]graph.MessageEmission, error) {
		return []graph.MessageEmission{{Message: final}}, nil
	})
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if err := runtime.WriteMessage(chunk); err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"stream-1", "stream-2"}
	var generated atomic.Int64
	var messages []prebuilt.Message
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
		MessageIDGenerator: func() (string, error) {
			index := generated.Add(1) - 1
			return ids[index], nil
		},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			messages = append(messages, event.Message.Message.(prebuilt.Message))
		}
	}
	if len(messages) != 2 || messages[0].MessageID() != "stream-1" || messages[1].MessageID() != "stream-2" {
		t.Fatalf("messages=%#v", messages)
	}
	if chunk.ID != "" || final.ID != "" {
		t.Fatalf("source messages mutated: chunk=%q final=%q", chunk.ID, final.ID)
	}
}

func TestMessageStreamIDGeneratorFailureIsTerminal(t *testing.T) {
	want := errors.New("entropy unavailable")
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteMessage(prebuilt.AssistantMessage{Content: "chunk"})
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var got error
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
		MessageIDGenerator: func() (string, error) {
			return "", want
		},
	}) {
		if event.Err != nil {
			got = event.Err
		}
	}
	if !errors.Is(got, want) {
		t.Fatalf("error=%v", got)
	}
}

func TestMessageStreamRejectsEmptyGeneratedID(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteMessage(prebuilt.UserMessage{Content: "hello"})
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var got error
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes:              []graph.StreamMode{graph.StreamMessages},
		MessageIDGenerator: func() (string, error) { return "", nil },
	}) {
		if event.Err != nil {
			got = event.Err
		}
	}
	if got == nil || !strings.Contains(got.Error(), "empty ID") {
		t.Fatalf("error=%v", got)
	}
}

func TestMessageStreamDefaultGeneratorAssignsUUID(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteMessage(prebuilt.UserMessage{Content: "hello"})
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var id string
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			id = event.Message.Message.(prebuilt.Message).MessageID()
		}
	}
	if len(id) != 36 || id[14] != '4' || (id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b') {
		t.Fatalf("message ID %q is not a UUID v4", id)
	}
}

func TestParallelMessageIDGenerationIsSerializedAndUnique(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	for _, name := range []graph.NodeID{"one", "two"} {
		name := name
		_ = builder.AddNode(name, func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), runtime.WriteMessage(prebuilt.AssistantMessage{Content: string(name)})
		})
		_ = builder.AddEdge(graph.START, name)
		_ = builder.AddEdge(name, graph.END)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	next := 0 // Deliberately not atomic: the stream owns generator serialization.
	seen := make(map[string]struct{})
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
		MessageIDGenerator: func() (string, error) {
			next++
			return fmt.Sprintf("parallel-%d", next), nil
		},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message != nil {
			seen[event.Message.Message.(prebuilt.Message).MessageID()] = struct{}{}
		}
	}
	if next != 2 || len(seen) != 2 {
		t.Fatalf("generated=%d seen=%v", next, seen)
	}
}

func TestRuntimeWriteMessageIncludesSchedulerMetadata(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if err := runtime.WriteMessage(testMessage{Role: "assistant", Content: "hel"}, map[string]any{
			"provider": "fake", "langgraph_node": "spoofed",
		}); err != nil {
			return graph.Command[customDelta]{}, err
		}
		if err := runtime.WriteMessage(testMessage{Role: "assistant", Content: "lo"}); err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	events := compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{
		ThreadID: "message-thread",
		RunID:    "message-run",
		Metadata: map[string]any{"assistant_id": "assistant-1"},
	}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}})
	var got []graph.StreamEvent[customState, customDelta]
	for event := range events {
		got = append(got, event)
	}
	if len(got) != 2 {
		t.Fatalf("message events=%d: %+v", len(got), got)
	}
	if got[0].Message == nil || got[1].Message == nil {
		t.Fatalf("missing message payload: %+v", got)
	}
	first, ok := got[0].Message.Message.(testMessage)
	if !ok || first.Content != "hel" {
		t.Fatalf("first message=%#v", got[0].Message.Message)
	}
	metadata := got[0].Message.Metadata
	if metadata["langgraph_node"] != "model" || metadata["langgraph_step"] != 0 ||
		metadata["thread_id"] != "message-thread" || metadata["run_id"] != "message-run" ||
		metadata["assistant_id"] != "assistant-1" || metadata["provider"] != "fake" {
		t.Fatalf("metadata=%v", metadata)
	}
	if metadata["langgraph_task_id"] == "" {
		t.Fatalf("task metadata=%v", metadata)
	}
}

func TestMessageExtractorEmitsMessagesFromNodeUpdates(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	builder.SetMessageExtractor(func(_ context.Context, delta customDelta) ([]graph.MessageEmission, error) {
		return []graph.MessageEmission{{
			Message:  testMessage{Role: "human", Content: "delta"},
			Metadata: map[string]any{"delta": delta.Add},
		}}, nil
	})
	_ = builder.AddNode("node", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 4}), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, _ := builder.Compile()
	var messages []*graph.MessageStreamEvent
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages},
	}) {
		messages = append(messages, event.Message)
	}
	if len(messages) != 1 || messages[0] == nil || messages[0].Metadata["delta"] != 4 {
		t.Fatalf("messages=%+v", messages)
	}
}

func TestMessageStreamDedupesFinalByIDButKeepsChunks(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	builder.SetMessageExtractor(func(context.Context, customDelta) ([]graph.MessageEmission, error) {
		return []graph.MessageEmission{
			{Message: identifiedMessage{ID: "shared", Content: "complete"}},
			{Message: identifiedMessage{ID: "other", Content: "other"}},
		}, nil
	})
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if err := runtime.WriteMessage(identifiedMessage{ID: "shared", Content: "hel"}); err != nil {
			return graph.Command[customDelta]{}, err
		}
		if err := runtime.WriteMessage(identifiedMessage{ID: "shared", Content: "lo"}); err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, _ := builder.Compile()
	var contents []string
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Message != nil {
			contents = append(contents, event.Message.Message.(identifiedMessage).Content)
		}
	}
	if !reflect.DeepEqual(contents, []string{"hel", "lo", "other"}) {
		t.Fatalf("contents=%v", contents)
	}
}

func TestMessageExtractorIsNotCalledWhenModeDisabled(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	var calls atomic.Int32
	builder.SetMessageExtractor(func(context.Context, customDelta) ([]graph.MessageEmission, error) {
		calls.Add(1)
		return nil, errors.New("must not run")
	})
	_ = builder.AddNode("node", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, _ := builder.Compile()
	if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamValues},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("extractor calls=%d", calls.Load())
	}
}

func TestSubgraphMessageStreamCarriesNamespace(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteMessage("child-token")
	})
	_ = childBuilder.AddEdge(graph.START, "model")
	_ = childBuilder.AddEdge("model", graph.END)
	child, _ := childBuilder.Compile()
	parentBuilder := graph.NewStateGraph(customReducer)
	_ = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
	})
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	parent, _ := parentBuilder.Compile()
	var found bool
	for event := range parent.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages}, Subgraphs: true,
	}) {
		if len(event.Namespace) > 0 && event.Subgraph != nil && event.Subgraph.Message != nil {
			found = event.Subgraph.Message.Message == "child-token" &&
				event.Subgraph.Message.Metadata["langgraph_node"] == "model"
		}
	}
	if !found {
		t.Fatal("no namespaced child message event")
	}
}

func TestMessageStreamCancellationUnblocksWriter(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	writerStopped := make(chan error, 1)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		for index := 0; index < 1000; index++ {
			if err := runtime.WriteMessage(index); err != nil {
				writerStopped <- err
				return graph.Command[customDelta]{}, err
			}
		}
		writerStopped <- nil
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, _ := builder.Compile()
	ctx, cancel := context.WithCancel(context.Background())
	events := compiled.StreamWithOptions(ctx, customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamMessages}, Buffer: 1,
	})
	if event, ok := <-events; !ok || event.Message == nil {
		t.Fatal("message stream closed before first chunk")
	}
	cancel()
	select {
	case err := <-writerStopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("writer err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("message writer remained blocked after cancellation")
	}
	for range events {
	}
}

func TestMessageMetadataUsesCurrentRunOnCheckpointReplay(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteMessage("token")
	})
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	saver := memory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.message-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.message-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "message-replay", Metadata: map[string]any{"request": "old"}}
	for range compiled.StreamWithOptions(context.Background(), customState{}, config, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
	}
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var inputCheckpoint string
	for _, item := range history {
		if len(item.Next) == 1 && item.Next[0] == "model" {
			inputCheckpoint = item.Config.CheckpointID
		}
	}
	if inputCheckpoint == "" {
		t.Fatal("input checkpoint not found")
	}
	var metadata map[string]any
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: inputCheckpoint, Metadata: map[string]any{"request": "new"},
	}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Message != nil {
			metadata = event.Message.Metadata
		}
	}
	if !reflect.DeepEqual(metadata["request"], "new") {
		t.Fatalf("metadata=%v", metadata)
	}
}
