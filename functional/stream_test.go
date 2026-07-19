package functional_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/functional"
	"github.com/ybszm/langgraph-go/graph"
)

func TestEntrypointStreamEmitsCustomFromEntrypointAndTask(t *testing.T) {
	task, _ := functional.NewTask("task", func(ctx context.Context, input int) (int, error) {
		if err := functional.Write(ctx, "task"); err != nil {
			return 0, err
		}
		return input * 2, nil
	})
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		if err := functional.Write(ctx, "entry"); err != nil {
			return 0, err
		}
		return task.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	events := entry.Stream(context.Background(), 3, functional.StreamOptions{Buffer: 1})
	var modes []functional.StreamMode
	var custom []any
	var output int
	for event := range events {
		modes = append(modes, event.Mode)
		if event.Mode == functional.StreamCustom {
			custom = append(custom, event.Custom)
		}
		if event.Mode == functional.StreamOutput {
			output = event.Output
		}
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if !reflect.DeepEqual(modes, []functional.StreamMode{functional.StreamCustom, functional.StreamCustom, functional.StreamOutput}) ||
		!reflect.DeepEqual(custom, []any{"entry", "task"}) || output != 6 {
		t.Fatalf("modes=%v custom=%v output=%d", modes, custom, output)
	}
}

func TestEntrypointStreamBackpressureCancellationUnblocksWriter(t *testing.T) {
	secondStarted := make(chan struct{})
	writeReturned := make(chan error, 1)
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, _ struct{}) (struct{}, error) {
		if err := functional.Write(ctx, "first"); err != nil {
			return struct{}{}, err
		}
		close(secondStarted)
		err := functional.Write(ctx, "second")
		writeReturned <- err
		return struct{}{}, err
	}, functional.EntrypointOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	events := entry.Stream(ctx, struct{}{}, functional.StreamOptions{Buffer: 1})
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second write did not start")
	}
	cancel()
	select {
	case err := <-writeReturned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("write error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked writer did not return")
	}
	for range events {
	}
}

func TestEntrypointStreamInvalidBufferAndInvokeWriteNoop(t *testing.T) {
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		if err := functional.Write(ctx, "ignored"); err != nil {
			return 0, err
		}
		return input, nil
	}, functional.EntrypointOptions{})
	if result, err := entry.Invoke(context.Background(), 7); err != nil || result != 7 {
		t.Fatalf("result=%d error=%v", result, err)
	}
	event := <-entry.Stream(context.Background(), 7, functional.StreamOptions{Buffer: -1})
	if event.Mode != functional.StreamError || event.Err == nil {
		t.Fatalf("event=%+v", event)
	}
}

func TestDurableEntrypointStreamCarriesInterrupts(t *testing.T) {
	entry, err := functional.NewDurableEntrypoint(
		"durable-stream",
		func(ctx context.Context, input string, _ *string) (functional.Final[string, string], error) {
			if err := functional.Write(ctx, "before-interrupt"); err != nil {
				return functional.Final[string, string]{}, err
			}
			response, err := functional.AwaitResume[string](ctx, input)
			return functional.Final[string, string]{Value: response, Save: response}, err
		},
		functional.DurableEntrypointConfig[string]{
			Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[string]("durable-stream", 1), EnableTaskRecovery: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	events := entry.Stream(context.Background(), "prompt", functional.DurableRunConfig{ThreadID: "thread"}, functional.StreamOptions{Buffer: 1})
	first := <-events
	second := <-events
	if first.Mode != functional.StreamCustom || first.Custom != "before-interrupt" ||
		second.Mode != functional.StreamError || !errors.Is(second.Err, functional.ErrInterrupted) || len(second.Interrupts) != 1 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestEntrypointDebugStreamReportsTaskLifecycle(t *testing.T) {
	task, _ := functional.NewTask("double", func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	})
	entry, _ := functional.NewEntrypoint("debug-entry", func(ctx context.Context, input int) (int, error) {
		return task.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	events := entry.Stream(context.Background(), 4, functional.StreamOptions{
		Buffer: 8, Modes: []functional.StreamMode{functional.StreamDebug, functional.StreamOutput},
	})
	var kinds []functional.DebugKind
	var taskID string
	output := 0
	for event := range events {
		if event.Mode == functional.StreamDebug {
			if event.Debug == nil {
				t.Fatal("debug event has nil payload")
			}
			kinds = append(kinds, event.Debug.Kind)
			if event.Debug.TaskName == "double" {
				if taskID == "" {
					taskID = event.Debug.TaskID
				} else if taskID != event.Debug.TaskID {
					t.Fatalf("task ID changed %q -> %q", taskID, event.Debug.TaskID)
				}
			}
		}
		if event.Mode == functional.StreamOutput {
			output = event.Output
		}
	}
	want := []functional.DebugKind{
		functional.DebugEntrypointStart, functional.DebugTaskStart,
		functional.DebugTaskResult, functional.DebugEntrypointResult,
	}
	if !reflect.DeepEqual(kinds, want) || taskID != "task:double:0" || output != 8 {
		t.Fatalf("kinds=%v taskID=%q output=%d", kinds, taskID, output)
	}
}

func TestDebugModeDoesNotLeakIntoLegacyStream(t *testing.T) {
	entry, _ := functional.NewEntrypoint("legacy", func(context.Context, int) (int, error) { return 1, nil }, functional.EntrypointOptions{})
	var modes []functional.StreamMode
	for event := range entry.Stream(context.Background(), 0, functional.StreamOptions{Buffer: 1}) {
		modes = append(modes, event.Mode)
	}
	if !reflect.DeepEqual(modes, []functional.StreamMode{functional.StreamOutput}) {
		t.Fatalf("legacy modes=%v", modes)
	}
}

func TestFunctionalMessageStreamCarriesTrustedTaskMetadata(t *testing.T) {
	metadata := map[string]any{"source": "tool", "task_id": "spoofed"}
	task, _ := functional.NewTask("emit", func(ctx context.Context, input string) (string, error) {
		if err := functional.WriteMessage(ctx, input, metadata); err != nil {
			return "", err
		}
		metadata["source"] = "mutated"
		return input, nil
	})
	entry, _ := functional.NewEntrypoint("message-entry", func(ctx context.Context, input string) (string, error) {
		return task.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	var message *functional.MessageEvent
	for event := range entry.Stream(context.Background(), "chunk", functional.StreamOptions{
		Buffer: 2, Modes: []functional.StreamMode{functional.StreamMessages, functional.StreamOutput},
	}) {
		if event.Mode == functional.StreamMessages {
			message = event.Message
		}
	}
	if message == nil || message.Message != "chunk" || message.Metadata["task_id"] != "task:emit:0" ||
		message.Metadata["task_name"] != "emit" || message.Metadata["source"] != "tool" {
		t.Fatalf("message=%+v", message)
	}
}

func TestWriteMessageInvokeNoopAndArgumentValidation(t *testing.T) {
	entry, _ := functional.NewEntrypoint("message-noop", func(ctx context.Context, input int) (int, error) {
		if err := functional.WriteMessage(ctx, "ignored"); err != nil {
			return 0, err
		}
		if err := functional.WriteMessage(ctx, "bad", map[string]any{}, map[string]any{}); err == nil {
			return 0, errors.New("multiple metadata maps accepted")
		}
		return input, nil
	}, functional.EntrypointOptions{})
	if result, err := entry.Invoke(context.Background(), 3); err != nil || result != 3 {
		t.Fatalf("Invoke result=%d err=%v", result, err)
	}
}

type providerChunkFixture struct {
	ID   string
	Text string
}

func TestFunctionalProviderChunkAdapterEmitsRichLifecycleWithOwnedExtensions(t *testing.T) {
	extensionBytes := []byte{1, 2, 3}
	adapterMetadata := map[string]any{"provider": "fixture", "region": "adapter"}
	adapter := functional.ProviderChunkAdapterFunc[providerChunkFixture](func(_ context.Context, chunk providerChunkFixture) (functional.ProviderChunk, error) {
		return functional.ProviderChunk{
			Message:  map[string]any{"role": "assistant", "id": chunk.ID, "content": chunk.Text},
			Metadata: adapterMetadata,
			ContentBlocks: []graph.ContentBlockStreamEvent{
				{Event: graph.ContentMessageStart, Role: "assistant", MessageID: chunk.ID},
				{Event: graph.ContentBlockStart, Index: 0, ContentBlock: map[string]any{"type": "text", "text": "", "provider_extension": map[string]any{"bytes": extensionBytes}}},
				{Event: graph.ContentBlockDelta, Index: 0, ContentBlock: map[string]any{"type": "text_delta", "text": chunk.Text}},
				{Event: graph.ContentBlockFinish, Index: 0, ContentBlock: map[string]any{"type": "text", "text": chunk.Text}},
				{Event: graph.ContentMessageFinish, MessageID: chunk.ID, Reason: "stop"},
			},
		}, nil
	})
	task, _ := functional.NewTask("provider", func(ctx context.Context, chunk providerChunkFixture) (string, error) {
		err := functional.WriteProviderChunk(ctx, chunk, adapter, map[string]any{"region": "caller", "task_id": "spoofed"})
		extensionBytes[0] = 9
		adapterMetadata["provider"] = "mutated"
		return chunk.Text, err
	})
	entry, _ := functional.NewEntrypoint("provider-entry", func(ctx context.Context, chunk providerChunkFixture) (string, error) {
		return task.Call(ctx, chunk).Await(ctx)
	}, functional.EntrypointOptions{})

	var events []*functional.MessageEvent
	for event := range entry.Stream(context.Background(), providerChunkFixture{ID: "msg-1", Text: "hello"}, functional.StreamOptions{
		Buffer: 8, Modes: []functional.StreamMode{functional.StreamMessages, functional.StreamOutput},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Mode == functional.StreamMessages {
			events = append(events, event.Message)
		}
	}
	if len(events) != 6 || events[0].Message == nil {
		t.Fatalf("message events=%+v", events)
	}
	wantKinds := []graph.ContentBlockEventKind{
		graph.ContentMessageStart, graph.ContentBlockStart, graph.ContentBlockDelta,
		graph.ContentBlockFinish, graph.ContentMessageFinish,
	}
	for index, want := range wantKinds {
		event := events[index+1]
		if event.ContentBlock == nil || event.ContentBlock.Event != want {
			t.Fatalf("event %d=%+v want=%q", index, event, want)
		}
		if event.Metadata["task_id"] != "task:provider:0" || event.Metadata["task_name"] != "provider" ||
			event.Metadata["provider"] != "fixture" || event.Metadata["region"] != "caller" {
			t.Fatalf("metadata %d=%+v", index, event.Metadata)
		}
	}
	extension := events[2].ContentBlock.ContentBlock["provider_extension"].(map[string]any)
	if extension["bytes"].([]byte)[0] != 1 {
		t.Fatalf("provider extension was aliased: %+v", extension)
	}
}

func TestFunctionalProviderChunkAdapterRejectsInvalidLifecycleEvent(t *testing.T) {
	adapter := functional.ProviderChunkAdapterFunc[string](func(context.Context, string) (functional.ProviderChunk, error) {
		return functional.ProviderChunk{ContentBlocks: []graph.ContentBlockStreamEvent{{
			Event: graph.ContentBlockDelta, Index: 0, ContentBlock: map[string]any{"text": "missing type"},
		}}}, nil
	})
	entry, _ := functional.NewEntrypoint("invalid-provider", func(ctx context.Context, input string) (string, error) {
		return input, functional.WriteProviderChunk(ctx, input, adapter)
	}, functional.EntrypointOptions{})
	if _, err := entry.Invoke(context.Background(), "x"); !errors.Is(err, graph.ErrInvalidContentBlockEvent) {
		t.Fatalf("Invoke err=%v", err)
	}
}
