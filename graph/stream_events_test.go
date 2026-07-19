package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
)

func TestStreamEventsV3EmitsRunTreeAndContentBlocks(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("model", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if err := runtime.WriteContentBlock(graph.ContentBlockStreamEvent{
			Event: graph.ContentBlockDelta, Index: 0,
			ContentBlock: map[string]any{"type": "text", "text": "hello"},
		}); err != nil {
			return graph.NoCommand[customDelta](), err
		}
		return graph.Update(customDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "model")
	_ = builder.AddEdge("model", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{RunID: "root", ParentRunID: "external", RunName: "agent", Tags: []string{"test"}}
	var events []graph.RunStreamEvent
	for event := range compiled.StreamEvents(context.Background(), customState{}, config, graph.StreamEventsV3) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		events = append(events, event)
	}
	got := make([]string, len(events))
	for index, event := range events {
		got[index] = event.Event + ":" + event.Name
	}
	want := []string{
		"on_chain_start:agent", "on_chain_start:model", "on_chat_model_stream:model",
		"on_chain_end:model", "on_chain_stream:agent", "on_chain_end:agent",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%+v", events)
	}
	if !reflect.DeepEqual(events[0].ParentIDs, []string{"external"}) ||
		!reflect.DeepEqual(events[1].ParentIDs, []string{"external", "root"}) ||
		events[1].RunID == "root" || !reflect.DeepEqual(events[2].Tags, []string{"test"}) {
		t.Fatalf("run tree=%+v", events)
	}
	chunk, ok := events[2].Data["chunk"].(*graph.ContentBlockStreamEvent)
	if !ok || chunk.ContentBlock["text"] != "hello" {
		t.Fatalf("message event=%+v", events[2])
	}
}

func TestStreamEventsRejectsUnsupportedVersion(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, _ := builder.Compile()
	events := compiled.StreamEvents(context.Background(), customState{}, graph.RunConfig{}, graph.StreamEventsVersion("v2"))
	event, ok := <-events
	if !ok || !errors.Is(event.Err, graph.ErrUnsupportedStreamEventsVersion) {
		t.Fatalf("event=%+v ok=%v", event, ok)
	}
}
