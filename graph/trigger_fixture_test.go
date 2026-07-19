package graph_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
)

func TestDebugTriggerLabelsMatchPython129Fixture(t *testing.T) {
	data, err := os.ReadFile("testdata/langgraph-1.2.9-trigger-labels.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		UpstreamVersion string            `json:"upstream_version"`
		UpstreamCommit  string            `json:"upstream_commit"`
		Labels          map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.UpstreamVersion != "1.2.9" || fixture.UpstreamCommit == "" {
		t.Fatalf("fixture provenance=%+v", fixture)
	}
	builder := graph.NewStateGraph(customReducer)
	for _, id := range []graph.NodeID{"source", "conditional-target", "left", "right", "join", "send-source", "sent"} {
		id := id
		if err := builder.AddNode(id, func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
			if id == "send-source" {
				return graph.Dispatch[customDelta](graph.SendTo("sent", customState{})), nil
			}
			return graph.NoCommand[customDelta](), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []graph.NodeID{"source", "left", "right", "send-source"} {
		_ = builder.AddEdge(graph.START, id)
	}
	if err := builder.AddNamedConditionalEdges("source", "choice", func(context.Context, customState) ([]graph.NodeID, error) {
		return []graph.NodeID{"conditional-target"}, nil
	}, "conditional-target"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddWaitingEdge([]graph.NodeID{"left", "right"}, "join"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []graph.NodeID{"conditional-target", "join", "sent"} {
		_ = builder.AddEdge(id, graph.END)
	}
	if err := builder.AddCommandDestinations("send-source", "sent"); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]string)
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamDebug}}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug == nil || event.Debug.Kind != graph.DebugTask || len(event.Debug.Triggers) != 1 {
			continue
		}
		switch event.Debug.Node {
		case "source":
			got["static"] = string(event.Debug.Triggers[0])
		case "conditional-target":
			got["conditional"] = string(event.Debug.Triggers[0])
		case "join":
			got["waiting"] = string(event.Debug.Triggers[0])
		case "sent":
			got["send"] = string(event.Debug.Triggers[0])
		}
	}
	if !reflect.DeepEqual(got, fixture.Labels) {
		t.Fatalf("triggers=%v fixture=%v", got, fixture.Labels)
	}
}
