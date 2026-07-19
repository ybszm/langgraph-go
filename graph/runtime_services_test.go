package graph_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
)

func TestAttachRuntimeServicesProvidesStableTypedInterruptResume(t *testing.T) {
	runtime, err := graph.AttachRuntimeServices(context.Background(), graph.Runtime{
		TaskID: "task", ThreadID: "thread", CheckpointNamespace: "ns", CheckpointID: "cp",
	}, graph.RuntimeServices{Resume: func(_ context.Context, request graph.InterruptRequest) (json.RawMessage, error) {
		if request.Interrupt.ID == "" || request.Interrupt.Namespace != "ns" || request.Index != 0 || string(request.Interrupt.Value) != `{"question":"continue?"}` {
			t.Fatalf("request=%+v", request)
		}
		return json.RawMessage(`"yes"`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := graph.AwaitResume[string](runtime, map[string]string{"question": "continue?"})
	if err != nil || answer != "yes" {
		t.Fatalf("answer=%q err=%v", answer, err)
	}
}

func TestAttachRuntimeServicesInterruptSequenceHasDistinctIDs(t *testing.T) {
	var ids []string
	runtime, _ := graph.AttachRuntimeServices(context.Background(), graph.Runtime{TaskID: "task", CheckpointNamespace: "ns"}, graph.RuntimeServices{
		Resume: func(_ context.Context, request graph.InterruptRequest) (json.RawMessage, error) {
			ids = append(ids, request.Interrupt.ID)
			return json.RawMessage(`1`), nil
		},
	})
	_, _ = graph.AwaitResume[int](runtime, "one")
	_, _ = graph.AwaitResume[int](runtime, "two")
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("ids=%v", ids)
	}
}

func TestRuntimeServicesCompositeInterruptScopesHaveLocalSequences(t *testing.T) {
	var requests []graph.InterruptRequest
	runtime, err := graph.AttachRuntimeServices(context.Background(), graph.Runtime{
		TaskID: "task", CheckpointNamespace: "ns",
	}, graph.RuntimeServices{Resume: func(_ context.Context, request graph.InterruptRequest) (json.RawMessage, error) {
		requests = append(requests, request)
		return json.RawMessage(`"ok"`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	order := graph.NewInterruptOrder(2)
	first := order.Runtime(runtime, 0)
	second := order.Runtime(runtime, 1)
	_, _ = graph.AwaitResume[string](first, "first-0")
	_, _ = graph.AwaitResume[string](first, "first-1")
	order.Done(0)
	_, _ = graph.AwaitResume[string](second, "second-0")
	_, _ = graph.AwaitResume[string](second, "second-1")
	order.Done(1)
	if len(requests) != 4 {
		t.Fatalf("requests=%+v", requests)
	}
	for index, request := range requests {
		wantScope := "child:0"
		if index >= 2 {
			wantScope = "child:1"
		}
		if request.Scope != wantScope || request.Index != index%2 {
			t.Fatalf("request[%d]=%+v", index, request)
		}
	}
	if requests[0].Interrupt.ID == requests[2].Interrupt.ID ||
		requests[1].Interrupt.ID == requests[3].Interrupt.ID {
		t.Fatalf("scoped IDs collide: %+v", requests)
	}
}
