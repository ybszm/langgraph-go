package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	cachememory "github.com/ybszm/langgraph-go/cache/memory"
	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type sendState struct {
	Subjects []string
	Subject  string
	Results  []string
}

type sendDelta struct{ Result string }

func sendReducer(_ context.Context, state sendState, updates []sendDelta) (sendState, error) {
	state.Subjects = append([]string(nil), state.Subjects...)
	state.Results = append([]string(nil), state.Results...)
	for _, update := range updates {
		if update.Result != "" {
			state.Results = append(state.Results, update.Result)
		}
	}
	return state, nil
}

func sendGraph(t *testing.T, saver checkpoint.Saver) *graph.CompiledGraph[sendState, sendDelta] {
	t.Helper()
	builder := graph.NewStateGraph(sendReducer)
	if err := builder.AddNode("plan", func(context.Context, sendState, graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.NoCommand[sendDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("worker", func(_ context.Context, state sendState, _ graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.Update(sendDelta{Result: state.Subject}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "plan"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddSendEdges("plan", func(_ context.Context, state sendState) ([]graph.Send[sendState], error) {
		result := make([]graph.Send[sendState], len(state.Subjects))
		for index, subject := range state.Subjects {
			result[index] = graph.Send[sendState]{Node: "worker", State: sendState{Subject: subject}}
		}
		return result, nil
	}, "worker"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("worker", graph.END); err != nil {
		t.Fatal(err)
	}
	if saver == nil {
		compiled, err := builder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[sendState, sendDelta]{
		Saver:       saver,
		StateCodec:  checkpoint.MustJSONCodec[sendState]("send/state", 1),
		DeltaCodec:  checkpoint.MustJSONCodec[sendDelta]("send/delta", 1),
		Clock:       checkpoint.ClockFunc(func() time.Time { return time.Unix(1_700_000_000, 0) }),
		IDGenerator: &sequenceIDGenerator{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestSendCreatesDistinctOrderedTasksForSameNode(t *testing.T) {
	compiled := sendGraph(t, nil)
	result, err := compiled.Invoke(context.Background(), sendState{Subjects: []string{"alpha", "beta", "alpha"}}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Results, []string{"alpha", "beta", "alpha"}) {
		t.Fatalf("results=%#v", result.Results)
	}
}

func commandSendGraph(t *testing.T, saver checkpoint.Saver) *graph.CompiledGraph[sendState, sendDelta] {
	t.Helper()
	builder := graph.NewStateGraph(sendReducer)
	_ = builder.AddNode("plan", func(_ context.Context, state sendState, _ graph.Runtime) (graph.Command[sendDelta], error) {
		sends := make([]graph.TaskSend, len(state.Subjects))
		for index, subject := range state.Subjects {
			sends[index] = graph.SendTo("worker", sendState{Subject: subject})
		}
		return graph.Dispatch[sendDelta](sends...), nil
	})
	_ = builder.AddNode("worker", func(_ context.Context, state sendState, _ graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.Update(sendDelta{Result: state.Subject}), nil
	})
	_ = builder.AddEdge(graph.START, "plan")
	_ = builder.AddCommandDestinations("plan", "worker")
	_ = builder.AddEdge("worker", graph.END)
	if saver == nil {
		compiled, err := builder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[sendState, sendDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[sendState]("command-send/state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[sendDelta]("command-send/delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestCommandDispatchCreatesRepeatedTypedTasks(t *testing.T) {
	compiled := commandSendGraph(t, nil)
	result, err := compiled.Invoke(context.Background(), sendState{Subjects: []string{"x", "y", "x"}}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Results, []string{"x", "y", "x"}) {
		t.Fatalf("Command Sends results=%v", result.Results)
	}
}

func TestCommandDispatchPersistsSendEnvelopeAndTaskInputs(t *testing.T) {
	compiled := commandSendGraph(t, memory.NewSaver())
	config := graph.RunConfig{ThreadID: "command-send"}
	result, err := compiled.Invoke(context.Background(), sendState{Subjects: []string{"a", "b"}}, config)
	if err != nil || !reflect.DeepEqual(result.Results, []string{"a", "b"}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	foundInputs := false
	for _, snapshot := range history {
		if len(snapshot.Tasks) == 2 && snapshot.Tasks[0].Input != nil && snapshot.Tasks[1].Input != nil {
			foundInputs = snapshot.Tasks[0].Input.Subject == "a" && snapshot.Tasks[1].Input.Subject == "b"
		}
	}
	if !foundInputs {
		t.Fatal("persisted Command Sends did not produce durable typed task inputs")
	}
}

func TestCommandDispatchRejectsWrongStateType(t *testing.T) {
	builder := graph.NewStateGraph(sendReducer)
	_ = builder.AddNode("plan", func(context.Context, sendState, graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.Dispatch[sendDelta](graph.SendTo("worker", "wrong")), nil
	})
	_ = builder.AddNode("worker", func(context.Context, sendState, graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.NoCommand[sendDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "plan")
	_ = builder.AddCommandDestinations("plan", "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), sendState{}, graph.RunConfig{}); err == nil {
		t.Fatal("wrong Command Send state type was accepted")
	}
}

func TestCommandDispatchRoundTripsThroughTaskCache(t *testing.T) {
	builder := graph.NewStateGraph(sendReducer)
	var calls atomic.Int32
	_ = builder.AddNode("plan", func(_ context.Context, state sendState, _ graph.Runtime) (graph.Command[sendDelta], error) {
		calls.Add(1)
		sends := make([]graph.TaskSend, len(state.Subjects))
		for index, subject := range state.Subjects {
			sends[index] = graph.SendTo("worker", sendState{Subject: subject})
		}
		return graph.Dispatch[sendDelta](sends...), nil
	}, graph.WithCachePolicy(graph.CachePolicy[sendState]{}))
	_ = builder.AddNode("worker", func(_ context.Context, state sendState, _ graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.Update(sendDelta{Result: state.Subject}), nil
	})
	_ = builder.AddEdge(graph.START, "plan")
	_ = builder.AddCommandDestinations("plan", "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, err := builder.Compile(graph.WithTaskCache[sendState, sendDelta](graph.TaskCacheConfig[sendDelta]{
		Store: cachememory.New(), Namespace: "command-send",
		StateCodec: checkpoint.MustJSONCodec[sendState]("cache-command-send/state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[sendDelta]("cache-command-send/delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	input := sendState{Subjects: []string{"a", "b"}}
	for iteration := 0; iteration < 2; iteration++ {
		result, err := compiled.Invoke(context.Background(), input, graph.RunConfig{})
		if err != nil || !reflect.DeepEqual(result.Results, []string{"a", "b"}) {
			t.Fatalf("iteration %d result=%+v err=%v", iteration, result, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("cached plan calls=%d", calls.Load())
	}
}

func TestSendTaskInputsPersistAndReplay(t *testing.T) {
	saver := memory.NewSaver()
	compiled := sendGraph(t, saver)
	config := graph.RunConfig{ThreadID: "send-persistence"}
	input := sendState{Subjects: []string{"a", "b"}}
	result, err := compiled.Invoke(context.Background(), input, config)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Results, []string{"a", "b"}) {
		t.Fatalf("result=%#v", result)
	}
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var fanout graph.StateSnapshot[sendState, sendDelta]
	for _, snapshot := range history {
		if len(snapshot.Next) == 2 {
			fanout = snapshot
			break
		}
	}
	if len(fanout.Tasks) != 2 || fanout.Tasks[0].Input == nil || fanout.Tasks[1].Input == nil {
		t.Fatalf("fanout tasks=%#v", fanout.Tasks)
	}
	if fanout.Tasks[0].Input.Subject != "a" || fanout.Tasks[1].Input.Subject != "b" {
		t.Fatalf("task inputs=%#v %#v", fanout.Tasks[0].Input, fanout.Tasks[1].Input)
	}
	replayed, err := compiled.Invoke(context.Background(), sendState{}, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: fanout.Config.CheckpointID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed.Results, []string{"a", "b"}) {
		t.Fatalf("replayed=%#v", replayed)
	}
}

func TestSendRouterRejectsUndeclaredTarget(t *testing.T) {
	builder := graph.NewStateGraph(sendReducer)
	if err := builder.AddNode("plan", func(context.Context, sendState, graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.NoCommand[sendDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("declared", func(context.Context, sendState, graph.Runtime) (graph.Command[sendDelta], error) {
		return graph.NoCommand[sendDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "plan"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddSendEdges("plan", func(context.Context, sendState) ([]graph.Send[sendState], error) {
		return []graph.Send[sendState]{{Node: "missing"}}, nil
	}, "declared"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("declared", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), sendState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrUnknownNode) {
		t.Fatalf("error=%v", err)
	}
}
