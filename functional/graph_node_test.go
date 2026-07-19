package functional_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/functional"
	"github.com/wahanbo/langgraph-go/graph"
)

type functionalNodeState struct{ Value int }
type functionalNodeDelta struct{ Add int }

func functionalNodeReducer(_ context.Context, state functionalNodeState, updates []functionalNodeDelta) (functionalNodeState, error) {
	for _, update := range updates {
		state.Value += update.Add
	}
	return state, nil
}

func TestFunctionalNewNodeRunsTasksInsideStateGraph(t *testing.T) {
	double, _ := functional.NewTask("double", func(_ context.Context, input int) (int, error) { return input * 2, nil })
	node, err := functional.NewNode("worker", func(ctx context.Context, state functionalNodeState, _ graph.Runtime) (graph.Command[functionalNodeDelta], error) {
		value, err := double.Call(ctx, state.Value).Await(ctx)
		if err != nil {
			return graph.Command[functionalNodeDelta]{}, err
		}
		return graph.Update(functionalNodeDelta{Add: value}), nil
	}, functional.EntrypointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	builder := graph.NewStateGraph(functionalNodeReducer)
	_ = builder.AddNode("worker", node)
	_ = builder.AddEdge(graph.START, "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), functionalNodeState{Value: 3}, graph.RunConfig{})
	if err != nil || result.Value != 9 {
		t.Fatalf("state=%+v error=%v", result, err)
	}
}

func TestFunctionalNewNodeUnawaitedTaskErrorFailsNode(t *testing.T) {
	boom := errors.New("boom")
	failing, _ := functional.NewTask("failing", func(context.Context, int) (int, error) { return 0, boom })
	node, err := functional.NewNode("worker", func(ctx context.Context, _ functionalNodeState, _ graph.Runtime) (graph.Command[functionalNodeDelta], error) {
		failing.Call(ctx, 1)
		return graph.NoCommand[functionalNodeDelta](), nil
	}, functional.EntrypointOptions{})
	if err != nil {
		t.Fatal(err)
	}
	builder := graph.NewStateGraph(functionalNodeReducer)
	_ = builder.AddNode("worker", node)
	_ = builder.AddEdge(graph.START, "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, _ := builder.Compile()
	_, err = compiled.Invoke(context.Background(), functionalNodeState{}, graph.RunConfig{})
	if !errors.Is(err, boom) {
		t.Fatalf("error=%v", err)
	}
}

func TestFunctionalNewNodeForwardsCustomStream(t *testing.T) {
	task, _ := functional.NewTask("writer", func(ctx context.Context, input int) (int, error) {
		if err := functional.Write(ctx, "from-task"); err != nil {
			return 0, err
		}
		return input, nil
	})
	node, _ := functional.NewNode("worker", func(ctx context.Context, state functionalNodeState, _ graph.Runtime) (graph.Command[functionalNodeDelta], error) {
		_, err := task.Call(ctx, state.Value).Await(ctx)
		return graph.NoCommand[functionalNodeDelta](), err
	}, functional.EntrypointOptions{})
	builder := graph.NewStateGraph(functionalNodeReducer)
	_ = builder.AddNode("worker", node)
	_ = builder.AddEdge(graph.START, "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, _ := builder.Compile()
	events := compiled.StreamWithOptions(context.Background(), functionalNodeState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamCustom}})
	seen := false
	for event := range events {
		if event.Mode == graph.StreamCustom && event.Custom == "from-task" {
			seen = true
		}
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if !seen {
		t.Fatal("missing functional custom event")
	}
}

func TestFunctionalGraphNodeDurablyResumesTaskInterruptAndReusesPeerResult(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	var draftCalls atomic.Int32
	var approvalCalls atomic.Int32
	draft, _ := functional.NewTask("draft", func(_ context.Context, input int) (int, error) {
		draftCalls.Add(1)
		return input * 2, nil
	}, functional.TaskOptions[int, int]{Persistence: &functional.TaskPersistencePolicy[int, int]{
		Codec: checkpoint.MustJSONCodec[int]("tests.graph-node-draft", 1),
	}})
	approval, _ := functional.NewTask("approval", func(ctx context.Context, _ int) (string, error) {
		approvalCalls.Add(1)
		return functional.AwaitResume[string](ctx, "approve draft?")
	})
	node, _ := functional.NewNode("worker", func(ctx context.Context, state functionalNodeState, _ graph.Runtime) (graph.Command[functionalNodeDelta], error) {
		value, err := draft.Call(ctx, state.Value).Await(ctx)
		if err != nil {
			return graph.Command[functionalNodeDelta]{}, err
		}
		if _, err := approval.Call(ctx, value).Await(ctx); err != nil {
			return graph.Command[functionalNodeDelta]{}, err
		}
		return graph.Update(functionalNodeDelta{Add: value}), nil
	}, functional.EntrypointOptions{MaxConcurrency: 2})
	builder := graph.NewStateGraph(functionalNodeReducer)
	_ = builder.AddNode("worker", node, graph.WithDynamicInterrupts())
	_ = builder.AddEdge(graph.START, "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[functionalNodeState, functionalNodeDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[functionalNodeState]("tests.graph-node-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[functionalNodeDelta]("tests.graph-node-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "functional-graph-node-interrupt"}
	if _, err := compiled.Invoke(context.Background(), functionalNodeState{Value: 2}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || len(snapshot.Interrupts) != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	prompts, err := graph.DecodeInterrupt[[]functional.Interrupt](snapshot.Interrupts[0])
	if err != nil || len(prompts) != 1 {
		t.Fatalf("prompts=%+v err=%v", prompts, err)
	}
	resume, _ := graph.Resume(map[string]any{prompts[0].ID: "approved"})
	result, err := compiled.Resume(context.Background(), config, resume)
	if err != nil || result.Value != 6 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if draftCalls.Load() != 1 || approvalCalls.Load() != 2 {
		t.Fatalf("draft calls=%d approval calls=%d", draftCalls.Load(), approvalCalls.Load())
	}
}

func TestFunctionalGraphNodeRecoversTaskResultAfterNodeFailure(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	var taskCalls atomic.Int32
	var nodeCalls atomic.Int32
	task, _ := functional.NewTask("expensive", func(_ context.Context, input int) (int, error) {
		taskCalls.Add(1)
		return input + 4, nil
	}, functional.TaskOptions[int, int]{Persistence: &functional.TaskPersistencePolicy[int, int]{
		Codec: checkpoint.MustJSONCodec[int]("tests.graph-node-expensive", 1),
	}})
	transient := errors.New("node failed after task commit")
	node, _ := functional.NewNode("worker", func(ctx context.Context, state functionalNodeState, _ graph.Runtime) (graph.Command[functionalNodeDelta], error) {
		value, err := task.Call(ctx, state.Value).Await(ctx)
		if err != nil {
			return graph.Command[functionalNodeDelta]{}, err
		}
		if nodeCalls.Add(1) == 1 {
			return graph.Command[functionalNodeDelta]{}, transient
		}
		return graph.Update(functionalNodeDelta{Add: value}), nil
	}, functional.EntrypointOptions{})
	builder := graph.NewStateGraph(functionalNodeReducer)
	_ = builder.AddNode("worker", node)
	_ = builder.AddEdge(graph.START, "worker")
	_ = builder.AddEdge("worker", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[functionalNodeState, functionalNodeDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[functionalNodeState]("tests.graph-node-recovery-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[functionalNodeDelta]("tests.graph-node-recovery-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "functional-graph-node-recovery"}
	if _, err := compiled.Invoke(context.Background(), functionalNodeState{Value: 1}, config); !errors.Is(err, transient) {
		t.Fatalf("first err=%v", err)
	}
	result, err := compiled.Invoke(context.Background(), functionalNodeState{Value: 100}, config)
	if err != nil || result.Value != 6 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if taskCalls.Load() != 1 || nodeCalls.Load() != 2 {
		t.Fatalf("task calls=%d node calls=%d", taskCalls.Load(), nodeCalls.Load())
	}
}
