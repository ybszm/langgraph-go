package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

type lifecycleRecorder struct {
	mu         sync.Mutex
	interrupts []graph.GraphInterruptEvent
	resumes    []graph.GraphResumeEvent
	order      []string
}

type traceRecorder struct {
	mu        sync.Mutex
	order     []string
	graphs    []any
	nodeStart []graph.NodeRunStartEvent
	nodeEnd   []graph.NodeRunEndEvent
	nodeError []graph.NodeRunErrorEvent
}

func (r *traceRecorder) record(label string, event any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, label)
	if event != nil {
		r.graphs = append(r.graphs, event)
	}
}

func (r *traceRecorder) OnInterrupt(context.Context, graph.GraphInterruptEvent) {}
func (r *traceRecorder) OnResume(context.Context, graph.GraphResumeEvent)       {}
func (r *traceRecorder) OnGraphStart(_ context.Context, event graph.GraphRunStartEvent) {
	r.record("graph:start", event)
}
func (r *traceRecorder) OnGraphEnd(_ context.Context, event graph.GraphRunEndEvent) {
	r.record("graph:end", event)
}
func (r *traceRecorder) OnGraphError(_ context.Context, event graph.GraphRunErrorEvent) {
	r.record("graph:error", event)
}
func (r *traceRecorder) OnNodeStart(_ context.Context, event graph.NodeRunStartEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, "node:start:"+string(event.Node))
	r.nodeStart = append(r.nodeStart, event)
}
func (r *traceRecorder) OnNodeEnd(_ context.Context, event graph.NodeRunEndEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, "node:end:"+string(event.Node))
	r.nodeEnd = append(r.nodeEnd, event)
}
func (r *traceRecorder) OnNodeError(_ context.Context, event graph.NodeRunErrorEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, "node:error:"+string(event.Node))
	r.nodeError = append(r.nodeError, event)
}

func TestRunCallbacksTraceGraphAndNodeSuccess(t *testing.T) {
	recorder := &traceRecorder{}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "first", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addNode(t, builder, "second", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 2}), nil
	})
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)
	config := graph.RunConfig{
		RunID: "trace-run", ParentRunID: "external-parent", RunName: "root", Tags: []string{"tenant:acme"},
		ThreadID: "trace-thread", Metadata: map[string]any{"tenant": "acme"},
		Callbacks: []graph.GraphCallback{recorder},
	}
	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{Total: 4}, config)
	if err != nil || result.Total != 7 {
		t.Fatalf("Invoke() result=%+v err=%v", result, err)
	}
	wantOrder := []string{"graph:start", "node:start:first", "node:end:first", "node:start:second", "node:end:second", "graph:end"}
	if !reflect.DeepEqual(recorder.order, wantOrder) {
		t.Fatalf("callback order=%v want=%v", recorder.order, wantOrder)
	}
	if len(recorder.graphs) != 2 || len(recorder.nodeStart) != 2 || len(recorder.nodeEnd) != 2 {
		t.Fatalf("graphs=%+v starts=%+v ends=%+v", recorder.graphs, recorder.nodeStart, recorder.nodeEnd)
	}
	start := recorder.nodeStart[0]
	if start.RunID == "trace-run" || start.ParentRunID != "trace-run" || start.Name != "first" ||
		!reflect.DeepEqual(start.Tags, []string{"tenant:acme"}) || start.ThreadID != "trace-thread" ||
		start.Node != "first" || start.Step != 0 || start.Attempt != 1 || start.TaskID == "" {
		t.Fatalf("node start=%+v", start)
	}
	graphStart := recorder.graphs[0].(graph.GraphRunStartEvent)
	if graphStart.ParentRunID != "external-parent" || graphStart.Name != "root" || !reflect.DeepEqual(graphStart.Tags, []string{"tenant:acme"}) {
		t.Fatalf("graph start=%+v", graphStart)
	}
	if start.Metadata["tenant"] != "acme" {
		t.Fatalf("node metadata=%v", start.Metadata)
	}
	config.Metadata["tenant"] = "changed"
	if start.Metadata["tenant"] != "acme" {
		t.Fatalf("callback metadata aliased caller: %v", start.Metadata)
	}
	if command, ok := recorder.nodeEnd[0].Output.(graph.Command[testDelta]); !ok || !command.HasUpdate || command.Update.Add != 1 {
		t.Fatalf("node output=%#v", recorder.nodeEnd[0].Output)
	}
}

func TestRunCallbacksTraceRetryAttemptsAndTerminalError(t *testing.T) {
	retryErr := errors.New("retry callback")
	fatalErr := errors.New("fatal callback")
	recorder := &traceRecorder{}
	builder := graph.NewStateGraph(testReducer)
	attempts := 0
	if err := builder.AddNode("retry", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		attempts++
		if attempts == 1 {
			return graph.NoCommand[testDelta](), retryErr
		}
		return graph.Update(testDelta{Add: 1}), nil
	}, graph.WithRetryPolicies(graph.RetryPolicy{
		MaxAttempts: 2, InitialInterval: 0, RetryOn: func(err error) bool { return errors.Is(err, retryErr) },
	})); err != nil {
		t.Fatal(err)
	}
	addNode(t, builder, "fatal", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), fatalErr
	})
	addEdge(t, builder, graph.START, "retry")
	addEdge(t, builder, "retry", "fatal")
	addEdge(t, builder, "fatal", graph.END)
	_, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{Callbacks: []graph.GraphCallback{recorder}})
	if !errors.Is(err, fatalErr) {
		t.Fatalf("Invoke() err=%v", err)
	}
	if len(recorder.nodeStart) != 3 || recorder.nodeStart[0].Attempt != 1 || recorder.nodeStart[1].Attempt != 2 {
		t.Fatalf("node starts=%+v", recorder.nodeStart)
	}
	if len(recorder.nodeError) != 2 || !errors.Is(recorder.nodeError[0].Err, retryErr) || !errors.Is(recorder.nodeError[1].Err, fatalErr) {
		t.Fatalf("node errors=%+v", recorder.nodeError)
	}
	if got := recorder.order[len(recorder.order)-1]; got != "graph:error" {
		t.Fatalf("last callback=%q order=%v", got, recorder.order)
	}
	graphError, ok := recorder.graphs[len(recorder.graphs)-1].(graph.GraphRunErrorEvent)
	if !ok || !errors.Is(graphError.Err, fatalErr) {
		t.Fatalf("graph error=%#v", recorder.graphs[len(recorder.graphs)-1])
	}
}

func TestRunCallbacksTraceCancellation(t *testing.T) {
	recorder := &traceRecorder{}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "wait", func(ctx context.Context, _ testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		<-ctx.Done()
		return graph.NoCommand[testDelta](), ctx.Err()
	})
	addEdge(t, builder, graph.START, "wait")
	addEdge(t, builder, "wait", graph.END)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := compileGraph(t, builder).Invoke(ctx, testState{}, graph.RunConfig{Callbacks: []graph.GraphCallback{recorder}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Invoke() err=%v", err)
	}
	// Cancellation before task dispatch is a graph error and does not invent a
	// node run that never started.
	if len(recorder.nodeStart) != 0 || !reflect.DeepEqual(recorder.order, []string{"graph:start", "graph:error"}) {
		t.Fatalf("starts=%+v order=%v", recorder.nodeStart, recorder.order)
	}
}

func TestRunCallbacksClassifyInterruptAsGraphEndAndInheritIntoSubgraph(t *testing.T) {
	recorder := &traceRecorder{}
	compiled, _ := subgraphFixture(t)
	config := graph.RunConfig{
		ThreadID: "trace-subgraph", RunID: "trace-nested", ParentRunID: "external", Tags: []string{"nested"},
		Callbacks: []graph.GraphCallback{recorder},
	}
	if _, err := compiled.Invoke(context.Background(), subParentState{Input: "seed"}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	var starts []graph.GraphRunStartEvent
	var ends []graph.GraphRunEndEvent
	for _, event := range recorder.graphs {
		switch event := event.(type) {
		case graph.GraphRunStartEvent:
			starts = append(starts, event)
		case graph.GraphRunEndEvent:
			ends = append(ends, event)
		case graph.GraphRunErrorEvent:
			t.Fatalf("interrupt reported as graph error: %+v", event)
		}
	}
	if len(starts) != 2 || len(ends) != 2 {
		t.Fatalf("starts=%+v ends=%+v order=%v", starts, ends, recorder.order)
	}
	if starts[0].RunID != "trace-nested" || starts[0].ParentRunID != "external" ||
		starts[1].RunID == starts[0].RunID || len(recorder.nodeStart) < 2 ||
		starts[1].ParentRunID != recorder.nodeStart[0].RunID || recorder.nodeStart[1].ParentRunID != starts[1].RunID ||
		!reflect.DeepEqual(starts[1].Tags, []string{"nested"}) {
		t.Fatalf("run tree starts=%+v nodes=%+v", starts, recorder.nodeStart)
	}
	if starts[0].CheckpointNamespace != "" || starts[1].CheckpointNamespace == "" ||
		ends[0].CheckpointNamespace == "" || ends[1].CheckpointNamespace != "" {
		t.Fatalf("starts=%+v ends=%+v", starts, ends)
	}
}

func (r *lifecycleRecorder) OnInterrupt(_ context.Context, event graph.GraphInterruptEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.interrupts = append(r.interrupts, event)
	r.order = append(r.order, "interrupt")
}

func (r *lifecycleRecorder) OnResume(_ context.Context, event graph.GraphResumeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resumes = append(r.resumes, event)
	r.order = append(r.order, "resume")
}

func TestGraphLifecycleCallbacksReportDynamicInterruptAndResume(t *testing.T) {
	recorder := &lifecycleRecorder{}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "human", func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "approve?")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	})
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	compiled := compilePersistentGraph(t, builder, checkpointmemory.NewSaver())
	config := graph.RunConfig{
		ThreadID: "callback-dynamic", RunID: "run-callback",
		Callbacks: []graph.GraphCallback{recorder},
	}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke error=%v", err)
	}
	if len(recorder.interrupts) != 1 {
		t.Fatalf("interrupt events=%+v", recorder.interrupts)
	}
	interrupted := recorder.interrupts[0]
	if interrupted.Status != graph.LifecyclePending || interrupted.RunID != "run-callback" ||
		interrupted.CheckpointID == "" || interrupted.CheckpointNamespace != "" || len(interrupted.Interrupts) != 1 {
		t.Fatalf("interrupt event=%+v", interrupted)
	}
	prompt, err := graph.DecodeInterrupt[string](interrupted.Interrupts[0])
	if err != nil || prompt != "approve?" {
		t.Fatalf("prompt=%q error=%v", prompt, err)
	}
	if _, err := compiled.Resume(context.Background(), config, graph.Continue()); !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatalf("invalid Resume error=%v", err)
	}
	if len(recorder.resumes) != 0 {
		t.Fatalf("invalid resume emitted callbacks=%+v", recorder.resumes)
	}
	command, _ := graph.Resume("yes")
	result, err := compiled.Resume(context.Background(), config, command)
	if err != nil || result.Total != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if len(recorder.resumes) != 1 || recorder.resumes[0].Status != graph.LifecyclePending ||
		recorder.resumes[0].CheckpointID != interrupted.CheckpointID {
		t.Fatalf("resume events=%+v", recorder.resumes)
	}
	if !reflect.DeepEqual(recorder.order, []string{"interrupt", "resume"}) {
		t.Fatalf("callback order=%v", recorder.order)
	}
}

func TestGraphLifecycleCallbacksReportStaticStatuses(t *testing.T) {
	for _, test := range []struct {
		name   string
		option graph.CompileOption[testState, testDelta]
		status graph.GraphLifecycleStatus
	}{
		{name: "before", option: graph.WithInterruptBefore[testState, testDelta]("node"), status: graph.LifecycleInterruptBefore},
		{name: "after", option: graph.WithInterruptAfter[testState, testDelta]("node"), status: graph.LifecycleInterruptAfter},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &lifecycleRecorder{}
			builder := graph.NewStateGraph(testReducer)
			addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
				return graph.Update(testDelta{Add: 1}), nil
			})
			addEdge(t, builder, graph.START, "node")
			addEdge(t, builder, "node", graph.END)
			compiled, err := builder.Compile(
				graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
					Saver:      checkpointmemory.NewSaver(),
					StateCodec: checkpoint.MustJSONCodec[testState]("tests/callback-state", 1),
					DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests/callback-delta", 1),
				}),
				test.option,
			)
			if err != nil {
				t.Fatal(err)
			}
			config := graph.RunConfig{ThreadID: "callback-static-" + test.name, Callbacks: []graph.GraphCallback{recorder}}
			if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
				t.Fatalf("Invoke error=%v", err)
			}
			if len(recorder.interrupts) != 1 || recorder.interrupts[0].Status != test.status || len(recorder.interrupts[0].Interrupts) != 0 {
				t.Fatalf("events=%+v", recorder.interrupts)
			}
		})
	}
}

func TestGraphLifecycleCallbackAggregatesParallelInterruptsInTaskOrder(t *testing.T) {
	recorder := &lifecycleRecorder{}
	builder := graph.NewStateGraph(testReducer)
	for _, node := range []graph.NodeID{"a", "b"} {
		node := node
		addNode(t, builder, node, func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
			_, err := graph.AwaitResume[string](runtime, string(node))
			return graph.NoCommand[testDelta](), err
		})
		addEdge(t, builder, graph.START, node)
		addEdge(t, builder, node, graph.END)
	}
	compiled := compilePersistentGraph(t, builder, checkpointmemory.NewSaver())
	config := graph.RunConfig{ThreadID: "callback-parallel", Callbacks: []graph.GraphCallback{recorder}}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke error=%v", err)
	}
	if len(recorder.interrupts) != 1 || len(recorder.interrupts[0].Interrupts) != 2 {
		t.Fatalf("events=%+v", recorder.interrupts)
	}
	var prompts []string
	for _, interrupt := range recorder.interrupts[0].Interrupts {
		prompt, err := graph.DecodeInterrupt[string](interrupt)
		if err != nil {
			t.Fatal(err)
		}
		prompts = append(prompts, prompt)
	}
	if !reflect.DeepEqual(prompts, []string{"a", "b"}) {
		t.Fatalf("prompts=%v", prompts)
	}
}

func TestGraphLifecycleCallbacksAreInheritedBySubgraphScopes(t *testing.T) {
	recorder := &lifecycleRecorder{}
	compiled, _ := subgraphFixture(t)
	config := graph.RunConfig{
		ThreadID: "callback-subgraph", RunID: "nested-run",
		Callbacks: []graph.GraphCallback{recorder},
	}
	if _, err := compiled.Invoke(context.Background(), subParentState{Input: "seed"}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke error=%v", err)
	}
	if len(recorder.interrupts) != 2 || recorder.interrupts[0].CheckpointNamespace == "" || recorder.interrupts[1].CheckpointNamespace != "" {
		t.Fatalf("interrupt scopes=%+v", recorder.interrupts)
	}
	command, _ := graph.Resume("approved")
	if _, err := compiled.Resume(context.Background(), config, command); err != nil {
		t.Fatal(err)
	}
	if len(recorder.resumes) != 2 || recorder.resumes[0].CheckpointNamespace != "" || recorder.resumes[1].CheckpointNamespace == "" {
		t.Fatalf("resume scopes=%+v", recorder.resumes)
	}
}
