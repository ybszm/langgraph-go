package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestResumeCommandJSONRoundTripsAllTransportModes(t *testing.T) {
	single, _ := graph.Resume(nil)
	byID, _ := graph.ResumeByID(map[string]any{"b": 2, "a": "one"})
	cases := []struct {
		command graph.ResumeCommand
		want    string
	}{
		{command: single, want: `{"value":null}`},
		{command: byID, want: `{"by_id":{"a":"one","b":2}}`},
		{command: graph.Continue(), want: `{"continue":true}`},
	}
	for _, test := range cases {
		encoded, err := json.Marshal(test.command)
		if err != nil || string(encoded) != test.want {
			t.Fatalf("Marshal()=%s want=%s err=%v", encoded, test.want, err)
		}
		var decoded graph.ResumeCommand
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		roundTrip, err := json.Marshal(decoded)
		if err != nil || string(roundTrip) != test.want {
			t.Fatalf("round trip=%s want=%s err=%v", roundTrip, test.want, err)
		}
	}
}

func TestResumeCommandJSONRejectsAmbiguousOrUnknownModes(t *testing.T) {
	for _, encoded := range []string{
		`{}`,
		`{"value":1,"continue":true}`,
		`{"by_id":{}}`,
		`{"unknown":1}`,
		`{"continue":false}`,
	} {
		var command graph.ResumeCommand
		if err := json.Unmarshal([]byte(encoded), &command); !errors.Is(err, graph.ErrInvalidResume) {
			t.Fatalf("Unmarshal(%s) err=%v", encoded, err)
		}
	}
}

func TestInterruptPersistsAndResumeRerunsNode(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	addNode(t, builder, "human", func(
		_ context.Context,
		_ testState,
		runtime graph.Runtime,
	) (graph.Command[testDelta], error) {
		calls.Add(1)
		answer, err := graph.AwaitResume[string](runtime, map[string]string{
			"question": "approve?",
		})
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	})
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-interrupt"}

	state, err := compiled.Invoke(context.Background(), testState{}, config)
	if !errors.Is(err, graph.ErrGraphInterrupt) || state.Total != 0 {
		t.Fatalf("Invoke() state=%#v err=%v", state, err)
	}
	var interruptErr *graph.GraphInterruptError
	if !errors.As(err, &interruptErr) || len(interruptErr.Interrupts) != 1 {
		t.Fatalf("interrupt error = %#v", err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || len(snapshot.Interrupts) != 1 || len(snapshot.Tasks) != 1 {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	prompt, err := graph.DecodeInterrupt[map[string]string](snapshot.Interrupts[0])
	if err != nil || prompt["question"] != "approve?" {
		t.Fatalf("prompt=%#v err=%v", prompt, err)
	}

	command, err := graph.Resume("approved")
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Resume(context.Background(), config, command)
	if err != nil {
		t.Fatalf("Resume(): %v", err)
	}
	if result.Total != 1 || !reflect.DeepEqual(result.Path, []string{"approved"}) {
		t.Fatalf("result = %#v", result)
	}
	if calls.Load() != 2 {
		t.Fatalf("node calls = %d, want initial + rerun", calls.Load())
	}
	final, err := compiled.GetState(context.Background(), config)
	if err != nil || len(final.Interrupts) != 0 || len(final.Next) != 0 {
		t.Fatalf("final snapshot=%#v err=%v", final, err)
	}
}

func TestInvokeCommandUsesUnifiedResumeEnvelopeAtomically(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("human", func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "approve?")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	}, graph.WithDynamicInterrupts()); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-command-resume"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	resume, err := graph.Resume("approved")
	if err != nil {
		t.Fatal(err)
	}
	envelope := graph.ResumeAsCommand[testDelta](resume)
	invalid := envelope
	invalid.HasUpdate = true
	invalid.Update = testDelta{Add: 99}
	invalid.Goto = []graph.NodeID{"missing"}
	if _, err := compiled.InvokeCommand(context.Background(), invalid, config); !errors.Is(err, graph.ErrUnknownNode) {
		t.Fatalf("invalid InvokeCommand() err=%v", err)
	}
	result, err := compiled.InvokeCommand(context.Background(), envelope, config)
	if err != nil || result.Total != 1 || !reflect.DeepEqual(result.Path, []string{"approved"}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestInvokeCommandCombinesResumeUpdateGotoAtomically(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("human", func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "approve?")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	}, graph.WithDynamicInterrupts()); err != nil {
		t.Fatal(err)
	}
	addNode(t, builder, "audit", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 100, Label: "audit"}), nil
	})
	addNode(t, builder, "worker", func(_ context.Context, input testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: input.Total, Label: "send"}), nil
	})
	addEdge(t, builder, graph.START, "human")
	if err := builder.AddConditionalEdges("human", func(context.Context, testState) ([]graph.NodeID, error) {
		return []graph.NodeID{graph.END}, nil
	}, graph.END, "audit", "worker"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "audit", graph.END)
	addEdge(t, builder, "worker", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-command-mixed"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	before, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resume, err := graph.Resume("approved")
	if err != nil {
		t.Fatal(err)
	}
	invalid := graph.ResumeAsCommand[testDelta](resume)
	invalid.HasUpdate = true
	invalid.Update = testDelta{Add: 999, Label: "invalid"}
	invalid.Goto = []graph.NodeID{"missing"}
	if _, err := compiled.InvokeCommand(context.Background(), invalid, config); !errors.Is(err, graph.ErrUnknownNode) {
		t.Fatalf("invalid mixed command err=%v", err)
	}
	afterInvalid, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(afterInvalid) != len(before) {
		t.Fatalf("invalid command published a checkpoint: before=%d after=%d", len(before), len(afterInvalid))
	}

	command := graph.WithResume(graph.UpdateAndGoto(testDelta{Add: 10, Label: "pre"}, "audit"), resume)
	command.Sends = []graph.TaskSend{graph.SendTo("worker", testState{Total: 7})}
	result, err := compiled.InvokeCommand(context.Background(), command, config)
	if err != nil {
		t.Fatalf("InvokeCommand() err=%v", err)
	}
	if result.Total != 118 || !reflect.DeepEqual(result.Path, []string{"pre", "approved", "audit", "send"}) {
		t.Fatalf("result = %#v", result)
	}
}

func TestResumeStreamEmitsReplayEventsAndTerminalErrors(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "human", func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "approve?")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		if err := runtime.WriteCustom("answer:" + answer); err != nil {
			return graph.Command[testDelta]{}, err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	})
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-resume-stream"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke error=%v", err)
	}
	command, _ := graph.Resume("approved")
	var custom any
	var values testState
	var taskResult bool
	for event := range compiled.ResumeStreamWithOptions(context.Background(), config, command, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamCustom, graph.StreamValues, graph.StreamDebug},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Mode == graph.StreamCustom {
			custom = event.Custom
		}
		if event.Mode == graph.StreamValues {
			values = event.State
		}
		if event.Debug != nil && event.Debug.Kind == graph.DebugTaskResult {
			taskResult = true
		}
	}
	if custom != "answer:approved" || values.Total != 1 || !reflect.DeepEqual(values.Path, []string{"approved"}) || !taskResult {
		t.Fatalf("custom=%v values=%#v taskResult=%v", custom, values, taskResult)
	}
	invalid, _ := graph.Resume("again")
	var terminal error
	for event := range compiled.ResumeStream(context.Background(), config, invalid) {
		if event.Err != nil {
			terminal = event.Err
		}
	}
	if !errors.Is(terminal, graph.ErrInvalidResume) {
		t.Fatalf("terminal error=%v", terminal)
	}
}

func TestInvokeCommandStreamCombinesResumeAndUpdate(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("human", func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "approve?")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	}, graph.WithDynamicInterrupts()); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-command-stream"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatal(err)
	}
	resume, _ := graph.Resume("approved")
	command := graph.WithResume(graph.Update(testDelta{Add: 10, Label: "pre"}), resume)
	var values testState
	for event := range compiled.InvokeCommandStreamWithOptions(context.Background(), command, config, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamValues},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Mode == graph.StreamValues {
			values = event.State
		}
	}
	if values.Total != 11 || !reflect.DeepEqual(values.Path, []string{"pre", "approved"}) {
		t.Fatalf("values = %#v", values)
	}
}

func TestResumeStreamCancellationUnblocksReplayWriter(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	writerStopped := make(chan error, 1)
	addNode(t, builder, "human", func(_ context.Context, _ testState, runtime graph.Runtime) (graph.Command[testDelta], error) {
		if _, err := graph.AwaitResume[string](runtime, "continue?"); err != nil {
			return graph.NoCommand[testDelta](), err
		}
		for index := 0; index < 1000; index++ {
			if err := runtime.WriteCustom(index); err != nil {
				writerStopped <- err
				return graph.Command[testDelta]{}, err
			}
		}
		writerStopped <- nil
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-resume-cancel"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatal(err)
	}
	command, _ := graph.Resume("go")
	ctx, cancel := context.WithCancel(context.Background())
	events := compiled.ResumeStreamWithOptions(ctx, config, command, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamCustom}, Buffer: 1,
	})
	if event, ok := <-events; !ok || event.Mode != graph.StreamCustom {
		t.Fatalf("first event=%+v open=%v", event, ok)
	}
	cancel()
	select {
	case err := <-writerStopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("writer error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("resume writer remained blocked")
	}
	for range events {
	}
}

func TestMultipleInterruptsInOneNodeResumeByOrder(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	addNode(t, builder, "survey", func(
		_ context.Context,
		_ testState,
		runtime graph.Runtime,
	) (graph.Command[testDelta], error) {
		calls.Add(1)
		first, err := graph.AwaitResume[string](runtime, "first question")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		second, err := graph.AwaitResume[string](runtime, "second question")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Label: first + "+" + second}), nil
	})
	addEdge(t, builder, graph.START, "survey")
	addEdge(t, builder, "survey", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-multiple-interrupts"}

	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("first interrupt error = %v", err)
	}
	firstResume, _ := graph.Resume("one")
	if _, err := compiled.Resume(context.Background(), config, firstResume); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("second interrupt error = %v", err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || len(snapshot.Interrupts) != 1 {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	prompt, err := graph.DecodeInterrupt[string](snapshot.Interrupts[0])
	if err != nil || prompt != "second question" {
		t.Fatalf("second prompt=%q err=%v", prompt, err)
	}
	secondResume, _ := graph.Resume("two")
	result, err := compiled.Resume(context.Background(), config, secondResume)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Path, []string{"one+two"}) || calls.Load() != 3 {
		t.Fatalf("result=%#v calls=%d", result, calls.Load())
	}
}

func TestParallelInterruptsRequireResumeByID(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	makeNode := func(label string) graph.Node[testState, testDelta] {
		return func(
			_ context.Context,
			_ testState,
			runtime graph.Runtime,
		) (graph.Command[testDelta], error) {
			answer, err := graph.AwaitResume[string](runtime, label+"?")
			if err != nil {
				return graph.NoCommand[testDelta](), err
			}
			return graph.Update(testDelta{Label: label + "=" + answer}), nil
		}
	}
	addNode(t, builder, "left", makeNode("left"))
	addNode(t, builder, "right", makeNode("right"))
	addEdge(t, builder, graph.START, "left")
	addEdge(t, builder, graph.START, "right")
	addEdge(t, builder, "left", graph.END)
	addEdge(t, builder, "right", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-parallel-interrupts"}

	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() error = %v", err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || len(snapshot.Interrupts) != 2 {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	single, _ := graph.Resume("wrong")
	if _, err := compiled.Resume(context.Background(), config, single); !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatalf("single resume error = %v", err)
	}
	values := make(map[string]any)
	for _, interrupt := range snapshot.Interrupts {
		prompt, decodeErr := graph.DecodeInterrupt[string](interrupt)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		values[interrupt.ID] = "answer-for-" + prompt
	}
	byID, err := graph.ResumeByID(values)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Resume(context.Background(), config, byID)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"left=answer-for-left?", "right=answer-for-right?"}
	if !reflect.DeepEqual(result.Path, want) {
		t.Fatalf("path=%#v want=%#v", result.Path, want)
	}
}

func TestAwaitResumeRequiresCheckpointer(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "human", func(
		_ context.Context,
		_ testState,
		runtime graph.Runtime,
	) (graph.Command[testDelta], error) {
		_, err := graph.AwaitResume[string](runtime, "question")
		return graph.NoCommand[testDelta](), err
	})
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	_, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrCheckpointerRequired) {
		t.Fatalf("error = %v", err)
	}
}

func TestInterruptStreamUsesControlEventInsteadOfError(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "human", func(
		_ context.Context,
		_ testState,
		runtime graph.Runtime,
	) (graph.Command[testDelta], error) {
		_, err := graph.AwaitResume[string](runtime, "question")
		return graph.NoCommand[testDelta](), err
	})
	addEdge(t, builder, graph.START, "human")
	addEdge(t, builder, "human", graph.END)
	events := compilePersistentGraph(t, builder, saver).Stream(
		context.Background(),
		testState{},
		graph.RunConfig{ThreadID: "thread-interrupt-stream"},
	)
	var last graph.StreamEvent[testState, testDelta]
	for event := range events {
		last = event
	}
	if last.Mode != graph.StreamInterrupt || last.Err != nil || len(last.Interrupts) != 1 {
		t.Fatalf("last event = %#v", last)
	}
}
