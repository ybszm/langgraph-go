package graph_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

type parallelInterruptParentState struct {
	Prompts []string
	Prompt  string
	Answers []string
}

type parallelInterruptParentDelta struct{ Answer string }

type parallelInterruptChildState struct {
	Prompt string
	Answer string
}

type parallelInterruptChildDelta struct{ Answer string }

func parallelInterruptGraph(t *testing.T) *graph.CompiledGraph[parallelInterruptParentState, parallelInterruptParentDelta] {
	t.Helper()
	childBuilder := graph.NewStateGraph(func(_ context.Context, state parallelInterruptChildState, updates []parallelInterruptChildDelta) (parallelInterruptChildState, error) {
		for _, update := range updates {
			state.Answer = update.Answer
		}
		return state, nil
	})
	if err := childBuilder.AddNode("ask", func(_ context.Context, state parallelInterruptChildState, runtime graph.Runtime) (graph.Command[parallelInterruptChildDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, state.Prompt)
		if err != nil {
			return graph.Command[parallelInterruptChildDelta]{}, err
		}
		return graph.Update(parallelInterruptChildDelta{Answer: answer}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := childBuilder.AddEdge(graph.START, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := childBuilder.AddEdge("ask", graph.END); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	parentBuilder := graph.NewStateGraph(func(_ context.Context, state parallelInterruptParentState, updates []parallelInterruptParentDelta) (parallelInterruptParentState, error) {
		state.Answers = append([]string(nil), state.Answers...)
		for _, update := range updates {
			if update.Answer != "" {
				state.Answers = append(state.Answers, update.Answer)
			}
		}
		return state, nil
	})
	if err := parentBuilder.AddNode("plan", func(context.Context, parallelInterruptParentState, graph.Runtime) (graph.Command[parallelInterruptParentDelta], error) {
		return graph.NoCommand[parallelInterruptParentDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddSubgraph(parentBuilder, "worker", child, graph.SubgraphAdapter[parallelInterruptParentState, parallelInterruptParentDelta, parallelInterruptChildState, parallelInterruptChildDelta]{
		Input: func(_ context.Context, parent parallelInterruptParentState) (parallelInterruptChildState, error) {
			return parallelInterruptChildState{Prompt: parent.Prompt}, nil
		},
		Output: func(_ context.Context, _ parallelInterruptParentState, child parallelInterruptChildState) (graph.Command[parallelInterruptParentDelta], error) {
			return graph.Update(parallelInterruptParentDelta{Answer: child.Answer}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[parallelInterruptChildState]("tests.parallel-child-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[parallelInterruptChildDelta]("tests.parallel-child-delta", 1),
	}); err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.AddEdge(graph.START, "plan"); err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.AddSendEdges("plan", func(_ context.Context, state parallelInterruptParentState) ([]graph.Send[parallelInterruptParentState], error) {
		sends := make([]graph.Send[parallelInterruptParentState], len(state.Prompts))
		for index, prompt := range state.Prompts {
			sends[index] = graph.Send[parallelInterruptParentState]{
				Node:  "worker",
				State: parallelInterruptParentState{Prompt: prompt},
			}
		}
		return sends, nil
	}, "worker"); err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.AddEdge("worker", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := parentBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[parallelInterruptParentState, parallelInterruptParentDelta]{
		Saver:      memory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[parallelInterruptParentState]("tests.parallel-parent-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[parallelInterruptParentDelta]("tests.parallel-parent-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestParallelSubgraphInterruptsResumeByID(t *testing.T) {
	compiled := parallelInterruptGraph(t)
	config := graph.RunConfig{ThreadID: "parallel-subgraph-interrupts", MaxConcurrency: 5}
	prompts := []string{"a", "b", "c", "d", "e"}

	paused, err := compiled.Invoke(context.Background(), parallelInterruptParentState{Prompts: prompts}, config)
	if !errors.Is(err, graph.ErrGraphInterrupt) || len(paused.Answers) != 0 {
		t.Fatalf("Invoke() state=%+v err=%v", paused, err)
	}
	var interruptErr *graph.GraphInterruptError
	if !errors.As(err, &interruptErr) || len(interruptErr.Interrupts) != len(prompts) {
		t.Fatalf("interrupt error=%v", err)
	}

	snapshot, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != len(prompts) || len(snapshot.Interrupts) != len(prompts) {
		t.Fatalf("tasks=%d interrupts=%d", len(snapshot.Tasks), len(snapshot.Interrupts))
	}
	resumeValues := make(map[string]any, len(prompts))
	seenIDs := make(map[string]struct{}, len(prompts))
	seenNamespaces := make(map[string]struct{}, len(prompts))
	for _, item := range snapshot.Interrupts {
		prompt, decodeErr := graph.DecodeInterrupt[string](item)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if item.ID == "" || item.Namespace == "" {
			t.Fatalf("interrupt has incomplete identity: %+v", item)
		}
		if _, duplicate := seenIDs[item.ID]; duplicate {
			t.Fatalf("duplicate interrupt id %q", item.ID)
		}
		if _, duplicate := seenNamespaces[item.Namespace]; duplicate {
			t.Fatalf("duplicate child namespace %q", item.Namespace)
		}
		seenIDs[item.ID] = struct{}{}
		seenNamespaces[item.Namespace] = struct{}{}
		resumeValues[item.ID] = fmt.Sprintf("human input for prompt %s", prompt)
	}

	single, err := graph.Resume("ambiguous")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Resume(context.Background(), config, single); !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatalf("single resume err=%v", err)
	}
	byID, err := graph.ResumeByID(resumeValues)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := compiled.Resume(context.Background(), config, byID)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(prompts))
	for index, prompt := range prompts {
		want[index] = fmt.Sprintf("human input for prompt %s", prompt)
	}
	if !reflect.DeepEqual(completed.Answers, want) {
		t.Fatalf("answers=%v want=%v", completed.Answers, want)
	}
	final, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Next) != 0 || len(final.Interrupts) != 0 {
		t.Fatalf("final next=%v interrupts=%v", final.Next, final.Interrupts)
	}
}

func TestParallelSubgraphInterruptsCanResumeInBatches(t *testing.T) {
	compiled := parallelInterruptGraph(t)
	config := graph.RunConfig{ThreadID: "parallel-subgraph-batched-resume", MaxConcurrency: 3}
	prompts := []string{"first", "second", "third"}
	if _, err := compiled.Invoke(context.Background(), parallelInterruptParentState{Prompts: prompts}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial Invoke() err=%v", err)
	}

	paused, err := compiled.GetState(context.Background(), config)
	if err != nil || len(paused.Interrupts) != 3 {
		t.Fatalf("paused interrupts=%d err=%v", len(paused.Interrupts), err)
	}
	firstPrompt, err := graph.DecodeInterrupt[string](paused.Interrupts[0])
	if err != nil {
		t.Fatal(err)
	}
	firstResume, err := graph.ResumeByID(map[string]any{paused.Interrupts[0].ID: "answer:" + firstPrompt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Resume(context.Background(), config, firstResume); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("partial Resume() err=%v", err)
	}

	partiallyResumed, err := compiled.GetState(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(partiallyResumed.Interrupts) != 2 || !reflect.DeepEqual(partiallyResumed.Values.Answers, []string{"answer:first"}) {
		t.Fatalf("partial state=%+v interrupts=%d", partiallyResumed.Values, len(partiallyResumed.Interrupts))
	}
	remaining := make(map[string]any, 2)
	for _, item := range partiallyResumed.Interrupts {
		prompt, decodeErr := graph.DecodeInterrupt[string](item)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		remaining[item.ID] = "answer:" + prompt
	}
	command, err := graph.ResumeByID(remaining)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := compiled.Resume(context.Background(), config, command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(completed.Answers, []string{"answer:first", "answer:second", "answer:third"}) {
		t.Fatalf("answers=%v", completed.Answers)
	}
}

func TestParallelSubgraphInterruptsResumeFromExactHistoricalCheckpoint(t *testing.T) {
	compiled := parallelInterruptGraph(t)
	ctx := context.Background()
	config := graph.RunConfig{ThreadID: "parallel-subgraph-historical-resume", MaxConcurrency: 3}
	prompts := []string{"a", "b", "c"}
	if _, err := compiled.Invoke(ctx, parallelInterruptParentState{Prompts: prompts}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial Invoke() err=%v", err)
	}
	paused, err := compiled.GetState(ctx, config)
	if err != nil || len(paused.Interrupts) != len(prompts) {
		t.Fatalf("paused interrupts=%d err=%v", len(paused.Interrupts), err)
	}
	oldValues := make(map[string]any, len(paused.Interrupts))
	newValues := make(map[string]any, len(paused.Interrupts))
	for _, item := range paused.Interrupts {
		prompt, decodeErr := graph.DecodeInterrupt[string](item)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		oldValues[item.ID] = "old:" + prompt
		newValues[item.ID] = "new:" + prompt
	}
	oldCommand, _ := graph.ResumeByID(oldValues)
	oldBranch, err := compiled.Resume(ctx, config, oldCommand)
	if err != nil || !reflect.DeepEqual(oldBranch.Answers, []string{"old:a", "old:b", "old:c"}) {
		t.Fatalf("old branch=%v err=%v", oldBranch.Answers, err)
	}
	newCommand, _ := graph.ResumeByID(newValues)
	newBranch, err := compiled.Resume(ctx, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: paused.Config.CheckpointID, MaxConcurrency: 3,
	}, newCommand)
	if err != nil || !reflect.DeepEqual(newBranch.Answers, []string{"new:a", "new:b", "new:c"}) {
		t.Fatalf("new branch=%v err=%v", newBranch.Answers, err)
	}
}
