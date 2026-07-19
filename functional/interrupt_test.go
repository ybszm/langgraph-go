package functional_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/functional"
)

func TestDurableEntrypointInterruptResumeReusesTasks(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	taskCodec := checkpoint.MustJSONCodec[string]("interrupt-task", 1)
	taskCalls := 0
	draft, err := functional.NewTask("draft", func(context.Context, string) (string, error) {
		taskCalls++
		return "draft", nil
	}, functional.TaskOptions[string, string]{Persistence: &functional.TaskPersistencePolicy[string, string]{Codec: taskCodec}})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := functional.NewDurableEntrypoint(
		"review",
		func(ctx context.Context, input string, _ *string) (functional.Final[string, string], error) {
			value, err := draft.Call(ctx, input).Await(ctx)
			if err != nil {
				return functional.Final[string, string]{}, err
			}
			review, err := functional.AwaitResume[string](ctx, map[string]any{"draft": value})
			if err != nil {
				return functional.Final[string, string]{}, err
			}
			result := value + ":" + review
			return functional.Final[string, string]{Value: result, Save: result}, nil
		},
		functional.DurableEntrypointConfig[string]{
			Saver: saver, Codec: checkpoint.MustJSONCodec[string]("interrupt-save", 1), EnableTaskRecovery: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	_, err = entry.Invoke(context.Background(), "topic", run)
	var interruptErr *functional.InterruptError
	if !errors.Is(err, functional.ErrInterrupted) || !errors.As(err, &interruptErr) || len(interruptErr.Interrupts) != 1 {
		t.Fatalf("error=%v", err)
	}
	interruptID := interruptErr.Interrupts[0].ID
	if interruptID == "" || taskCalls != 1 {
		t.Fatalf("interrupt=%+v task calls=%d", interruptErr.Interrupts, taskCalls)
	}
	result, err := entry.Resume(context.Background(), "topic", run, map[string]any{interruptID: "approved"})
	if err != nil || result != "draft:approved" || taskCalls != 1 {
		t.Fatalf("result=%q task calls=%d error=%v", result, taskCalls, err)
	}
}

func TestDurableEntrypointRejectsUnknownResumeID(t *testing.T) {
	entry, err := functional.NewDurableEntrypoint(
		"review",
		func(ctx context.Context, input string, _ *string) (functional.Final[string, string], error) {
			value, err := functional.AwaitResume[string](ctx, input)
			return functional.Final[string, string]{Value: value, Save: value}, err
		},
		functional.DurableEntrypointConfig[string]{
			Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[string]("unknown-resume", 1), EnableTaskRecovery: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	_, _ = entry.Invoke(context.Background(), "prompt", run)
	_, err = entry.Resume(context.Background(), "prompt", run, map[string]any{"missing": "value"})
	if !errors.Is(err, functional.ErrInvalidResume) {
		t.Fatalf("error=%v", err)
	}
}

func TestDurableEntrypointSupportsSequentialPartialResumes(t *testing.T) {
	entry, err := functional.NewDurableEntrypoint(
		"survey",
		func(ctx context.Context, input string, _ *string) (functional.Final[string, string], error) {
			first, err := functional.AwaitResume[string](ctx, "first")
			if err != nil {
				return functional.Final[string, string]{}, err
			}
			second, err := functional.AwaitResume[string](ctx, "second")
			if err != nil {
				return functional.Final[string, string]{}, err
			}
			value := input + ":" + first + ":" + second
			return functional.Final[string, string]{Value: value, Save: value}, nil
		},
		functional.DurableEntrypointConfig[string]{
			Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[string]("partial-resume", 1), EnableTaskRecovery: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	_, err = entry.Invoke(context.Background(), "input", run)
	var firstInterrupt *functional.InterruptError
	if !errors.As(err, &firstInterrupt) {
		t.Fatal(err)
	}
	_, err = entry.Resume(context.Background(), "input", run, map[string]any{firstInterrupt.Interrupts[0].ID: "one"})
	var secondInterrupt *functional.InterruptError
	if !errors.As(err, &secondInterrupt) || secondInterrupt.Interrupts[0].ID == firstInterrupt.Interrupts[0].ID {
		t.Fatalf("error=%v", err)
	}
	result, err := entry.Resume(context.Background(), "input", run, map[string]any{secondInterrupt.Interrupts[0].ID: "two"})
	if err != nil || result != "input:one:two" {
		t.Fatalf("result=%q error=%v", result, err)
	}
}

func TestAwaitResumeRequiresDurableEntrypoint(t *testing.T) {
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input string) (string, error) {
		return functional.AwaitResume[string](ctx, input)
	}, functional.EntrypointOptions{})
	_, err := entry.Invoke(context.Background(), "prompt")
	if !errors.Is(err, functional.ErrInterruptRequiresDurability) {
		t.Fatalf("error=%v", err)
	}
}

func TestParallelTasksAggregateInterrupts(t *testing.T) {
	ready := make(chan struct{}, 4)
	release := make(chan struct{})
	responses := make(chan string, 2)
	approval, err := functional.NewTask("approval", func(ctx context.Context, prompt string) (string, error) {
		ready <- struct{}{}
		<-release
		if prompt == "second?" {
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		response, err := functional.AwaitResume[string](ctx, prompt)
		if err == nil {
			responses <- prompt + ":" + response
		}
		return response, err
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := functional.NewDurableEntrypoint(
		"parallel-review",
		func(ctx context.Context, input string, _ *string) (functional.Final[string, string], error) {
			first := approval.Call(ctx, "first?")
			approval.Call(ctx, "second?")
			if _, err := first.Await(ctx); err != nil {
				return functional.Final[string, string]{}, err
			}
			return functional.Final[string, string]{Value: input, Save: input}, nil
		},
		functional.DurableEntrypointConfig[string]{
			Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[string]("parallel-functional-interrupt", 1), EnableTaskRecovery: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "parallel-functional"}
	done := make(chan error, 1)
	go func() {
		_, invokeErr := entry.Invoke(context.Background(), "done", run)
		done <- invokeErr
	}()
	<-ready
	<-ready
	close(release)
	invokeErr := <-done
	var interrupted *functional.InterruptError
	if !errors.As(invokeErr, &interrupted) || len(interrupted.Interrupts) != 2 {
		t.Fatalf("interrupt error=%v payload=%+v", invokeErr, interrupted)
	}
	resumes := make(map[string]any, 2)
	for _, interrupt := range interrupted.Interrupts {
		var prompt string
		if err := json.Unmarshal(interrupt.Value, &prompt); err != nil {
			t.Fatal(err)
		}
		resumes[interrupt.ID] = "answer:" + prompt
	}
	result, err := entry.Resume(context.Background(), "done", run, resumes)
	if err != nil || result != "done" {
		t.Fatalf("Resume result=%q err=%v", result, err)
	}
	got := []string{<-responses, <-responses}
	sort.Strings(got)
	want := []string{"first?:answer:first?", "second?:answer:second?"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("responses=%v", got)
	}
}
