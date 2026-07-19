package sdk

import (
	"context"
	"reflect"
	"testing"

	temporaladapter "github.com/wahanbo/langgraph-go/backend/temporal"
	"github.com/wahanbo/langgraph-go/graph"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

type fakeRun struct {
	id, runID string
	result    any
}

func (r *fakeRun) GetID() string    { return r.id }
func (r *fakeRun) GetRunID() string { return r.runID }
func (r *fakeRun) Get(_ context.Context, valuePtr interface{}) error {
	reflect.ValueOf(valuePtr).Elem().Set(reflect.ValueOf(r.result))
	return nil
}
func (r *fakeRun) GetWithOptions(ctx context.Context, valuePtr interface{}, _ client.WorkflowRunGetOptions) error {
	return r.Get(ctx, valuePtr)
}

type fakeStarter struct {
	options  client.StartWorkflowOptions
	workflow any
	args     []interface{}
	run      client.WorkflowRun
}

func (f *fakeStarter) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow any, args ...interface{}) (client.WorkflowRun, error) {
	f.options, f.workflow, f.args = options, workflow, args
	return f.run, nil
}

func TestClientMapsSerializableRequestAndTypedResult(t *testing.T) {
	starter := &fakeStarter{run: &fakeRun{id: "wf", runID: "run", result: "done"}}
	workflowType := func() {}
	binding, err := newClient[int, string](starter, "graph-queue", workflowType, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"tenant": "a"}
	handle, err := binding.StartWorkflow(context.Background(), temporaladapter.StartRequest[int]{WorkflowID: "wf", Input: 7, Config: graph.RunConfig{ThreadID: "thread", RunID: "run", Tags: []string{"x"}, Metadata: metadata, Callbacks: []graph.GraphCallback{graph.GraphCallbackFuncs{}}}})
	if err != nil {
		t.Fatal(err)
	}
	metadata["tenant"] = "changed"
	request, ok := starter.args[0].(WorkflowRequest[int])
	if !ok || request.Input != 7 || request.Config.ThreadID != "thread" || request.Config.Metadata["tenant"] != "a" {
		t.Fatalf("unexpected workflow request: %#v", starter.args[0])
	}
	if starter.options.ID != "wf" || starter.options.TaskQueue != "graph-queue" {
		t.Fatalf("unexpected options: %#v", starter.options)
	}
	if handle.WorkflowID() != "wf" || handle.RunID() != "run" {
		t.Fatal("workflow identity was not preserved")
	}
	result, err := handle.Get(context.Background())
	if err != nil || result != "done" {
		t.Fatalf("result=%q err=%v", result, err)
	}
}

type encoded struct{ value any }

func (e encoded) HasValue() bool { return true }
func (e encoded) Get(ptr interface{}) error {
	reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(e.value))
	return nil
}

type fakeUpdateHandle struct{ value any }

func (h fakeUpdateHandle) WorkflowID() string { return "wf" }
func (h fakeUpdateHandle) RunID() string      { return "run" }
func (h fakeUpdateHandle) UpdateID() string   { return "request" }
func (h fakeUpdateHandle) Get(_ context.Context, ptr interface{}) error {
	reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(h.value))
	return nil
}

type fakeControl struct {
	signal     any
	signalName string
	update     client.UpdateWorkflowOptions
	query      any
}

func (f *fakeControl) SignalWorkflow(_ context.Context, _, _, name string, arg interface{}) error {
	f.signalName, f.signal = name, arg
	return nil
}
func (f *fakeControl) UpdateWorkflow(_ context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	f.update = options
	return fakeUpdateHandle{value: "updated"}, nil
}
func (f *fakeControl) QueryWorkflow(_ context.Context, _, _, _ string, args ...interface{}) (converter.EncodedValue, error) {
	f.query = args[0]
	return encoded{value: 42}, nil
}

func TestControlClientPreservesTypedEnvelopes(t *testing.T) {
	fake := &fakeControl{}
	binding := &ControlClient[string, string, string, string, int]{client: fake}
	ref := temporaladapter.WorkflowRef{WorkflowID: "wf", RunID: "run"}
	signal := temporaladapter.SignalRequest[string]{Ref: ref, Name: "sig", RequestID: "s1", Payload: "resume"}
	if err := binding.SignalWorkflow(context.Background(), signal); err != nil {
		t.Fatal(err)
	}
	if fake.signalName != "sig" || !reflect.DeepEqual(fake.signal, signal) {
		t.Fatal("signal envelope changed")
	}
	update := temporaladapter.UpdateRequest[string]{Ref: ref, Name: "upd", RequestID: "request", Payload: "resume"}
	got, err := binding.UpdateWorkflow(context.Background(), update)
	if err != nil || got != "updated" || fake.update.WaitForStage != client.WorkflowUpdateStageCompleted || !reflect.DeepEqual(fake.update.Args[0], update) {
		t.Fatalf("update=%q options=%#v err=%v", got, fake.update, err)
	}
	query := temporaladapter.QueryRequest[string]{Ref: ref, Name: "query", Args: "state"}
	state, err := binding.QueryWorkflow(context.Background(), query)
	if err != nil || state != 42 || !reflect.DeepEqual(fake.query, query) {
		t.Fatalf("query=%d err=%v", state, err)
	}
}
