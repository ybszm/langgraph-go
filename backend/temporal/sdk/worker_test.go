package sdk

import (
	"context"
	"testing"

	temporaladapter "github.com/wahanbo/langgraph-go/backend/temporal"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type fakeWorker struct {
	workflows        []workflow.RegisterOptions
	activities       []activity.RegisterOptions
	started, stopped bool
}

func (w *fakeWorker) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	w.workflows = append(w.workflows, options)
}
func (w *fakeWorker) RegisterActivityWithOptions(_ interface{}, options activity.RegisterOptions) {
	w.activities = append(w.activities, options)
}
func (w *fakeWorker) Start() error                 { w.started = true; return nil }
func (w *fakeWorker) Run(<-chan interface{}) error { w.started = true; return nil }
func (w *fakeWorker) Stop()                        { w.stopped = true }

func TestRegistrarAppliesWorkerDeploymentPlan(t *testing.T) {
	fake := &fakeWorker{}
	var queue string
	var options worker.Options
	registrar, err := newRegistrar(func(q string, o worker.Options) sdkWorker { queue, options = q, o; return fake }, "langgraph", worker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := temporaladapter.NewWorkerPlan(temporaladapter.WorkerSpec{TaskQueue: "queue", BuildID: "build-2", Compatibility: temporaladapter.VersionCompatible})
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.SetWorkflow("graph.workflow", func(workflow.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := plan.AddActivity("z.node", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := plan.AddActivity("a.node", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := plan.Apply(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	if queue != "queue" || !options.DeploymentOptions.UseVersioning || options.DeploymentOptions.Version.DeploymentName != "langgraph" || options.DeploymentOptions.Version.BuildID != "build-2" || options.DeploymentOptions.DefaultVersioningBehavior != workflow.VersioningBehaviorAutoUpgrade || !options.DisableRegistrationAliasing {
		t.Fatalf("unexpected worker options: %#v", options)
	}
	if len(fake.workflows) != 1 || fake.workflows[0].Name != "graph.workflow" || fake.workflows[0].VersioningBehavior != workflow.VersioningBehaviorAutoUpgrade {
		t.Fatalf("workflow options: %#v", fake.workflows)
	}
	if len(fake.activities) != 2 || fake.activities[0].Name != "a.node" || fake.activities[1].Name != "z.node" {
		t.Fatalf("activities not deterministic: %#v", fake.activities)
	}
	if err := registrar.Start(); err != nil {
		t.Fatal(err)
	}
	registrar.Stop()
	if !fake.started || !fake.stopped {
		t.Fatal("worker lifecycle was not delegated")
	}
}
