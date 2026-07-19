package temporal_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/backend/temporal"
)

type fakeRegistrar struct{ calls []string }

func (r *fakeRegistrar) ConfigureVersioning(_ context.Context, spec temporal.VersioningSpec) error {
	r.calls = append(r.calls, "version:"+spec.BuildID)
	return nil
}
func (r *fakeRegistrar) RegisterWorkflow(_ context.Context, registration temporal.Registration) error {
	r.calls = append(r.calls, "workflow:"+registration.Name)
	return nil
}
func (r *fakeRegistrar) RegisterActivity(_ context.Context, registration temporal.Registration) error {
	r.calls = append(r.calls, "activity:"+registration.Name)
	return nil
}

func TestWorkerPlanAppliesDeterministicRegistrationOrder(t *testing.T) {
	plan, err := temporal.NewWorkerPlan(temporal.WorkerSpec{TaskQueue: "graphs", BuildID: "build-7", Compatibility: temporal.VersionCompatible})
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.SetWorkflow("graph.workflow", func() {}); err != nil {
		t.Fatal(err)
	}
	if err := plan.AddActivity("node.z", func() {}); err != nil {
		t.Fatal(err)
	}
	if err := plan.AddActivity("node.a", func() {}); err != nil {
		t.Fatal(err)
	}
	registrar := &fakeRegistrar{}
	if err := plan.Apply(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	want := []string{"version:build-7", "workflow:graph.workflow", "activity:node.a", "activity:node.z"}
	if !reflect.DeepEqual(registrar.calls, want) {
		t.Fatalf("calls=%v", registrar.calls)
	}
}

func TestWorkerPlanRejectsDuplicateAndHonorsCancellation(t *testing.T) {
	plan, _ := temporal.NewWorkerPlan(temporal.WorkerSpec{TaskQueue: "graphs", BuildID: "build"})
	_ = plan.SetWorkflow("workflow", func() {})
	_ = plan.AddActivity("node", func() {})
	if err := plan.AddActivity("node", func() {}); !errors.Is(err, temporal.ErrDuplicateRegistration) {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	registrar := &fakeRegistrar{}
	if err := plan.Apply(ctx, registrar); !errors.Is(err, context.Canceled) || len(registrar.calls) != 0 {
		t.Fatalf("err=%v calls=%v", err, registrar.calls)
	}
}
