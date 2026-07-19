package sdk

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/client"
)

type fakeDeployment struct {
	token   []byte
	current client.WorkerDeploymentSetCurrentVersionOptions
	ramping client.WorkerDeploymentSetRampingVersionOptions
	version client.WorkerDeploymentVersionDescription
	deleted client.WorkerDeploymentDeleteVersionOptions
}

func (f *fakeDeployment) Describe(context.Context, client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error) {
	return client.WorkerDeploymentDescribeResponse{ConflictToken: f.token}, nil
}
func (f *fakeDeployment) SetCurrentVersion(_ context.Context, o client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error) {
	f.current = o
	return client.WorkerDeploymentSetCurrentVersionResponse{}, nil
}
func (f *fakeDeployment) SetRampingVersion(_ context.Context, o client.WorkerDeploymentSetRampingVersionOptions) (client.WorkerDeploymentSetRampingVersionResponse, error) {
	f.ramping = o
	return client.WorkerDeploymentSetRampingVersionResponse{}, nil
}
func (f *fakeDeployment) DescribeVersion(context.Context, client.WorkerDeploymentDescribeVersionOptions) (client.WorkerDeploymentVersionDescription, error) {
	return f.version, nil
}
func (f *fakeDeployment) DeleteVersion(_ context.Context, o client.WorkerDeploymentDeleteVersionOptions) (client.WorkerDeploymentDeleteVersionResponse, error) {
	f.deleted = o
	return client.WorkerDeploymentDeleteVersionResponse{}, nil
}

func TestDeploymentManagerUsesConflictAndDrainageGuards(t *testing.T) {
	fake := &fakeDeployment{token: []byte("token")}
	manager, err := newDeploymentManager(func(name string) deploymentHandle {
		if name != "graph" {
			t.Fatalf("name=%q", name)
		}
		return fake
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Promote(context.Background(), "graph", "b2"); err != nil {
		t.Fatal(err)
	}
	if fake.current.BuildID != "b2" || string(fake.current.ConflictToken) != "token" || fake.current.AllowNoPollers || fake.current.IgnoreMissingTaskQueues {
		t.Fatalf("unsafe promotion options: %#v", fake.current)
	}
	if err := manager.Ramp(context.Background(), "graph", "b3", 25); err != nil {
		t.Fatal(err)
	}
	if fake.ramping.BuildID != "b3" || fake.ramping.Percentage != 25 || string(fake.ramping.ConflictToken) != "token" {
		t.Fatalf("ramp options: %#v", fake.ramping)
	}
	if err := manager.Deprecate(context.Background(), "graph", "b1"); !errors.Is(err, ErrVersionNotDrained) {
		t.Fatalf("expected drainage guard, got %v", err)
	}
	fake.version.Info.DrainageInfo = &client.WorkerDeploymentVersionDrainageInfo{DrainageStatus: client.WorkerDeploymentVersionDrainageStatusDrained}
	if err := manager.Deprecate(context.Background(), "graph", "b1"); err != nil {
		t.Fatal(err)
	}
	if fake.deleted.BuildID != "b1" || fake.deleted.SkipDrainage {
		t.Fatalf("unsafe delete: %#v", fake.deleted)
	}
	if err := manager.Ramp(context.Background(), "graph", "b3", 101); err == nil {
		t.Fatal("invalid ramp accepted")
	}
}
