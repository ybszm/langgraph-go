package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/backend/temporal"
	"github.com/wahanbo/langgraph-go/checkpoint"
)

type fakeCheckpointSaver struct {
	found bool
	gets  int
}

func (s *fakeCheckpointSaver) GetTuple(_ context.Context, config checkpoint.Config) (checkpoint.Tuple, bool, error) {
	s.gets++
	return checkpoint.Tuple{Config: config}, s.found, nil
}
func (*fakeCheckpointSaver) List(context.Context, checkpoint.ListOptions) ([]checkpoint.Tuple, error) {
	return nil, nil
}
func (*fakeCheckpointSaver) Put(context.Context, checkpoint.Config, checkpoint.Checkpoint, checkpoint.Metadata, map[string]string) (checkpoint.Config, error) {
	return checkpoint.Config{}, nil
}
func (*fakeCheckpointSaver) PutWrites(context.Context, checkpoint.Config, []checkpoint.PendingWrite) error {
	return nil
}
func (*fakeCheckpointSaver) DeleteThread(context.Context, string) error { return nil }

type fakeContinuationClient struct {
	request temporal.ContinueRequest[workflowInput]
	calls   int
}

func (c *fakeContinuationClient) ContinueAsNew(_ context.Context, request temporal.ContinueRequest[workflowInput]) error {
	c.calls++
	c.request = request
	return nil
}

func TestContinuationRequiresCommittedExactCheckpoint(t *testing.T) {
	saver := &fakeCheckpointSaver{found: true}
	driver := &fakeContinuationClient{}
	coordinator, err := temporal.NewContinuationCoordinator(driver, saver, temporal.ContinuePolicy{MaxHistoryEvents: 100})
	if err != nil {
		t.Fatal(err)
	}
	continued, err := coordinator.MaybeContinue(context.Background(), temporal.ContinueRequest[workflowInput]{
		Input:      workflowInput{Value: 7},
		Checkpoint: checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp-9"},
		Stats:      temporal.HistoryStats{Events: 100}, Generation: 2,
	})
	if err != nil || !continued || driver.calls != 1 || driver.request.Checkpoint.CheckpointID != "cp-9" || driver.request.Generation != 2 {
		t.Fatalf("continued=%v request=%+v calls=%d err=%v", continued, driver.request, driver.calls, err)
	}
}

func TestContinuationBelowThresholdDoesNotTouchCheckpoint(t *testing.T) {
	saver := &fakeCheckpointSaver{found: true}
	driver := &fakeContinuationClient{}
	coordinator, _ := temporal.NewContinuationCoordinator(driver, saver, temporal.ContinuePolicy{MaxHistoryEvents: 100})
	continued, err := coordinator.MaybeContinue(context.Background(), temporal.ContinueRequest[workflowInput]{
		Stats: temporal.HistoryStats{Events: 99},
	})
	if err != nil || continued || saver.gets != 0 || driver.calls != 0 {
		t.Fatalf("continued=%v gets=%d calls=%d err=%v", continued, saver.gets, driver.calls, err)
	}
}

func TestContinuationRejectsUncommittedCheckpoint(t *testing.T) {
	saver := &fakeCheckpointSaver{found: false}
	coordinator, _ := temporal.NewContinuationCoordinator(&fakeContinuationClient{}, saver, temporal.ContinuePolicy{MaxSteps: 1})
	_, err := coordinator.MaybeContinue(context.Background(), temporal.ContinueRequest[workflowInput]{
		Checkpoint: checkpoint.Config{ThreadID: "thread", CheckpointID: "missing"},
		Stats:      temporal.HistoryStats{Steps: 1},
	})
	if !errors.Is(err, temporal.ErrCheckpointNotCommitted) {
		t.Fatalf("err=%v", err)
	}
}
