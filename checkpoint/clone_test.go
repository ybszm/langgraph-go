package checkpoint_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
)

func TestCloneCheckpointIsolatesTaskTriggers(t *testing.T) {
	source := checkpoint.Checkpoint{Next: []checkpoint.Task{{ID: "task", Name: "node", Triggers: []string{"source"}}}}
	cloned := checkpoint.CloneCheckpoint(source)
	cloned.Next[0].Triggers[0] = "mutated"
	if source.Next[0].Triggers[0] != "source" {
		t.Fatalf("source triggers=%v", source.Next[0].Triggers)
	}
}

func TestCheckpointRejectsEmptyTaskTrigger(t *testing.T) {
	value := checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: "checkpoint", Timestamp: time.Unix(1, 0),
		Values: map[string]checkpoint.EncodedValue{}, ChannelVersions: map[string]string{},
		VersionsSeen: map[string]map[string]string{}, Next: []checkpoint.Task{{ID: "task", Name: "node", Triggers: []string{""}}},
	}
	if err := value.Validate(); !errors.Is(err, checkpoint.ErrInvalidCheckpoint) {
		t.Fatalf("Validate error=%v", err)
	}
}
