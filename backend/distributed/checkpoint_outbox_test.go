package distributed_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/checkpoint"
)

type writeSaver struct {
	tuple checkpoint.Tuple
	puts  int
}

type checkpointJSONCodec[P any] struct{}

func (checkpointJSONCodec[P]) Encode(value P) (checkpoint.EncodedValue, error) {
	data, err := json.Marshal(value)
	return checkpoint.EncodedValue{Type: "json", Version: 1, Data: data}, err
}
func (checkpointJSONCodec[P]) Decode(value checkpoint.EncodedValue) (P, error) {
	var decoded P
	err := json.Unmarshal(value.Data, &decoded)
	return decoded, err
}

func (s *writeSaver) GetTuple(context.Context, checkpoint.Config) (checkpoint.Tuple, bool, error) {
	return s.tuple, true, nil
}
func (*writeSaver) List(context.Context, checkpoint.ListOptions) ([]checkpoint.Tuple, error) {
	return nil, nil
}
func (*writeSaver) Put(context.Context, checkpoint.Config, checkpoint.Checkpoint, checkpoint.Metadata, map[string]string) (checkpoint.Config, error) {
	return checkpoint.Config{}, nil
}
func (s *writeSaver) PutWrites(_ context.Context, _ checkpoint.Config, writes []checkpoint.PendingWrite) error {
	s.puts++
	s.tuple.PendingWrites = append(s.tuple.PendingWrites, writes...)
	return nil
}
func (*writeSaver) DeleteThread(context.Context, string) error { return nil }

func TestCheckpointOutboxWritesAndVerifiesPendingResult(t *testing.T) {
	config := checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp"}
	saver := &writeSaver{tuple: checkpoint.Tuple{Config: config}}
	outbox, err := distributed.NewCheckpointOutbox(saver, checkpointJSONCodec[completion]{})
	if err != nil {
		t.Fatal(err)
	}
	err = outbox.Commit(context.Background(), distributed.Completion[distributed.CheckpointResult[completion]]{
		TaskID: "task", Attempt: 1, LeaseToken: "lease",
		Value: distributed.CheckpointResult[completion]{Config: config, Channel: checkpoint.TaskResultChannel, Index: 0, Value: completion{Value: 7}},
	})
	if err != nil || saver.puts != 1 || len(saver.tuple.PendingWrites) != 1 {
		t.Fatalf("puts=%d writes=%+v err=%v", saver.puts, saver.tuple.PendingWrites, err)
	}
	write := saver.tuple.PendingWrites[0]
	if write.TaskID != "task" || write.Channel != checkpoint.TaskResultChannel || !bytes.Contains(write.Value.Data, []byte("7")) {
		t.Fatalf("write=%+v", write)
	}
	// A redelivery with a fresh lease and identical result is idempotent.
	err = outbox.Commit(context.Background(), distributed.Completion[distributed.CheckpointResult[completion]]{
		TaskID: "task", Attempt: 2, LeaseToken: "lease-2",
		Value: distributed.CheckpointResult[completion]{Config: config, Channel: checkpoint.TaskResultChannel, Index: 0, Value: completion{Value: 7}},
	})
	if err != nil || saver.puts != 1 {
		t.Fatalf("puts=%d err=%v", saver.puts, err)
	}
}

func TestCheckpointOutboxDetectsExistingDifferentResult(t *testing.T) {
	config := checkpoint.Config{ThreadID: "thread", CheckpointID: "cp"}
	codec := checkpointJSONCodec[completion]{}
	encoded, _ := codec.Encode(completion{Value: 1})
	saver := &writeSaver{tuple: checkpoint.Tuple{Config: config, PendingWrites: []checkpoint.PendingWrite{{
		TaskID: "task", Index: 0, Channel: checkpoint.TaskResultChannel,
		Value: encoded,
	}}}}
	outbox, _ := distributed.NewCheckpointOutbox(saver, codec)
	err := outbox.Commit(context.Background(), distributed.Completion[distributed.CheckpointResult[completion]]{
		TaskID: "task", Attempt: 2, LeaseToken: "lease",
		Value: distributed.CheckpointResult[completion]{Config: config, Channel: checkpoint.TaskResultChannel, Index: 0, Value: completion{Value: 2}},
	})
	if !errors.Is(err, distributed.ErrCompletionConflict) || saver.puts != 0 {
		t.Fatalf("puts=%d err=%v", saver.puts, err)
	}
}
