package distributed_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
)

type jsonCodec[P any] struct{}

func (jsonCodec[P]) Encode(value P) ([]byte, error) { return json.Marshal(value) }
func (jsonCodec[P]) Decode(data []byte) (P, error) {
	var value P
	err := json.Unmarshal(data, &value)
	return value, err
}

func TestCodecQueueIsolatesPayloadAliases(t *testing.T) {
	now := time.Date(2026, 7, 19, 5, 0, 0, 0, time.UTC)
	index := 0
	queue, err := distributed.NewCodecMemoryQueue(distributed.QueueOptions{
		Clock:       func() time.Time { return now },
		IDGenerator: func() string { index++; return string(rune('0' + index)) },
	}, jsonCodec[map[string][]int]{})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string][]int{"values": {1}}
	_, _ = queue.Enqueue(context.Background(), distributed.EnqueueRequest[map[string][]int]{Payload: payload})
	payload["values"][0] = 99
	claimed, _ := queue.Claim(context.Background(), "worker", 1, time.Minute)
	if claimed[0].Payload["values"][0] != 1 {
		t.Fatalf("payload=%v", claimed[0].Payload)
	}
	claimed[0].Payload["values"][0] = 88
	_ = queue.Nack(context.Background(), claimed[0].Lease, 0)
	claimed, _ = queue.Claim(context.Background(), "worker", 1, time.Minute)
	if claimed[0].Payload["values"][0] != 1 {
		t.Fatalf("reclaimed payload=%v", claimed[0].Payload)
	}
}

func TestCodecQueueIdempotentEnqueueBindsPayload(t *testing.T) {
	now := time.Now()
	ids := 0
	queue, _ := distributed.NewCodecMemoryQueue(distributed.QueueOptions{
		Clock:       func() time.Time { return now },
		IDGenerator: func() string { ids++; return "id-" + string(rune('0'+ids)) },
	}, jsonCodec[taskPayload]{})
	request := distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 3}, IdempotencyKey: "submit-1"}
	first, err := queue.Enqueue(context.Background(), request)
	second, secondErr := queue.Enqueue(context.Background(), request)
	if err != nil || secondErr != nil || first != second || ids != 1 {
		t.Fatalf("first=%q second=%q ids=%d err=%v/%v", first, second, ids, err, secondErr)
	}
	_, err = queue.Enqueue(context.Background(), distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 4}, IdempotencyKey: "submit-1"})
	if !errors.Is(err, distributed.ErrIdempotencyConflict) {
		t.Fatalf("err=%v", err)
	}
}
