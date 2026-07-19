package distributed_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
)

func TestMemoryEventLogAppendAndCursorReplay(t *testing.T) {
	now := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	ids := []string{"event-1", "event-2"}
	index := 0
	log, err := distributed.NewMemoryEventLog(distributed.EventLogOptions{
		Clock:       func() time.Time { return now },
		IDGenerator: func() string { value := ids[index]; index++; return value },
	})
	if err != nil {
		t.Fatal(err)
	}
	data := json.RawMessage(`{"value":1}`)
	first, _ := log.Append(context.Background(), distributed.AppendEvent{ThreadID: "thread", RunID: "run", TaskID: "task", Mode: "updates", Data: data})
	data[9] = '9'
	second, _ := log.Append(context.Background(), distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "values", Data: json.RawMessage(`{"value":2}`)})
	if first.ID != "event-1" || first.Sequence != 1 || second.Sequence != 2 || !first.CreatedAt.Equal(now) {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	events, err := log.List(context.Background(), distributed.EventQuery{ThreadID: "thread", RunID: "run", AfterID: first.ID, Limit: 10})
	if err != nil || len(events) != 1 || events[0].ID != second.ID || string(events[0].Data) != `{"value":2}` {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	all, _ := log.List(context.Background(), distributed.EventQuery{ThreadID: "thread", RunID: "run"})
	if string(all[0].Data) != `{"value":1}` {
		t.Fatalf("source alias mutated log: %s", all[0].Data)
	}
}

func TestMemoryEventLogIdempotencyAndMissingCursor(t *testing.T) {
	count := 0
	log, _ := distributed.NewMemoryEventLog(distributed.EventLogOptions{Clock: time.Now, IDGenerator: func() string { count++; return "event" }})
	request := distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "updates", Data: json.RawMessage(`1`), IdempotencyKey: "task:custom:0"}
	first, err := log.Append(context.Background(), request)
	replayed, replayErr := log.Append(context.Background(), request)
	if err != nil || replayErr != nil || first.ID != replayed.ID || count != 1 {
		t.Fatalf("first=%+v replay=%+v count=%d err=%v/%v", first, replayed, count, err, replayErr)
	}
	request.Data = json.RawMessage(`2`)
	if _, err := log.Append(context.Background(), request); !errors.Is(err, distributed.ErrEventConflict) {
		t.Fatalf("err=%v", err)
	}
	if _, err := log.List(context.Background(), distributed.EventQuery{ThreadID: "thread", RunID: "run", AfterID: "missing"}); !errors.Is(err, distributed.ErrEventCursorNotFound) {
		t.Fatalf("err=%v", err)
	}
}
