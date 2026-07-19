package distributed_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
)

func TestMemoryEventLogTailDeliversFutureEventsAndClosesOnTerminal(t *testing.T) {
	sequence := 0
	log, _ := distributed.NewMemoryEventLog(distributed.EventLogOptions{
		Clock: time.Now, IDGenerator: func() string { sequence++; return string(rune('0' + sequence)) },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tail := log.Tail(ctx, distributed.EventQuery{ThreadID: "thread", RunID: "run", Buffer: 1})
	_, _ = log.Append(context.Background(), distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "updates", Data: json.RawMessage(`1`)})
	_, _ = log.Append(context.Background(), distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "done", Data: json.RawMessage(`null`), Terminal: true})
	var events []distributed.Event
	for result := range tail {
		if result.Error != nil {
			t.Fatal(result.Error)
		}
		events = append(events, result.Event)
	}
	if len(events) != 2 || events[0].Sequence != 1 || !events[1].Terminal {
		t.Fatalf("events=%+v", events)
	}
}

func TestMemoryEventLogTailCancellationClosesWhileIdle(t *testing.T) {
	log, _ := distributed.NewMemoryEventLog(distributed.EventLogOptions{Clock: time.Now, IDGenerator: func() string { return "id" }})
	ctx, cancel := context.WithCancel(context.Background())
	tail := log.Tail(ctx, distributed.EventQuery{ThreadID: "thread", RunID: "run"})
	cancel()
	select {
	case _, open := <-tail:
		if open {
			t.Fatal("unexpected tail event")
		}
	case <-time.After(time.Second):
		t.Fatal("tail did not close on cancellation")
	}
}
