package distributed_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wahanbo/langgraph-go/backend/distributed"
)

func distributedPostgresDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("LANGGRAPH_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set LANGGRAPH_POSTGRES_DSN for PostgreSQL distributed integration")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.PingContext(context.Background()); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPostgresEventLogMultiInstanceSequenceTailAndRetention(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	var ids atomic.Int64
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	options := distributed.PostgresEventLogOptions{EventLogOptions: distributed.EventLogOptions{Clock: func() time.Time { return now }, IDGenerator: func() string { return fmt.Sprintf("event-%03d", ids.Add(1)) }}, PollInterval: 5 * time.Millisecond}
	left, err := distributed.NewPostgresEventLog(db, options)
	if err != nil {
		t.Fatal(err)
	}
	right, err := distributed.NewPostgresEventLog(db, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = left.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err = right.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `TRUNCATE distributed_event_streams CASCADE`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			target := left
			if i%2 == 1 {
				target = right
			}
			_, err := target.Append(ctx, distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "updates", Data: json.RawMessage(fmt.Sprintf(`{"value":%d}`, i))})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	events, err := left.List(ctx, distributed.EventQuery{ThreadID: "thread", RunID: "run"})
	if err != nil || len(events) != 20 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
	sequences := make([]int, len(events))
	for i, event := range events {
		sequences[i] = int(event.Sequence)
	}
	sort.Ints(sequences)
	for i, value := range sequences {
		if value != i+1 {
			t.Fatalf("sequences=%v", sequences)
		}
	}
	tail := right.Tail(ctx, distributed.EventQuery{ThreadID: "thread", RunID: "run", AfterID: events[len(events)-1].ID, Buffer: 1})
	terminal, err := left.Append(ctx, distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "done", Data: json.RawMessage(`null`), Terminal: true})
	if err != nil {
		t.Fatal(err)
	}
	result, open := <-tail
	if !open || result.Error != nil || result.Event.ID != terminal.ID || !result.Event.Terminal {
		t.Fatalf("tail=%+v open=%v", result, open)
	}
	if _, open = <-tail; open {
		t.Fatal("tail remained open after terminal")
	}
	if _, err = right.Append(ctx, distributed.AppendEvent{ThreadID: "thread", RunID: "run", Mode: "late", Data: json.RawMessage(`null`)}); !errors.Is(err, distributed.ErrEventConflict) {
		t.Fatalf("late err=%v", err)
	}
	stats, err := left.Stats(ctx)
	if err != nil || stats.Streams != 1 || stats.Events != 21 || stats.TerminalStreams != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	count, err := right.Prune(ctx, now.Add(time.Second))
	if err != nil || count != 1 {
		t.Fatalf("prune=%d err=%v", count, err)
	}
	stats, err = left.Stats(ctx)
	if err != nil || stats.Streams != 0 || stats.Events != 0 {
		t.Fatalf("stats after prune=%+v err=%v", stats, err)
	}
}

func TestPostgresEventLogConcurrentIdempotencyAcrossInstances(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	var ids atomic.Int64
	options := distributed.PostgresEventLogOptions{EventLogOptions: distributed.EventLogOptions{Clock: time.Now, IDGenerator: func() string { return fmt.Sprintf("idem-%d", ids.Add(1)) }}, PollInterval: time.Millisecond}
	left, _ := distributed.NewPostgresEventLog(db, options)
	right, _ := distributed.NewPostgresEventLog(db, options)
	if err := left.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE distributed_event_streams CASCADE`); err != nil {
		t.Fatal(err)
	}
	request := distributed.AppendEvent{ThreadID: "idem-thread", RunID: "idem-run", Mode: "updates", Data: json.RawMessage(`{"same":true}`), IdempotencyKey: "task-1"}
	start := make(chan struct{})
	results := make([]distributed.Event, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, target := range []*distributed.PostgresEventLog{left, right} {
		wg.Add(1)
		go func(i int, target *distributed.PostgresEventLog) {
			defer wg.Done()
			<-start
			results[i], errs[i] = target.Append(ctx, request)
		}(i, target)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || results[0].ID != results[1].ID || ids.Load() != 1 {
		t.Fatalf("results=%+v ids=%d errs=%v", results, ids.Load(), errs)
	}
	events, err := left.List(ctx, distributed.EventQuery{ThreadID: "idem-thread", RunID: "idem-run"})
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}
