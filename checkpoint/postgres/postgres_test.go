package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointpostgres "github.com/ybszm/langgraph-go/checkpoint/postgres"
	"github.com/ybszm/langgraph-go/checkpoint/savertest"
)

const postgresDSNEnv = "LANGGRAPH_POSTGRES_DSN"

func TestInvalidConstructors(t *testing.T) {
	if _, err := checkpointpostgres.New(nil); !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("New(nil) err=%v", err)
	}
	if _, err := checkpointpostgres.Open(context.Background(), ""); !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("Open(empty) err=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := checkpointpostgres.Open(canceled, "postgres://unused"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open(canceled) err=%v", err)
	}
}

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv(postgresDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run PostgreSQL integration tests", postgresDSNEnv)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func resetTables(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`TRUNCATE checkpoint_writes, checkpoints, checkpoint_blobs`); err != nil {
		t.Fatal(err)
	}
}

func TestSaverContract(t *testing.T) {
	db := integrationDB(t)
	bootstrap, err := checkpointpostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	savertest.Run(t, func(t *testing.T) checkpoint.Saver {
		resetTables(t, db)
		saver, err := checkpointpostgres.New(db)
		if err != nil {
			t.Fatal(err)
		}
		return saver
	})
}

func TestPutTransactionRollsBackBlobs(t *testing.T) {
	db := integrationDB(t)
	saver, err := checkpointpostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := saver.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	resetTables(t, db)
	if _, err := db.Exec(`ALTER TABLE checkpoints DROP CONSTRAINT IF EXISTS reject_bad_id`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE checkpoints ADD CONSTRAINT reject_bad_id CHECK (checkpoint_id <> 'bad')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`ALTER TABLE checkpoints DROP CONSTRAINT IF EXISTS reject_bad_id`) })
	value := checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: "bad",
		Timestamp: contractTimestamp(), Values: map[string]checkpoint.EncodedValue{
			"state": {Type: "tests.string", Version: 1, Data: []byte("rollback")},
		},
		ChannelVersions: map[string]string{"state": "bad-version"},
		VersionsSeen:    map[string]map[string]string{},
	}
	_, err = saver.Put(context.Background(), checkpoint.Config{ThreadID: "thread"}, value, nil, value.ChannelVersions)
	if err == nil {
		t.Fatal("Put unexpectedly succeeded")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM checkpoint_blobs WHERE version='bad-version'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("transaction leaked %d blobs", count)
	}
}

func TestConcurrentSaverInstancesAndCancellation(t *testing.T) {
	db := integrationDB(t)
	bootstrap, err := checkpointpostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	resetTables(t, db)
	var group sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			saver, newErr := checkpointpostgres.New(db)
			if newErr != nil {
				t.Errorf("New: %v", newErr)
				return
			}
			id := string(rune('a' + worker))
			value := checkpoint.Checkpoint{
				Version: checkpoint.CurrentVersion, ID: id, Timestamp: contractTimestamp(),
				Values:          map[string]checkpoint.EncodedValue{"state": {Type: "tests.string", Version: 1, Data: []byte(id)}},
				ChannelVersions: map[string]string{"state": id}, VersionsSeen: map[string]map[string]string{},
			}
			if _, putErr := saver.Put(context.Background(), checkpoint.Config{ThreadID: "thread-" + id}, value, checkpoint.Metadata{"worker": worker}, value.ChannelVersions); putErr != nil {
				t.Errorf("Put(%d): %v", worker, putErr)
			}
		}()
	}
	group.Wait()
	listed, err := bootstrap.List(context.Background(), checkpoint.ListOptions{})
	if err != nil || len(listed) != 12 {
		t.Fatalf("List len=%d err=%v", len(listed), err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = bootstrap.List(canceled, checkpoint.ListOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled List err=%v", err)
	}
}

func contractTimestamp() time.Time {
	return time.Date(2026, 7, 18, 15, 0, 0, 0, time.UTC)
}
