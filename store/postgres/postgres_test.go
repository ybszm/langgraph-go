package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wahanbo/langgraph-go/store"
	storepostgres "github.com/wahanbo/langgraph-go/store/postgres"
	"github.com/wahanbo/langgraph-go/store/storetest"
)

const postgresDSNEnv = "LANGGRAPH_POSTGRES_DSN"

func TestInvalidConstructors(t *testing.T) {
	if _, err := storepostgres.New(nil); !errors.Is(err, store.ErrInvalidOperation) {
		t.Fatalf("New(nil) err=%v", err)
	}
	if _, err := storepostgres.Open(context.Background(), ""); !errors.Is(err, store.ErrInvalidOperation) {
		t.Fatalf("Open(empty) err=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := storepostgres.Open(canceled, "postgres://unused"); !errors.Is(err, context.Canceled) {
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

func resetItems(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`TRUNCATE store_items`); err != nil {
		t.Fatal(err)
	}
}

func TestStoreContract(t *testing.T) {
	db := integrationDB(t)
	bootstrap, err := storepostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	storetest.Run(t, func(t *testing.T) store.Store {
		resetItems(t, db)
		s, err := storepostgres.New(db)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestBatchRollsBackAllWrites(t *testing.T) {
	db := integrationDB(t)
	s, err := storepostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	resetItems(t, db)
	if _, err := db.Exec(`ALTER TABLE store_items DROP CONSTRAINT IF EXISTS reject_bad_store_key`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE store_items ADD CONSTRAINT reject_bad_store_key CHECK (key <> 'bad')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`ALTER TABLE store_items DROP CONSTRAINT IF EXISTS reject_bad_store_key`) })
	_, err = s.Batch(context.Background(), []store.Operation{
		store.PutOp{Namespace: store.Namespace{"atomic"}, Key: "good", Value: store.Value{"value": 1}},
		store.PutOp{Namespace: store.Namespace{"atomic"}, Key: "bad", Value: store.Value{"value": 2}},
	})
	if err == nil {
		t.Fatal("Batch unexpectedly succeeded")
	}
	item, getErr := s.Get(context.Background(), store.Namespace{"atomic"}, "good")
	if getErr != nil || item != nil {
		t.Fatalf("partial write item=%+v err=%v", item, getErr)
	}
}

func TestConcurrentStoreInstances(t *testing.T) {
	db := integrationDB(t)
	bootstrap, err := storepostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	resetItems(t, db)
	var group sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			s, newErr := storepostgres.New(db)
			if newErr != nil {
				t.Errorf("New: %v", newErr)
				return
			}
			if putErr := s.Put(context.Background(), store.Namespace{"workers", fmt.Sprintf("%02d", worker)}, "item", store.Value{"worker": worker}); putErr != nil {
				t.Errorf("Put(%d): %v", worker, putErr)
			}
		}()
	}
	group.Wait()
	items, err := bootstrap.Search(context.Background(), store.Namespace{"workers"}, store.SearchOptions{Limit: 20})
	if err != nil || len(items) != 12 {
		t.Fatalf("Search len=%d err=%v", len(items), err)
	}
}

func TestSearchPushdownPreservesFilterSemantics(t *testing.T) {
	db := integrationDB(t)
	s, err := storepostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	resetItems(t, db)
	for _, item := range []struct {
		namespace store.Namespace
		key       string
		value     store.Value
	}{
		{store.Namespace{"docs", "public"}, "guide", store.Value{"active": true, "meta": map[string]any{"kind": "guide"}, "rank": 3}},
		{store.Namespace{"docs", "private"}, "private", store.Value{"active": true, "meta": map[string]any{"kind": "guide"}, "rank": 4}},
		{store.Namespace{"docs", "public"}, "draft", store.Value{"active": false, "meta": map[string]any{"kind": "guide"}, "rank": 5}},
	} {
		if err := s.Put(context.Background(), item.namespace, item.key, item.value); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.Search(context.Background(), store.Namespace{"docs", "public"}, store.SearchOptions{
		Filter: store.Value{"active": true, "meta": map[string]any{"kind": "guide"}, "rank": map[string]any{"$gte": 2}},
		Limit:  10,
	})
	if err != nil || len(items) != 1 || items[0].Key != "guide" {
		t.Fatalf("Search items=%+v err=%v", items, err)
	}
}
