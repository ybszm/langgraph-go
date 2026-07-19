package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/store"
	storesqlite "github.com/ybszm/langgraph-go/store/sqlite"
	"github.com/ybszm/langgraph-go/store/storetest"
)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s, err := storesqlite.Open(context.Background(), filepath.Join(t.TempDir(), "contract.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persistent.db")
	s, err := storesqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), store.Namespace{"users", "123"}, "prefs", store.Value{"theme": "dark"}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Get(context.Background(), store.Namespace{"users", "123"}, "prefs")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storesqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	after, err := reopened.Get(context.Background(), store.Namespace{"users", "123"}, "prefs")
	if err != nil || after == nil || after.Value["theme"] != "dark" || after.CreatedAt != before.CreatedAt {
		t.Fatalf("reopened item=%+v before=%+v err=%v", after, before, err)
	}
}

func TestBatchRollsBackAllWrites(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "atomic.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := storesqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_bad_store_key BEFORE INSERT ON store_items WHEN NEW.key='bad' BEGIN SELECT RAISE(ABORT, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
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

func TestInvalidConstructors(t *testing.T) {
	if _, err := storesqlite.New(nil); !errors.Is(err, store.ErrInvalidOperation) {
		t.Fatalf("New(nil) err=%v", err)
	}
	if _, err := storesqlite.Open(context.Background(), ""); !errors.Is(err, store.ErrInvalidOperation) {
		t.Fatalf("Open(empty) err=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := storesqlite.Open(canceled, filepath.Join(t.TempDir(), "canceled.db")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open(canceled) err=%v", err)
	}
}

func TestSearchPushesNamespaceBeforeDecodingRows(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "pushdown.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := storesqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), store.Namespace{"docs"}, "good", store.Value{"kind": "guide", "rank": 2}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().UnixNano()
	if _, err := db.Exec(`INSERT INTO store_items(namespace,key,value,created_at,updated_at) VALUES (?,?,?,?,?)`, `["outside"]`, "broken", `{`, now, now); err != nil {
		t.Fatal(err)
	}
	items, err := s.Search(context.Background(), store.Namespace{"docs"}, store.SearchOptions{
		Filter: store.Value{"kind": "guide", "rank": map[string]any{"$gte": 2}}, Limit: 10,
	})
	if err != nil || len(items) != 1 || items[0].Key != "good" {
		t.Fatalf("Search items=%+v err=%v", items, err)
	}
}
