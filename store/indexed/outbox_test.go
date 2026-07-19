package indexed_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/store"
	"github.com/wahanbo/langgraph-go/store/indexed"
	storememory "github.com/wahanbo/langgraph-go/store/memory"
	storesqlite "github.com/wahanbo/langgraph-go/store/sqlite"
	vectormemory "github.com/wahanbo/langgraph-go/store/vector/memory"
	_ "modernc.org/sqlite"
)

func TestSQLiteVectorOutboxRepairsAfterHandleRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vector-outbox.db")
	openOutbox := func() (*stdsql.DB, *indexed.SQLiteOutbox) {
		db, err := stdsql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		outbox, err := indexed.NewSQLiteOutbox(db)
		if err != nil {
			t.Fatal(err)
		}
		if err := outbox.Setup(ctx); err != nil {
			t.Fatal(err)
		}
		return db, outbox
	}
	embedder := &fakeEmbedder{vectors: map[string][]float32{"document": {1, 0}, "query": {1, 0}}}
	delegate, _ := vectormemory.New(2, store.VectorCosine)
	vectorIndex := &toggleIndex{delegate: delegate, fail: true}
	base := storememory.New()
	firstDB, firstOutbox := openOutbox()
	first, err := indexed.New(base, indexed.Config{
		Embedder: embedder, Index: vectorIndex, Fields: []string{"text"}, Outbox: firstOutbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, store.Namespace{"docs"}, "one", store.Value{"text": "document"}); !errors.Is(err, indexed.ErrIndexSync) {
		t.Fatalf("Put err=%v", err)
	}
	pending, err := firstOutbox.List(ctx, 0)
	if err != nil || len(pending) != 1 || pending[0].Key != "one" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}

	vectorIndex.mu.Lock()
	vectorIndex.fail = false
	vectorIndex.mu.Unlock()
	secondDB, secondOutbox := openOutbox()
	defer secondDB.Close()
	second, err := indexed.New(base, indexed.Config{
		Embedder: embedder, Index: vectorIndex, Fields: []string{"text"}, Outbox: secondOutbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	if repaired, err := second.RepairPending(ctx, 10); err != nil || repaired != 1 {
		t.Fatalf("RepairPending repaired=%d err=%v", repaired, err)
	}
	if repaired, err := second.RepairPending(ctx, 10); err != nil || repaired != 0 {
		t.Fatalf("idempotent RepairPending repaired=%d err=%v", repaired, err)
	}
	items, err := second.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Query: "query", Limit: 1})
	if err != nil || len(items) != 1 || items[0].Score == nil || *items[0].Score != 1 {
		t.Fatalf("repaired search=%+v err=%v", items, err)
	}
}

func TestVectorOutboxDiscardsIntentWhenBaseCommitDidNotHappen(t *testing.T) {
	ctx := context.Background()
	db, err := stdsql.Open("sqlite", filepath.Join(t.TempDir(), "stale.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	outbox, _ := indexed.NewSQLiteOutbox(db)
	if err := outbox.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	base := storememory.New()
	_ = base.Put(ctx, store.Namespace{"docs"}, "one", store.Value{"text": "still present"})
	index, _ := vectormemory.New(2, store.VectorCosine)
	_ = index.Upsert(ctx, []store.VectorDocument{{Namespace: store.Namespace{"docs"}, Key: "one", Field: "text", Vector: []float32{1, 0}}})
	_, err = outbox.Enqueue(ctx, []indexed.VectorMutation{{Namespace: store.Namespace{"docs"}, Key: "one", Missing: true}})
	if err != nil {
		t.Fatal(err)
	}
	workflow, _ := indexed.New(base, indexed.Config{
		Embedder: &fakeEmbedder{vectors: map[string][]float32{"query": {1, 0}}}, Index: index, Fields: []string{"text"}, Outbox: outbox,
	})
	if repaired, err := workflow.RepairPending(ctx, 0); err != nil || repaired != 1 {
		t.Fatalf("RepairPending repaired=%d err=%v", repaired, err)
	}
	matches, err := index.Search(ctx, store.VectorQuery{NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 1})
	if err != nil || len(matches) != 1 {
		t.Fatalf("stale pre-commit intent changed index: matches=%+v err=%v", matches, err)
	}
}

func TestIndexedSweepExpiredActivelyDeletesExternalVector(t *testing.T) {
	ctx := context.Background()
	db, err := stdsql.Open("sqlite", filepath.Join(t.TempDir(), "ttl-vector.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base, err := storesqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	outbox, _ := indexed.NewSQLiteOutbox(db)
	if err := outbox.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	vectorIndex, _ := vectormemory.New(2, store.VectorCosine)
	workflow, err := indexed.New(base, indexed.Config{
		Embedder: &fakeEmbedder{vectors: map[string][]float32{"document": {1, 0}}},
		Index:    vectorIndex, Fields: []string{"text"}, Outbox: outbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.PutWithTTL(ctx, store.Namespace{"ttl"}, "expired", store.Value{"text": "document"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE store_items SET expires_at=0 WHERE key='expired'"); err != nil {
		t.Fatal(err)
	}
	deleted, err := workflow.SweepExpired(ctx)
	if err != nil || deleted != 1 {
		t.Fatalf("SweepExpired deleted=%d err=%v", deleted, err)
	}
	matches, err := vectorIndex.Search(ctx, store.VectorQuery{NamespacePrefix: store.Namespace{"ttl"}, Vector: []float32{1, 0}, Limit: 10})
	if err != nil || len(matches) != 0 {
		t.Fatalf("expired external vector remains: matches=%+v err=%v", matches, err)
	}
	pending, err := outbox.List(ctx, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("cleanup outbox=%+v err=%v", pending, err)
	}
}
