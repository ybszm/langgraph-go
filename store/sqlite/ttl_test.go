package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/store"
	_ "modernc.org/sqlite"
)

func TestTTLRefreshSweepAndClear(t *testing.T) {
	db, err := sql.Open("sqlite", "file:ttl-refresh?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 7, 18, 17, 0, 0, 0, time.UTC)
	now := t0
	s.now = func() time.Time { return now }
	namespace := store.Namespace{"ttl"}
	if err := s.PutWithTTL(context.Background(), namespace, "refresh", store.Value{"value": 1}, time.Minute); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(30 * time.Second)
	if _, err := s.GetWithTTLRefresh(context.Background(), namespace, "refresh", true); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(70 * time.Second)
	deleted, err := s.SweepExpired(context.Background())
	if err != nil || deleted != 0 {
		t.Fatalf("early sweep deleted=%d err=%v", deleted, err)
	}
	now = t0.Add(91 * time.Second)
	deleted, err = s.SweepExpired(context.Background())
	if err != nil || deleted != 1 {
		t.Fatalf("expired sweep deleted=%d err=%v", deleted, err)
	}

	now = t0
	if err := s.PutWithTTL(context.Background(), namespace, "cleared", store.Value{"value": 2}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), namespace, "cleared", store.Value{"value": 3}); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(2 * time.Minute)
	deleted, err = s.SweepExpired(context.Background())
	if err != nil || deleted != 0 {
		t.Fatalf("cleared TTL sweep deleted=%d err=%v", deleted, err)
	}
	item, err := s.Get(context.Background(), namespace, "cleared")
	if err != nil || item == nil || item.Value["value"] != 3 {
		t.Fatalf("cleared item=%+v err=%v", item, err)
	}
}

func TestTTLNoRefreshAndValidation(t *testing.T) {
	db, err := sql.Open("sqlite", "file:ttl-no-refresh?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
	now := t0
	s.now = func() time.Time { return now }
	namespace := store.Namespace{"ttl"}
	if err := s.PutWithTTL(context.Background(), namespace, "no-refresh", store.Value{"value": 1}, time.Minute); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(30 * time.Second)
	if _, err := s.GetWithTTLRefresh(context.Background(), namespace, "no-refresh", false); err != nil {
		t.Fatal(err)
	}
	now = t0.Add(61 * time.Second)
	deleted, err := s.SweepExpired(context.Background())
	if err != nil || deleted != 1 {
		t.Fatalf("no-refresh sweep deleted=%d err=%v", deleted, err)
	}
	if err := s.PutWithTTL(context.Background(), namespace, "invalid", store.Value{"value": 1}, 0); !errors.Is(err, store.ErrInvalidTTL) {
		t.Fatalf("zero TTL err=%v", err)
	}
}

func TestSchemaV1MigratesToTTLWithoutDataLoss(t *testing.T) {
	db, err := sql.Open("sqlite", "file:ttl-migration?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	created := time.Date(2026, 7, 18, 19, 0, 0, 0, time.UTC).UnixNano()
	for _, statement := range []string{
		`CREATE TABLE store_migrations (version INTEGER PRIMARY KEY)`,
		`INSERT INTO store_migrations(version) VALUES (1)`,
		`CREATE TABLE store_items (
			namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			PRIMARY KEY(namespace,key)
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO store_items(namespace,key,value,created_at,updated_at) VALUES (?,?,?,?,?)`, `["legacy"]`, "item", `{"value":1}`, created, created); err != nil {
		t.Fatal(err)
	}
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	item, err := s.Get(context.Background(), store.Namespace{"legacy"}, "item")
	if err != nil || item == nil || item.Value["value"] != 1 {
		t.Fatalf("legacy item=%+v err=%v", item, err)
	}
	var version int
	if err := db.QueryRow(`SELECT max(version) FROM store_migrations`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
}
