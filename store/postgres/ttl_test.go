package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wahanbo/langgraph-go/store"
)

func ttlIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("LANGGRAPH_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set LANGGRAPH_POSTGRES_DSN to run PostgreSQL TTL tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPostgresTTLRefreshSweepAndClear(t *testing.T) {
	db := ttlIntegrationDB(t)
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE store_items`); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 7, 18, 20, 0, 0, 0, time.UTC)
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
	if deleted, err := s.SweepExpired(context.Background()); err != nil || deleted != 0 {
		t.Fatalf("early sweep deleted=%d err=%v", deleted, err)
	}
	now = t0.Add(91 * time.Second)
	if deleted, err := s.SweepExpired(context.Background()); err != nil || deleted != 1 {
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
	if deleted, err := s.SweepExpired(context.Background()); err != nil || deleted != 0 {
		t.Fatalf("cleared sweep deleted=%d err=%v", deleted, err)
	}
}

func TestPostgresTTLNoRefreshAndValidation(t *testing.T) {
	db := ttlIntegrationDB(t)
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE store_items`); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 7, 18, 21, 0, 0, 0, time.UTC)
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
	if deleted, err := s.SweepExpired(context.Background()); err != nil || deleted != 1 {
		t.Fatalf("no-refresh sweep deleted=%d err=%v", deleted, err)
	}
	if err := s.PutWithTTL(context.Background(), namespace, "invalid", store.Value{"value": 1}, 0); !errors.Is(err, store.ErrInvalidTTL) {
		t.Fatalf("zero TTL err=%v", err)
	}
}

func TestPostgresSchemaV1MigratesToTTLWithoutDataLoss(t *testing.T) {
	db := ttlIntegrationDB(t)
	for _, statement := range []string{
		`DROP TABLE IF EXISTS store_items CASCADE`,
		`DROP TABLE IF EXISTS store_migrations CASCADE`,
		`CREATE TABLE store_migrations (version INTEGER PRIMARY KEY)`,
		`INSERT INTO store_migrations(version) VALUES (1)`,
		`CREATE TABLE store_items (
			namespace TEXT NOT NULL, key TEXT NOT NULL, value JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY(namespace,key)
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	created := time.Date(2026, 7, 18, 22, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO store_items(namespace,key,value,created_at,updated_at) VALUES ($1,$2,$3::jsonb,$4,$4)`, `["legacy"]`, "item", `{"value":1}`, created); err != nil {
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
