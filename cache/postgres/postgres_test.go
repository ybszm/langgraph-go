package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ybszm/langgraph-go/cache"
	cachepostgres "github.com/ybszm/langgraph-go/cache/postgres"
)

func TestSetupUsesAdvisoryTransactionAndIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := cachepostgres.New(db)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext('langgraph-task-cache-schema'))`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS langgraph_task_cache").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE INDEX IF NOT EXISTS langgraph_task_cache_expiry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetReturnsLiveValuesAndTransactionallyEvictsExpired(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	store, _ := cachepostgres.New(db, cachepostgres.Options{Clock: func() time.Time { return now }})
	live := cache.Key{Namespace: "ns", Key: "live"}
	expired := cache.Key{Namespace: "ns", Key: "expired"}
	missing := cache.Key{Namespace: "ns", Key: "missing"}
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT data, expires_at FROM langgraph_task_cache WHERE namespace = $1 AND cache_key = $2`)).
		WithArgs("ns", "live").
		WillReturnRows(sqlmock.NewRows([]string{"data", "expires_at"}).AddRow([]byte("live"), now.Add(time.Hour)))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT data, expires_at FROM langgraph_task_cache WHERE namespace = $1 AND cache_key = $2`)).
		WithArgs("ns", "expired").
		WillReturnRows(sqlmock.NewRows([]string{"data", "expires_at"}).AddRow([]byte("old"), now))
	mock.ExpectExec("DELETE FROM langgraph_task_cache").
		WithArgs("ns", "expired", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT data, expires_at FROM langgraph_task_cache WHERE namespace = $1 AND cache_key = $2`)).
		WithArgs("ns", "missing").WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()
	values, err := store.Get(context.Background(), []cache.Key{live, expired, missing})
	if err != nil || len(values) != 1 || string(values[live]) != "live" {
		t.Fatalf("values=%v err=%v", values, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSetIsAtomicAndClearUsesBoundNamespaces(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	store, _ := cachepostgres.New(db, cachepostgres.Options{Clock: func() time.Time { return now }})
	first := cache.Key{Namespace: "a", Key: "1"}
	second := cache.Key{Namespace: "b", Key: "2"}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO langgraph_task_cache").
		WithArgs("a", "1", []byte("one"), now.Add(time.Minute)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO langgraph_task_cache").
		WithArgs("b", "2", []byte("two"), nil).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.Set(context.Background(), map[cache.Key]cache.Item{
		first:  {Data: []byte("one"), TTL: time.Minute},
		second: {Data: []byte("two")},
	}); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM langgraph_task_cache WHERE namespace IN ($1, $2)`)).
		WithArgs("a", "b").WillReturnResult(sqlmock.NewResult(0, 2))
	if err := store.Clear(context.Background(), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(context.Background(), map[cache.Key]cache.Item{
		first: {Data: []byte("invalid"), TTL: -time.Second},
	}); err == nil {
		t.Fatal("negative TTL accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLCacheIntegrationWhenConfigured(t *testing.T) {
	dsn := os.Getenv("LANGGRAPH_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set LANGGRAPH_POSTGRES_DSN to run PostgreSQL cache integration")
	}
	ctx := context.Background()
	first, err := cachepostgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := cachepostgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	namespace := "cache-integration-" + time.Now().UTC().Format("20060102150405.000000000")
	defer first.Clear(ctx, []string{namespace})
	key := cache.Key{Namespace: namespace, Key: "shared"}
	if err := first.Set(ctx, map[cache.Key]cache.Item{key: {Data: []byte("shared"), TTL: time.Minute}}); err != nil {
		t.Fatal(err)
	}
	values, err := second.Get(ctx, []cache.Key{key})
	if err != nil || string(values[key]) != "shared" {
		t.Fatalf("values=%v err=%v", values, err)
	}
}
