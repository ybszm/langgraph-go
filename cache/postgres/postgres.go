// Package postgres provides a durable multi-process task-result cache.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ybszm/langgraph-go/cache"
)

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS langgraph_task_cache (
    namespace TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    data BYTEA NOT NULL,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (namespace, cache_key)
)`,
	`CREATE INDEX IF NOT EXISTS langgraph_task_cache_expiry
    ON langgraph_task_cache (expires_at) WHERE expires_at IS NOT NULL`,
}

// Options controls PostgreSQL cache behavior.
type Options struct {
	// Clock defaults to UTC time.Now and is injectable for deterministic TTL tests.
	Clock func() time.Time
}

// Store is a concurrency-safe PostgreSQL task cache.
type Store struct {
	mu    sync.Mutex
	db    *sql.DB
	owned bool
	setup bool
	now   func() time.Time
}

// New constructs a Store over an existing database handle without taking ownership.
func New(db *sql.DB, options ...Options) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("PostgreSQL cache database is nil")
	}
	resolved, err := resolveOptions(options)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, now: resolved.Clock}, nil
}

// Open opens a pgx database/sql handle, applies the schema, and returns an
// owning Store.
func Open(ctx context.Context, dsn string, options ...Options) (*Store, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("PostgreSQL cache DSN is empty")
	}
	resolved, err := resolveOptions(options)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL cache: %w", err)
	}
	store := &Store{db: db, owned: true, now: resolved.Clock}
	if err := store.Setup(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func resolveOptions(options []Options) (Options, error) {
	if len(options) > 1 {
		return Options{}, fmt.Errorf("PostgreSQL cache accepts at most one Options value")
	}
	var result Options
	if len(options) == 1 {
		result = options[0]
	}
	if result.Clock == nil {
		result.Clock = func() time.Time { return time.Now().UTC() }
	}
	return result, nil
}

// Setup creates the cache schema under a transaction-scoped advisory lock.
func (s *Store) Setup(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setup {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL cache setup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtext('langgraph-task-cache-schema'))`,
	); err != nil {
		return fmt.Errorf("lock PostgreSQL cache schema: %w", err)
	}
	for _, statement := range schemaStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("setup PostgreSQL cache: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL cache setup: %w", err)
	}
	s.setup = true
	return nil
}

// Close closes only database handles owned by Open.
func (s *Store) Close() error {
	if s == nil || s.db == nil || !s.owned {
		return nil
	}
	return s.db.Close()
}

// Get implements cache.Store with lazy transactional expiry deletion.
func (s *Store) Get(ctx context.Context, keys []cache.Key) (map[cache.Key][]byte, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin PostgreSQL cache get: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now().UTC()
	result := make(map[cache.Key][]byte)
	for _, key := range keys {
		var data []byte
		var expires sql.NullTime
		err := tx.QueryRowContext(ctx,
			`SELECT data, expires_at FROM langgraph_task_cache WHERE namespace = $1 AND cache_key = $2`,
			key.Namespace, key.Key,
		).Scan(&data, &expires)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return nil, fmt.Errorf("get PostgreSQL cache item: %w", err)
		case expires.Valid && !now.Before(expires.Time):
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM langgraph_task_cache
WHERE namespace = $1 AND cache_key = $2 AND expires_at <= $3`,
				key.Namespace, key.Key, now,
			); err != nil {
				return nil, fmt.Errorf("evict PostgreSQL cache item: %w", err)
			}
		default:
			result[key] = append([]byte(nil), data...)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL cache get: %w", err)
	}
	return result, nil
}

// Set implements cache.Store as an atomic upsert transaction.
func (s *Store) Set(ctx context.Context, items map[cache.Key]cache.Item) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	for _, item := range items {
		if item.TTL < 0 {
			return fmt.Errorf("cache TTL cannot be negative")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL cache set: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now().UTC()
	keys := sortedKeys(items)
	for _, key := range keys {
		item := items[key]
		var expires any
		if item.TTL > 0 {
			expires = now.Add(item.TTL)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO langgraph_task_cache(namespace, cache_key, data, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT(namespace, cache_key) DO UPDATE SET
    data = excluded.data,
    expires_at = excluded.expires_at
`, key.Namespace, key.Key, append([]byte(nil), item.Data...), expires); err != nil {
			return fmt.Errorf("set PostgreSQL cache item: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL cache set: %w", err)
	}
	return nil
}

func sortedKeys[T any](items map[cache.Key]T) []cache.Key {
	keys := make([]cache.Key, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Namespace == keys[j].Namespace {
			return keys[i].Key < keys[j].Key
		}
		return keys[i].Namespace < keys[j].Namespace
	})
	return keys
}

// Clear implements cache.Store. Nil namespaces clears all rows.
func (s *Store) Clear(ctx context.Context, namespaces []string) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if namespaces == nil {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM langgraph_task_cache`); err != nil {
			return fmt.Errorf("clear PostgreSQL cache: %w", err)
		}
		return nil
	}
	if len(namespaces) == 0 {
		return nil
	}
	arguments := make([]any, len(namespaces))
	placeholders := make([]string, len(namespaces))
	for index, namespace := range namespaces {
		arguments[index] = namespace
		placeholders[index] = fmt.Sprintf("$%d", index+1)
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM langgraph_task_cache WHERE namespace IN (`+strings.Join(placeholders, ", ")+`)`,
		arguments...,
	); err != nil {
		return fmt.Errorf("clear PostgreSQL cache namespaces: %w", err)
	}
	return nil
}

func validContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("cache context is nil")
	}
	return ctx.Err()
}

var _ cache.Store = (*Store)(nil)
