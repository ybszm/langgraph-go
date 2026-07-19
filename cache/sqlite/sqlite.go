// Package sqlite provides a durable, process-safe task-result cache.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ybszm/langgraph-go/cache"
	_ "modernc.org/sqlite"
)

// Options controls SQLite cache behavior.
type Options struct {
	// Clock defaults to time.Now and is injectable for deterministic TTL tests.
	Clock func() time.Time
	// BusyTimeout defaults to five seconds.
	BusyTimeout time.Duration
}

// Store is a durable cache backed by one SQLite database.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Open opens or creates a SQLite cache database and applies its schema.
func Open(ctx context.Context, dataSourceName string, options Options) (*Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf("cache context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dataSourceName) == "" {
		return nil, fmt.Errorf("SQLite cache data source name is empty")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.BusyTimeout < 0 {
		return nil, fmt.Errorf("SQLite cache busy timeout cannot be negative")
	}
	if options.BusyTimeout == 0 {
		options.BusyTimeout = 5 * time.Second
	}
	db, err := sql.Open("sqlite", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("open SQLite cache: %w", err)
	}
	// PRAGMAs are connection-local; one connection per Store keeps their
	// semantics stable while separate Store instances still coordinate through
	// SQLite WAL and busy timeout.
	db.SetMaxOpenConns(1)
	store := &Store{db: db, now: options.Clock}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", options.BusyTimeout.Milliseconds())); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure SQLite cache busy timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure SQLite cache journal mode: %w", err)
	}
	if err := store.setup(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) setup(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS langgraph_task_cache (
    namespace TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    data BLOB NOT NULL,
    expires_at_ns INTEGER,
    PRIMARY KEY (namespace, cache_key)
)`)
	if err != nil {
		return fmt.Errorf("setup SQLite cache: %w", err)
	}
	return nil
}

// Close releases this Store's database handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Get implements cache.Store with lazy, transactional TTL eviction.
func (s *Store) Get(ctx context.Context, keys []cache.Key) (map[cache.Key][]byte, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin SQLite cache get: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now().UnixNano()
	result := make(map[cache.Key][]byte)
	for _, key := range keys {
		var data []byte
		var expires sql.NullInt64
		err := tx.QueryRowContext(ctx,
			"SELECT data, expires_at_ns FROM langgraph_task_cache WHERE namespace = ? AND cache_key = ?",
			key.Namespace, key.Key,
		).Scan(&data, &expires)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return nil, fmt.Errorf("get SQLite cache item: %w", err)
		case expires.Valid && expires.Int64 <= now:
			if _, err := tx.ExecContext(ctx,
				"DELETE FROM langgraph_task_cache WHERE namespace = ? AND cache_key = ? AND expires_at_ns <= ?",
				key.Namespace, key.Key, now,
			); err != nil {
				return nil, fmt.Errorf("evict SQLite cache item: %w", err)
			}
		default:
			result[key] = append([]byte(nil), data...)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit SQLite cache get: %w", err)
	}
	return result, nil
}

// Set implements cache.Store as one atomic upsert transaction.
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
		return fmt.Errorf("begin SQLite cache set: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now()
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
	for _, key := range keys {
		item := items[key]
		var expires any
		if item.TTL > 0 {
			expires = now.Add(item.TTL).UnixNano()
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO langgraph_task_cache(namespace, cache_key, data, expires_at_ns)
VALUES (?, ?, ?, ?)
ON CONFLICT(namespace, cache_key) DO UPDATE SET
    data = excluded.data,
    expires_at_ns = excluded.expires_at_ns
`, key.Namespace, key.Key, append([]byte(nil), item.Data...), expires); err != nil {
			return fmt.Errorf("set SQLite cache item: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite cache set: %w", err)
	}
	return nil
}

// Clear implements cache.Store. Nil namespaces clears the full cache; an
// empty non-nil slice is a no-op.
func (s *Store) Clear(ctx context.Context, namespaces []string) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if namespaces == nil {
		if _, err := s.db.ExecContext(ctx, "DELETE FROM langgraph_task_cache"); err != nil {
			return fmt.Errorf("clear SQLite cache: %w", err)
		}
		return nil
	}
	if len(namespaces) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(namespaces)), ",")
	arguments := make([]any, len(namespaces))
	for index, namespace := range namespaces {
		arguments[index] = namespace
	}
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM langgraph_task_cache WHERE namespace IN ("+placeholders+")", arguments...,
	); err != nil {
		return fmt.Errorf("clear SQLite cache namespaces: %w", err)
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
