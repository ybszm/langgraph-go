// Package sqlite implements the long-term memory Store using SQLite.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wahanbo/langgraph-go/store"
	"github.com/wahanbo/langgraph-go/store/internal/storeutil"
	_ "modernc.org/sqlite"
)

const schemaVersion = 2

var setupStatements = []string{
	`PRAGMA journal_mode=WAL`,
	`PRAGMA busy_timeout=5000`,
	`CREATE TABLE IF NOT EXISTS store_migrations (
		version INTEGER PRIMARY KEY
	)`,
}

var migrations = []struct {
	version    int
	statements []string
}{
	{1, []string{
		`CREATE TABLE IF NOT EXISTS store_items (
			namespace TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (namespace, key)
		)`,
		`CREATE INDEX IF NOT EXISTS store_items_updated ON store_items (updated_at DESC)`,
	}},
	{2, []string{
		`ALTER TABLE store_items ADD COLUMN expires_at INTEGER`,
		`ALTER TABLE store_items ADD COLUMN ttl_nanos INTEGER`,
		`CREATE INDEX IF NOT EXISTS store_items_expiry ON store_items (expires_at) WHERE expires_at IS NOT NULL`,
	}},
}

// Store is a concurrency-safe SQLite implementation of store.Store. It is
// intended for local and single-process deployments.
type Store struct {
	mu      sync.Mutex
	db      *sql.DB
	owned   bool
	setup   bool
	closing bool
	now     func() time.Time
}

// New constructs a Store over an existing SQLite database handle. New does
// not take ownership of db.
func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: SQLite database is nil", store.ErrInvalidOperation)
	}
	return &Store{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

// Open opens dsn with the pure-Go SQLite driver and initializes the schema.
func Open(ctx context.Context, dsn string) (*Store, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: SQLite DSN is empty", store.ErrInvalidOperation)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite store: %w", err)
	}
	s := &Store{db: db, owned: true, now: func() time.Time { return time.Now().UTC() }}
	if err := s.Setup(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Setup creates the Store schema and applies idempotent migrations.
func (s *Store) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setupLocked(ctx)
}

func (s *Store) setupLocked(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: SQLite store is nil", store.ErrInvalidOperation)
	}
	if s.closing {
		return fmt.Errorf("SQLite store is closed")
	}
	if s.setup {
		return nil
	}
	for _, statement := range setupStatements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set up SQLite store schema: %w", err)
		}
	}
	var storedVersion sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT max(version) FROM store_migrations`).Scan(&storedVersion); err != nil {
		return fmt.Errorf("read SQLite store schema version: %w", err)
	}
	version := 0
	if storedVersion.Valid {
		version = int(storedVersion.Int64)
	}
	if version > schemaVersion {
		return fmt.Errorf("unsupported SQLite store schema version %d", version)
	}
	if version < schemaVersion {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin SQLite store migration: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		for _, migration := range migrations {
			if migration.version <= version {
				continue
			}
			for _, statement := range migration.statements {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("apply SQLite store migration %d: %w", migration.version, err)
				}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO store_migrations(version) VALUES (?)`, migration.version); err != nil {
				return fmt.Errorf("record SQLite store migration %d: %w", migration.version, err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit SQLite store migrations: %w", err)
		}
	}
	s.setup = true
	return nil
}

// Close closes the handle opened by Open. It does not close a handle supplied
// to New, but the Store itself must not be reused afterward.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil
	}
	s.closing = true
	if s.owned && s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Get implements store.Store.
func (s *Store) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	return s.GetWithTTLRefresh(ctx, namespace, key, true)
}

// GetWithTTLRefresh retrieves an item and optionally refreshes its expiration.
func (s *Store) GetWithTTLRefresh(ctx context.Context, namespace store.Namespace, key string, refresh bool) (*store.Item, error) {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return nil, err
	}
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key, RefreshTTL: refresh}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

// Search implements store.Store. Query is scoreless until an embedding index
// is configured in a future capability loop.
func (s *Store) Search(ctx context.Context, prefix store.Namespace, options store.SearchOptions) ([]store.SearchItem, error) {
	return s.SearchWithTTLRefresh(ctx, prefix, options, true)
}

// SearchWithTTLRefresh searches items and optionally refreshes expiration for
// every returned item.
func (s *Store) SearchWithTTLRefresh(ctx context.Context, prefix store.Namespace, options store.SearchOptions, refresh bool) ([]store.SearchItem, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit == 0 {
		options.Limit = 10
	}
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: prefix, Filter: options.Filter, Limit: options.Limit,
		Offset: options.Offset, Query: options.Query, RefreshTTL: refresh,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

// Put implements store.Store.
func (s *Store) Put(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	if value == nil {
		return fmt.Errorf("%w: Put value is nil; use Delete", store.ErrInvalidOperation)
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value}})
	return err
}

// PutWithTTL stores an item with a positive time-to-live. Any later ordinary
// Put clears that expiration.
func (s *Store) PutWithTTL(ctx context.Context, namespace store.Namespace, key string, value store.Value, ttl time.Duration) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	if value == nil {
		return fmt.Errorf("%w: Put value is nil; use Delete", store.ErrInvalidOperation)
	}
	if ttl <= 0 {
		return fmt.Errorf("%w: TTL must be positive", store.ErrInvalidTTL)
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value, TTL: ttl}})
	return err
}

// Delete implements store.Store.
func (s *Store) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key}})
	return err
}

// ListNamespaces implements store.Store.
func (s *Store) ListNamespaces(ctx context.Context, options store.ListNamespacesOptions) ([]store.Namespace, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit == 0 {
		options.Limit = 100
	}
	conditions := make([]store.MatchCondition, 0, 2)
	if len(options.Prefix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchPrefix, Path: options.Prefix})
	}
	if len(options.Suffix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchSuffix, Path: options.Suffix})
	}
	results, err := s.Batch(ctx, []store.Operation{store.ListNamespacesOp{
		MatchConditions: conditions, MaxDepth: options.MaxDepth,
		Limit: options.Limit, Offset: options.Offset,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Namespaces, nil
}

type itemKey struct {
	namespace string
	key       string
}

// Batch evaluates reads from one transaction snapshot, then atomically
// applies deduplicated writes using last-write-wins.
func (s *Store) Batch(ctx context.Context, operations []store.Operation) ([]store.Result, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin SQLite store batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	results := make([]store.Result, len(operations))
	puts := make(map[itemKey]store.PutOp)
	putOrder := make([]itemKey, 0)
	refreshes := make(map[itemKey]struct{})
	var snapshot []store.Item
	snapshotLoaded := false
	loadSnapshot := func() ([]store.Item, error) {
		if snapshotLoaded {
			return snapshot, nil
		}
		snapshotLoaded = true
		items, err := loadAll(ctx, tx)
		if err != nil {
			return nil, err
		}
		snapshot = items
		return snapshot, nil
	}
	for index, operation := range operations {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		switch op := operation.(type) {
		case store.GetOp:
			item, err := getTx(ctx, tx, op.Namespace, op.Key)
			if err != nil {
				return nil, err
			}
			results[index].Item = item
			if item != nil && op.RefreshTTL {
				namespace, err := storeutil.EncodeNamespace(op.Namespace)
				if err != nil {
					return nil, err
				}
				refreshes[itemKey{namespace: namespace, key: op.Key}] = struct{}{}
			}
		case store.SearchOp:
			matches, err := searchTx(ctx, tx, op)
			if err != nil {
				return nil, err
			}
			results[index].Items = matches
			if op.RefreshTTL {
				for _, item := range matches {
					namespace, err := storeutil.EncodeNamespace(item.Namespace)
					if err != nil {
						return nil, err
					}
					refreshes[itemKey{namespace: namespace, key: item.Key}] = struct{}{}
				}
			}
		case store.ListNamespacesOp:
			items, err := loadSnapshot()
			if err != nil {
				return nil, err
			}
			namespaces, err := storeutil.ListNamespaces(items, op)
			if err != nil {
				return nil, err
			}
			results[index].Namespaces = namespaces
		case store.PutOp:
			namespace, err := storeutil.EncodeNamespace(op.Namespace)
			if err != nil {
				return nil, err
			}
			key := itemKey{namespace: namespace, key: op.Key}
			if _, exists := puts[key]; !exists {
				putOrder = append(putOrder, key)
			}
			puts[key] = op
		default:
			return nil, fmt.Errorf("%w: %T", store.ErrInvalidOperation, operation)
		}
	}
	prepared := make(map[itemKey][]byte, len(puts))
	for key, op := range puts {
		if op.TTL < 0 {
			return nil, fmt.Errorf("%w: TTL cannot be negative", store.ErrInvalidTTL)
		}
		if op.Value == nil {
			continue
		}
		data, err := json.Marshal(op.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: value is not JSON serializable: %v", store.ErrInvalidOperation, err)
		}
		prepared[key] = data
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	refreshNow := s.now().UTC().UnixNano()
	for key := range refreshes {
		if _, err := tx.ExecContext(ctx, `UPDATE store_items
			SET expires_at=?+ttl_nanos
			WHERE namespace=? AND key=? AND ttl_nanos IS NOT NULL`, refreshNow, key.namespace, key.key); err != nil {
			return nil, fmt.Errorf("refresh SQLite store TTL: %w", err)
		}
	}
	for _, key := range putOrder {
		op := puts[key]
		if op.Value == nil {
			if _, err := tx.ExecContext(ctx, `DELETE FROM store_items WHERE namespace=? AND key=?`, key.namespace, key.key); err != nil {
				return nil, fmt.Errorf("delete SQLite store item: %w", err)
			}
			continue
		}
		now := s.now().UTC().UnixNano()
		var expiresAt any
		var ttlNanos any
		if op.TTL > 0 {
			expires := now + int64(op.TTL)
			if expires <= now {
				return nil, fmt.Errorf("%w: TTL overflows expiration", store.ErrInvalidTTL)
			}
			expiresAt, ttlNanos = expires, int64(op.TTL)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO store_items(namespace,key,value,created_at,updated_at,expires_at,ttl_nanos)
			VALUES (?,?,?,?,?,?,?)
			ON CONFLICT(namespace,key) DO UPDATE SET
				value=excluded.value, updated_at=excluded.updated_at,
				expires_at=excluded.expires_at, ttl_nanos=excluded.ttl_nanos`,
			key.namespace, key.key, prepared[key], now, now, expiresAt, ttlNanos,
		); err != nil {
			return nil, fmt.Errorf("put SQLite store item: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit SQLite store batch: %w", err)
	}
	return results, nil
}

// SweepExpired deletes items whose best-effort TTL deadline has passed.
func (s *Store) SweepExpired(ctx context.Context) (int64, error) {
	items, err := s.SweepExpiredItems(ctx)
	return int64(len(items)), err
}

// SweepExpiredItems atomically deletes expired rows and returns their durable
// identities for external-index cleanup.
func (s *Store) SweepExpiredItems(ctx context.Context) ([]store.ItemIdentity, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `DELETE FROM store_items WHERE expires_at IS NOT NULL AND expires_at<=? RETURNING namespace,key`, s.now().UTC().UnixNano())
	if err != nil {
		return nil, fmt.Errorf("sweep SQLite store TTL: %w", err)
	}
	defer rows.Close()
	items := make([]store.ItemIdentity, 0)
	for rows.Next() {
		var namespaceData, key string
		if err := rows.Scan(&namespaceData, &key); err != nil {
			return nil, fmt.Errorf("scan SQLite expired item: %w", err)
		}
		namespace, err := storeutil.DecodeNamespace(namespaceData)
		if err != nil {
			return nil, err
		}
		items = append(items, store.ItemIdentity{Namespace: namespace, Key: key})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite expired items: %w", err)
	}
	return items, nil
}

func getTx(ctx context.Context, tx *sql.Tx, namespace store.Namespace, key string) (*store.Item, error) {
	encodedNamespace, err := storeutil.EncodeNamespace(namespace)
	if err != nil {
		return nil, err
	}
	var namespaceData string
	var valueData []byte
	var createdAt, updatedAt int64
	var item store.Item
	err = tx.QueryRowContext(ctx, `SELECT namespace,key,value,created_at,updated_at FROM store_items WHERE namespace=? AND key=?`, encodedNamespace, key).
		Scan(&namespaceData, &item.Key, &valueData, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get SQLite store item: %w", err)
	}
	item.Namespace, err = storeutil.DecodeNamespace(namespaceData)
	if err != nil {
		return nil, err
	}
	item.Value, err = storeutil.DecodeValue(valueData)
	if err != nil {
		return nil, err
	}
	item.CreatedAt, item.UpdatedAt = time.Unix(0, createdAt).UTC(), time.Unix(0, updatedAt).UTC()
	return &item, nil
}

func loadAll(ctx context.Context, tx *sql.Tx) ([]store.Item, error) {
	rows, err := tx.QueryContext(ctx, `SELECT namespace,key,value,created_at,updated_at FROM store_items`)
	if err != nil {
		return nil, fmt.Errorf("load SQLite store snapshot: %w", err)
	}
	defer rows.Close()
	items := make([]store.Item, 0)
	for rows.Next() {
		var namespaceData string
		var valueData []byte
		var createdAt, updatedAt int64
		var item store.Item
		if err := rows.Scan(&namespaceData, &item.Key, &valueData, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan SQLite store item: %w", err)
		}
		item.Namespace, err = storeutil.DecodeNamespace(namespaceData)
		if err != nil {
			return nil, err
		}
		item.Value, err = storeutil.DecodeValue(valueData)
		if err != nil {
			return nil, err
		}
		item.CreatedAt, item.UpdatedAt = time.Unix(0, createdAt).UTC(), time.Unix(0, updatedAt).UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite store snapshot: %w", err)
	}
	return items, nil
}

func searchTx(ctx context.Context, tx *sql.Tx, operation store.SearchOp) ([]store.SearchItem, error) {
	query, arguments, err := buildSearchQuery(operation)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query SQLite store search candidates: %w", err)
	}
	defer rows.Close()
	items := make([]store.Item, 0)
	for rows.Next() {
		var namespaceData string
		var valueData []byte
		var createdAt, updatedAt int64
		var item store.Item
		if err := rows.Scan(&namespaceData, &item.Key, &valueData, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan SQLite store search candidate: %w", err)
		}
		item.Namespace, err = storeutil.DecodeNamespace(namespaceData)
		if err != nil {
			return nil, err
		}
		item.Value, err = storeutil.DecodeValue(valueData)
		if err != nil {
			return nil, err
		}
		item.CreatedAt = time.Unix(0, createdAt).UTC()
		item.UpdatedAt = time.Unix(0, updatedAt).UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite store search candidates: %w", err)
	}
	// Re-evaluate the complete backend-neutral predicate after safe pushdown so
	// type/error behavior remains identical across Store implementations.
	return storeutil.Search(items, operation)
}

func buildSearchQuery(operation store.SearchOp) (string, []any, error) {
	if operation.Limit < 0 || operation.Offset < 0 {
		return "", nil, fmt.Errorf("%w: negative search pagination", store.ErrInvalidOperation)
	}
	var namespacePredicates []string
	arguments := make([]any, 0, 1+len(operation.NamespacePrefix)*2+len(operation.Filter)*2)
	if len(operation.NamespacePrefix) > 0 {
		namespacePredicates = append(namespacePredicates, "json_array_length(namespace)>=?")
		arguments = append(arguments, len(operation.NamespacePrefix))
		for index, label := range operation.NamespacePrefix {
			namespacePredicates = append(namespacePredicates, "json_extract(namespace, ?)=?")
			arguments = append(arguments, fmt.Sprintf("$[%d]", index), label)
		}
	}
	filterPredicates, filterArguments, err := compileSafeFilter(operation.Filter, nil)
	if err != nil {
		return "", nil, err
	}
	arguments = append(arguments, filterArguments...)
	namespaceWhere := "1=1"
	if len(namespacePredicates) > 0 {
		namespaceWhere = strings.Join(namespacePredicates, " AND ")
	}
	filterWhere := "1=1"
	if len(filterPredicates) > 0 {
		filterWhere = strings.Join(filterPredicates, " AND ")
	}
	query := `WITH namespace_candidates AS MATERIALIZED (
		SELECT namespace,key,value,created_at,updated_at FROM store_items WHERE ` + namespaceWhere + `
	) SELECT namespace,key,value,created_at,updated_at FROM namespace_candidates WHERE ` + filterWhere
	return query, arguments, nil
}

func compileSafeFilter(filter map[string]any, prefix []string) ([]string, []any, error) {
	keys := make([]string, 0, len(filter))
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var predicates []string
	var arguments []any
	for _, key := range keys {
		expected := filter[key]
		path := append(append([]string(nil), prefix...), key)
		if object, ok := filterObject(expected); ok {
			hasOperator := false
			for nestedKey := range object {
				if strings.HasPrefix(nestedKey, "$") {
					hasOperator = true
					break
				}
			}
			if !hasOperator {
				nestedPredicates, nestedArguments, err := compileSafeFilter(object, path)
				if err != nil {
					return nil, nil, err
				}
				predicates = append(predicates, nestedPredicates...)
				arguments = append(arguments, nestedArguments...)
				continue
			}
			operatorKeys := make([]string, 0, len(object))
			for operator := range object {
				operatorKeys = append(operatorKeys, operator)
			}
			sort.Strings(operatorKeys)
			for _, operator := range operatorKeys {
				operand := object[operator]
				switch operator {
				case "$eq":
					if isSQLScalar(operand) {
						predicates = append(predicates, "json_extract(value, ?) IS ?")
						arguments = append(arguments, sqliteJSONPath(path), operand)
					}
				case "$ne", "$gt", "$gte", "$lt", "$lte":
					// Numeric comparison stays in the backend-neutral evaluator:
					// pushing it could hide required non-numeric-value errors.
					// $ne also stays there because SQLite considers false and 0
					// equal while Go's JSON-like contract keeps their types apart.
				default:
					return nil, nil, fmt.Errorf("%w: filter operator %q", store.ErrUnsupportedQuery, operator)
				}
			}
			continue
		}
		if isSQLScalar(expected) {
			predicates = append(predicates, "json_extract(value, ?) IS ?")
			arguments = append(arguments, sqliteJSONPath(path), expected)
		}
	}
	return predicates, arguments, nil
}

func filterObject(value any) (map[string]any, bool) {
	switch value := value.(type) {
	case map[string]any:
		return value, true
	case store.Value:
		return map[string]any(value), true
	default:
		return nil, false
	}
}

func isSQLScalar(value any) bool {
	if value == nil {
		return true
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.String || kind == reflect.Bool ||
		kind >= reflect.Int && kind <= reflect.Float64
}

func sqliteJSONPath(parts []string) string {
	var result strings.Builder
	result.WriteByte('$')
	for _, part := range parts {
		encoded, _ := json.Marshal(part)
		result.WriteByte('.')
		result.Write(encoded)
	}
	return result.String()
}

func validatePublic(ctx context.Context, namespace store.Namespace, key string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := store.ValidateNamespace(namespace); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: key is empty", store.ErrInvalidOperation)
	}
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", store.ErrInvalidOperation)
	}
	return ctx.Err()
}

var _ store.Store = (*Store)(nil)
var _ store.TTLStore = (*Store)(nil)
