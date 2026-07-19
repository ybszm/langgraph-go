// Package postgres implements the long-term memory Store using PostgreSQL.
package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wahanbo/langgraph-go/store"
)

const schemaVersion = 2

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS store_migrations (
		version INTEGER PRIMARY KEY
	)`,
	`CREATE TABLE IF NOT EXISTS store_items (
		namespace TEXT NOT NULL,
		key TEXT NOT NULL,
		value JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (namespace, key)
	)`,
	`CREATE INDEX IF NOT EXISTS store_items_updated
		ON store_items (updated_at DESC)`,
	`ALTER TABLE store_items ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ`,
	`ALTER TABLE store_items ADD COLUMN IF NOT EXISTS ttl_micros BIGINT`,
	`CREATE INDEX IF NOT EXISTS store_items_expiry
		ON store_items (expires_at) WHERE expires_at IS NOT NULL`,
}

// Store is a concurrency-safe PostgreSQL implementation of store.Store.
type Store struct {
	mu      sync.Mutex
	db      *sql.DB
	owned   bool
	setup   bool
	closing bool
	now     func() time.Time
}

// New constructs a Store over an existing PostgreSQL database handle. New
// does not take ownership of db.
func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: PostgreSQL database is nil", store.ErrInvalidOperation)
	}
	return &Store{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

// Open opens dsn with pgx's database/sql driver and initializes the schema.
func Open(ctx context.Context, dsn string) (*Store, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL DSN is empty", store.ErrInvalidOperation)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL store: %w", err)
	}
	s := &Store{db: db, owned: true, now: func() time.Time { return time.Now().UTC() }}
	if err := s.Setup(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Setup creates the Store schema. An advisory transaction lock coordinates
// concurrent setup calls across processes.
func (s *Store) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: PostgreSQL store is nil", store.ErrInvalidOperation)
	}
	if s.closing {
		return fmt.Errorf("PostgreSQL store is closed")
	}
	if s.setup {
		return nil
	}
	var fastVersion sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT max(version) FROM store_migrations`).Scan(&fastVersion); err == nil && fastVersion.Valid {
		if int(fastVersion.Int64) > schemaVersion {
			return fmt.Errorf("unsupported PostgreSQL store schema version %d", fastVersion.Int64)
		}
		if int(fastVersion.Int64) == schemaVersion {
			s.setup = true
			return nil
		}
	} else if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL store schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('langgraph-go-store-schema-v1'))`); err != nil {
		return fmt.Errorf("lock PostgreSQL store schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, schemaStatements[0]); err != nil {
		return fmt.Errorf("set up PostgreSQL store migrations table: %w", err)
	}
	var lockedVersion sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT max(version) FROM store_migrations`).Scan(&lockedVersion); err != nil {
		return fmt.Errorf("read PostgreSQL store schema version: %w", err)
	}
	if lockedVersion.Valid && int(lockedVersion.Int64) > schemaVersion {
		return fmt.Errorf("unsupported PostgreSQL store schema version %d", lockedVersion.Int64)
	}
	if lockedVersion.Valid && int(lockedVersion.Int64) == schemaVersion {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit PostgreSQL store schema check: %w", err)
		}
		s.setup = true
		return nil
	}
	for _, statement := range schemaStatements[1:] {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set up PostgreSQL store schema: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO store_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING`, schemaVersion); err != nil {
		return fmt.Errorf("record PostgreSQL store schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL store schema: %w", err)
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

func (s *Store) ensureSetup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	ready, closing := s.setup, s.closing
	s.mu.Unlock()
	if closing {
		return fmt.Errorf("PostgreSQL store is closed")
	}
	if ready {
		return nil
	}
	return s.Setup(ctx)
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

// Batch evaluates all reads in one Repeatable Read pre-write snapshot, then
// atomically applies deduplicated writes using last-write-wins.
func (s *Store) Batch(ctx context.Context, operations []store.Operation) ([]store.Result, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSetup(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("begin PostgreSQL store batch: %w", err)
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
				namespace, err := encodeNamespace(op.Namespace)
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
					namespace, err := encodeNamespace(item.Namespace)
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
			namespaces, err := listSnapshot(items, op)
			if err != nil {
				return nil, err
			}
			results[index].Namespaces = namespaces
		case store.PutOp:
			if op.TTL < 0 {
				return nil, fmt.Errorf("%w: TTL cannot be negative", store.ErrInvalidTTL)
			}
			namespace, err := encodeNamespace(op.Namespace)
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
	refreshNow := s.now().UTC()
	for key := range refreshes {
		if _, err := tx.ExecContext(ctx, `UPDATE store_items
			SET expires_at=$1::timestamptz+(ttl_micros * interval '1 microsecond')
			WHERE namespace=$2 AND key=$3 AND ttl_micros IS NOT NULL`, refreshNow, key.namespace, key.key); err != nil {
			return nil, fmt.Errorf("refresh PostgreSQL store TTL: %w", err)
		}
	}
	for _, key := range putOrder {
		op := puts[key]
		if op.Value == nil {
			if _, err := tx.ExecContext(ctx, `DELETE FROM store_items WHERE namespace=$1 AND key=$2`, key.namespace, key.key); err != nil {
				return nil, fmt.Errorf("delete PostgreSQL store item: %w", err)
			}
			continue
		}
		now := s.now().UTC()
		var expiresAt any
		var ttlMicros any
		if op.TTL > 0 {
			micros := op.TTL.Microseconds()
			if micros == 0 {
				micros = 1
			}
			expiresAt, ttlMicros = now.Add(time.Duration(micros)*time.Microsecond), micros
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO store_items(namespace,key,value,created_at,updated_at,expires_at,ttl_micros)
			VALUES ($1,$2,$3::jsonb,$4,$4,$5,$6)
			ON CONFLICT(namespace,key) DO UPDATE SET
				value=EXCLUDED.value, updated_at=EXCLUDED.updated_at,
				expires_at=EXCLUDED.expires_at, ttl_micros=EXCLUDED.ttl_micros`,
			key.namespace, key.key, prepared[key], now, expiresAt, ttlMicros,
		); err != nil {
			return nil, fmt.Errorf("put PostgreSQL store item: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL store batch: %w", err)
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
	if err := s.ensureSetup(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `DELETE FROM store_items WHERE expires_at IS NOT NULL AND expires_at<=$1 RETURNING namespace,key`, s.now().UTC())
	if err != nil {
		return nil, fmt.Errorf("sweep PostgreSQL store TTL: %w", err)
	}
	defer rows.Close()
	items := make([]store.ItemIdentity, 0)
	for rows.Next() {
		var namespaceData, key string
		if err := rows.Scan(&namespaceData, &key); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL expired item: %w", err)
		}
		namespace, err := decodeNamespace(namespaceData)
		if err != nil {
			return nil, err
		}
		items = append(items, store.ItemIdentity{Namespace: namespace, Key: key})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL expired items: %w", err)
	}
	return items, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func getTx(ctx context.Context, queryer queryer, namespace store.Namespace, key string) (*store.Item, error) {
	encodedNamespace, err := encodeNamespace(namespace)
	if err != nil {
		return nil, err
	}
	var namespaceData string
	var valueData []byte
	var item store.Item
	err = queryer.QueryRowContext(ctx, `SELECT namespace,key,value,created_at,updated_at FROM store_items WHERE namespace=$1 AND key=$2`, encodedNamespace, key).
		Scan(&namespaceData, &item.Key, &valueData, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get PostgreSQL store item: %w", err)
	}
	item.Namespace, err = decodeNamespace(namespaceData)
	if err != nil {
		return nil, err
	}
	item.Value, err = decodeValue(valueData)
	if err != nil {
		return nil, err
	}
	item.CreatedAt, item.UpdatedAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC()
	return &item, nil
}

func loadAll(ctx context.Context, queryer queryer) ([]store.Item, error) {
	rows, err := queryer.QueryContext(ctx, `SELECT namespace,key,value,created_at,updated_at FROM store_items`)
	if err != nil {
		return nil, fmt.Errorf("load PostgreSQL store snapshot: %w", err)
	}
	defer rows.Close()
	items := make([]store.Item, 0)
	for rows.Next() {
		var namespaceData string
		var valueData []byte
		var item store.Item
		if err := rows.Scan(&namespaceData, &item.Key, &valueData, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL store item: %w", err)
		}
		item.Namespace, err = decodeNamespace(namespaceData)
		if err != nil {
			return nil, err
		}
		item.Value, err = decodeValue(valueData)
		if err != nil {
			return nil, err
		}
		item.CreatedAt, item.UpdatedAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL store snapshot: %w", err)
	}
	return items, nil
}

func searchTx(ctx context.Context, queryer queryer, operation store.SearchOp) ([]store.SearchItem, error) {
	query, arguments, err := buildSearchQuery(operation)
	if err != nil {
		return nil, err
	}
	rows, err := queryer.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query PostgreSQL store search candidates: %w", err)
	}
	defer rows.Close()
	items := make([]store.Item, 0)
	for rows.Next() {
		var namespaceData string
		var valueData []byte
		var item store.Item
		if err := rows.Scan(&namespaceData, &item.Key, &valueData, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL store search candidate: %w", err)
		}
		item.Namespace, err = decodeNamespace(namespaceData)
		if err != nil {
			return nil, err
		}
		item.Value, err = decodeValue(valueData)
		if err != nil {
			return nil, err
		}
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL store search candidates: %w", err)
	}
	return searchSnapshot(items, operation)
}

type pgSearchBuilder struct {
	arguments []any
}

func (b *pgSearchBuilder) bind(value any) string {
	b.arguments = append(b.arguments, value)
	return fmt.Sprintf("$%d", len(b.arguments))
}

func buildSearchQuery(operation store.SearchOp) (string, []any, error) {
	if operation.Limit < 0 || operation.Offset < 0 {
		return "", nil, fmt.Errorf("%w: negative search pagination", store.ErrInvalidOperation)
	}
	builder := &pgSearchBuilder{}
	var namespacePredicates []string
	if len(operation.NamespacePrefix) > 0 {
		length := builder.bind(len(operation.NamespacePrefix))
		namespacePredicates = append(namespacePredicates, "jsonb_array_length(namespace::jsonb)>="+length+"::int")
		for index, label := range operation.NamespacePrefix {
			position := builder.bind(index)
			value := builder.bind(label)
			namespacePredicates = append(namespacePredicates, "namespace::jsonb ->> "+position+"::int = "+value)
		}
	}
	filterPredicates, err := compilePGSafeFilter(builder, operation.Filter, nil)
	if err != nil {
		return "", nil, err
	}
	namespaceWhere := "TRUE"
	if len(namespacePredicates) > 0 {
		namespaceWhere = strings.Join(namespacePredicates, " AND ")
	}
	filterWhere := "TRUE"
	if len(filterPredicates) > 0 {
		filterWhere = strings.Join(filterPredicates, " AND ")
	}
	query := `WITH namespace_candidates AS MATERIALIZED (
		SELECT namespace,key,value,created_at,updated_at FROM store_items WHERE ` + namespaceWhere + `
	) SELECT namespace,key,value,created_at,updated_at FROM namespace_candidates WHERE ` + filterWhere
	return query, builder.arguments, nil
}

func compilePGSafeFilter(builder *pgSearchBuilder, filter map[string]any, prefix []string) ([]string, error) {
	keys := make([]string, 0, len(filter))
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var predicates []string
	for _, key := range keys {
		expected := filter[key]
		path := append(append([]string(nil), prefix...), key)
		if object, ok := objectValue(expected); ok {
			hasOperator := false
			for nestedKey := range object {
				if strings.HasPrefix(nestedKey, "$") {
					hasOperator = true
					break
				}
			}
			if !hasOperator {
				nested, err := compilePGSafeFilter(builder, object, path)
				if err != nil {
					return nil, err
				}
				predicates = append(predicates, nested...)
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
					if isPGJSONScalar(operand) {
						predicate, err := pgEqualityPredicate(builder, path, operand)
						if err != nil {
							return nil, err
						}
						predicates = append(predicates, predicate)
					}
				case "$ne", "$gt", "$gte", "$lt", "$lte":
					// Preserve the shared evaluator's typed inequality and
					// non-numeric error behavior.
				default:
					return nil, fmt.Errorf("%w: filter operator %q", store.ErrUnsupportedQuery, operator)
				}
			}
			continue
		}
		if isPGJSONScalar(expected) {
			predicate, err := pgEqualityPredicate(builder, path, expected)
			if err != nil {
				return nil, err
			}
			predicates = append(predicates, predicate)
		}
	}
	return predicates, nil
}

func pgEqualityPredicate(builder *pgSearchBuilder, path []string, expected any) (string, error) {
	pathPlaceholders := make([]string, len(path))
	for index, part := range path {
		pathPlaceholders[index] = builder.bind(part)
	}
	expression := "value #> ARRAY[" + strings.Join(pathPlaceholders, ",") + "]::text[]"
	if expected == nil {
		return "(" + expression + " IS NULL OR " + expression + " = 'null'::jsonb)", nil
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		return "", fmt.Errorf("%w: filter operand is not JSON serializable: %v", store.ErrUnsupportedQuery, err)
	}
	operand := builder.bind(string(encoded))
	return expression + " = " + operand + "::jsonb", nil
}

func isPGJSONScalar(value any) bool {
	if value == nil {
		return true
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.String || kind == reflect.Bool ||
		kind >= reflect.Int && kind <= reflect.Float64
}

func searchSnapshot(items []store.Item, op store.SearchOp) ([]store.SearchItem, error) {
	if op.Limit < 0 || op.Offset < 0 {
		return nil, fmt.Errorf("%w: negative search pagination", store.ErrInvalidOperation)
	}
	candidates := make([]store.Item, 0)
	for _, item := range items {
		if !hasPrefix(item.Namespace, op.NamespacePrefix) {
			continue
		}
		matched, err := matchesFilter(item.Value, op.Filter)
		if err != nil {
			return nil, err
		}
		if matched {
			candidates = append(candidates, item)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := namespaceString(candidates[i].Namespace), namespaceString(candidates[j].Namespace)
		if a != b {
			return a < b
		}
		return candidates[i].Key < candidates[j].Key
	})
	limit := op.Limit
	if limit == 0 {
		limit = 10
	}
	start := min(op.Offset, len(candidates))
	end := min(start+limit, len(candidates))
	result := make([]store.SearchItem, 0, end-start)
	for _, item := range candidates[start:end] {
		result = append(result, store.SearchItem{Item: item})
	}
	return result, nil
}

func listSnapshot(items []store.Item, op store.ListNamespacesOp) ([]store.Namespace, error) {
	if op.Limit < 0 || op.Offset < 0 || op.MaxDepth < 0 {
		return nil, fmt.Errorf("%w: negative namespace pagination/depth", store.ErrInvalidOperation)
	}
	set := make(map[string]store.Namespace)
	for _, item := range items {
		namespace := item.Namespace
		matched := true
		for _, condition := range op.MatchConditions {
			ok, err := matchNamespace(namespace, condition)
			if err != nil {
				return nil, err
			}
			if !ok {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if op.MaxDepth > 0 && len(namespace) > op.MaxDepth {
			namespace = namespace[:op.MaxDepth]
		}
		set[namespaceString(namespace)] = cloneNamespace(namespace)
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	limit := op.Limit
	if limit == 0 {
		limit = 100
	}
	start := min(op.Offset, len(keys))
	end := min(start+limit, len(keys))
	result := make([]store.Namespace, 0, end-start)
	for _, key := range keys[start:end] {
		result = append(result, set[key])
	}
	return result, nil
}

func encodeNamespace(namespace store.Namespace) (string, error) {
	data, err := json.Marshal(namespace)
	if err != nil {
		return "", fmt.Errorf("%w: encode namespace: %v", store.ErrInvalidOperation, err)
	}
	return string(data), nil
}

func decodeNamespace(data string) (store.Namespace, error) {
	var namespace store.Namespace
	if err := json.Unmarshal([]byte(data), &namespace); err != nil {
		return nil, fmt.Errorf("decode PostgreSQL store namespace: %w", err)
	}
	return namespace, nil
}

func decodeValue(data []byte) (store.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode PostgreSQL store value: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unexpected trailing JSON value")
	}
	return store.Value(normalizeNumbers(value).(map[string]any)), nil
}

func normalizeNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		text := typed.String()
		if !strings.ContainsAny(text, ".eE") {
			if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
				if int64(int(integer)) == integer {
					return int(integer)
				}
				return integer
			}
			if unsigned, err := strconv.ParseUint(text, 10, 64); err == nil {
				return unsigned
			}
		}
		if number, err := typed.Float64(); err == nil {
			return number
		}
		return text
	case map[string]any:
		for key, item := range typed {
			typed[key] = normalizeNumbers(item)
		}
		return typed
	case []any:
		for index, item := range typed {
			typed[index] = normalizeNumbers(item)
		}
		return typed
	default:
		return value
	}
}

func matchesFilter(value, filter store.Value) (bool, error) {
	for key, expected := range filter {
		matched, err := compareValues(value[key], expected)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func compareValues(actual, expected any) (bool, error) {
	if object, ok := objectValue(expected); ok {
		hasOperator := false
		for key := range object {
			if strings.HasPrefix(key, "$") {
				hasOperator = true
				break
			}
		}
		if hasOperator {
			for operator, operand := range object {
				matched, err := applyOperator(actual, operator, operand)
				if err != nil || !matched {
					return matched, err
				}
			}
			return true, nil
		}
		actualObject, ok := objectValue(actual)
		if !ok {
			return false, nil
		}
		for key, nested := range object {
			matched, err := compareValues(actualObject[key], nested)
			if err != nil || !matched {
				return matched, err
			}
		}
		return true, nil
	}
	return reflect.DeepEqual(actual, expected), nil
}

func objectValue(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case store.Value:
		return map[string]any(typed), true
	default:
		return nil, false
	}
}

func applyOperator(actual any, operator string, operand any) (bool, error) {
	switch operator {
	case "$eq":
		return reflect.DeepEqual(actual, operand), nil
	case "$ne":
		return !reflect.DeepEqual(actual, operand), nil
	case "$gt", "$gte", "$lt", "$lte":
		a, err := numeric(actual)
		if err != nil {
			return false, err
		}
		b, err := numeric(operand)
		if err != nil {
			return false, err
		}
		switch operator {
		case "$gt":
			return a > b, nil
		case "$gte":
			return a >= b, nil
		case "$lt":
			return a < b, nil
		default:
			return a <= b, nil
		}
	default:
		return false, fmt.Errorf("%w: filter operator %q", store.ErrUnsupportedQuery, operator)
	}
}

func numeric(value any) (float64, error) {
	ref := reflect.ValueOf(value)
	if !ref.IsValid() {
		return 0, fmt.Errorf("%w: nil is not numeric", store.ErrUnsupportedQuery)
	}
	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(ref.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(ref.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return ref.Float(), nil
	case reflect.String:
		result, err := strconv.ParseFloat(ref.String(), 64)
		if err == nil {
			return result, nil
		}
	}
	return 0, fmt.Errorf("%w: %T is not numeric", store.ErrUnsupportedQuery, value)
}

func matchNamespace(namespace store.Namespace, condition store.MatchCondition) (bool, error) {
	if len(namespace) < len(condition.Path) {
		return false, nil
	}
	start := 0
	switch condition.Type {
	case store.MatchPrefix:
	case store.MatchSuffix:
		start = len(namespace) - len(condition.Path)
	default:
		return false, fmt.Errorf("%w: namespace match type %q", store.ErrUnsupportedQuery, condition.Type)
	}
	for index, part := range condition.Path {
		if part != "*" && namespace[start+index] != part {
			return false, nil
		}
	}
	return true, nil
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

func namespaceString(namespace store.Namespace) string { return strings.Join(namespace, "\x00") }
func cloneNamespace(namespace store.Namespace) store.Namespace {
	return append(store.Namespace(nil), namespace...)
}
func hasPrefix(namespace, prefix store.Namespace) bool {
	if len(namespace) < len(prefix) {
		return false
	}
	for index := range prefix {
		if namespace[index] != prefix[index] {
			return false
		}
	}
	return true
}

var _ store.Store = (*Store)(nil)
var _ store.TTLStore = (*Store)(nil)
