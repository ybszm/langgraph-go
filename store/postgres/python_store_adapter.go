package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/wahanbo/langgraph-go/store"
	"github.com/wahanbo/langgraph-go/store/internal/storeutil"
)

// PythonStoreAdapter implements Store against langgraph-checkpoint-postgres
// 3.1.0's physical `store` schema. It is intentionally separate from the
// native Go PostgreSQL Store because the migration ledgers are incompatible.
type PythonStoreAdapter struct {
	db  *sql.DB
	now func() time.Time
}

// NewPythonStoreAdapter constructs an adapter over an existing pgx database.
func NewPythonStoreAdapter(db *sql.DB) (*PythonStoreAdapter, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: Python PostgreSQL store DB is nil", store.ErrInvalidOperation)
	}
	return &PythonStoreAdapter{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

var pythonStoreMigrations = []string{
	`CREATE TABLE IF NOT EXISTS store (
prefix text NOT NULL, key text NOT NULL, value jsonb NOT NULL,
created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (prefix, key))`,
	`CREATE INDEX IF NOT EXISTS store_prefix_idx ON store (prefix text_pattern_ops)`,
	`ALTER TABLE store ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS ttl_minutes INT`,
	`CREATE INDEX IF NOT EXISTS idx_store_expires_at ON store (expires_at) WHERE expires_at IS NOT NULL`,
}

// Setup applies the exact upstream non-vector migration ledger (versions 0-3).
func (s *PythonStoreAdapter) Setup(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", store.ErrInvalidOperation)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS store_migrations (v INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("setup Python store migration table: %w", err)
	}
	var current sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT max(v) FROM store_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read Python store migration: %w", err)
	}
	version := -1
	if current.Valid {
		version = int(current.Int64)
	}
	if version >= len(pythonStoreMigrations) {
		return fmt.Errorf("%w: Python store schema version %d is newer than %d", store.ErrInvalidOperation, version, len(pythonStoreMigrations)-1)
	}
	for next := version + 1; next < len(pythonStoreMigrations); next++ {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, pythonStoreMigrations[next]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply Python store migration %d: %w", next, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO store_migrations(v) VALUES ($1)`, next); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record Python store migration %d: %w", next, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit Python store migration %d: %w", next, err)
		}
	}
	return nil
}

func (s *PythonStoreAdapter) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key, RefreshTTL: true}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *PythonStoreAdapter) Search(ctx context.Context, namespace store.Namespace, options store.SearchOptions) ([]store.SearchItem, error) {
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: namespace, Filter: options.Filter, Query: options.Query,
		Limit: options.Limit, Offset: options.Offset, RefreshTTL: true,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *PythonStoreAdapter) Put(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value}})
	return err
}

func (s *PythonStoreAdapter) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key}})
	return err
}

func (s *PythonStoreAdapter) ListNamespaces(ctx context.Context, options store.ListNamespacesOptions) ([]store.Namespace, error) {
	conditions := make([]store.MatchCondition, 0, 2)
	if len(options.Prefix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchPrefix, Path: options.Prefix})
	}
	if len(options.Suffix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchSuffix, Path: options.Suffix})
	}
	results, err := s.Batch(ctx, []store.Operation{store.ListNamespacesOp{
		MatchConditions: conditions, MaxDepth: options.MaxDepth, Limit: options.Limit, Offset: options.Offset,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Namespaces, nil
}

func (s *PythonStoreAdapter) PutWithTTL(ctx context.Context, namespace store.Namespace, key string, value store.Value, ttl time.Duration) error {
	if ttl <= 0 {
		return store.ErrInvalidTTL
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value, TTL: ttl, TTLSet: true}})
	return err
}

func (s *PythonStoreAdapter) GetWithTTLRefresh(ctx context.Context, namespace store.Namespace, key string, refresh bool) (*store.Item, error) {
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key, RefreshTTL: refresh, RefreshTTLSet: true}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *PythonStoreAdapter) SearchWithTTLRefresh(ctx context.Context, namespace store.Namespace, options store.SearchOptions, refresh bool) ([]store.SearchItem, error) {
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: namespace, Filter: options.Filter, Query: options.Query,
		Limit: options.Limit, Offset: options.Offset, RefreshTTL: refresh, RefreshTTLSet: true,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *PythonStoreAdapter) SweepExpired(ctx context.Context) (int64, error) {
	items, err := s.SweepExpiredItems(ctx)
	return int64(len(items)), err
}

func (s *PythonStoreAdapter) SweepExpiredItems(ctx context.Context) ([]store.ItemIdentity, error) {
	rows, err := s.db.QueryContext(ctx, `DELETE FROM store WHERE expires_at IS NOT NULL AND expires_at<=$1 RETURNING prefix,key`, s.now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []store.ItemIdentity
	for rows.Next() {
		var prefix, key string
		if err := rows.Scan(&prefix, &key); err != nil {
			return nil, err
		}
		result = append(result, store.ItemIdentity{Namespace: pythonNamespace(prefix), Key: key})
	}
	return result, rows.Err()
}

func (s *PythonStoreAdapter) Batch(ctx context.Context, operations []store.Operation) ([]store.Result, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", store.ErrInvalidOperation)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	snapshot, err := loadPythonStoreItems(ctx, tx)
	if err != nil {
		return nil, err
	}
	results := make([]store.Result, len(operations))
	type positionedPut struct {
		op       store.PutOp
		position int
	}
	puts := make(map[string]positionedPut)
	refresh := make(map[string]store.ItemIdentity)
	for index, raw := range operations {
		switch op := raw.(type) {
		case store.GetOp:
			if err := validatePythonIdentity(op.Namespace, op.Key); err != nil {
				return nil, err
			}
			for _, item := range snapshot {
				if samePythonIdentity(item, op.Namespace, op.Key) {
					copy := item
					copy.Namespace = append(store.Namespace(nil), item.Namespace...)
					results[index].Item = &copy
					if op.RefreshTTL {
						refresh[pythonStoreIdentity(op.Namespace, op.Key)] = store.ItemIdentity{Namespace: op.Namespace, Key: op.Key}
					}
					break
				}
			}
		case store.SearchOp:
			if op.Query != "" {
				return nil, store.ErrUnsupportedQuery
			}
			items, err := storeutil.Search(snapshot, op)
			if err != nil {
				return nil, err
			}
			results[index].Items = items
			if op.RefreshTTL {
				for _, item := range items {
					refresh[pythonStoreIdentity(item.Namespace, item.Key)] = store.ItemIdentity{Namespace: item.Namespace, Key: item.Key}
				}
			}
		case store.ListNamespacesOp:
			namespaces, err := storeutil.ListNamespaces(snapshot, op)
			if err != nil {
				return nil, err
			}
			results[index].Namespaces = namespaces
		case store.PutOp:
			if err := validatePythonIdentity(op.Namespace, op.Key); err != nil {
				return nil, err
			}
			puts[pythonStoreIdentity(op.Namespace, op.Key)] = positionedPut{op: op, position: index}
		default:
			return nil, fmt.Errorf("%w: operation %T", store.ErrInvalidOperation, raw)
		}
	}
	for _, item := range refresh {
		if _, err := tx.ExecContext(ctx, `UPDATE store SET expires_at=NOW()+(ttl_minutes||' minutes')::interval WHERE prefix=$1 AND key=$2 AND ttl_minutes IS NOT NULL`, pythonPrefix(item.Namespace), item.Key); err != nil {
			return nil, err
		}
	}
	ordered := make([]positionedPut, 0, len(puts))
	for _, put := range puts {
		ordered = append(ordered, put)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].position < ordered[j].position })
	for _, positioned := range ordered {
		op := positioned.op
		if op.Value == nil {
			if _, err := tx.ExecContext(ctx, `DELETE FROM store WHERE prefix=$1 AND key=$2`, pythonPrefix(op.Namespace), op.Key); err != nil {
				return nil, err
			}
			continue
		}
		encoded, err := json.Marshal(op.Value)
		if err != nil {
			return nil, err
		}
		var expires any
		var minutes any
		if op.TTL > 0 {
			expires = s.now().Add(op.TTL)
			minutes = int64(math.Ceil(op.TTL.Minutes()))
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO store(prefix,key,value,created_at,updated_at,expires_at,ttl_minutes)
VALUES($1,$2,$3::jsonb,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,$4,$5)
ON CONFLICT(prefix,key) DO UPDATE SET value=EXCLUDED.value,updated_at=CURRENT_TIMESTAMP,expires_at=EXCLUDED.expires_at,ttl_minutes=EXCLUDED.ttl_minutes`,
			pythonPrefix(op.Namespace), op.Key, encoded, expires, minutes); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

func loadPythonStoreItems(ctx context.Context, tx *sql.Tx) ([]store.Item, error) {
	rows, err := tx.QueryContext(ctx, `SELECT prefix,key,value,created_at,updated_at FROM store ORDER BY updated_at DESC,prefix,key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []store.Item
	for rows.Next() {
		var prefix string
		var value []byte
		var item store.Item
		if err := rows.Scan(&prefix, &item.Key, &value, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.Namespace = pythonNamespace(prefix)
		if err := json.Unmarshal(value, &item.Value); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func validatePythonIdentity(namespace store.Namespace, key string) error {
	if err := store.ValidateNamespace(namespace); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: key is empty", store.ErrInvalidOperation)
	}
	return nil
}

func pythonPrefix(namespace store.Namespace) string { return strings.Join(namespace, ".") }
func pythonNamespace(prefix string) store.Namespace {
	return store.Namespace(strings.Split(prefix, "."))
}
func pythonStoreIdentity(namespace store.Namespace, key string) string {
	return pythonPrefix(namespace) + "\x00" + key
}
func samePythonIdentity(item store.Item, namespace store.Namespace, key string) bool {
	return item.Key == key && pythonPrefix(item.Namespace) == pythonPrefix(namespace)
}

var _ store.ExpiredItemStore = (*PythonStoreAdapter)(nil)
