package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteControlStore persists the remote control plane in a shared SQLite DB.
// Separate Server instances and processes may open the same WAL database file.
type SQLiteControlStore struct{ db *sql.DB }

// OpenSQLiteControlStore opens and migrates a durable control store. The caller
// owns the returned database through Close.
func OpenSQLiteControlStore(ctx context.Context, dsn string) (*SQLiteControlStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	store, err := NewSQLiteControlStore(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// NewSQLiteControlStore migrates an existing SQLite database handle.
func NewSQLiteControlStore(ctx context.Context, db *sql.DB) (*SQLiteControlStore, error) {
	if db == nil {
		return nil, fmt.Errorf("remote sqlite control store requires database")
	}
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS remote_threads (id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS remote_runs (thread_id TEXT NOT NULL, id TEXT NOT NULL, status TEXT NOT NULL, output BLOB, error BLOB, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(thread_id,id))`,
		`CREATE TABLE IF NOT EXISTS remote_idempotency (thread_id TEXT NOT NULL, key TEXT NOT NULL, request_hash BLOB NOT NULL, run_id TEXT NOT NULL, PRIMARY KEY(thread_id,key))`,
		`CREATE INDEX IF NOT EXISTS remote_runs_created_idx ON remote_runs(thread_id,created_at,id)`,
		`CREATE INDEX IF NOT EXISTS remote_runs_retention_idx ON remote_runs(status,updated_at)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return nil, fmt.Errorf("remote sqlite migration: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE remote_threads ADD COLUMN metadata BLOB`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return nil, fmt.Errorf("remote sqlite metadata migration: %w", err)
	}
	return &SQLiteControlStore{db: db}, nil
}

func (s *SQLiteControlStore) Close() error { return s.db.Close() }
func (s *SQLiteControlStore) CreateThread(ctx context.Context, thread Thread) error {
	metadata, err := json.Marshal(thread.Metadata)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO remote_threads(id,created_at,updated_at,metadata) VALUES(?,?,?,?)`, thread.ID, timeNanos(thread.CreatedAt), timeNanos(thread.UpdatedAt), nullableBytes(metadata))
	if sqliteConstraint(err) {
		return ErrControlConflict
	}
	return err
}
func (s *SQLiteControlStore) GetThread(ctx context.Context, id string) (Thread, bool, error) {
	var item Thread
	var created, updated int64
	var metadata []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,created_at,updated_at,metadata FROM remote_threads WHERE id=?`, id).Scan(&item.ID, &created, &updated, &metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return Thread{}, false, nil
	}
	if err != nil {
		return Thread{}, false, err
	}
	item.CreatedAt = timeFromNanos(created)
	item.UpdatedAt = timeFromNanos(updated)
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &item.Metadata); err != nil {
			return Thread{}, false, err
		}
	}
	return item, true, nil
}
func (s *SQLiteControlStore) ListThreads(ctx context.Context, options ListOptions) ([]Thread, error) {
	query, args := limitOffset(`SELECT id,created_at,updated_at,metadata FROM remote_threads ORDER BY created_at,id`, options)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Thread{}
	for rows.Next() {
		var item Thread
		var created, updated int64
		var metadata []byte
		if err := rows.Scan(&item.ID, &created, &updated, &metadata); err != nil {
			return nil, err
		}
		item.CreatedAt = timeFromNanos(created)
		item.UpdatedAt = timeFromNanos(updated)
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &item.Metadata); err != nil {
				return nil, err
			}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s *SQLiteControlStore) DeleteThread(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{`DELETE FROM remote_idempotency WHERE thread_id=?`, `DELETE FROM remote_runs WHERE thread_id=?`} {
		if _, err = tx.ExecContext(ctx, query, id); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM remote_threads WHERE id=?`, id)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrControlNotFound
	}
	return tx.Commit()
}

func (s *SQLiteControlStore) CreateRun(ctx context.Context, run StoredRun, key string, hash []byte) (StoredRun, bool, error) {
	var last error
	for attempt := 0; attempt < 12; attempt++ {
		stored, created, err := s.createRunOnce(ctx, run, key, hash)
		if !sqliteBusy(err) {
			return stored, created, err
		}
		last = err
		delay := time.Duration(attempt+1) * time.Millisecond
		select {
		case <-ctx.Done():
			return StoredRun{}, false, ctx.Err()
		case <-time.After(delay):
		}
	}
	return StoredRun{}, false, last
}

func (s *SQLiteControlStore) createRunOnce(ctx context.Context, run StoredRun, key string, hash []byte) (StoredRun, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return StoredRun{}, false, err
	}
	defer tx.Rollback()
	var ignored string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM remote_threads WHERE id=?`, run.ThreadID).Scan(&ignored); errors.Is(err, sql.ErrNoRows) {
		return StoredRun{}, false, ErrControlNotFound
	} else if err != nil {
		return StoredRun{}, false, err
	}
	if key != "" {
		var priorHash []byte
		var priorID string
		err = tx.QueryRowContext(ctx, `SELECT request_hash,run_id FROM remote_idempotency WHERE thread_id=? AND key=?`, run.ThreadID, key).Scan(&priorHash, &priorID)
		if err == nil {
			if !bytes.Equal(priorHash, hash) {
				return StoredRun{}, false, ErrIdempotencyConflict
			}
			prior, ok, err := getStoredRun(ctx, tx, run.ThreadID, priorID)
			if err != nil {
				return StoredRun{}, false, err
			}
			if !ok {
				return StoredRun{}, false, ErrControlConflict
			}
			return prior, false, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return StoredRun{}, false, err
		}
	}
	errorJSON, err := json.Marshal(run.Error)
	if err != nil {
		return StoredRun{}, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO remote_runs(thread_id,id,status,output,error,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, run.ThreadID, run.ID, string(run.Status), nullableBytes(run.Output), nullableBytes(errorJSON), timeNanos(run.CreatedAt), timeNanos(run.UpdatedAt))
	if sqliteConstraint(err) {
		return StoredRun{}, false, ErrControlConflict
	}
	if err != nil {
		return StoredRun{}, false, err
	}
	if key != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO remote_idempotency(thread_id,key,request_hash,run_id) VALUES(?,?,?,?)`, run.ThreadID, key, hash, run.ID)
		if sqliteConstraint(err) {
			_ = tx.Rollback()
			return s.resolveIdempotency(ctx, run.ThreadID, key, hash)
		}
		if err != nil {
			return StoredRun{}, false, err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE remote_threads SET updated_at=? WHERE id=?`, timeNanos(run.UpdatedAt), run.ThreadID)
	if err != nil {
		return StoredRun{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return StoredRun{}, false, err
	}
	return cloneStoredRun(run), true, nil
}

func (s *SQLiteControlStore) resolveIdempotency(ctx context.Context, threadID, key string, hash []byte) (StoredRun, bool, error) {
	var priorHash []byte
	var priorID string
	err := s.db.QueryRowContext(ctx, `SELECT request_hash,run_id FROM remote_idempotency WHERE thread_id=? AND key=?`, threadID, key).Scan(&priorHash, &priorID)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredRun{}, false, ErrControlConflict
	}
	if err != nil {
		return StoredRun{}, false, err
	}
	if !bytes.Equal(priorHash, hash) {
		return StoredRun{}, false, ErrIdempotencyConflict
	}
	prior, ok, err := s.GetRun(ctx, threadID, priorID)
	if err != nil {
		return StoredRun{}, false, err
	}
	if !ok {
		return StoredRun{}, false, ErrControlConflict
	}
	return prior, false, nil
}
func (s *SQLiteControlStore) GetRun(ctx context.Context, threadID, id string) (StoredRun, bool, error) {
	return getStoredRun(ctx, s.db, threadID, id)
}
func (s *SQLiteControlStore) ListRuns(ctx context.Context, threadID string, options ListOptions) ([]StoredRun, error) {
	if _, ok, err := s.GetThread(ctx, threadID); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrControlNotFound
	}
	query, args := limitOffset(`SELECT id,thread_id,status,output,error,created_at,updated_at FROM remote_runs WHERE thread_id=? ORDER BY created_at,id`, options, threadID)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []StoredRun{}
	for rows.Next() {
		item, err := scanStoredRun(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s *SQLiteControlStore) TransitionRun(ctx context.Context, threadID, id string, expected []RunStatus, next StoredRun) (StoredRun, bool, error) {
	if len(expected) == 0 {
		current, ok, err := s.GetRun(ctx, threadID, id)
		return current, false, chooseMissing(ok, err)
	}
	current, ok, err := s.GetRun(ctx, threadID, id)
	if err != nil {
		return StoredRun{}, false, err
	}
	if !ok {
		return StoredRun{}, false, ErrControlNotFound
	}
	placeholders := make([]string, len(expected))
	args := []any{string(next.Status), nullableBytes(next.Output)}
	errorJSON, err := json.Marshal(next.Error)
	if err != nil {
		return StoredRun{}, false, err
	}
	args = append(args, nullableBytes(errorJSON), timeNanos(next.UpdatedAt), threadID, id)
	for i, status := range expected {
		placeholders[i] = "?"
		args = append(args, string(status))
	}
	query := `UPDATE remote_runs SET status=?,output=?,error=?,updated_at=? WHERE thread_id=? AND id=? AND status IN (` + strings.Join(placeholders, ",") + `)`
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return StoredRun{}, false, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		current, ok, err = s.GetRun(ctx, threadID, id)
		if err != nil {
			return StoredRun{}, false, err
		}
		if !ok {
			return StoredRun{}, false, ErrControlNotFound
		}
		return current, false, nil
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE remote_threads SET updated_at=? WHERE id=?`, timeNanos(next.UpdatedAt), threadID)
	next.ID = id
	next.ThreadID = threadID
	next.CreatedAt = current.CreatedAt
	return cloneStoredRun(next), true, nil
}
func (s *SQLiteControlStore) PruneRuns(ctx context.Context, before time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	args := []any{string(RunSuccess), string(RunError), string(RunCanceled), timeNanos(before)}
	_, err = tx.ExecContext(ctx, `DELETE FROM remote_idempotency WHERE EXISTS (SELECT 1 FROM remote_runs r WHERE r.thread_id=remote_idempotency.thread_id AND r.id=remote_idempotency.run_id AND r.status IN (?,?,?) AND r.updated_at < ?)`, args...)
	if err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM remote_runs WHERE status IN (?,?,?) AND updated_at < ?`, args...)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

type rowScanner interface{ Scan(...any) error }
type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getStoredRun(ctx context.Context, q queryRower, threadID, id string) (StoredRun, bool, error) {
	item, err := scanStoredRun(q.QueryRowContext(ctx, `SELECT id,thread_id,status,output,error,created_at,updated_at FROM remote_runs WHERE thread_id=? AND id=?`, threadID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return StoredRun{}, false, nil
	}
	return item, err == nil, err
}
func scanStoredRun(row rowScanner) (StoredRun, error) {
	var item StoredRun
	var status string
	var output, errorJSON []byte
	var created, updated int64
	if err := row.Scan(&item.ID, &item.ThreadID, &status, &output, &errorJSON, &created, &updated); err != nil {
		return StoredRun{}, err
	}
	item.Status = RunStatus(status)
	item.Output = append(json.RawMessage(nil), output...)
	if len(errorJSON) > 0 && string(errorJSON) != "null" {
		if err := json.Unmarshal(errorJSON, &item.Error); err != nil {
			return StoredRun{}, err
		}
	}
	item.CreatedAt = timeFromNanos(created)
	item.UpdatedAt = timeFromNanos(updated)
	return item, nil
}
func limitOffset(base string, options ListOptions, prefix ...any) (string, []any) {
	args := append([]any(nil), prefix...)
	if options.Limit > 0 {
		base += " LIMIT ?"
		args = append(args, options.Limit)
		if options.Offset > 0 {
			base += " OFFSET ?"
			args = append(args, options.Offset)
		}
	} else if options.Offset > 0 {
		base += " LIMIT -1 OFFSET ?"
		args = append(args, options.Offset)
	}
	return base, args
}
func nullableBytes(value []byte) any {
	if len(value) == 0 || string(value) == "null" {
		return nil
	}
	return value
}
func timeNanos(value time.Time) int64     { return value.UTC().UnixNano() }
func timeFromNanos(value int64) time.Time { return time.Unix(0, value).UTC() }
func sqliteConstraint(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "constraint")
}
func sqliteBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked") || strings.Contains(message, "sqlite_busy")
}
func chooseMissing(ok bool, err error) error {
	if err != nil {
		return err
	}
	if !ok {
		return ErrControlNotFound
	}
	return nil
}
