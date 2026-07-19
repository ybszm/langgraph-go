package distributed

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wahanbo/langgraph-go/graph"
)

const postgresInterruptSchemaVersion = 1

// InterruptStats is an operational snapshot of external-input records.
type InterruptStats struct{ Pending, Resumed int64 }

// PostgresInterruptStore is a durable multi-instance InterruptStore.
type PostgresInterruptStore struct {
	mu                   sync.Mutex
	db                   *sql.DB
	clock                func() time.Time
	owned, setup, closed bool
}

func NewPostgresInterruptStore(db *sql.DB, clock func() time.Time) (*PostgresInterruptStore, error) {
	if db == nil || clock == nil {
		return nil, fmt.Errorf("%w: PostgreSQL interrupt store requires database and clock", ErrInvalidRequest)
	}
	return &PostgresInterruptStore{db: db, clock: clock}, nil
}
func OpenPostgresInterruptStore(ctx context.Context, dsn string, clock func() time.Time) (*PostgresInterruptStore, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL interrupt DSN is empty", ErrInvalidRequest)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	store, err := NewPostgresInterruptStore(db, clock)
	if err != nil {
		db.Close()
		return nil, err
	}
	store.owned = true
	if err = store.Setup(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}
func (s *PostgresInterruptStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	owned := s.owned
	db := s.db
	s.mu.Unlock()
	if owned {
		return db.Close()
	}
	return nil
}
func (s *PostgresInterruptStore) Provider() graph.ResumeProvider { return s.Await }
func (s *PostgresInterruptStore) Setup(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("PostgreSQL interrupt store is closed")
	}
	if s.setup {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(684773293)`); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS distributed_interrupt_migrations(version INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS distributed_interrupts(thread_id TEXT NOT NULL,interrupt_id TEXT NOT NULL,task_id TEXT NOT NULL,checkpoint_id TEXT NOT NULL DEFAULT '',idx INTEGER NOT NULL,namespace TEXT NOT NULL DEFAULT '',prompt BYTEA NOT NULL,status TEXT NOT NULL,resume BYTEA,created_at TIMESTAMPTZ NOT NULL,resumed_at TIMESTAMPTZ,PRIMARY KEY(thread_id,interrupt_id),CHECK(status IN ('pending','resumed')))`,
		`CREATE INDEX IF NOT EXISTS distributed_interrupt_thread_idx ON distributed_interrupts(thread_id,created_at,interrupt_id)`,
		`CREATE INDEX IF NOT EXISTS distributed_interrupt_retention_idx ON distributed_interrupts(status,resumed_at)`,
		`INSERT INTO distributed_interrupt_migrations(version) VALUES(1) ON CONFLICT DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("setup PostgreSQL interrupt store: %w", err)
		}
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM distributed_interrupt_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > postgresInterruptSchemaVersion {
		return fmt.Errorf("unsupported PostgreSQL interrupt schema version %d", version)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.setup = true
	return nil
}
func (s *PostgresInterruptStore) ensureSetup(ctx context.Context) error {
	s.mu.Lock()
	ready := s.setup
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return fmt.Errorf("PostgreSQL interrupt store is closed")
	}
	if ready {
		return nil
	}
	return s.Setup(ctx)
}

func (s *PostgresInterruptStore) Await(ctx context.Context, request graph.InterruptRequest) (json.RawMessage, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(request.ThreadID) || !validID(request.TaskID) || !validID(request.Interrupt.ID) || request.Index < 0 || !json.Valid(request.Interrupt.Value) {
		return nil, fmt.Errorf("%w: invalid interrupt request", ErrInvalidRequest)
	}
	if err := s.ensureSetup(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// validID rejects newlines, so this text-safe separator is unambiguous and
	// can be passed to PostgreSQL's hashtextextended (which rejects NUL bytes).
	key := request.ThreadID + "\n" + request.Interrupt.ID
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,684773293))`, key); err != nil {
		return nil, err
	}
	record, ok, err := getPostgresInterrupt(ctx, tx, request.ThreadID, request.Interrupt.ID, false)
	if err != nil {
		return nil, err
	}
	if ok {
		if !sameInterrupt(&record, request) {
			return nil, ErrInterruptConflict
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		if record.Status == InterruptResumed {
			return append(json.RawMessage(nil), record.Resume...), nil
		}
		return nil, &InterruptPendingError{Interrupt: cloneInterrupt(record.Interrupt)}
	}
	now := s.clock()
	if now.IsZero() {
		return nil, fmt.Errorf("%w: interrupt clock returned zero time", ErrInvalidRequest)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO distributed_interrupts(thread_id,interrupt_id,task_id,checkpoint_id,idx,namespace,prompt,status,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,'pending',$8)`, request.ThreadID, request.Interrupt.ID, request.TaskID, request.CheckpointID, request.Index, request.Interrupt.Namespace, []byte(request.Interrupt.Value), now)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return nil, &InterruptPendingError{Interrupt: cloneInterrupt(request.Interrupt)}
}
func (s *PostgresInterruptStore) Resume(ctx context.Context, threadID, interruptID string, value any) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validID(threadID) || !validID(interruptID) {
		return fmt.Errorf("%w: thread and interrupt ID are required", ErrInvalidRequest)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: encode resume: %v", ErrInvalidRequest, err)
	}
	if err = s.ensureSetup(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, ok, err := getPostgresInterrupt(ctx, tx, threadID, interruptID, true)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInterruptNotFound
	}
	if record.Status == InterruptResumed {
		if !bytes.Equal(record.Resume, encoded) {
			return ErrInterruptConflict
		}
		return tx.Commit()
	}
	now := s.clock()
	if now.IsZero() {
		return fmt.Errorf("%w: interrupt clock returned zero time", ErrInvalidRequest)
	}
	_, err = tx.ExecContext(ctx, `UPDATE distributed_interrupts SET status='resumed',resume=$3,resumed_at=$4 WHERE thread_id=$1 AND interrupt_id=$2`, threadID, interruptID, encoded, now)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *PostgresInterruptStore) List(ctx context.Context, threadID string) ([]InterruptRecord, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(threadID) {
		return nil, fmt.Errorf("%w: thread ID is required", ErrInvalidRequest)
	}
	if err := s.ensureSetup(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT thread_id,interrupt_id,task_id,checkpoint_id,idx,namespace,prompt,status,resume,created_at,resumed_at FROM distributed_interrupts WHERE thread_id=$1 ORDER BY created_at,interrupt_id`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []InterruptRecord{}
	for rows.Next() {
		record, err := scanPostgresInterrupt(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

// Prune deletes only resumed records completed before the cutoff.
func (s *PostgresInterruptStore) Prune(ctx context.Context, before time.Time) (int64, error) {
	if err := s.ensureSetup(ctx); err != nil {
		return 0, err
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM distributed_interrupts WHERE status='resumed' AND resumed_at<$1`, before)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}
func (s *PostgresInterruptStore) Stats(ctx context.Context) (InterruptStats, error) {
	if err := s.ensureSetup(ctx); err != nil {
		return InterruptStats{}, err
	}
	var stats InterruptStats
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FILTER(WHERE status='pending'),COUNT(*) FILTER(WHERE status='resumed') FROM distributed_interrupts`).Scan(&stats.Pending, &stats.Resumed)
	return stats, err
}

type postgresInterruptScanner interface{ Scan(...any) error }
type postgresInterruptQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getPostgresInterrupt(ctx context.Context, q postgresInterruptQuery, threadID, interruptID string, lock bool) (InterruptRecord, bool, error) {
	query := `SELECT thread_id,interrupt_id,task_id,checkpoint_id,idx,namespace,prompt,status,resume,created_at,resumed_at FROM distributed_interrupts WHERE thread_id=$1 AND interrupt_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	record, err := scanPostgresInterrupt(q.QueryRowContext(ctx, query, threadID, interruptID))
	if errors.Is(err, sql.ErrNoRows) {
		return InterruptRecord{}, false, nil
	}
	return record, err == nil, err
}
func scanPostgresInterrupt(row postgresInterruptScanner) (InterruptRecord, error) {
	var record InterruptRecord
	var id, status string
	var prompt, resume []byte
	var resumed sql.NullTime
	if err := row.Scan(&record.ThreadID, &id, &record.TaskID, &record.CheckpointID, &record.Index, &record.Interrupt.Namespace, &prompt, &status, &resume, &record.CreatedAt, &resumed); err != nil {
		return InterruptRecord{}, err
	}
	record.Interrupt.ID = id
	record.Interrupt.Value = append(json.RawMessage(nil), prompt...)
	record.Status = InterruptStatus(status)
	record.Resume = append(json.RawMessage(nil), resume...)
	if resumed.Valid {
		value := resumed.Time
		record.ResumedAt = &value
	}
	return record, nil
}
