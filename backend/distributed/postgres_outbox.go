package distributed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresOutboxSchemaVersion = 1

// OutboxStats reports retained durable completion count.
type OutboxStats struct{ Completions int64 }

// PostgresOutbox durably first-writes typed task results. In leased mode it
// deletes the matching current queue lease in the same transaction.
type PostgresOutbox[R any] struct {
	mu                             sync.Mutex
	db                             *sql.DB
	codec                          PayloadCodec[R]
	clock                          func() time.Time
	ackLease, owned, setup, closed bool
}

func NewPostgresOutbox[R any](db *sql.DB, codec PayloadCodec[R], clock func() time.Time) (*PostgresOutbox[R], error) {
	return newPostgresOutbox(db, codec, clock, false)
}
func NewPostgresLeasedOutbox[R any](db *sql.DB, codec PayloadCodec[R], clock func() time.Time) (*PostgresOutbox[R], error) {
	return newPostgresOutbox(db, codec, clock, true)
}
func newPostgresOutbox[R any](db *sql.DB, codec PayloadCodec[R], clock func() time.Time, ack bool) (*PostgresOutbox[R], error) {
	if db == nil || codec == nil || clock == nil {
		return nil, fmt.Errorf("%w: PostgreSQL outbox requires database, codec, and clock", ErrInvalidRequest)
	}
	return &PostgresOutbox[R]{db: db, codec: codec, clock: clock, ackLease: ack}, nil
}
func OpenPostgresOutbox[R any](ctx context.Context, dsn string, codec PayloadCodec[R], clock func() time.Time, ackLease bool) (*PostgresOutbox[R], error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL outbox DSN is empty", ErrInvalidRequest)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	outbox, err := newPostgresOutbox(db, codec, clock, ackLease)
	if err != nil {
		db.Close()
		return nil, err
	}
	outbox.owned = true
	if err = outbox.Setup(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return outbox, nil
}
func (o *PostgresOutbox[R]) AcknowledgesLease() bool { return o.ackLease }
func (o *PostgresOutbox[R]) Close() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	owned := o.owned
	db := o.db
	o.mu.Unlock()
	if owned {
		return db.Close()
	}
	return nil
}
func (o *PostgresOutbox[R]) Setup(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return fmt.Errorf("PostgreSQL outbox is closed")
	}
	if o.setup {
		return nil
	}
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(684773294)`); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS distributed_outbox_migrations(version INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS distributed_outbox(task_id TEXT PRIMARY KEY,attempt INTEGER NOT NULL,lease_token TEXT NOT NULL,worker_id TEXT NOT NULL DEFAULT '',payload BYTEA NOT NULL,result_hash BYTEA NOT NULL,created_at TIMESTAMPTZ NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS distributed_outbox_retention_idx ON distributed_outbox(created_at)`,
		`INSERT INTO distributed_outbox_migrations(version) VALUES(1) ON CONFLICT DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("setup PostgreSQL outbox: %w", err)
		}
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM distributed_outbox_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > postgresOutboxSchemaVersion {
		return fmt.Errorf("unsupported PostgreSQL outbox schema version %d", version)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	o.setup = true
	return nil
}
func (o *PostgresOutbox[R]) ensureSetup(ctx context.Context) error {
	o.mu.Lock()
	ready := o.setup
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return fmt.Errorf("PostgreSQL outbox is closed")
	}
	if ready {
		return nil
	}
	return o.Setup(ctx)
}

func (o *PostgresOutbox[R]) Commit(ctx context.Context, completion Completion[R]) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validID(completion.TaskID) || !validID(completion.LeaseToken) || completion.Attempt < 1 || o.ackLease && !validID(completion.WorkerID) {
		return fmt.Errorf("%w: completion task, attempt, lease, and worker are required", ErrInvalidRequest)
	}
	if err := o.ensureSetup(ctx); err != nil {
		return err
	}
	encoded, err := o.codec.Encode(completion.Value)
	if err != nil {
		return fmt.Errorf("%w: encode completion: %v", ErrInvalidRequest, err)
	}
	if _, err = o.codec.Decode(encoded); err != nil {
		return fmt.Errorf("%w: decode completion: %v", ErrInvalidRequest, err)
	}
	hash := sha256.Sum256(encoded)
	now := o.clock()
	if now.IsZero() {
		return fmt.Errorf("%w: outbox clock returned zero time", ErrInvalidRequest)
	}
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,684773294))`, completion.TaskID); err != nil {
		return err
	}
	var priorHash []byte
	err = tx.QueryRowContext(ctx, `SELECT result_hash FROM distributed_outbox WHERE task_id=$1 FOR UPDATE`, completion.TaskID).Scan(&priorHash)
	if err == nil {
		if !bytes.Equal(priorHash, hash[:]) {
			return ErrCompletionConflict
		}
		if o.ackLease {
			if err = o.ackLeaseTx(ctx, tx, completion, now, true); err != nil {
				return err
			}
		}
		return tx.Commit()
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO distributed_outbox(task_id,attempt,lease_token,worker_id,payload,result_hash,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, completion.TaskID, completion.Attempt, completion.LeaseToken, completion.WorkerID, encoded, hash[:], now)
	if err != nil {
		return err
	}
	if o.ackLease {
		if err = o.ackLeaseTx(ctx, tx, completion, now, false); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (o *PostgresOutbox[R]) ackLeaseTx(ctx context.Context, tx *sql.Tx, completion Completion[R], now time.Time, allowMissing bool) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM distributed_queue_tasks WHERE id=$1 AND lease_token=$2 AND worker_id=$3 AND lease_expires_at>$4`, completion.TaskID, completion.LeaseToken, completion.WorkerID, now)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 1 {
		return nil
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM distributed_queue_tasks WHERE id=$1)`, completion.TaskID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrLeaseLost
	}
	if allowMissing {
		return nil
	}
	return ErrTaskNotFound
}
func (o *PostgresOutbox[R]) Get(ctx context.Context, taskID string) (Completion[R], bool, error) {
	var zero Completion[R]
	if err := validContext(ctx); err != nil {
		return zero, false, err
	}
	if !validID(taskID) {
		return zero, false, fmt.Errorf("%w: task ID is required", ErrInvalidRequest)
	}
	if err := o.ensureSetup(ctx); err != nil {
		return zero, false, err
	}
	var completion Completion[R]
	var encoded []byte
	err := o.db.QueryRowContext(ctx, `SELECT task_id,attempt,lease_token,worker_id,payload FROM distributed_outbox WHERE task_id=$1`, taskID).Scan(&completion.TaskID, &completion.Attempt, &completion.LeaseToken, &completion.WorkerID, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	completion.Value, err = o.codec.Decode(encoded)
	if err != nil {
		return zero, false, fmt.Errorf("%w: decode committed completion: %v", ErrInvalidRequest, err)
	}
	return completion, true, nil
}
func (o *PostgresOutbox[R]) Prune(ctx context.Context, before time.Time) (int64, error) {
	if err := o.ensureSetup(ctx); err != nil {
		return 0, err
	}
	result, err := o.db.ExecContext(ctx, `DELETE FROM distributed_outbox WHERE created_at<$1`, before)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}
func (o *PostgresOutbox[R]) Stats(ctx context.Context) (OutboxStats, error) {
	if err := o.ensureSetup(ctx); err != nil {
		return OutboxStats{}, err
	}
	var stats OutboxStats
	err := o.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM distributed_outbox`).Scan(&stats.Completions)
	return stats, err
}
