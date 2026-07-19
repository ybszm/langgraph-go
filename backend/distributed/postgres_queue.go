package distributed

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresQueueSchemaVersion = 1

// QueueStats is an operational snapshot at the queue's injected clock time.
type QueueStats struct{ Total, Ready, Delayed, Leased, ExpiredLeases, IdempotencyBindings int64 }

// PostgresQueue is a typed, codec-backed, multi-worker leased Queue.
type PostgresQueue[P any] struct {
	mu                   sync.Mutex
	db                   *sql.DB
	codec                PayloadCodec[P]
	clock                func() time.Time
	ids                  func() string
	owned, setup, closed bool
}

func NewPostgresQueue[P any](db *sql.DB, options QueueOptions, codec PayloadCodec[P]) (*PostgresQueue[P], error) {
	if db == nil || options.Clock == nil || options.IDGenerator == nil || codec == nil {
		return nil, fmt.Errorf("%w: PostgreSQL queue requires database, clock, ID generator, and codec", ErrInvalidRequest)
	}
	return &PostgresQueue[P]{db: db, codec: codec, clock: options.Clock, ids: options.IDGenerator}, nil
}
func OpenPostgresQueue[P any](ctx context.Context, dsn string, options QueueOptions, codec PayloadCodec[P]) (*PostgresQueue[P], error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL queue DSN is empty", ErrInvalidRequest)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	queue, err := NewPostgresQueue(db, options, codec)
	if err != nil {
		db.Close()
		return nil, err
	}
	queue.owned = true
	if err = queue.Setup(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return queue, nil
}
func (q *PostgresQueue[P]) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	owned := q.owned
	db := q.db
	q.mu.Unlock()
	if owned {
		return db.Close()
	}
	return nil
}
func (q *PostgresQueue[P]) Setup(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("PostgreSQL queue is closed")
	}
	if q.setup {
		return nil
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(684773292)`); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS distributed_queue_migrations(version INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS distributed_queue_tasks(id TEXT PRIMARY KEY,payload BYTEA NOT NULL,attempt INTEGER NOT NULL DEFAULT 0,enqueued_at TIMESTAMPTZ NOT NULL,available_at TIMESTAMPTZ NOT NULL,lease_token TEXT,worker_id TEXT,lease_expires_at TIMESTAMPTZ)`,
		`CREATE TABLE IF NOT EXISTS distributed_queue_idempotency(key TEXT PRIMARY KEY,request_hash BYTEA NOT NULL,task_id TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS distributed_queue_claim_idx ON distributed_queue_tasks(available_at,enqueued_at,id)`,
		`CREATE INDEX IF NOT EXISTS distributed_queue_lease_idx ON distributed_queue_tasks(lease_expires_at)`,
		`CREATE INDEX IF NOT EXISTS distributed_queue_idempotency_retention_idx ON distributed_queue_idempotency(created_at)`,
		`INSERT INTO distributed_queue_migrations(version) VALUES(1) ON CONFLICT DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("setup PostgreSQL queue: %w", err)
		}
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM distributed_queue_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > postgresQueueSchemaVersion {
		return fmt.Errorf("unsupported PostgreSQL queue schema version %d", version)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	q.setup = true
	return nil
}
func (q *PostgresQueue[P]) ensureSetup(ctx context.Context) error {
	q.mu.Lock()
	ready := q.setup
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return fmt.Errorf("PostgreSQL queue is closed")
	}
	if ready {
		return nil
	}
	return q.Setup(ctx)
}

func (q *PostgresQueue[P]) Enqueue(ctx context.Context, request EnqueueRequest[P]) (string, error) {
	if err := validContext(ctx); err != nil {
		return "", err
	}
	if request.IdempotencyKey != "" && !validID(request.IdempotencyKey) {
		return "", fmt.Errorf("%w: idempotency key is invalid", ErrInvalidRequest)
	}
	if err := q.ensureSetup(ctx); err != nil {
		return "", err
	}
	encoded, err := q.codec.Encode(request.Payload)
	if err != nil {
		return "", fmt.Errorf("%w: encode payload: %v", ErrInvalidRequest, err)
	}
	if _, err = q.codec.Decode(encoded); err != nil {
		return "", fmt.Errorf("%w: decode payload clone: %v", ErrInvalidRequest, err)
	}
	hash := queueFingerprint(encoded, request.TaskID, request.AvailableAt)
	now := q.clock()
	if now.IsZero() {
		return "", fmt.Errorf("%w: queue clock returned zero time", ErrInvalidRequest)
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if request.IdempotencyKey != "" {
		if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,684773292))`, request.IdempotencyKey); err != nil {
			return "", err
		}
		var priorHash []byte
		var priorID string
		err = tx.QueryRowContext(ctx, `SELECT request_hash,task_id FROM distributed_queue_idempotency WHERE key=$1`, request.IdempotencyKey).Scan(&priorHash, &priorID)
		if err == nil {
			if !bytes.Equal(priorHash, hash[:]) {
				return "", ErrIdempotencyConflict
			}
			return priorID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
	}
	id := request.TaskID
	if id == "" {
		id = q.ids()
	}
	if !validID(id) {
		return "", fmt.Errorf("%w: generated task ID is invalid", ErrInvalidRequest)
	}
	available := request.AvailableAt
	if available.IsZero() {
		available = now
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO distributed_queue_tasks(id,payload,enqueued_at,available_at) VALUES($1,$2,$3,$4)`, id, encoded, now, available)
	if err != nil {
		if postgresUnique(err) {
			return "", fmt.Errorf("%w: duplicate task ID %q", ErrInvalidRequest, id)
		}
		return "", err
	}
	if request.IdempotencyKey != "" {
		if _, err = tx.ExecContext(ctx, `INSERT INTO distributed_queue_idempotency(key,request_hash,task_id,created_at) VALUES($1,$2,$3,$4)`, request.IdempotencyKey, hash[:], id, now); err != nil {
			return "", err
		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

func (q *PostgresQueue[P]) Claim(ctx context.Context, workerID string, limit int, duration time.Duration) ([]Task[P], error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(workerID) || limit <= 0 || duration <= 0 {
		return nil, fmt.Errorf("%w: worker, positive limit, and lease duration are required", ErrInvalidRequest)
	}
	if err := q.ensureSetup(ctx); err != nil {
		return nil, err
	}
	now := q.clock()
	if now.IsZero() {
		return nil, fmt.Errorf("%w: queue clock returned zero time", ErrInvalidRequest)
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,payload,attempt,enqueued_at,available_at FROM distributed_queue_tasks WHERE available_at<=$1 AND (lease_expires_at IS NULL OR lease_expires_at<=$1) ORDER BY available_at,enqueued_at,id FOR UPDATE SKIP LOCKED LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id                  string
		encoded             []byte
		attempt             int
		enqueued, available time.Time
		payload             P
	}
	candidates := []candidate{}
	for rows.Next() {
		var item candidate
		if err = rows.Scan(&item.id, &item.encoded, &item.attempt, &item.enqueued, &item.available); err != nil {
			rows.Close()
			return nil, err
		}
		item.payload, err = q.codec.Decode(item.encoded)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: decode claimed payload: %v", ErrInvalidRequest, err)
		}
		candidates = append(candidates, item)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	result := make([]Task[P], len(candidates))
	for i, item := range candidates {
		token := q.ids()
		if !validID(token) {
			return nil, fmt.Errorf("%w: generated lease token is invalid", ErrInvalidRequest)
		}
		expires := now.Add(duration)
		command, err := tx.ExecContext(ctx, `UPDATE distributed_queue_tasks SET attempt=attempt+1,lease_token=$2,worker_id=$3,lease_expires_at=$4 WHERE id=$1`, item.id, token, workerID, expires)
		if err != nil {
			return nil, err
		}
		affected, _ := command.RowsAffected()
		if affected != 1 {
			return nil, ErrLeaseLost
		}
		result[i] = Task[P]{ID: item.id, Payload: item.payload, Attempt: item.attempt + 1, EnqueuedAt: item.enqueued, AvailableAt: item.available, Lease: Lease{TaskID: item.id, Token: token, WorkerID: workerID, ExpiresAt: expires}}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (q *PostgresQueue[P]) Ack(ctx context.Context, lease Lease) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if err := q.ensureSetup(ctx); err != nil {
		return err
	}
	now := q.clock()
	result, err := q.db.ExecContext(ctx, `DELETE FROM distributed_queue_tasks WHERE id=$1 AND lease_token=$2 AND worker_id=$3 AND lease_expires_at>$4`, lease.TaskID, lease.Token, lease.WorkerID, now)
	if err != nil {
		return err
	}
	return q.leaseResult(ctx, lease.TaskID, result)
}
func (q *PostgresQueue[P]) Nack(ctx context.Context, lease Lease, delay time.Duration) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("%w: nack delay cannot be negative", ErrInvalidRequest)
	}
	if err := q.ensureSetup(ctx); err != nil {
		return err
	}
	now := q.clock()
	result, err := q.db.ExecContext(ctx, `UPDATE distributed_queue_tasks SET available_at=$4,lease_token=NULL,worker_id=NULL,lease_expires_at=NULL WHERE id=$1 AND lease_token=$2 AND worker_id=$3 AND lease_expires_at>$5`, lease.TaskID, lease.Token, lease.WorkerID, now.Add(delay), now)
	if err != nil {
		return err
	}
	return q.leaseResult(ctx, lease.TaskID, result)
}
func (q *PostgresQueue[P]) Heartbeat(ctx context.Context, lease Lease, extension time.Duration) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if extension <= 0 {
		return fmt.Errorf("%w: heartbeat extension must be positive", ErrInvalidRequest)
	}
	if err := q.ensureSetup(ctx); err != nil {
		return err
	}
	now := q.clock()
	result, err := q.db.ExecContext(ctx, `UPDATE distributed_queue_tasks SET lease_expires_at=$4 WHERE id=$1 AND lease_token=$2 AND worker_id=$3 AND lease_expires_at>$5`, lease.TaskID, lease.Token, lease.WorkerID, now.Add(extension), now)
	if err != nil {
		return err
	}
	return q.leaseResult(ctx, lease.TaskID, result)
}
func (q *PostgresQueue[P]) leaseResult(ctx context.Context, taskID string, result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	var exists bool
	err = q.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM distributed_queue_tasks WHERE id=$1)`, taskID).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return ErrTaskNotFound
	}
	return ErrLeaseLost
}

// ReleaseExpiredLeases clears expired ownership for operational visibility;
// Claim is correct even when this maintenance method is never called.
func (q *PostgresQueue[P]) ReleaseExpiredLeases(ctx context.Context) (int64, error) {
	if err := q.ensureSetup(ctx); err != nil {
		return 0, err
	}
	now := q.clock()
	result, err := q.db.ExecContext(ctx, `UPDATE distributed_queue_tasks SET lease_token=NULL,worker_id=NULL,lease_expires_at=NULL WHERE lease_expires_at<=$1`, now)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}

// PruneIdempotency removes old bindings only after their task was acknowledged.
func (q *PostgresQueue[P]) PruneIdempotency(ctx context.Context, before time.Time) (int64, error) {
	if err := q.ensureSetup(ctx); err != nil {
		return 0, err
	}
	result, err := q.db.ExecContext(ctx, `DELETE FROM distributed_queue_idempotency i WHERE i.created_at<$1 AND NOT EXISTS(SELECT 1 FROM distributed_queue_tasks t WHERE t.id=i.task_id)`, before)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}
func (q *PostgresQueue[P]) Stats(ctx context.Context) (QueueStats, error) {
	if err := q.ensureSetup(ctx); err != nil {
		return QueueStats{}, err
	}
	now := q.clock()
	var stats QueueStats
	err := q.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER(WHERE available_at<=$1 AND (lease_expires_at IS NULL OR lease_expires_at<=$1)),COUNT(*) FILTER(WHERE available_at>$1),COUNT(*) FILTER(WHERE lease_expires_at>$1),COUNT(*) FILTER(WHERE lease_expires_at IS NOT NULL AND lease_expires_at<=$1) FROM distributed_queue_tasks`, now).Scan(&stats.Total, &stats.Ready, &stats.Delayed, &stats.Leased, &stats.ExpiredLeases)
	if err != nil {
		return stats, err
	}
	err = q.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM distributed_queue_idempotency`).Scan(&stats.IdempotencyBindings)
	return stats, err
}
