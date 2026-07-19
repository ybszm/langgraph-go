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
)

const postgresEventSchemaVersion = 1

// PostgresEventLogOptions controls durable identity, time, and Tail polling.
type PostgresEventLogOptions struct {
	EventLogOptions
	PollInterval time.Duration
}

// EventLogStats is an operational snapshot of durable event storage.
type EventLogStats struct {
	Streams         int64
	Events          int64
	TerminalStreams int64
}

// PostgresEventLog is a multi-instance EventLog backed by PostgreSQL row locks.
type PostgresEventLog struct {
	mu                   sync.Mutex
	db                   *sql.DB
	owned, setup, closed bool
	clock                func() time.Time
	ids                  func() string
	poll                 time.Duration
}

func NewPostgresEventLog(db *sql.DB, options PostgresEventLogOptions) (*PostgresEventLog, error) {
	if db == nil || options.Clock == nil || options.IDGenerator == nil {
		return nil, fmt.Errorf("%w: PostgreSQL event database, clock, and ID generator are required", ErrInvalidRequest)
	}
	if options.PollInterval < 0 {
		return nil, fmt.Errorf("%w: event poll interval cannot be negative", ErrInvalidRequest)
	}
	if options.PollInterval == 0 {
		options.PollInterval = 50 * time.Millisecond
	}
	return &PostgresEventLog{db: db, clock: options.Clock, ids: options.IDGenerator, poll: options.PollInterval}, nil
}
func OpenPostgresEventLog(ctx context.Context, dsn string, options PostgresEventLogOptions) (*PostgresEventLog, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL event DSN is empty", ErrInvalidRequest)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	log, err := NewPostgresEventLog(db, options)
	if err != nil {
		db.Close()
		return nil, err
	}
	log.owned = true
	if err = log.Setup(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return log, nil
}
func (l *PostgresEventLog) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	owned := l.owned
	db := l.db
	l.mu.Unlock()
	if owned {
		return db.Close()
	}
	return nil
}

func (l *PostgresEventLog) Setup(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fmt.Errorf("PostgreSQL event log is closed")
	}
	if l.setup {
		return nil
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(684773291)`); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS distributed_event_migrations(version INTEGER PRIMARY KEY)`,
		`CREATE TABLE IF NOT EXISTS distributed_event_streams(thread_id TEXT NOT NULL,run_id TEXT NOT NULL,next_sequence BIGINT NOT NULL DEFAULT 1,terminal BOOLEAN NOT NULL DEFAULT FALSE,updated_at TIMESTAMPTZ NOT NULL,PRIMARY KEY(thread_id,run_id))`,
		`CREATE TABLE IF NOT EXISTS distributed_events(id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,run_id TEXT NOT NULL,task_id TEXT NOT NULL DEFAULT '',mode TEXT NOT NULL,sequence BIGINT NOT NULL,data JSONB NOT NULL,terminal BOOLEAN NOT NULL,created_at TIMESTAMPTZ NOT NULL,UNIQUE(thread_id,run_id,sequence),FOREIGN KEY(thread_id,run_id) REFERENCES distributed_event_streams(thread_id,run_id) ON DELETE CASCADE)`,
		`CREATE TABLE IF NOT EXISTS distributed_event_idempotency(thread_id TEXT NOT NULL,run_id TEXT NOT NULL,key TEXT NOT NULL,request_hash BYTEA NOT NULL,event_id TEXT NOT NULL,PRIMARY KEY(thread_id,run_id,key),FOREIGN KEY(event_id) REFERENCES distributed_events(id) ON DELETE CASCADE)`,
		`CREATE INDEX IF NOT EXISTS distributed_events_stream_idx ON distributed_events(thread_id,run_id,sequence)`,
		`CREATE INDEX IF NOT EXISTS distributed_event_retention_idx ON distributed_event_streams(terminal,updated_at)`,
		`INSERT INTO distributed_event_migrations(version) VALUES(1) ON CONFLICT DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("setup PostgreSQL event log: %w", err)
		}
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM distributed_event_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > postgresEventSchemaVersion {
		return fmt.Errorf("unsupported PostgreSQL event schema version %d", version)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	l.setup = true
	return nil
}
func (l *PostgresEventLog) ensureSetup(ctx context.Context) error {
	l.mu.Lock()
	ready := l.setup
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return fmt.Errorf("PostgreSQL event log is closed")
	}
	if ready {
		return nil
	}
	return l.Setup(ctx)
}

func (l *PostgresEventLog) Append(ctx context.Context, request AppendEvent) (Event, error) {
	if err := validContext(ctx); err != nil {
		return Event{}, err
	}
	if !validID(request.ThreadID) || !validID(request.RunID) || !validID(request.Mode) || request.IdempotencyKey != "" && !validID(request.IdempotencyKey) || !json.Valid(request.Data) {
		return Event{}, fmt.Errorf("%w: invalid event identity, mode, idempotency key, or JSON data", ErrInvalidRequest)
	}
	if err := l.ensureSetup(ctx); err != nil {
		return Event{}, err
	}
	now := l.clock()
	if now.IsZero() {
		return Event{}, fmt.Errorf("%w: event clock returned zero time", ErrInvalidRequest)
	}
	hash := eventFingerprint(request)
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO distributed_event_streams(thread_id,run_id,updated_at) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, request.ThreadID, request.RunID, now)
	if err != nil {
		return Event{}, err
	}
	var sequence int64
	var terminal bool
	if err = tx.QueryRowContext(ctx, `SELECT next_sequence,terminal FROM distributed_event_streams WHERE thread_id=$1 AND run_id=$2 FOR UPDATE`, request.ThreadID, request.RunID).Scan(&sequence, &terminal); err != nil {
		return Event{}, err
	}
	if request.IdempotencyKey != "" {
		var priorHash []byte
		var priorID string
		err = tx.QueryRowContext(ctx, `SELECT request_hash,event_id FROM distributed_event_idempotency WHERE thread_id=$1 AND run_id=$2 AND key=$3`, request.ThreadID, request.RunID, request.IdempotencyKey).Scan(&priorHash, &priorID)
		if err == nil {
			if !bytes.Equal(priorHash, hash[:]) {
				return Event{}, ErrEventConflict
			}
			event, ok, err := getPostgresEvent(ctx, tx, request.ThreadID, request.RunID, priorID)
			if err != nil {
				return Event{}, err
			}
			if !ok {
				return Event{}, fmt.Errorf("%w: idempotency binding event is missing", ErrEventConflict)
			}
			return event, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return Event{}, err
		}
	}
	if terminal {
		return Event{}, fmt.Errorf("%w: cannot append after terminal event", ErrEventConflict)
	}
	id := l.ids()
	if !validID(id) {
		return Event{}, fmt.Errorf("%w: generated event ID is invalid", ErrInvalidRequest)
	}
	event := Event{ID: id, ThreadID: request.ThreadID, RunID: request.RunID, TaskID: request.TaskID, Mode: request.Mode, Sequence: uint64(sequence), Data: append(json.RawMessage(nil), request.Data...), Terminal: request.Terminal, CreatedAt: now}
	_, err = tx.ExecContext(ctx, `INSERT INTO distributed_events(id,thread_id,run_id,task_id,mode,sequence,data,terminal,created_at) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)`, id, request.ThreadID, request.RunID, request.TaskID, request.Mode, sequence, []byte(request.Data), request.Terminal, now)
	if err != nil {
		if postgresUnique(err) {
			return Event{}, fmt.Errorf("%w: duplicate event ID %q", ErrInvalidRequest, id)
		}
		return Event{}, err
	}
	if request.IdempotencyKey != "" {
		if _, err = tx.ExecContext(ctx, `INSERT INTO distributed_event_idempotency(thread_id,run_id,key,request_hash,event_id) VALUES($1,$2,$3,$4,$5)`, request.ThreadID, request.RunID, request.IdempotencyKey, hash[:], id); err != nil {
			return Event{}, err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE distributed_event_streams SET next_sequence=$3,terminal=$4,updated_at=$5 WHERE thread_id=$1 AND run_id=$2`, request.ThreadID, request.RunID, sequence+1, request.Terminal, now)
	if err != nil {
		return Event{}, err
	}
	if err = tx.Commit(); err != nil {
		return Event{}, err
	}
	return event, nil
}

func (l *PostgresEventLog) List(ctx context.Context, query EventQuery) ([]Event, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(query.ThreadID) || !validID(query.RunID) || query.Limit < 0 || query.Buffer < 0 {
		return nil, fmt.Errorf("%w: invalid event query", ErrInvalidRequest)
	}
	if err := l.ensureSetup(ctx); err != nil {
		return nil, err
	}
	afterSequence := int64(0)
	if query.AfterID != "" {
		err := l.db.QueryRowContext(ctx, `SELECT sequence FROM distributed_events WHERE thread_id=$1 AND run_id=$2 AND id=$3`, query.ThreadID, query.RunID, query.AfterID).Scan(&afterSequence)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrEventCursorNotFound
		}
		if err != nil {
			return nil, err
		}
	}
	sqlQuery := `SELECT id,thread_id,run_id,task_id,mode,sequence,data,terminal,created_at FROM distributed_events WHERE thread_id=$1 AND run_id=$2 AND sequence>$3 ORDER BY sequence`
	args := []any{query.ThreadID, query.RunID, afterSequence}
	if query.Limit > 0 {
		sqlQuery += ` LIMIT $4`
		args = append(args, query.Limit)
	}
	rows, err := l.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Event{}
	for rows.Next() {
		event, err := scanPostgresEvent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}
func (l *PostgresEventLog) Tail(ctx context.Context, query EventQuery) <-chan EventResult {
	buffer := query.Buffer
	if buffer < 0 {
		buffer = 0
	}
	output := make(chan EventResult, buffer)
	go func() {
		defer close(output)
		after := query.AfterID
		for {
			batch := query
			batch.AfterID = after
			events, err := l.List(ctx, batch)
			if err != nil {
				select {
				case output <- EventResult{Error: err}:
				case <-ctx.Done():
				}
				return
			}
			for _, event := range events {
				select {
				case output <- EventResult{Event: event}:
					after = event.ID
				case <-ctx.Done():
					return
				}
				if event.Terminal {
					return
				}
			}
			if len(events) > 0 {
				continue
			}
			terminal, err := l.streamTerminal(ctx, query.ThreadID, query.RunID)
			if err != nil {
				select {
				case output <- EventResult{Error: err}:
				case <-ctx.Done():
				}
				return
			}
			if terminal {
				return
			}
			timer := time.NewTimer(l.poll)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}()
	return output
}
func (l *PostgresEventLog) streamTerminal(ctx context.Context, threadID, runID string) (bool, error) {
	var terminal bool
	err := l.db.QueryRowContext(ctx, `SELECT terminal FROM distributed_event_streams WHERE thread_id=$1 AND run_id=$2`, threadID, runID).Scan(&terminal)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return terminal, err
}

// Prune removes complete event streams older than before in one transaction.
func (l *PostgresEventLog) Prune(ctx context.Context, before time.Time) (int64, error) {
	if err := l.ensureSetup(ctx); err != nil {
		return 0, err
	}
	result, err := l.db.ExecContext(ctx, `DELETE FROM distributed_event_streams WHERE terminal=TRUE AND updated_at<$1`, before)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}

// Stats returns counts useful for retention and backlog monitoring.
func (l *PostgresEventLog) Stats(ctx context.Context) (EventLogStats, error) {
	if err := l.ensureSetup(ctx); err != nil {
		return EventLogStats{}, err
	}
	var stats EventLogStats
	err := l.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER(WHERE terminal) FROM distributed_event_streams`).Scan(&stats.Streams, &stats.TerminalStreams)
	if err != nil {
		return stats, err
	}
	err = l.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM distributed_events`).Scan(&stats.Events)
	return stats, err
}

type postgresEventScanner interface{ Scan(...any) error }
type postgresEventQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getPostgresEvent(ctx context.Context, q postgresEventQuery, threadID, runID, id string) (Event, bool, error) {
	event, err := scanPostgresEvent(q.QueryRowContext(ctx, `SELECT id,thread_id,run_id,task_id,mode,sequence,data,terminal,created_at FROM distributed_events WHERE thread_id=$1 AND run_id=$2 AND id=$3`, threadID, runID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	return event, err == nil, err
}
func scanPostgresEvent(row postgresEventScanner) (Event, error) {
	var event Event
	var sequence int64
	var data []byte
	if err := row.Scan(&event.ID, &event.ThreadID, &event.RunID, &event.TaskID, &event.Mode, &sequence, &data, &event.Terminal, &event.CreatedAt); err != nil {
		return Event{}, err
	}
	event.Sequence = uint64(sequence)
	event.Data = append(json.RawMessage(nil), data...)
	return event, nil
}
func postgresUnique(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "duplicate key") || strings.Contains(message, "sqlstate 23505")
}
