// Package sqlite implements durable checkpoint storage backed by SQLite.
//
// The saver is intended for local and single-process deployments. Operations
// are serialized per Saver, and every checkpoint write stores channel blobs
// and the checkpoint head in one database transaction.
package sqlite

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

	"github.com/ybszm/langgraph-go/checkpoint"
	_ "modernc.org/sqlite"
)

const schemaVersion = 2

var schemaStatements = []string{
	`PRAGMA busy_timeout=5000`,
	`PRAGMA journal_mode=WAL`,
	`CREATE TABLE IF NOT EXISTS checkpoint_migrations (
		version INTEGER PRIMARY KEY
	)`,
	`CREATE TABLE IF NOT EXISTS checkpoints (
		thread_id TEXT NOT NULL,
		checkpoint_ns TEXT NOT NULL DEFAULT '',
		checkpoint_id TEXT NOT NULL,
		parent_checkpoint_id TEXT,
		checkpoint BLOB NOT NULL,
		metadata BLOB,
		PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	)`,
	`CREATE TABLE IF NOT EXISTS checkpoint_blobs (
		thread_id TEXT NOT NULL,
		checkpoint_ns TEXT NOT NULL DEFAULT '',
		channel TEXT NOT NULL,
		version TEXT NOT NULL,
		present INTEGER NOT NULL,
		type TEXT,
		value_version INTEGER,
		value BLOB,
		PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
	)`,
	`CREATE TABLE IF NOT EXISTS checkpoint_writes (
		thread_id TEXT NOT NULL,
		checkpoint_ns TEXT NOT NULL DEFAULT '',
		checkpoint_id TEXT NOT NULL,
		task_id TEXT NOT NULL,
		idx INTEGER NOT NULL,
		task_path TEXT NOT NULL DEFAULT '',
		channel TEXT NOT NULL,
		type TEXT NOT NULL,
		value_version INTEGER NOT NULL,
		value BLOB,
		PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
	)`,
	`CREATE INDEX IF NOT EXISTS checkpoints_history
		ON checkpoints (thread_id, checkpoint_ns, checkpoint_id DESC)`,
}

// Saver stores checkpoints in a database/sql SQLite database. A Saver is safe
// for concurrent use. Callers that construct it with New retain ownership of
// the database handle; Open returns a Saver whose Close method closes it.
type Saver struct {
	mu      sync.Mutex
	db      *sql.DB
	owned   bool
	setup   bool
	closing bool
}

// New constructs a Saver over an existing SQLite database handle. The schema
// is initialized lazily, or explicitly with Setup. New does not take ownership
// of db.
func New(db *sql.DB) (*Saver, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: SQLite database is nil", checkpoint.ErrInvalidConfig)
	}
	return &Saver{db: db}, nil
}

// Open opens dsn with the pure-Go SQLite driver, initializes the schema, and
// returns a Saver that owns the resulting database handle.
func Open(ctx context.Context, dsn string) (*Saver, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: SQLite DSN is empty", checkpoint.ErrInvalidConfig)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite checkpoint database: %w", err)
	}
	saver := &Saver{db: db, owned: true}
	if err := saver.Setup(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return saver, nil
}

// Setup creates the checkpoint schema and applies idempotent migrations.
func (s *Saver) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setupLocked(ctx)
}

func (s *Saver) setupLocked(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: SQLite saver is nil", checkpoint.ErrInvalidConfig)
	}
	if s.closing {
		return fmt.Errorf("SQLite checkpoint saver is closed")
	}
	if s.setup {
		return nil
	}
	// Connection pragmas are safe before inspecting the migration ledger. Domain
	// tables must not be touched until a future schema version is rejected.
	for _, statement := range schemaStatements[:2] {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set up SQLite checkpoint schema: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, schemaStatements[2]); err != nil {
		return fmt.Errorf("set up SQLite checkpoint migration ledger: %w", err)
	}
	var recorded sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT max(version) FROM checkpoint_migrations`).Scan(&recorded); err != nil {
		return fmt.Errorf("read SQLite checkpoint schema version: %w", err)
	}
	version := 0
	if recorded.Valid {
		version = int(recorded.Int64)
	}
	if version < 0 || version > schemaVersion {
		return fmt.Errorf("unsupported SQLite checkpoint schema version %d", version)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite checkpoint schema migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range schemaStatements[3:] {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set up SQLite checkpoint schema: %w", err)
		}
	}
	hasTaskPath, err := sqliteColumnExists(ctx, tx, "checkpoint_writes", "task_path")
	if err != nil {
		return fmt.Errorf("inspect SQLite checkpoint_writes schema: %w", err)
	}
	if !hasTaskPath {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE checkpoint_writes ADD COLUMN task_path TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate SQLite checkpoint writes to schema v2: %w", err)
		}
	}
	for next := version + 1; next <= schemaVersion; next++ {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO checkpoint_migrations(version) VALUES (?)`, next); err != nil {
			return fmt.Errorf("record SQLite checkpoint schema version %d: %w", next, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite checkpoint schema migration: %w", err)
	}
	s.setup = true
	return nil
}

func sqliteColumnExists(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typeName string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// Close closes the database handle opened by Open. For a Saver created with
// New, Close is a no-op because the caller owns the handle.
func (s *Saver) Close() error {
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

// Put implements checkpoint.Saver. Channel blobs and the checkpoint row are
// committed atomically.
func (s *Saver) Put(ctx context.Context, parent checkpoint.Config, value checkpoint.Checkpoint, metadata checkpoint.Metadata, newVersions map[string]string) (checkpoint.Config, error) {
	if err := contextError(ctx); err != nil {
		return checkpoint.Config{}, err
	}
	if err := parent.Validate(); err != nil {
		return checkpoint.Config{}, err
	}
	if err := value.Validate(); err != nil {
		return checkpoint.Config{}, err
	}
	clonedMetadata, err := checkpoint.CloneMetadata(metadata)
	if err != nil {
		return checkpoint.Config{}, err
	}
	stored := checkpoint.CloneCheckpoint(value)
	stored.Values = nil
	checkpointData, err := json.Marshal(stored)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("encode checkpoint %q: %w", value.ID, err)
	}
	metadataData, err := json.Marshal(clonedMetadata)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("encode checkpoint metadata: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return checkpoint.Config{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("begin SQLite checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	channels := sortedKeys(newVersions)
	for _, channel := range channels {
		version := newVersions[channel]
		encoded, present := value.Values[channel]
		var typeName any
		var valueVersion any
		var data any
		if present {
			typeName, valueVersion, data = encoded.Type, encoded.Version, encoded.Data
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO checkpoint_blobs
				(thread_id, checkpoint_ns, channel, version, present, type, value_version, value)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(thread_id, checkpoint_ns, channel, version) DO UPDATE SET
				present=excluded.present, type=excluded.type,
				value_version=excluded.value_version, value=excluded.value`,
			parent.ThreadID, parent.Namespace, channel, version, boolInt(present), typeName, valueVersion, data,
		); err != nil {
			return checkpoint.Config{}, fmt.Errorf("store SQLite channel blob %q: %w", channel, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO checkpoints
			(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(thread_id, checkpoint_ns, checkpoint_id) DO UPDATE SET
			parent_checkpoint_id=excluded.parent_checkpoint_id,
			checkpoint=excluded.checkpoint, metadata=excluded.metadata`,
		parent.ThreadID, parent.Namespace, value.ID, nullableString(parent.CheckpointID), checkpointData, metadataData,
	); err != nil {
		return checkpoint.Config{}, fmt.Errorf("store SQLite checkpoint %q: %w", value.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return checkpoint.Config{}, fmt.Errorf("commit SQLite checkpoint %q: %w", value.ID, err)
	}
	return checkpoint.Config{ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID}, nil
}

// PutWrites implements checkpoint.Saver. Non-negative write indexes use
// first-write-wins; reserved negative indexes are replaceable.
func (s *Saver) PutWrites(ctx context.Context, config checkpoint.Config, writes []checkpoint.PendingWrite) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if config.CheckpointID == "" {
		return fmt.Errorf("%w: checkpoint ID is empty for pending writes", checkpoint.ErrInvalidConfig)
	}
	for _, write := range writes {
		if write.TaskID == "" || write.Channel == "" {
			return fmt.Errorf("%w: pending write has an empty task or channel", checkpoint.ErrInvalidCheckpoint)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite pending-write transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM checkpoints WHERE thread_id=? AND checkpoint_ns=? AND checkpoint_id=?`, config.ThreadID, config.Namespace, config.CheckpointID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: pending-write checkpoint %q does not exist", checkpoint.ErrInvalidConfig, config.CheckpointID)
		}
		return fmt.Errorf("find SQLite pending-write checkpoint: %w", err)
	}
	for _, write := range writes {
		query := `INSERT OR IGNORE INTO checkpoint_writes
			(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, task_path, channel, type, value_version, value)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if write.Index < 0 {
			query = `INSERT INTO checkpoint_writes
				(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, task_path, channel, type, value_version, value)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO UPDATE SET
					task_path=excluded.task_path, channel=excluded.channel,
					type=excluded.type, value_version=excluded.value_version, value=excluded.value`
		}
		if _, err := tx.ExecContext(ctx, query,
			config.ThreadID, config.Namespace, config.CheckpointID,
			write.TaskID, write.Index, write.TaskPath, write.Channel,
			write.Value.Type, write.Value.Version, write.Value.Data,
		); err != nil {
			return fmt.Errorf("store SQLite pending write task=%q index=%d: %w", write.TaskID, write.Index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite pending writes: %w", err)
	}
	return nil
}

// GetTuple implements checkpoint.Saver.
func (s *Saver) GetTuple(ctx context.Context, config checkpoint.Config) (checkpoint.Tuple, bool, error) {
	if err := contextError(ctx); err != nil {
		return checkpoint.Tuple{}, false, err
	}
	if err := config.Validate(); err != nil {
		return checkpoint.Tuple{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return checkpoint.Tuple{}, false, err
	}
	record, found, err := s.getRecordLocked(ctx, config)
	if err != nil || !found {
		return checkpoint.Tuple{}, found, err
	}
	tuple, err := s.hydrateLocked(ctx, record)
	return tuple, err == nil, err
}

// List implements checkpoint.Saver. Results are newest-first by checkpoint ID
// with deterministic thread and namespace tie-breakers.
func (s *Saver) List(ctx context.Context, options checkpoint.ListOptions) ([]checkpoint.Tuple, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit < 0 {
		return nil, fmt.Errorf("%w: list limit cannot be negative", checkpoint.ErrInvalidConfig)
	}
	if options.Config != nil && options.Config.ThreadID == "" {
		return nil, fmt.Errorf("%w: list thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return nil, err
	}

	query := strings.Builder{}
	query.WriteString(`SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata FROM checkpoints`)
	predicates := make([]string, 0, 5)
	args := make([]any, 0, 5)
	if options.Config != nil {
		predicates = append(predicates, "thread_id=?")
		args = append(args, options.Config.ThreadID)
		if !options.AllNamespaces {
			predicates = append(predicates, "checkpoint_ns=?")
			args = append(args, options.Config.Namespace)
		}
		if options.Config.CheckpointID != "" {
			predicates = append(predicates, "checkpoint_id=?")
			args = append(args, options.Config.CheckpointID)
		}
	}
	if options.Before != nil && options.Before.CheckpointID != "" {
		predicates = append(predicates, "checkpoint_id<?")
		args = append(args, options.Before.CheckpointID)
	}
	if len(predicates) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(predicates, " AND "))
	}
	query.WriteString(" ORDER BY checkpoint_id DESC, thread_id ASC, checkpoint_ns ASC")
	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list SQLite checkpoints: %w", err)
	}
	records := make([]storedRecord, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		if metadataMatches(record.metadata, options.Filter) {
			records = append(records, record)
			if options.Limit > 0 && len(records) == options.Limit {
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate SQLite checkpoints: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite checkpoint rows: %w", err)
	}
	result := make([]checkpoint.Tuple, 0, len(records))
	for _, record := range records {
		tuple, err := s.hydrateLocked(ctx, record)
		if err != nil {
			return nil, err
		}
		result = append(result, tuple)
	}
	return result, nil
}

// DeleteThread implements checkpoint.Saver and atomically removes the
// thread's checkpoint rows, channel blobs, and pending writes.
func (s *Saver) DeleteThread(ctx context.Context, threadID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if threadID == "" {
		return fmt.Errorf("%w: thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite thread deletion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, table := range []string{"checkpoint_writes", "checkpoints", "checkpoint_blobs"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE thread_id=?", threadID); err != nil {
			return fmt.Errorf("delete SQLite %s for thread %q: %w", table, threadID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite thread deletion: %w", err)
	}
	return nil
}

// PruneThread implements retention.ThreadPruner. It keeps the newest keepLatest
// checkpoint IDs for the thread (all namespaces) and deletes older rows.
func (s *Saver) PruneThread(ctx context.Context, threadID string, keepLatest int) (int, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if threadID == "" {
		return 0, fmt.Errorf("%w: thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	if keepLatest < 0 {
		return 0, fmt.Errorf("%w: keepLatest cannot be negative", checkpoint.ErrInvalidConfig)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.setupLocked(ctx); err != nil {
		return 0, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT checkpoint_id FROM checkpoints WHERE thread_id=? ORDER BY checkpoint_id DESC`,
		threadID,
	)
	if err != nil {
		return 0, fmt.Errorf("list SQLite checkpoint IDs for prune: %w", err)
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan SQLite checkpoint ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if keepLatest > 0 && len(ids) <= keepLatest {
		return 0, nil
	}
	drop := ids
	if keepLatest > 0 {
		drop = ids[keepLatest:]
	}
	if len(drop) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin SQLite prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	removed := 0
	for _, id := range drop {
		// Writes and checkpoint rows are keyed by checkpoint_id; blobs are
		// versioned separately and cleaned opportunistically with the row set.
		res, err := tx.ExecContext(ctx, `DELETE FROM checkpoint_writes WHERE thread_id=? AND checkpoint_id=?`, threadID, id)
		if err != nil {
			return 0, fmt.Errorf("prune SQLite writes %q: %w", id, err)
		}
		_, _ = res.RowsAffected()
		res, err = tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id=? AND checkpoint_id=?`, threadID, id)
		if err != nil {
			return 0, fmt.Errorf("prune SQLite checkpoint %q: %w", id, err)
		}
		n, _ := res.RowsAffected()
		removed += int(n)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit SQLite prune: %w", err)
	}
	return removed, nil
}

type storedRecord struct {
	config       checkpoint.Config
	parentID     string
	value        checkpoint.Checkpoint
	metadata     checkpoint.Metadata
	metadataData []byte
}

type rowScanner interface {
	Scan(...any) error
}

func scanRecord(row rowScanner) (storedRecord, error) {
	var record storedRecord
	var parent sql.NullString
	var checkpointData []byte
	if err := row.Scan(&record.config.ThreadID, &record.config.Namespace, &record.config.CheckpointID, &parent, &checkpointData, &record.metadataData); err != nil {
		return storedRecord{}, fmt.Errorf("scan SQLite checkpoint: %w", err)
	}
	if err := json.Unmarshal(checkpointData, &record.value); err != nil {
		return storedRecord{}, fmt.Errorf("decode SQLite checkpoint %q: %w", record.config.CheckpointID, err)
	}
	metadata, err := decodeMetadata(record.metadataData)
	if err != nil {
		return storedRecord{}, fmt.Errorf("decode SQLite checkpoint metadata %q: %w", record.config.CheckpointID, err)
	}
	record.metadata = metadata
	if parent.Valid {
		record.parentID = parent.String
	}
	return record, nil
}

func (s *Saver) getRecordLocked(ctx context.Context, config checkpoint.Config) (storedRecord, bool, error) {
	query := `SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata
		FROM checkpoints WHERE thread_id=? AND checkpoint_ns=?`
	args := []any{config.ThreadID, config.Namespace}
	if config.CheckpointID != "" {
		query += " AND checkpoint_id=?"
		args = append(args, config.CheckpointID)
	} else {
		query += " ORDER BY checkpoint_id DESC LIMIT 1"
	}
	record, err := scanRecord(s.db.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return storedRecord{}, false, nil
	}
	if err != nil {
		return storedRecord{}, false, err
	}
	return record, true, nil
}

func (s *Saver) hydrateLocked(ctx context.Context, record storedRecord) (checkpoint.Tuple, error) {
	value := checkpoint.CloneCheckpoint(record.value)
	value.Values = make(map[string]checkpoint.EncodedValue)
	channels := sortedKeys(value.ChannelVersions)
	for _, channel := range channels {
		var present int
		var typeName sql.NullString
		var version sql.NullInt64
		var data []byte
		err := s.db.QueryRowContext(ctx, `SELECT present, type, value_version, value FROM checkpoint_blobs
			WHERE thread_id=? AND checkpoint_ns=? AND channel=? AND version=?`,
			record.config.ThreadID, record.config.Namespace, channel, value.ChannelVersions[channel],
		).Scan(&present, &typeName, &version, &data)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return checkpoint.Tuple{}, fmt.Errorf("load SQLite channel blob %q: %w", channel, err)
		}
		if present != 0 {
			value.Values[channel] = checkpoint.EncodedValue{Type: typeName.String, Version: int(version.Int64), Data: append([]byte(nil), data...)}
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT task_id, task_path, idx, channel, type, value_version, value
		FROM checkpoint_writes WHERE thread_id=? AND checkpoint_ns=? AND checkpoint_id=?
		ORDER BY task_id ASC, idx ASC`, record.config.ThreadID, record.config.Namespace, record.config.CheckpointID)
	if err != nil {
		return checkpoint.Tuple{}, fmt.Errorf("load SQLite pending writes: %w", err)
	}
	pending := make([]checkpoint.PendingWrite, 0)
	for rows.Next() {
		var write checkpoint.PendingWrite
		if err := rows.Scan(&write.TaskID, &write.TaskPath, &write.Index, &write.Channel, &write.Value.Type, &write.Value.Version, &write.Value.Data); err != nil {
			_ = rows.Close()
			return checkpoint.Tuple{}, fmt.Errorf("scan SQLite pending write: %w", err)
		}
		write.Value.Data = append([]byte(nil), write.Value.Data...)
		pending = append(pending, write)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return checkpoint.Tuple{}, fmt.Errorf("iterate SQLite pending writes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return checkpoint.Tuple{}, fmt.Errorf("close SQLite pending writes: %w", err)
	}
	metadata, err := checkpoint.CloneMetadata(record.metadata)
	if err != nil {
		return checkpoint.Tuple{}, err
	}
	var parent *checkpoint.Config
	if record.parentID != "" {
		parent = &checkpoint.Config{ThreadID: record.config.ThreadID, Namespace: record.config.Namespace, CheckpointID: record.parentID}
	}
	return checkpoint.Tuple{
		Config: record.config, Checkpoint: value, Metadata: metadata,
		ParentConfig: parent, PendingWrites: pending,
	}, nil
}

func decodeMetadata(data []byte) (checkpoint.Metadata, error) {
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	return checkpoint.Metadata(normalizeNumbers(raw).(map[string]any)), nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("unexpected trailing JSON value")
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

func metadataMatches(metadata, filter checkpoint.Metadata) bool {
	for key, expected := range filter {
		actual, exists := metadata[key]
		if !exists || !jsonEquivalent(actual, expected) {
			return false
		}
	}
	return true
}

func jsonEquivalent(left, right any) bool {
	leftData, leftErr := json.Marshal(left)
	rightData, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return reflect.DeepEqual(left, right)
	}
	return bytes.Equal(leftData, rightData)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", checkpoint.ErrInvalidConfig)
	}
	return ctx.Err()
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

var _ checkpoint.Saver = (*Saver)(nil)
