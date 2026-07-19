package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

const (
	PythonSelectCheckpointSQL = `SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata FROM checkpoints WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3`
	PythonSelectBlobsSQL      = `SELECT channel, version, type, blob FROM checkpoint_blobs WHERE thread_id = $1 AND checkpoint_ns = $2 ORDER BY channel, version`
	PythonSelectWritesSQL     = `SELECT task_id, task_path, idx, channel, type, blob FROM checkpoint_writes WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3 ORDER BY task_id, idx`
	PythonSelectSendsSQL      = `SELECT type, blob FROM checkpoint_writes WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3 AND channel = $4 ORDER BY task_path, task_id, idx`
	PythonInsertBlobSQL       = `INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING`
	PythonUpsertCheckpointSQL = `INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO UPDATE SET checkpoint = EXCLUDED.checkpoint, metadata = EXCLUDED.metadata`
	PythonUpsertWriteSQL      = `INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO UPDATE SET channel = EXCLUDED.channel, type = EXCLUDED.type, blob = EXCLUDED.blob`
	PythonInsertWriteSQL      = `INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING`
)

var pythonPostgresMigrations = []string{
	`CREATE TABLE IF NOT EXISTS checkpoint_migrations (v INTEGER PRIMARY KEY)`,
	`CREATE TABLE IF NOT EXISTS checkpoints (
		thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '', checkpoint_id TEXT NOT NULL,
		parent_checkpoint_id TEXT, type TEXT, checkpoint JSONB NOT NULL, metadata JSONB NOT NULL DEFAULT '{}',
		PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id))`,
	`CREATE TABLE IF NOT EXISTS checkpoint_blobs (
		thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '', channel TEXT NOT NULL,
		version TEXT NOT NULL, type TEXT NOT NULL, blob BYTEA,
		PRIMARY KEY (thread_id, checkpoint_ns, channel, version))`,
	`CREATE TABLE IF NOT EXISTS checkpoint_writes (
		thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '', checkpoint_id TEXT NOT NULL,
		task_id TEXT NOT NULL, idx INTEGER NOT NULL, channel TEXT NOT NULL, type TEXT, blob BYTEA NOT NULL,
		PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx))`,
	`ALTER TABLE checkpoint_blobs ALTER COLUMN blob DROP NOT NULL`,
	`SELECT 1`,
	`CREATE INDEX IF NOT EXISTS checkpoints_thread_id_idx ON checkpoints(thread_id)`,
	`CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread_id_idx ON checkpoint_blobs(thread_id)`,
	`CREATE INDEX IF NOT EXISTS checkpoint_writes_thread_id_idx ON checkpoint_writes(thread_id)`,
	`ALTER TABLE checkpoint_writes ADD COLUMN IF NOT EXISTS task_path TEXT NOT NULL DEFAULT ''`,
}

type PythonTypedValue = checkpoint.PythonTypedValue
type PythonPendingWrite = checkpoint.PythonPendingWrite
type PythonTuple = checkpoint.PythonTuple

// PythonAdapter reads and writes the physical schema used by
// langgraph-checkpoint-postgres 1.2.9. It remains separate from the native Go
// Saver because the two checkpoint envelopes persist different scheduler data.
type PythonAdapter struct {
	db              *sql.DB
	valueSerializer checkpoint.PythonValueSerializer
}

// NewPythonAdapter constructs an adapter over a caller-owned database.
func NewPythonAdapter(db *sql.DB, valueSerializer checkpoint.PythonValueSerializer) (*PythonAdapter, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: Python PostgreSQL database is nil", checkpoint.ErrInvalidConfig)
	}
	defaults := checkpoint.DefaultPythonSerializer{}
	if valueSerializer == nil {
		valueSerializer = defaults
	}
	return &PythonAdapter{db: db, valueSerializer: valueSerializer}, nil
}

// Setup applies the ordered migration ledger from
// langgraph-checkpoint-postgres 1.2.9. Regular CREATE INDEX is used instead of
// CONCURRENTLY so the complete adapter setup remains atomic.
func (a *PythonAdapter) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Python PostgreSQL schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('langgraph-python-checkpoint-schema'))`); err != nil {
		return fmt.Errorf("lock Python PostgreSQL schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, pythonPostgresMigrations[0]); err != nil {
		return fmt.Errorf("create Python PostgreSQL migration ledger: %w", err)
	}
	var recorded sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT max(v) FROM checkpoint_migrations`).Scan(&recorded); err != nil {
		return fmt.Errorf("read Python PostgreSQL schema version: %w", err)
	}
	version := -1
	if recorded.Valid {
		version = int(recorded.Int64)
	}
	if version >= len(pythonPostgresMigrations) {
		return fmt.Errorf("unsupported Python PostgreSQL checkpoint schema version %d", version)
	}
	for next := version + 1; next < len(pythonPostgresMigrations); next++ {
		if _, err := tx.ExecContext(ctx, pythonPostgresMigrations[next]); err != nil {
			return fmt.Errorf("apply Python PostgreSQL schema migration %d: %w", next, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_migrations(v) VALUES ($1) ON CONFLICT DO NOTHING`, next); err != nil {
			return fmt.Errorf("record Python PostgreSQL schema migration %d: %w", next, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Python PostgreSQL schema migrations: %w", err)
	}
	return nil
}

// GetTuple returns an exact or latest upstream Python checkpoint, hydrating
// version-matched channel blobs and preserving pending writes as typed bytes.
func (a *PythonAdapter) GetTuple(ctx context.Context, config checkpoint.Config) (PythonTuple, bool, error) {
	if err := contextError(ctx); err != nil {
		return PythonTuple{}, false, err
	}
	if err := config.Validate(); err != nil {
		return PythonTuple{}, false, err
	}
	query := PythonSelectCheckpointSQL
	args := []any{config.ThreadID, config.Namespace, config.CheckpointID}
	if config.CheckpointID == "" {
		query = `SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata FROM checkpoints WHERE thread_id = $1 AND checkpoint_ns = $2 ORDER BY checkpoint_id DESC LIMIT 1`
		args = args[:2]
	}
	var resolved checkpoint.Config
	var parentID sql.NullString
	var checkpointData, metadataData []byte
	if err := a.db.QueryRowContext(ctx, query, args...).Scan(&resolved.ThreadID, &resolved.Namespace, &resolved.CheckpointID, &parentID, &checkpointData, &metadataData); err != nil {
		if err == sql.ErrNoRows {
			return PythonTuple{}, false, nil
		}
		return PythonTuple{}, false, fmt.Errorf("read Python PostgreSQL checkpoint: %w", err)
	}
	value, err := decodePythonPostgresCheckpoint(checkpointData)
	if err != nil {
		return PythonTuple{}, false, err
	}
	rows, err := a.db.QueryContext(ctx, PythonSelectBlobsSQL, resolved.ThreadID, resolved.Namespace)
	if err != nil {
		return PythonTuple{}, false, fmt.Errorf("read Python PostgreSQL channel blobs: %w", err)
	}
	for rows.Next() {
		var channel, version, typeName string
		var blob []byte
		if err := rows.Scan(&channel, &version, &typeName, &blob); err != nil {
			_ = rows.Close()
			return PythonTuple{}, false, fmt.Errorf("scan Python PostgreSQL channel blob: %w", err)
		}
		expected, exists := value.ChannelVersions[channel]
		if !exists || expected.String() != version || typeName == "empty" {
			continue
		}
		decoded, err := a.valueSerializer.DecodeValue(typeName, blob)
		if err != nil {
			_ = rows.Close()
			return PythonTuple{}, false, fmt.Errorf("decode Python PostgreSQL channel %q: %w", channel, err)
		}
		value.ChannelValues[channel] = decoded
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return PythonTuple{}, false, fmt.Errorf("iterate Python PostgreSQL channel blobs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return PythonTuple{}, false, fmt.Errorf("close Python PostgreSQL channel blobs: %w", err)
	}
	metadata, err := decodePythonMetadata(metadataData)
	if err != nil {
		return PythonTuple{}, false, err
	}
	tuple := PythonTuple{Config: resolved, Checkpoint: value, Metadata: metadata}
	if parentID.Valid && parentID.String != "" {
		parent := checkpoint.Config{ThreadID: resolved.ThreadID, Namespace: resolved.Namespace, CheckpointID: parentID.String}
		tuple.ParentConfig = &parent
		if err := a.hydratePendingSends(ctx, &tuple.Checkpoint, parent); err != nil {
			return PythonTuple{}, false, err
		}
	}
	writes, err := a.db.QueryContext(ctx, PythonSelectWritesSQL, resolved.ThreadID, resolved.Namespace, resolved.CheckpointID)
	if err != nil {
		return PythonTuple{}, false, fmt.Errorf("read Python PostgreSQL pending writes: %w", err)
	}
	defer writes.Close()
	for writes.Next() {
		var write PythonPendingWrite
		if err := writes.Scan(&write.TaskID, &write.TaskPath, &write.Index, &write.Channel, &write.Value.Type, &write.Value.Data); err != nil {
			return PythonTuple{}, false, fmt.Errorf("scan Python PostgreSQL pending write: %w", err)
		}
		write.Value.Data = append([]byte(nil), write.Value.Data...)
		tuple.PendingWrites = append(tuple.PendingWrites, write)
	}
	if err := writes.Err(); err != nil {
		return PythonTuple{}, false, fmt.Errorf("iterate Python PostgreSQL pending writes: %w", err)
	}
	return tuple, true, nil
}

func decodePythonPostgresCheckpoint(data []byte) (checkpoint.PythonCheckpoint, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return checkpoint.PythonCheckpoint{}, fmt.Errorf("%w: decode Python PostgreSQL checkpoint JSONB: %v", checkpoint.ErrInvalidCheckpoint, err)
	}
	if value, exists := object["channel_values"]; !exists || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		object["channel_values"] = json.RawMessage(`{}`)
	}
	normalized, err := json.Marshal(object)
	if err != nil {
		return checkpoint.PythonCheckpoint{}, fmt.Errorf("%w: normalize Python PostgreSQL checkpoint JSONB: %v", checkpoint.ErrInvalidCheckpoint, err)
	}
	return checkpoint.DecodePythonCheckpoint(normalized)
}

func (a *PythonAdapter) hydratePendingSends(ctx context.Context, value *checkpoint.PythonCheckpoint, parent checkpoint.Config) error {
	rows, err := a.db.QueryContext(ctx, PythonSelectSendsSQL, parent.ThreadID, parent.Namespace, parent.CheckpointID, checkpoint.PythonTasksChannel)
	if err != nil {
		return fmt.Errorf("read Python PostgreSQL pending sends: %w", err)
	}
	defer rows.Close()
	var sends []json.RawMessage
	for rows.Next() {
		var typeName string
		var blob []byte
		if err := rows.Scan(&typeName, &blob); err != nil {
			return fmt.Errorf("scan Python PostgreSQL pending send: %w", err)
		}
		decoded, err := a.valueSerializer.DecodeValue(typeName, blob)
		if err != nil {
			return fmt.Errorf("decode Python PostgreSQL pending send: %w", err)
		}
		sends = append(sends, decoded)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate Python PostgreSQL pending sends: %w", err)
	}
	if len(sends) == 0 {
		return nil
	}
	encoded, err := json.Marshal(sends)
	if err != nil {
		return fmt.Errorf("encode Python PostgreSQL pending sends: %w", err)
	}
	value.ChannelValues[checkpoint.PythonTasksChannel] = encoded
	var maximum checkpoint.PythonChannelVersion
	for _, version := range value.ChannelVersions {
		if maximum.Kind() == "" || version.String() > maximum.String() {
			maximum = version
		}
	}
	if maximum.Kind() == "" {
		maximum = checkpoint.NewPythonIntegerVersion(1)
	}
	value.ChannelVersions[checkpoint.PythonTasksChannel] = maximum
	return nil
}

// Put stores a portable checkpoint using upstream primitive-inlining and
// versioned blob/tombstone rules. newVersions identifies channels written in
// this checkpoint, matching BaseCheckpointSaver.put.
func (a *PythonAdapter) Put(ctx context.Context, parent checkpoint.Config, value checkpoint.PythonCheckpoint, metadata checkpoint.Metadata, newVersions map[string]checkpoint.PythonChannelVersion) (checkpoint.Config, error) {
	if err := contextError(ctx); err != nil {
		return checkpoint.Config{}, err
	}
	if err := parent.Validate(); err != nil {
		return checkpoint.Config{}, err
	}
	if err := value.Validate(); err != nil {
		return checkpoint.Config{}, err
	}
	encoded, err := checkpoint.EncodePythonCheckpoint(value)
	if err != nil {
		return checkpoint.Config{}, err
	}
	stored, err := checkpoint.DecodePythonCheckpoint(encoded)
	if err != nil {
		return checkpoint.Config{}, err
	}
	for channel, raw := range stored.ChannelValues {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) != 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
			if _, exists := stored.ChannelVersions[channel]; !exists {
				return checkpoint.Config{}, fmt.Errorf("%w: Python PostgreSQL blob channel %q has no version", checkpoint.ErrInvalidCheckpoint, channel)
			}
			delete(stored.ChannelValues, channel)
		}
	}
	checkpointData, err := checkpoint.EncodePythonCheckpoint(stored)
	if err != nil {
		return checkpoint.Config{}, err
	}
	clonedMetadata, err := checkpoint.CloneMetadata(metadata)
	if err != nil {
		return checkpoint.Config{}, err
	}
	metadataData, err := json.Marshal(clonedMetadata)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("encode Python PostgreSQL metadata: %w", err)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("begin Python PostgreSQL checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	channels := make([]string, 0, len(newVersions))
	for channel := range newVersions {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	for _, channel := range channels {
		version := newVersions[channel]
		declared, exists := value.ChannelVersions[channel]
		if !exists || declared.String() != version.String() {
			return checkpoint.Config{}, fmt.Errorf("%w: Python PostgreSQL new version for %q does not match checkpoint", checkpoint.ErrInvalidCheckpoint, channel)
		}
		raw, present := value.ChannelValues[channel]
		if present {
			trimmed := bytes.TrimSpace(raw)
			if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
				continue
			}
			typeName, blob, err := a.valueSerializer.EncodeValue(raw)
			if err != nil {
				return checkpoint.Config{}, fmt.Errorf("encode Python PostgreSQL channel %q: %w", channel, err)
			}
			if typeName == "" {
				return checkpoint.Config{}, fmt.Errorf("%w: Python value serializer returned an empty type", checkpoint.ErrCodecMismatch)
			}
			if _, err := tx.ExecContext(ctx, PythonInsertBlobSQL, parent.ThreadID, parent.Namespace, channel, version.String(), typeName, blob); err != nil {
				return checkpoint.Config{}, fmt.Errorf("write Python PostgreSQL channel blob: %w", err)
			}
		} else if _, err := tx.ExecContext(ctx, PythonInsertBlobSQL, parent.ThreadID, parent.Namespace, channel, version.String(), "empty", nil); err != nil {
			return checkpoint.Config{}, fmt.Errorf("write Python PostgreSQL channel tombstone: %w", err)
		}
	}
	var parentID any
	if parent.CheckpointID != "" {
		parentID = parent.CheckpointID
	}
	if _, err := tx.ExecContext(ctx, PythonUpsertCheckpointSQL, parent.ThreadID, parent.Namespace, value.ID, parentID, checkpointData, metadataData); err != nil {
		return checkpoint.Config{}, fmt.Errorf("write Python PostgreSQL checkpoint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return checkpoint.Config{}, fmt.Errorf("commit Python PostgreSQL checkpoint: %w", err)
	}
	return checkpoint.Config{ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID}, nil
}

// PutWrites atomically stores opaque typed pending writes with upstream
// replace semantics for negative special indexes and first-write-wins for
// ordinary indexes.
func (a *PythonAdapter) PutWrites(ctx context.Context, config checkpoint.Config, writes []PythonPendingWrite) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if config.CheckpointID == "" {
		return fmt.Errorf("%w: Python PostgreSQL writes require a checkpoint ID", checkpoint.ErrInvalidConfig)
	}
	ordered := append([]PythonPendingWrite(nil), writes...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].TaskID != ordered[j].TaskID {
			return ordered[i].TaskID < ordered[j].TaskID
		}
		return ordered[i].Index < ordered[j].Index
	})
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Python PostgreSQL writes transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, write := range ordered {
		if write.TaskID == "" || write.Channel == "" || write.Value.Type == "" {
			return fmt.Errorf("%w: incomplete Python PostgreSQL pending write", checkpoint.ErrInvalidCheckpoint)
		}
		query := PythonInsertWriteSQL
		if write.Index < 0 {
			query = PythonUpsertWriteSQL
		}
		if _, err := tx.ExecContext(ctx, query, config.ThreadID, config.Namespace, config.CheckpointID, write.TaskID, write.TaskPath, write.Index, write.Channel, write.Value.Type, write.Value.Data); err != nil {
			return fmt.Errorf("write Python PostgreSQL pending value: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Python PostgreSQL writes: %w", err)
	}
	return nil
}

// DeleteThread removes upstream Python checkpoints, blobs, and writes in one
// transaction.
func (a *PythonAdapter) DeleteThread(ctx context.Context, threadID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if threadID == "" {
		return fmt.Errorf("%w: Python PostgreSQL thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Python PostgreSQL delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`DELETE FROM checkpoint_writes WHERE thread_id = $1`,
		`DELETE FROM checkpoints WHERE thread_id = $1`,
		`DELETE FROM checkpoint_blobs WHERE thread_id = $1`,
	} {
		if _, err := tx.ExecContext(ctx, statement, threadID); err != nil {
			return fmt.Errorf("delete Python PostgreSQL thread: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Python PostgreSQL thread delete: %w", err)
	}
	return nil
}

// List returns upstream Python tuples newest-first with metadata subset filter,
// namespace selection, before, and limit semantics.
func (a *PythonAdapter) List(ctx context.Context, options checkpoint.ListOptions) ([]PythonTuple, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit < 0 {
		return nil, fmt.Errorf("%w: list limit cannot be negative", checkpoint.ErrInvalidConfig)
	}
	if options.Config != nil && options.Config.ThreadID == "" {
		return nil, fmt.Errorf("%w: list thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	predicates := make([]string, 0, 5)
	args := make([]any, 0, 5)
	add := func(expression string, value any) {
		args = append(args, value)
		predicates = append(predicates, fmt.Sprintf(expression, len(args)))
	}
	if options.Config != nil {
		add("thread_id = $%d", options.Config.ThreadID)
		if !options.AllNamespaces {
			add("checkpoint_ns = $%d", options.Config.Namespace)
		}
		if options.Config.CheckpointID != "" {
			add("checkpoint_id = $%d", options.Config.CheckpointID)
		}
	}
	if options.Before != nil && options.Before.CheckpointID != "" {
		add("checkpoint_id < $%d", options.Before.CheckpointID)
	}
	query := `SELECT thread_id, checkpoint_ns, checkpoint_id, metadata FROM checkpoints`
	if len(predicates) != 0 {
		query += " WHERE " + strings.Join(predicates, " AND ")
	}
	query += ` ORDER BY checkpoint_id DESC, thread_id ASC, checkpoint_ns ASC`
	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list Python PostgreSQL checkpoints: %w", err)
	}
	type candidate struct {
		config checkpoint.Config
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		var metadataData []byte
		if err := rows.Scan(&item.config.ThreadID, &item.config.Namespace, &item.config.CheckpointID, &metadataData); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Python PostgreSQL checkpoint list: %w", err)
		}
		metadata, err := decodePythonMetadata(metadataData)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		if metadataMatches(metadata, options.Filter) {
			candidates = append(candidates, item)
			if options.Limit > 0 && len(candidates) == options.Limit {
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate Python PostgreSQL checkpoint list: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Python PostgreSQL checkpoint list: %w", err)
	}
	result := make([]PythonTuple, 0, len(candidates))
	for _, item := range candidates {
		tuple, ok, err := a.GetTuple(ctx, item.config)
		if err != nil {
			return nil, err
		}
		if ok {
			result = append(result, tuple)
		}
	}
	return result, nil
}

func decodePythonMetadata(data []byte) (checkpoint.Metadata, error) {
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return checkpoint.Metadata{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value checkpoint.Metadata
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("%w: decode Python PostgreSQL metadata: %v", checkpoint.ErrInvalidCheckpoint, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing Python PostgreSQL metadata", checkpoint.ErrInvalidCheckpoint)
	}
	return value, nil
}
