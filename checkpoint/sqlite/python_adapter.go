package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/ybszm/langgraph-go/checkpoint"
)

type PythonCheckpointSerializer = checkpoint.PythonCheckpointSerializer
type PythonTypedValue = checkpoint.PythonTypedValue
type PythonPendingWrite = checkpoint.PythonPendingWrite
type PythonTuple = checkpoint.PythonTuple

// PythonAdapter reads and writes the physical schema used by
// langgraph-checkpoint-sqlite 1.2.9. It is intentionally separate from Saver:
// a portable Python checkpoint does not persist the Go runtime's Next/Waiting
// task plan and therefore cannot masquerade as a resumable Go Checkpoint.
type PythonAdapter struct {
	mu         sync.Mutex
	db         *sql.DB
	serializer PythonCheckpointSerializer
}

// NewPythonAdapter constructs an adapter over a caller-owned database. A nil
// serializer selects the JSON-compatible default MessagePack implementation.
func NewPythonAdapter(db *sql.DB, serializer PythonCheckpointSerializer) (*PythonAdapter, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: Python SQLite database is nil", checkpoint.ErrInvalidConfig)
	}
	if serializer == nil {
		serializer = checkpoint.DefaultPythonSerializer{}
	}
	return &PythonAdapter{db: db, serializer: serializer}, nil
}

// Setup creates the exact checkpoints/writes tables used by the upstream
// Python SQLite saver. Do not mix this schema with the native Go Saver schema
// in the same database.
func (a *PythonAdapter) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '',
			checkpoint_id TEXT NOT NULL, parent_checkpoint_id TEXT, type TEXT,
			checkpoint BLOB, metadata BLOB,
			PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
		)`,
		`CREATE TABLE IF NOT EXISTS writes (
			thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '',
			checkpoint_id TEXT NOT NULL, task_id TEXT NOT NULL, idx INTEGER NOT NULL,
			channel TEXT NOT NULL, type TEXT, value BLOB,
			PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
		)`,
	}
	for _, statement := range statements {
		if _, err := a.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set up Python SQLite checkpoint schema: %w", err)
		}
	}
	return nil
}

// GetTuple returns an exact checkpoint or the latest checkpoint in a thread
// and namespace, together with writes ordered by task ID and index.
func (a *PythonAdapter) GetTuple(ctx context.Context, config checkpoint.Config) (PythonTuple, bool, error) {
	if err := contextError(ctx); err != nil {
		return PythonTuple{}, false, err
	}
	if err := config.Validate(); err != nil {
		return PythonTuple{}, false, err
	}
	query := `SELECT thread_id, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata
		FROM checkpoints WHERE thread_id = ? AND checkpoint_ns = ?`
	args := []any{config.ThreadID, config.Namespace}
	if config.CheckpointID != "" {
		query += ` AND checkpoint_id = ?`
		args = append(args, config.CheckpointID)
	} else {
		query += ` ORDER BY checkpoint_id DESC LIMIT 1`
	}
	var threadID, checkpointID string
	var parentID, typeName sql.NullString
	var payload, metadataData []byte
	if err := a.db.QueryRowContext(ctx, query, args...).Scan(&threadID, &checkpointID, &parentID, &typeName, &payload, &metadataData); err != nil {
		if err == sql.ErrNoRows {
			return PythonTuple{}, false, nil
		}
		return PythonTuple{}, false, fmt.Errorf("read Python SQLite checkpoint: %w", err)
	}
	if !typeName.Valid {
		return PythonTuple{}, false, fmt.Errorf("%w: Python checkpoint serializer type is null", checkpoint.ErrCodecMismatch)
	}
	value, err := a.serializer.Decode(typeName.String, payload)
	if err != nil {
		return PythonTuple{}, false, err
	}
	metadata := checkpoint.Metadata{}
	if len(metadataData) != 0 {
		if err := json.Unmarshal(metadataData, &metadata); err != nil {
			return PythonTuple{}, false, fmt.Errorf("%w: decode Python checkpoint metadata: %v", checkpoint.ErrInvalidCheckpoint, err)
		}
	}
	resolved := checkpoint.Config{ThreadID: threadID, Namespace: config.Namespace, CheckpointID: checkpointID}
	tuple := PythonTuple{Config: resolved, Checkpoint: value, Metadata: metadata}
	if parentID.Valid && parentID.String != "" {
		parent := checkpoint.Config{ThreadID: threadID, Namespace: config.Namespace, CheckpointID: parentID.String}
		tuple.ParentConfig = &parent
	}
	rows, err := a.db.QueryContext(ctx, `SELECT task_id, idx, channel, type, value FROM writes
		WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ? ORDER BY task_id, idx`,
		threadID, config.Namespace, checkpointID)
	if err != nil {
		return PythonTuple{}, false, fmt.Errorf("read Python SQLite pending writes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var write PythonPendingWrite
		var writeType sql.NullString
		if err := rows.Scan(&write.TaskID, &write.Index, &write.Channel, &writeType, &write.Value.Data); err != nil {
			return PythonTuple{}, false, fmt.Errorf("scan Python SQLite pending write: %w", err)
		}
		if writeType.Valid {
			write.Value.Type = writeType.String
		}
		write.Value.Data = append([]byte(nil), write.Value.Data...)
		tuple.PendingWrites = append(tuple.PendingWrites, write)
	}
	if err := rows.Err(); err != nil {
		return PythonTuple{}, false, fmt.Errorf("iterate Python SQLite pending writes: %w", err)
	}
	return tuple, true, nil
}

// Put stores a portable checkpoint in the upstream Python table layout.
func (a *PythonAdapter) Put(ctx context.Context, parent checkpoint.Config, value checkpoint.PythonCheckpoint, metadata checkpoint.Metadata) (checkpoint.Config, error) {
	if err := contextError(ctx); err != nil {
		return checkpoint.Config{}, err
	}
	if err := parent.Validate(); err != nil {
		return checkpoint.Config{}, err
	}
	if err := value.Validate(); err != nil {
		return checkpoint.Config{}, err
	}
	typeName, payload, err := a.serializer.Encode(value)
	if err != nil {
		return checkpoint.Config{}, err
	}
	if typeName == "" {
		return checkpoint.Config{}, fmt.Errorf("%w: Python checkpoint serializer returned an empty type", checkpoint.ErrCodecMismatch)
	}
	cloned, err := checkpoint.CloneMetadata(metadata)
	if err != nil {
		return checkpoint.Config{}, err
	}
	metadataData, err := json.Marshal(cloned)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("encode Python checkpoint metadata: %w", err)
	}
	var parentID any
	if parent.CheckpointID != "" {
		parentID = parent.CheckpointID
	}
	if _, err := a.db.ExecContext(ctx, `INSERT OR REPLACE INTO checkpoints
		(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, parent.ThreadID, parent.Namespace, value.ID, parentID, typeName, payload, metadataData); err != nil {
		return checkpoint.Config{}, fmt.Errorf("write Python SQLite checkpoint: %w", err)
	}
	return checkpoint.Config{ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID}, nil
}

// PutWrites stores opaque typed writes atomically. Negative special indexes use
// replace semantics; ordinary task writes remain first-write-wins.
func (a *PythonAdapter) PutWrites(ctx context.Context, config checkpoint.Config, writes []PythonPendingWrite) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if config.CheckpointID == "" {
		return fmt.Errorf("%w: Python pending writes require a checkpoint ID", checkpoint.ErrInvalidConfig)
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
		return fmt.Errorf("begin Python SQLite writes transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, write := range ordered {
		if write.TaskID == "" || write.Channel == "" || write.Value.Type == "" {
			return fmt.Errorf("%w: incomplete Python pending write", checkpoint.ErrInvalidCheckpoint)
		}
		verb := "INSERT OR IGNORE"
		if write.Index < 0 {
			verb = "INSERT OR REPLACE"
		}
		query := verb + ` INTO writes
			(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
		if _, err := tx.ExecContext(ctx, query, config.ThreadID, config.Namespace, config.CheckpointID,
			write.TaskID, write.Index, write.Channel, write.Value.Type, write.Value.Data); err != nil {
			return fmt.Errorf("write Python SQLite pending value: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Python SQLite writes transaction: %w", err)
	}
	return nil
}

// List returns Python tuples newest-first with the same thread/namespace,
// before, metadata subset-filter, and limit semantics as checkpoint.ListOptions.
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
	query := `SELECT thread_id, checkpoint_ns, checkpoint_id, metadata FROM checkpoints`
	predicates := make([]string, 0, 5)
	args := make([]any, 0, 5)
	if options.Config != nil {
		predicates = append(predicates, "thread_id = ?")
		args = append(args, options.Config.ThreadID)
		if !options.AllNamespaces {
			predicates = append(predicates, "checkpoint_ns = ?")
			args = append(args, options.Config.Namespace)
		}
		if options.Config.CheckpointID != "" {
			predicates = append(predicates, "checkpoint_id = ?")
			args = append(args, options.Config.CheckpointID)
		}
	}
	if options.Before != nil && options.Before.CheckpointID != "" {
		predicates = append(predicates, "checkpoint_id < ?")
		args = append(args, options.Before.CheckpointID)
	}
	if len(predicates) != 0 {
		query += " WHERE " + strings.Join(predicates, " AND ")
	}
	query += " ORDER BY checkpoint_id DESC, thread_id ASC, checkpoint_ns ASC"
	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list Python SQLite checkpoints: %w", err)
	}
	type candidate struct {
		config   checkpoint.Config
		metadata checkpoint.Metadata
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		var metadataData []byte
		if err := rows.Scan(&item.config.ThreadID, &item.config.Namespace, &item.config.CheckpointID, &metadataData); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan Python SQLite checkpoint list: %w", err)
		}
		item.metadata, err = decodeMetadata(metadataData)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("decode Python SQLite list metadata: %w", err)
		}
		if metadataMatches(item.metadata, options.Filter) {
			candidates = append(candidates, item)
			if options.Limit > 0 && len(candidates) == options.Limit {
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate Python SQLite checkpoint list: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close Python SQLite checkpoint list: %w", err)
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

// DeleteThread removes upstream Python checkpoints and writes atomically.
func (a *PythonAdapter) DeleteThread(ctx context.Context, threadID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if threadID == "" {
		return fmt.Errorf("%w: Python SQLite thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Python SQLite delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM writes WHERE thread_id = ?`, threadID); err != nil {
		return fmt.Errorf("delete Python SQLite writes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id = ?`, threadID); err != nil {
		return fmt.Errorf("delete Python SQLite checkpoints: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Python SQLite thread delete: %w", err)
	}
	return nil
}
