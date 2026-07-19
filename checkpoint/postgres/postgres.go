// Package postgres implements production checkpoint storage backed by
// PostgreSQL.
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

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wahanbo/langgraph-go/checkpoint"
)

const schemaVersion = 2

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS checkpoint_migrations (
		version INTEGER PRIMARY KEY
	)`,
	`CREATE TABLE IF NOT EXISTS checkpoints (
		thread_id TEXT NOT NULL,
		checkpoint_ns TEXT NOT NULL DEFAULT '',
		checkpoint_id TEXT NOT NULL,
		parent_checkpoint_id TEXT,
		checkpoint JSONB NOT NULL,
		metadata JSONB NOT NULL DEFAULT 'null'::jsonb,
		PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	)`,
	`CREATE TABLE IF NOT EXISTS checkpoint_blobs (
		thread_id TEXT NOT NULL,
		checkpoint_ns TEXT NOT NULL DEFAULT '',
		channel TEXT NOT NULL,
		version TEXT NOT NULL,
		present BOOLEAN NOT NULL,
		type TEXT,
		value_version INTEGER,
		value BYTEA,
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
		value BYTEA,
		PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
	)`,
	`CREATE INDEX IF NOT EXISTS checkpoints_history
		ON checkpoints (thread_id, checkpoint_ns, checkpoint_id DESC)`,
	`CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread
		ON checkpoint_blobs (thread_id)`,
	`CREATE INDEX IF NOT EXISTS checkpoint_writes_thread
		ON checkpoint_writes (thread_id)`,
}

var schemaV2Statements = []string{
	`ALTER TABLE checkpoint_writes ADD COLUMN IF NOT EXISTS task_path TEXT NOT NULL DEFAULT ''`,
	`CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread ON checkpoint_blobs (thread_id)`,
	`CREATE INDEX IF NOT EXISTS checkpoint_writes_thread ON checkpoint_writes (thread_id)`,
}

type postgresSchemaMigration struct {
	version    int
	statements []string
}

var postgresSchemaMigrations = []postgresSchemaMigration{
	{version: 1, statements: schemaStatements[1:]},
	{version: 2, statements: schemaV2Statements},
}

// Saver stores checkpoints in PostgreSQL. It is safe for concurrent use and
// relies on database transactions rather than a process-wide operation lock.
type Saver struct {
	mu      sync.Mutex
	db      *sql.DB
	owned   bool
	setup   bool
	closing bool
}

// New constructs a Saver over an existing PostgreSQL database handle. New
// does not take ownership of db.
func New(db *sql.DB) (*Saver, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: PostgreSQL database is nil", checkpoint.ErrInvalidConfig)
	}
	return &Saver{db: db}, nil
}

// Open opens dsn with pgx's database/sql driver and initializes the schema.
// The returned Saver owns the database handle.
func Open(ctx context.Context, dsn string) (*Saver, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL DSN is empty", checkpoint.ErrInvalidConfig)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL checkpoint database: %w", err)
	}
	saver := &Saver{db: db, owned: true}
	if err := saver.Setup(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return saver, nil
}

// Setup creates the checkpoint schema. An advisory transaction lock makes
// concurrent setup calls from different Saver instances safe.
func (s *Saver) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: PostgreSQL saver is nil", checkpoint.ErrInvalidConfig)
	}
	if s.closing {
		return fmt.Errorf("PostgreSQL checkpoint saver is closed")
	}
	if s.setup {
		return nil
	}
	var fastVersion sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT max(version) FROM checkpoint_migrations`).Scan(&fastVersion); err == nil && fastVersion.Valid {
		if int(fastVersion.Int64) > schemaVersion {
			return fmt.Errorf("unsupported PostgreSQL checkpoint schema version %d", fastVersion.Int64)
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
		return fmt.Errorf("begin PostgreSQL checkpoint schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('langgraph-go-checkpoint-schema'))`); err != nil {
		return fmt.Errorf("lock PostgreSQL checkpoint schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, schemaStatements[0]); err != nil {
		return fmt.Errorf("set up PostgreSQL checkpoint migrations table: %w", err)
	}
	var lockedVersion sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT max(version) FROM checkpoint_migrations`).Scan(&lockedVersion); err != nil {
		return fmt.Errorf("read PostgreSQL checkpoint schema version: %w", err)
	}
	if lockedVersion.Valid && int(lockedVersion.Int64) > schemaVersion {
		return fmt.Errorf("unsupported PostgreSQL checkpoint schema version %d", lockedVersion.Int64)
	}
	if lockedVersion.Valid && int(lockedVersion.Int64) == schemaVersion {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit PostgreSQL checkpoint schema check: %w", err)
		}
		s.setup = true
		return nil
	}
	version := 0
	if lockedVersion.Valid {
		version = int(lockedVersion.Int64)
	}
	for _, migration := range postgresSchemaMigrations {
		if migration.version <= version {
			continue
		}
		for _, statement := range migration.statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate PostgreSQL checkpoint schema to v%d: %w", migration.version, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_migrations(version) VALUES ($1) ON CONFLICT DO NOTHING`, migration.version); err != nil {
			return fmt.Errorf("record PostgreSQL checkpoint schema version %d: %w", migration.version, err)
		}
		version = migration.version
	}
	var finalVersion int
	if err := tx.QueryRowContext(ctx, `SELECT max(version) FROM checkpoint_migrations`).Scan(&finalVersion); err != nil {
		return fmt.Errorf("read PostgreSQL checkpoint schema version: %w", err)
	}
	if finalVersion != schemaVersion {
		return fmt.Errorf("unsupported PostgreSQL checkpoint schema version %d", finalVersion)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL checkpoint schema: %w", err)
	}
	s.setup = true
	return nil
}

// Close closes the handle opened by Open. It does not close a handle supplied
// to New, but the Saver itself must not be reused afterward.
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

func (s *Saver) ensureSetup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	ready, closing := s.setup, s.closing
	s.mu.Unlock()
	if closing {
		return fmt.Errorf("PostgreSQL checkpoint saver is closed")
	}
	if ready {
		return nil
	}
	return s.Setup(ctx)
}

// Put implements checkpoint.Saver and atomically commits channel blobs with
// the checkpoint row.
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
	if err := s.ensureSetup(ctx); err != nil {
		return checkpoint.Config{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("begin PostgreSQL checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, channel := range sortedKeys(newVersions) {
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
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT(thread_id, checkpoint_ns, channel, version) DO UPDATE SET
				present=EXCLUDED.present, type=EXCLUDED.type,
				value_version=EXCLUDED.value_version, value=EXCLUDED.value`,
			parent.ThreadID, parent.Namespace, channel, version, present, typeName, valueVersion, data,
		); err != nil {
			return checkpoint.Config{}, fmt.Errorf("store PostgreSQL channel blob %q: %w", channel, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO checkpoints
			(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb)
		ON CONFLICT(thread_id, checkpoint_ns, checkpoint_id) DO UPDATE SET
			parent_checkpoint_id=EXCLUDED.parent_checkpoint_id,
			checkpoint=EXCLUDED.checkpoint, metadata=EXCLUDED.metadata`,
		parent.ThreadID, parent.Namespace, value.ID, nullableString(parent.CheckpointID), checkpointData, metadataData,
	); err != nil {
		return checkpoint.Config{}, fmt.Errorf("store PostgreSQL checkpoint %q: %w", value.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return checkpoint.Config{}, fmt.Errorf("commit PostgreSQL checkpoint %q: %w", value.ID, err)
	}
	return checkpoint.Config{ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID}, nil
}

// PutWrites implements checkpoint.Saver. Non-negative indexes are
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
	if err := s.ensureSetup(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL pending-write transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM checkpoints WHERE thread_id=$1 AND checkpoint_ns=$2 AND checkpoint_id=$3`, config.ThreadID, config.Namespace, config.CheckpointID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: pending-write checkpoint %q does not exist", checkpoint.ErrInvalidConfig, config.CheckpointID)
		}
		return fmt.Errorf("find PostgreSQL pending-write checkpoint: %w", err)
	}
	for _, write := range writes {
		query := `INSERT INTO checkpoint_writes
			(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, task_path, channel, type, value_version, value)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT(thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING`
		if write.Index < 0 {
			query = `INSERT INTO checkpoint_writes
				(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, task_path, channel, type, value_version, value)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
				ON CONFLICT(thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO UPDATE SET
					task_path=EXCLUDED.task_path, channel=EXCLUDED.channel,
					type=EXCLUDED.type, value_version=EXCLUDED.value_version, value=EXCLUDED.value`
		}
		if _, err := tx.ExecContext(ctx, query,
			config.ThreadID, config.Namespace, config.CheckpointID,
			write.TaskID, write.Index, write.TaskPath, write.Channel,
			write.Value.Type, write.Value.Version, write.Value.Data,
		); err != nil {
			return fmt.Errorf("store PostgreSQL pending write task=%q index=%d: %w", write.TaskID, write.Index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL pending writes: %w", err)
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
	if err := s.ensureSetup(ctx); err != nil {
		return checkpoint.Tuple{}, false, err
	}
	query := `SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata
		FROM checkpoints WHERE thread_id=$1 AND checkpoint_ns=$2`
	args := []any{config.ThreadID, config.Namespace}
	if config.CheckpointID != "" {
		query += " AND checkpoint_id=$3"
		args = append(args, config.CheckpointID)
	} else {
		query += " ORDER BY checkpoint_id DESC LIMIT 1"
	}
	record, err := scanRecord(s.db.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return checkpoint.Tuple{}, false, nil
	}
	if err != nil {
		return checkpoint.Tuple{}, false, err
	}
	tuple, err := s.hydrate(ctx, record)
	return tuple, err == nil, err
}

// List implements checkpoint.Saver.
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
	if err := s.ensureSetup(ctx); err != nil {
		return nil, err
	}
	query := strings.Builder{}
	query.WriteString(`SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata FROM checkpoints`)
	predicates := make([]string, 0, 5)
	args := make([]any, 0, 5)
	add := func(predicate string, value any) {
		args = append(args, value)
		predicates = append(predicates, fmt.Sprintf(predicate, len(args)))
	}
	if options.Config != nil {
		add("thread_id=$%d", options.Config.ThreadID)
		if !options.AllNamespaces {
			add("checkpoint_ns=$%d", options.Config.Namespace)
		}
		if options.Config.CheckpointID != "" {
			add("checkpoint_id=$%d", options.Config.CheckpointID)
		}
	}
	if options.Before != nil && options.Before.CheckpointID != "" {
		add("checkpoint_id<$%d", options.Before.CheckpointID)
	}
	if len(predicates) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(predicates, " AND "))
	}
	query.WriteString(" ORDER BY checkpoint_id DESC, thread_id ASC, checkpoint_ns ASC")
	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL checkpoints: %w", err)
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
		return nil, fmt.Errorf("iterate PostgreSQL checkpoints: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close PostgreSQL checkpoint rows: %w", err)
	}
	result := make([]checkpoint.Tuple, 0, len(records))
	for _, record := range records {
		tuple, err := s.hydrate(ctx, record)
		if err != nil {
			return nil, err
		}
		result = append(result, tuple)
	}
	return result, nil
}

// DeleteThread implements checkpoint.Saver.
func (s *Saver) DeleteThread(ctx context.Context, threadID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if threadID == "" {
		return fmt.Errorf("%w: thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	if err := s.ensureSetup(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL thread deletion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, table := range []string{"checkpoint_writes", "checkpoints", "checkpoint_blobs"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE thread_id=$1", threadID); err != nil {
			return fmt.Errorf("delete PostgreSQL %s for thread %q: %w", table, threadID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL thread deletion: %w", err)
	}
	return nil
}

type storedRecord struct {
	config   checkpoint.Config
	parentID string
	value    checkpoint.Checkpoint
	metadata checkpoint.Metadata
}

type rowScanner interface{ Scan(...any) error }

func scanRecord(row rowScanner) (storedRecord, error) {
	var record storedRecord
	var parent sql.NullString
	var checkpointData, metadataData []byte
	if err := row.Scan(&record.config.ThreadID, &record.config.Namespace, &record.config.CheckpointID, &parent, &checkpointData, &metadataData); err != nil {
		return storedRecord{}, fmt.Errorf("scan PostgreSQL checkpoint: %w", err)
	}
	if err := json.Unmarshal(checkpointData, &record.value); err != nil {
		return storedRecord{}, fmt.Errorf("decode PostgreSQL checkpoint %q: %w", record.config.CheckpointID, err)
	}
	metadata, err := decodeMetadata(metadataData)
	if err != nil {
		return storedRecord{}, fmt.Errorf("decode PostgreSQL checkpoint metadata %q: %w", record.config.CheckpointID, err)
	}
	record.metadata = metadata
	if parent.Valid {
		record.parentID = parent.String
	}
	return record, nil
}

func (s *Saver) hydrate(ctx context.Context, record storedRecord) (checkpoint.Tuple, error) {
	value := checkpoint.CloneCheckpoint(record.value)
	value.Values = make(map[string]checkpoint.EncodedValue)
	for _, channel := range sortedKeys(value.ChannelVersions) {
		var present bool
		var typeName sql.NullString
		var version sql.NullInt64
		var data []byte
		err := s.db.QueryRowContext(ctx, `SELECT present, type, value_version, value FROM checkpoint_blobs
			WHERE thread_id=$1 AND checkpoint_ns=$2 AND channel=$3 AND version=$4`,
			record.config.ThreadID, record.config.Namespace, channel, value.ChannelVersions[channel],
		).Scan(&present, &typeName, &version, &data)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return checkpoint.Tuple{}, fmt.Errorf("load PostgreSQL channel blob %q: %w", channel, err)
		}
		if present {
			value.Values[channel] = checkpoint.EncodedValue{Type: typeName.String, Version: int(version.Int64), Data: append([]byte(nil), data...)}
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT task_id, task_path, idx, channel, type, value_version, value
		FROM checkpoint_writes WHERE thread_id=$1 AND checkpoint_ns=$2 AND checkpoint_id=$3
		ORDER BY task_id ASC, idx ASC`, record.config.ThreadID, record.config.Namespace, record.config.CheckpointID)
	if err != nil {
		return checkpoint.Tuple{}, fmt.Errorf("load PostgreSQL pending writes: %w", err)
	}
	pending := make([]checkpoint.PendingWrite, 0)
	for rows.Next() {
		var write checkpoint.PendingWrite
		if err := rows.Scan(&write.TaskID, &write.TaskPath, &write.Index, &write.Channel, &write.Value.Type, &write.Value.Version, &write.Value.Data); err != nil {
			_ = rows.Close()
			return checkpoint.Tuple{}, fmt.Errorf("scan PostgreSQL pending write: %w", err)
		}
		write.Value.Data = append([]byte(nil), write.Value.Data...)
		pending = append(pending, write)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return checkpoint.Tuple{}, fmt.Errorf("iterate PostgreSQL pending writes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return checkpoint.Tuple{}, fmt.Errorf("close PostgreSQL pending writes: %w", err)
	}
	metadata, err := checkpoint.CloneMetadata(record.metadata)
	if err != nil {
		return checkpoint.Tuple{}, err
	}
	var parent *checkpoint.Config
	if record.parentID != "" {
		parent = &checkpoint.Config{ThreadID: record.config.ThreadID, Namespace: record.config.Namespace, CheckpointID: record.parentID}
	}
	return checkpoint.Tuple{Config: record.config, Checkpoint: value, Metadata: metadata, ParentConfig: parent, PendingWrites: pending}, nil
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
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unexpected trailing JSON value")
	}
	return checkpoint.Metadata(normalizeNumbers(raw).(map[string]any)), nil
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

var _ checkpoint.Saver = (*Saver)(nil)
