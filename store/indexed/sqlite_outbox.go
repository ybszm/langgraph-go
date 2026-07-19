package indexed

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SQLiteOutbox durably journals vector synchronization intents. It can use
// the same database as a SQLite Store or a dedicated SQLite database.
type SQLiteOutbox struct {
	db *sql.DB
}

// NewSQLiteOutbox validates a SQLite outbox handle. Call Setup before use.
func NewSQLiteOutbox(db *sql.DB) (*SQLiteOutbox, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: SQLite outbox DB is nil", ErrOutbox)
	}
	return &SQLiteOutbox{db: db}, nil
}

// Setup creates the idempotent durable journal schema.
func (o *SQLiteOutbox) Setup(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrOutbox)
	}
	if _, err := o.db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		return fmt.Errorf("%w: busy timeout: %v", ErrOutbox, err)
	}
	_, err := o.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS langgraph_vector_outbox (
sequence INTEGER PRIMARY KEY AUTOINCREMENT,
id TEXT NOT NULL UNIQUE,
payload TEXT NOT NULL
)`)
	if err != nil {
		return fmt.Errorf("%w: setup SQLite schema: %v", ErrOutbox, err)
	}
	return nil
}

func (o *SQLiteOutbox) Enqueue(ctx context.Context, mutations []VectorMutation) ([]string, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrOutbox)
	}
	ids := make([]string, len(mutations))
	payloads := make([][]byte, len(mutations))
	for index, mutation := range mutations {
		id, err := randomOutboxID()
		if err != nil {
			return nil, err
		}
		payload, err := json.Marshal(mutation)
		if err != nil {
			return nil, fmt.Errorf("%w: encode mutation: %v", ErrOutbox, err)
		}
		ids[index], payloads[index] = id, payload
	}
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: begin enqueue: %v", ErrOutbox, err)
	}
	defer tx.Rollback()
	for index, id := range ids {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO langgraph_vector_outbox(id, payload) VALUES (?, ?)", id, string(payloads[index]),
		); err != nil {
			return nil, fmt.Errorf("%w: insert mutation: %v", ErrOutbox, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("%w: commit enqueue: %v", ErrOutbox, err)
	}
	return ids, nil
}

func (o *SQLiteOutbox) List(ctx context.Context, limit int) ([]PendingVectorMutation, error) {
	if ctx == nil || limit < 0 {
		return nil, fmt.Errorf("%w: invalid list context or limit", ErrOutbox)
	}
	query := "SELECT id, payload FROM langgraph_vector_outbox ORDER BY sequence"
	arguments := []any(nil)
	if limit > 0 {
		query += " LIMIT ?"
		arguments = append(arguments, limit)
	}
	rows, err := o.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("%w: list mutations: %v", ErrOutbox, err)
	}
	defer rows.Close()
	result := make([]PendingVectorMutation, 0)
	for rows.Next() {
		var id, payload string
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, fmt.Errorf("%w: scan mutation: %v", ErrOutbox, err)
		}
		mutation := PendingVectorMutation{ID: id}
		if err := json.Unmarshal([]byte(payload), &mutation.VectorMutation); err != nil {
			return nil, fmt.Errorf("%w: decode mutation %s: %v", ErrOutbox, id, err)
		}
		result = append(result, mutation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate mutations: %v", ErrOutbox, err)
	}
	return result, nil
}

func (o *SQLiteOutbox) Ack(ctx context.Context, ids []string) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrOutbox)
	}
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: begin acknowledge: %v", ErrOutbox, err)
	}
	defer tx.Rollback()
	for _, id := range ids {
		if id == "" {
			return fmt.Errorf("%w: empty mutation ID", ErrOutbox)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM langgraph_vector_outbox WHERE id = ?", id); err != nil {
			return fmt.Errorf("%w: acknowledge mutation: %v", ErrOutbox, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: commit acknowledge: %v", ErrOutbox, err)
	}
	return nil
}

func randomOutboxID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("%w: generate ID: %v", ErrOutbox, err)
	}
	return hex.EncodeToString(value), nil
}

var _ VectorOutbox = (*SQLiteOutbox)(nil)
