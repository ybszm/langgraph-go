package distributed

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

// PostgresCheckpointOutbox atomically first-writes a checkpoint pending result
// and acknowledges the current PostgreSQL queue lease in the same transaction.
// The checkpoint and queue schemas must be set up on the same database.
type PostgresCheckpointOutbox[R any] struct {
	db    *sql.DB
	codec checkpoint.Codec[R]
	clock func() time.Time
}

func NewPostgresCheckpointOutbox[R any](db *sql.DB, codec checkpoint.Codec[R], clock func() time.Time) (*PostgresCheckpointOutbox[R], error) {
	if db == nil || nilLike(codec) || clock == nil {
		return nil, fmt.Errorf("%w: PostgreSQL checkpoint outbox requires database, codec, and clock", ErrInvalidRequest)
	}
	return &PostgresCheckpointOutbox[R]{db: db, codec: codec, clock: clock}, nil
}
func (*PostgresCheckpointOutbox[R]) AcknowledgesLease() bool { return true }

func (o *PostgresCheckpointOutbox[R]) Commit(ctx context.Context, completion Completion[CheckpointResult[R]]) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	result := completion.Value
	if !validID(completion.TaskID) || !validID(completion.LeaseToken) || !validID(completion.WorkerID) || completion.Attempt < 1 || result.Config.CheckpointID == "" || result.Index < 0 || !validID(result.Channel) {
		return fmt.Errorf("%w: exact checkpoint, task, lease, worker, channel, and non-negative index are required", ErrInvalidRequest)
	}
	if err := result.Config.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	encoded, err := o.codec.Encode(result.Value)
	if err != nil {
		return &WorkerError{Operation: "encode-checkpoint-result", TaskID: completion.TaskID, Err: err}
	}
	if _, err = o.codec.Decode(encoded); err != nil {
		return &WorkerError{Operation: "decode-checkpoint-result", TaskID: completion.TaskID, Err: err}
	}
	now := o.clock()
	if now.IsZero() {
		return fmt.Errorf("%w: checkpoint outbox clock returned zero time", ErrInvalidRequest)
	}
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	lockKey := fmt.Sprintf("%d:%s:%d:%s:%d:%s:%d", len(result.Config.ThreadID), result.Config.ThreadID, len(result.Config.Namespace), result.Config.Namespace, len(result.Config.CheckpointID), result.Config.CheckpointID, result.Index)
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,684773295))`, lockKey); err != nil {
		return err
	}
	var checkpointExists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM checkpoints WHERE thread_id=$1 AND checkpoint_ns=$2 AND checkpoint_id=$3)`, result.Config.ThreadID, result.Config.Namespace, result.Config.CheckpointID).Scan(&checkpointExists); err != nil {
		return err
	}
	if !checkpointExists {
		return &WorkerError{Operation: "get-checkpoint", TaskID: completion.TaskID, Err: checkpoint.ErrNotFound}
	}
	var storedChannel, storedType string
	var storedVersion int
	var storedData []byte
	selectWrite := `SELECT channel,type,value_version,value FROM checkpoint_writes WHERE thread_id=$1 AND checkpoint_ns=$2 AND checkpoint_id=$3 AND task_id=$4 AND idx=$5`
	err = tx.QueryRowContext(ctx, selectWrite, result.Config.ThreadID, result.Config.Namespace, result.Config.CheckpointID, completion.TaskID, result.Index).Scan(&storedChannel, &storedType, &storedVersion, &storedData)
	existed := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !existed {
		_, err = tx.ExecContext(ctx, `INSERT INTO checkpoint_writes(thread_id,checkpoint_ns,checkpoint_id,task_id,idx,task_path,channel,type,value_version,value) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, result.Config.ThreadID, result.Config.Namespace, result.Config.CheckpointID, completion.TaskID, result.Index, result.TaskPath, result.Channel, encoded.Type, encoded.Version, encoded.Data)
		if err != nil {
			return err
		}
		storedChannel = result.Channel
		storedType = encoded.Type
		storedVersion = encoded.Version
		storedData = encoded.Data
	}
	if storedChannel != result.Channel || storedType != encoded.Type || storedVersion != encoded.Version || !bytes.Equal(storedData, encoded.Data) {
		return ErrCompletionConflict
	}
	ack, err := tx.ExecContext(ctx, `DELETE FROM distributed_queue_tasks WHERE id=$1 AND lease_token=$2 AND worker_id=$3 AND lease_expires_at>$4`, completion.TaskID, completion.LeaseToken, completion.WorkerID, now)
	if err != nil {
		return err
	}
	affected, _ := ack.RowsAffected()
	if affected != 1 {
		var taskExists bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM distributed_queue_tasks WHERE id=$1)`, completion.TaskID).Scan(&taskExists); err != nil {
			return err
		}
		if taskExists {
			return ErrLeaseLost
		}
		if !existed {
			return ErrTaskNotFound
		}
	}
	return tx.Commit()
}
