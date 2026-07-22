// Package retention provides checkpoint history pruning helpers.
//
// Savers that implement ThreadPruner can drop old checkpoints inside a thread
// while keeping the newest N. All savers support deleting entire idle threads
// via checkpoint.Saver.DeleteThread.
package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// ThreadPruner optionally deletes older checkpoints within one thread while
// retaining the newest KeepLatest snapshots (across namespaces when requested).
type ThreadPruner interface {
	PruneThread(ctx context.Context, threadID string, keepLatest int) (removed int, err error)
}

// Policy configures automatic history cleanup.
type Policy struct {
	// KeepLatest retains this many newest checkpoints per thread when the
	// saver implements ThreadPruner. Zero disables per-thread pruning.
	KeepLatest int
	// MaxThreadAge deletes entire threads whose newest checkpoint is older
	// than this duration. Zero disables age-based thread deletion.
	MaxThreadAge time.Duration
	// Now overrides the wall clock used for MaxThreadAge (tests).
	Now func() time.Time
}

// Apply prunes using the saver's List/DeleteThread capabilities and optional
// ThreadPruner implementation.
func Apply(ctx context.Context, saver checkpoint.Saver, policy Policy) (threadsDeleted, checkpointsRemoved int, err error) {
	if saver == nil {
		return 0, 0, fmt.Errorf("retention: saver is nil")
	}
	if policy.KeepLatest < 0 {
		return 0, 0, fmt.Errorf("retention: KeepLatest cannot be negative")
	}
	if policy.MaxThreadAge < 0 {
		return 0, 0, fmt.Errorf("retention: MaxThreadAge cannot be negative")
	}
	if policy.KeepLatest == 0 && policy.MaxThreadAge == 0 {
		return 0, 0, nil
	}
	now := time.Now().UTC()
	if policy.Now != nil {
		now = policy.Now().UTC()
	}

	// Age-based whole-thread deletion uses the newest checkpoint timestamp.
	if policy.MaxThreadAge > 0 {
		tuples, listErr := saver.List(ctx, checkpoint.ListOptions{})
		if listErr != nil {
			return 0, 0, listErr
		}
		newest := map[string]time.Time{}
		for _, tuple := range tuples {
			thread := tuple.Config.ThreadID
			ts := tuple.Checkpoint.Timestamp
			if prev, ok := newest[thread]; !ok || ts.After(prev) {
				newest[thread] = ts
			}
		}
		for thread, ts := range newest {
			if now.Sub(ts) > policy.MaxThreadAge {
				if delErr := saver.DeleteThread(ctx, thread); delErr != nil {
					return threadsDeleted, checkpointsRemoved, delErr
				}
				threadsDeleted++
			}
		}
	}

	if policy.KeepLatest > 0 {
		if pruner, ok := saver.(ThreadPruner); ok {
			tuples, listErr := saver.List(ctx, checkpoint.ListOptions{})
			if listErr != nil {
				return threadsDeleted, checkpointsRemoved, listErr
			}
			seen := map[string]struct{}{}
			for _, tuple := range tuples {
				thread := tuple.Config.ThreadID
				if _, done := seen[thread]; done {
					continue
				}
				seen[thread] = struct{}{}
				removed, pruneErr := pruner.PruneThread(ctx, thread, policy.KeepLatest)
				if pruneErr != nil {
					return threadsDeleted, checkpointsRemoved, pruneErr
				}
				checkpointsRemoved += removed
			}
		}
	}
	return threadsDeleted, checkpointsRemoved, nil
}
