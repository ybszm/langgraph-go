package remote

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

func storedRunFromTyped[O any](run Run[O]) (StoredRun, error) {
	stored := StoredRun{ID: run.ID, ThreadID: run.ThreadID, Status: run.Status, Error: run.Error, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt}
	if run.Output != nil {
		encoded, err := json.Marshal(run.Output)
		if err != nil {
			return StoredRun{}, err
		}
		stored.Output = encoded
	}
	return stored, nil
}

func typedRunFromStored[O any](stored StoredRun) (Run[O], error) {
	run := Run[O]{ID: stored.ID, ThreadID: stored.ThreadID, Status: stored.Status, Error: stored.Error, CreatedAt: stored.CreatedAt, UpdatedAt: stored.UpdatedAt}
	if len(stored.Output) > 0 {
		var output O
		if err := json.Unmarshal(stored.Output, &output); err != nil {
			return Run[O]{}, err
		}
		run.Output = &output
	}
	return run, nil
}

func (s *Server[I, O]) writeStoreError(writer http.ResponseWriter, err error, operation string) {
	switch {
	case errors.Is(err, ErrControlNotFound):
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "control resource not found")
	case errors.Is(err, ErrControlConflict), errors.Is(err, ErrIdempotencyConflict):
		s.writeControlError(writer, http.StatusConflict, CodeConflict, err.Error())
	default:
		s.writeControlError(writer, http.StatusInternalServerError, CodeProtocol, operation+": "+err.Error())
	}
}

func (s *Server[I, O]) threadExists(ctx context.Context, threadID string) (bool, error) {
	if !isNil(s.options.ControlStore) {
		_, ok, err := s.options.ControlStore.GetThread(ctx, threadID)
		return ok, err
	}
	s.control.mu.RLock()
	defer s.control.mu.RUnlock()
	_, ok := s.control.threads[threadID]
	return ok, nil
}

// PruneRuns deletes terminal control records last updated before the cutoff.
// Running runs are never removed. Durable graph checkpoints are unaffected.
func (s *Server[I, O]) PruneRuns(ctx context.Context, before time.Time) (int64, error) {
	if !isNil(s.options.ControlStore) {
		return s.options.ControlStore.PruneRuns(ctx, before)
	}
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	var count int64
	for threadID, runs := range s.control.runs {
		for id, record := range runs {
			if terminalRun(record.run.Status) && record.run.UpdatedAt.Before(before) {
				delete(runs, id)
				count++
				for key, claim := range s.control.idempotency {
					if claim.runID == id && len(key) > len(threadID) && key[:len(threadID)+1] == threadID+"\x00" {
						delete(s.control.idempotency, key)
					}
				}
			}
		}
	}
	return count, nil
}
