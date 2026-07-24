package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/ybszm/langgraph-go/checkpoint"
)

type asyncSaverCommand func(context.Context) error

// asyncQueueSaver preserves Saver call order while allowing graph execution to
// overlap storage IO. Drafts remain readable in memory so checkpoint planning
// never depends on the background writer catching up.
type asyncQueueSaver struct {
	base   checkpoint.Saver
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu       sync.Mutex
	drafts   map[string]checkpoint.Tuple
	writes   map[string][]checkpoint.PendingWrite
	latestID map[string]string
	firstErr error

	enqueueMu sync.Mutex
	closed    bool

	commands chan asyncSaverCommand
	done     chan struct{}
}

func newAsyncQueueSaver(
	ctx context.Context,
	base checkpoint.Saver,
	cancel context.CancelCauseFunc,
) *asyncQueueSaver {
	saver := &asyncQueueSaver{
		base: base, ctx: ctx, cancel: cancel,
		drafts: make(map[string]checkpoint.Tuple), writes: make(map[string][]checkpoint.PendingWrite),
		latestID: make(map[string]string),
		commands: make(chan asyncSaverCommand, 64), done: make(chan struct{}),
	}
	go saver.run()
	return saver
}

func (s *asyncQueueSaver) run() {
	defer close(s.done)
	for command := range s.commands {
		s.mu.Lock()
		failed := s.firstErr != nil
		s.mu.Unlock()
		if failed {
			continue
		}
		if err := command(s.ctx); err != nil {
			s.fail(err)
		}
	}
}

func (s *asyncQueueSaver) fail(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	first := s.firstErr
	s.mu.Unlock()
	s.cancel(first)
}

func (s *asyncQueueSaver) enqueue(command asyncSaverCommand) error {
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if s.closed {
		return fmt.Errorf("%w: async saver already flushed", checkpoint.ErrInvalidConfig)
	}
	s.mu.Lock()
	if s.firstErr != nil {
		err := s.firstErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	select {
	case s.commands <- command:
		return nil
	case <-s.ctx.Done():
		if cause := context.Cause(s.ctx); cause != nil {
			return cause
		}
		return s.ctx.Err()
	}
}

func (s *asyncQueueSaver) GetTuple(
	ctx context.Context,
	config checkpoint.Config,
) (checkpoint.Tuple, bool, error) {
	if err := ctx.Err(); err != nil {
		return checkpoint.Tuple{}, false, err
	}
	s.mu.Lock()
	id := config.CheckpointID
	if id == "" {
		id = s.latestID[checkpointScopeKey(config)]
	}
	key := checkpointTupleKey(checkpoint.Config{
		ThreadID: config.ThreadID, Namespace: config.Namespace, CheckpointID: id,
	})
	if tuple, ok := s.drafts[key]; ok {
		result := cloneExitTuple(tuple)
		result.PendingWrites = clonePendingWrites(s.writes[key])
		s.mu.Unlock()
		return result, true, nil
	}
	err := s.firstErr
	s.mu.Unlock()
	if err != nil {
		return checkpoint.Tuple{}, false, err
	}
	return s.base.GetTuple(ctx, config)
}

func (s *asyncQueueSaver) List(
	ctx context.Context,
	options checkpoint.ListOptions,
) ([]checkpoint.Tuple, error) {
	s.mu.Lock()
	drafts := snapshotBufferedTuples(s.drafts, s.writes)
	err := s.firstErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return listWithBufferedTuples(ctx, s.base, options, drafts)
}

func (s *asyncQueueSaver) DeleteThread(ctx context.Context, threadID string) error {
	return s.base.DeleteThread(ctx, threadID)
}

func (s *asyncQueueSaver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	newVersions map[string]string,
) (checkpoint.Config, error) {
	if err := ctx.Err(); err != nil {
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
	config := checkpoint.Config{
		ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID,
	}
	tuple := checkpoint.Tuple{
		Config: config, Checkpoint: checkpoint.CloneCheckpoint(value), Metadata: clonedMetadata,
	}
	if parent.CheckpointID != "" {
		parentCopy := parent
		tuple.ParentConfig = &parentCopy
	}
	versions := make(map[string]string, len(newVersions))
	for channel, version := range newVersions {
		versions[channel] = version
	}
	valueCopy := checkpoint.CloneCheckpoint(value)

	s.mu.Lock()
	s.drafts[checkpointTupleKey(config)] = tuple
	s.latestID[checkpointScopeKey(config)] = value.ID
	s.mu.Unlock()
	err = s.enqueue(func(commandCtx context.Context) error {
		stored, putErr := s.base.Put(commandCtx, parent, valueCopy, clonedMetadata, versions)
		if putErr != nil {
			return putErr
		}
		if stored != config {
			return fmt.Errorf(
				"%w: async saver stored config %+v, want %+v",
				checkpoint.ErrInvalidConfig, stored, config,
			)
		}
		return nil
	})
	if err != nil {
		return checkpoint.Config{}, err
	}
	return config, nil
}

func (s *asyncQueueSaver) PutWrites(
	ctx context.Context,
	config checkpoint.Config,
	writes []checkpoint.PendingWrite,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if config.CheckpointID == "" {
		return fmt.Errorf("%w: checkpoint ID is empty for pending writes", checkpoint.ErrInvalidConfig)
	}
	cloned := clonePendingWrites(writes)
	key := checkpointTupleKey(config)
	s.mu.Lock()
	s.writes[key] = append(s.writes[key], cloned...)
	s.mu.Unlock()
	return s.enqueue(func(commandCtx context.Context) error {
		return s.base.PutWrites(commandCtx, config, cloned)
	})
}

func (s *asyncQueueSaver) Flush() error {
	defer s.cancel(nil)
	s.enqueueMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.commands)
	}
	s.enqueueMu.Unlock()
	<-s.done
	s.mu.Lock()
	err := s.firstErr
	s.mu.Unlock()
	return err
}

func (s *asyncQueueSaver) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}
