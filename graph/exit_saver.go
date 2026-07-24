package graph

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// exitBufferSaver presents a normal Saver to one run while retaining every
// intermediate checkpoint in memory. Flush publishes only the latest boundary
// in each active namespace, deepest first, matching nested exit durability.
type exitBufferSaver struct {
	base checkpoint.Saver

	mu         sync.Mutex
	drafts     map[string]checkpoint.Tuple
	writes     map[string][]checkpoint.PendingWrite
	latestID   map[string]string
	baseParent map[string]checkpoint.Config
	scopeOrder []string
	flushed    bool
}

func newExitBufferSaver(base checkpoint.Saver) *exitBufferSaver {
	return &exitBufferSaver{
		base:       base,
		drafts:     make(map[string]checkpoint.Tuple),
		writes:     make(map[string][]checkpoint.PendingWrite),
		latestID:   make(map[string]string),
		baseParent: make(map[string]checkpoint.Config),
	}
}

func (s *exitBufferSaver) GetTuple(
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
	s.mu.Unlock()
	return s.base.GetTuple(ctx, config)
}

func (s *exitBufferSaver) List(
	ctx context.Context,
	options checkpoint.ListOptions,
) ([]checkpoint.Tuple, error) {
	s.mu.Lock()
	drafts := snapshotBufferedTuples(s.drafts, s.writes)
	s.mu.Unlock()
	return listWithBufferedTuples(ctx, s.base, options, drafts)
}

func (s *exitBufferSaver) DeleteThread(ctx context.Context, threadID string) error {
	return s.base.DeleteThread(ctx, threadID)
}

func (s *exitBufferSaver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	_ map[string]string,
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

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flushed {
		return checkpoint.Config{}, fmt.Errorf("%w: exit saver already flushed", checkpoint.ErrInvalidConfig)
	}
	scope := checkpointScopeKey(config)
	if _, exists := s.baseParent[scope]; !exists {
		s.baseParent[scope] = parent
		s.scopeOrder = append(s.scopeOrder, scope)
	}
	s.drafts[checkpointTupleKey(config)] = tuple
	s.latestID[scope] = value.ID
	return config, nil
}

func (s *exitBufferSaver) PutWrites(
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flushed {
		return fmt.Errorf("%w: exit saver already flushed", checkpoint.ErrInvalidConfig)
	}
	key := checkpointTupleKey(config)
	s.writes[key] = append(s.writes[key], clonePendingWrites(writes)...)
	return nil
}

// Flush publishes the latest checkpoint. Pending writes are only needed when
// they target that same recovery boundary (for example an interrupt or failed
// super-step); writes for superseded in-memory checkpoints are discarded.
func (s *exitBufferSaver) Flush(ctx context.Context) (checkpoint.Config, error) {
	type boundary struct {
		tuple      checkpoint.Tuple
		baseParent checkpoint.Config
		writes     []checkpoint.PendingWrite
	}
	s.mu.Lock()
	if s.flushed {
		s.mu.Unlock()
		return checkpoint.Config{}, nil
	}
	s.flushed = true
	boundaries := make([]boundary, 0, len(s.scopeOrder))
	for index := len(s.scopeOrder) - 1; index >= 0; index-- {
		scope := s.scopeOrder[index]
		latestID := s.latestID[scope]
		tuple := s.drafts[scope+"\x00"+latestID]
		boundaries = append(boundaries, boundary{
			tuple:      cloneExitTuple(tuple),
			baseParent: s.baseParent[scope],
			writes:     clonePendingWrites(s.writes[checkpointTupleKey(tuple.Config)]),
		})
	}
	s.mu.Unlock()

	if len(boundaries) == 0 {
		return checkpoint.Config{}, nil
	}
	var last checkpoint.Config
	for _, item := range boundaries {
		baseVersions := map[string]string{}
		if item.baseParent.CheckpointID != "" {
			base, found, err := s.base.GetTuple(ctx, item.baseParent)
			if err != nil {
				return checkpoint.Config{}, err
			}
			if !found {
				return checkpoint.Config{}, checkpoint.ErrNotFound
			}
			baseVersions = base.Checkpoint.ChannelVersions
		}
		newVersions := make(map[string]string)
		for channel, version := range item.tuple.Checkpoint.ChannelVersions {
			if baseVersions[channel] != version {
				newVersions[channel] = version
			}
		}
		stored, err := s.base.Put(
			ctx, item.baseParent, item.tuple.Checkpoint, item.tuple.Metadata, newVersions,
		)
		if err != nil {
			return checkpoint.Config{}, err
		}
		if len(item.writes) > 0 {
			if err := s.base.PutWrites(ctx, stored, item.writes); err != nil {
				return checkpoint.Config{}, err
			}
		}
		last = stored
	}
	return last, nil
}

func checkpointScopeKey(config checkpoint.Config) string {
	return config.ThreadID + "\x00" + config.Namespace
}

func checkpointTupleKey(config checkpoint.Config) string {
	return checkpointScopeKey(config) + "\x00" + config.CheckpointID
}

func snapshotBufferedTuples(
	drafts map[string]checkpoint.Tuple,
	writes map[string][]checkpoint.PendingWrite,
) []checkpoint.Tuple {
	result := make([]checkpoint.Tuple, 0, len(drafts))
	for key, tuple := range drafts {
		cloned := cloneExitTuple(tuple)
		cloned.PendingWrites = clonePendingWrites(writes[key])
		result = append(result, cloned)
	}
	return result
}

func listWithBufferedTuples(
	ctx context.Context,
	base checkpoint.Saver,
	options checkpoint.ListOptions,
	drafts []checkpoint.Tuple,
) ([]checkpoint.Tuple, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Limit < 0 {
		return nil, fmt.Errorf("%w: list limit cannot be negative", checkpoint.ErrInvalidConfig)
	}
	if options.Config != nil && options.Config.ThreadID == "" {
		return nil, fmt.Errorf("%w: list thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	stored, err := base.List(ctx, options)
	if err != nil {
		return nil, err
	}
	combined := make(map[string]checkpoint.Tuple, len(stored)+len(drafts))
	for _, tuple := range stored {
		combined[checkpointTupleKey(tuple.Config)] = tuple
	}
	for _, tuple := range drafts {
		if !bufferedTupleMatches(tuple, options) {
			continue
		}
		combined[checkpointTupleKey(tuple.Config)] = tuple
	}
	result := make([]checkpoint.Tuple, 0, len(combined))
	for _, tuple := range combined {
		result = append(result, cloneExitTuple(tuple))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Config.CheckpointID != result[j].Config.CheckpointID {
			return result[i].Config.CheckpointID > result[j].Config.CheckpointID
		}
		if result[i].Config.ThreadID != result[j].Config.ThreadID {
			return result[i].Config.ThreadID < result[j].Config.ThreadID
		}
		return result[i].Config.Namespace < result[j].Config.Namespace
	})
	if options.Limit > 0 && len(result) > options.Limit {
		result = result[:options.Limit]
	}
	return result, nil
}

func bufferedTupleMatches(tuple checkpoint.Tuple, options checkpoint.ListOptions) bool {
	if options.Config != nil {
		if tuple.Config.ThreadID != options.Config.ThreadID {
			return false
		}
		if !options.AllNamespaces && tuple.Config.Namespace != options.Config.Namespace {
			return false
		}
		if options.Config.CheckpointID != "" &&
			tuple.Config.CheckpointID != options.Config.CheckpointID {
			return false
		}
	}
	if options.Before != nil && options.Before.CheckpointID != "" &&
		tuple.Config.CheckpointID >= options.Before.CheckpointID {
		return false
	}
	for key, expected := range options.Filter {
		if actual, exists := tuple.Metadata[key]; !exists || !reflect.DeepEqual(actual, expected) {
			return false
		}
	}
	return true
}

func cloneExitTuple(source checkpoint.Tuple) checkpoint.Tuple {
	result := source
	result.Checkpoint = checkpoint.CloneCheckpoint(source.Checkpoint)
	result.Metadata, _ = checkpoint.CloneMetadata(source.Metadata)
	if source.ParentConfig != nil {
		parent := *source.ParentConfig
		result.ParentConfig = &parent
	}
	result.PendingWrites = clonePendingWrites(source.PendingWrites)
	return result
}

func clonePendingWrites(source []checkpoint.PendingWrite) []checkpoint.PendingWrite {
	result := make([]checkpoint.PendingWrite, len(source))
	for index, write := range source {
		result[index] = write
		result[index].Value = checkpoint.CloneEncodedValue(write.Value)
	}
	return result
}
