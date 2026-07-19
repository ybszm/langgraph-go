// Package memory implements an in-process checkpoint saver for tests and local
// development. It is not intended as a production persistence backend.
package memory

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/ybszm/langgraph-go/checkpoint"
)

type checkpointKey struct {
	threadID  string
	namespace string
	id        string
}

type blobKey struct {
	threadID  string
	namespace string
	channel   string
	version   string
}

type writeKey struct {
	taskID string
	index  int
}

type blob struct {
	present bool
	value   checkpoint.EncodedValue
}

type record struct {
	checkpoint checkpoint.Checkpoint
	metadata   checkpoint.Metadata
	parentID   string
}

// Saver stores checkpoints, channel blobs, and pending writes in memory.
// All methods are safe for concurrent use.
type Saver struct {
	mu      sync.RWMutex
	storage map[checkpointKey]record
	blobs   map[blobKey]blob
	writes  map[checkpointKey]map[writeKey]checkpoint.PendingWrite
}

// NewSaver creates an empty in-memory saver.
func NewSaver() *Saver {
	return &Saver{
		storage: make(map[checkpointKey]record),
		blobs:   make(map[blobKey]blob),
		writes:  make(map[checkpointKey]map[writeKey]checkpoint.PendingWrite),
	}
}

// Put implements checkpoint.Saver.
func (s *Saver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	newVersions map[string]string,
) (checkpoint.Config, error) {
	if err := ctxError(ctx); err != nil {
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
	key := checkpointKey{
		threadID:  parent.ThreadID,
		namespace: parent.Namespace,
		id:        value.ID,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for channel, version := range newVersions {
		encoded, exists := value.Values[channel]
		s.blobs[blobKey{
			threadID:  parent.ThreadID,
			namespace: parent.Namespace,
			channel:   channel,
			version:   version,
		}] = blob{present: exists, value: checkpoint.CloneEncodedValue(encoded)}
	}
	s.storage[key] = record{
		checkpoint: stored,
		metadata:   clonedMetadata,
		parentID:   parent.CheckpointID,
	}
	return checkpoint.Config{
		ThreadID:     parent.ThreadID,
		Namespace:    parent.Namespace,
		CheckpointID: value.ID,
	}, nil
}

// PutWrites implements checkpoint.Saver. Writes use first-write-wins for the
// same task ID and non-negative write index, matching LangGraph memory saver
// retry behavior.
func (s *Saver) PutWrites(
	ctx context.Context,
	config checkpoint.Config,
	writes []checkpoint.PendingWrite,
) error {
	if err := ctxError(ctx); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if config.CheckpointID == "" {
		return fmt.Errorf("%w: checkpoint ID is empty for pending writes", checkpoint.ErrInvalidConfig)
	}

	key := checkpointKey{
		threadID:  config.ThreadID,
		namespace: config.Namespace,
		id:        config.CheckpointID,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.storage[key]; !exists {
		return fmt.Errorf("%w: pending-write checkpoint %q does not exist", checkpoint.ErrInvalidConfig, config.CheckpointID)
	}
	if s.writes[key] == nil {
		s.writes[key] = make(map[writeKey]checkpoint.PendingWrite)
	}
	for _, write := range writes {
		if write.TaskID == "" || write.Channel == "" {
			return fmt.Errorf("%w: pending write has an empty task or channel", checkpoint.ErrInvalidCheckpoint)
		}
		writeKey := writeKey{taskID: write.TaskID, index: write.Index}
		if write.Index >= 0 {
			if _, exists := s.writes[key][writeKey]; exists {
				continue
			}
		}
		write.Value = checkpoint.CloneEncodedValue(write.Value)
		s.writes[key][writeKey] = write
	}
	return nil
}

// GetTuple implements checkpoint.Saver.
func (s *Saver) GetTuple(
	ctx context.Context,
	config checkpoint.Config,
) (checkpoint.Tuple, bool, error) {
	if err := ctxError(ctx); err != nil {
		return checkpoint.Tuple{}, false, err
	}
	if err := config.Validate(); err != nil {
		return checkpoint.Tuple{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	key, found := s.findKeyLocked(config)
	if !found {
		return checkpoint.Tuple{}, false, nil
	}
	tuple, err := s.tupleLocked(key)
	return tuple, err == nil, err
}

// List implements checkpoint.Saver.
func (s *Saver) List(
	ctx context.Context,
	options checkpoint.ListOptions,
) ([]checkpoint.Tuple, error) {
	if err := ctxError(ctx); err != nil {
		return nil, err
	}
	if options.Limit < 0 {
		return nil, fmt.Errorf("%w: list limit cannot be negative", checkpoint.ErrInvalidConfig)
	}
	if options.Config != nil && options.Config.ThreadID == "" {
		return nil, fmt.Errorf("%w: list thread ID is empty", checkpoint.ErrInvalidConfig)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]checkpointKey, 0, len(s.storage))
	for key, stored := range s.storage {
		if options.Config != nil {
			if key.threadID != options.Config.ThreadID {
				continue
			}
			if !options.AllNamespaces && key.namespace != options.Config.Namespace {
				continue
			}
			if options.Config.CheckpointID != "" && key.id != options.Config.CheckpointID {
				continue
			}
		}
		if options.Before != nil && options.Before.CheckpointID != "" && key.id >= options.Before.CheckpointID {
			continue
		}
		if !metadataMatches(stored.metadata, options.Filter) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].id != keys[j].id {
			return keys[i].id > keys[j].id
		}
		if keys[i].threadID != keys[j].threadID {
			return keys[i].threadID < keys[j].threadID
		}
		return keys[i].namespace < keys[j].namespace
	})
	if options.Limit > 0 && len(keys) > options.Limit {
		keys = keys[:options.Limit]
	}

	result := make([]checkpoint.Tuple, 0, len(keys))
	for _, key := range keys {
		tuple, err := s.tupleLocked(key)
		if err != nil {
			return nil, err
		}
		result = append(result, tuple)
	}
	return result, nil
}

// DeleteThread implements checkpoint.Saver.
func (s *Saver) DeleteThread(ctx context.Context, threadID string) error {
	if err := ctxError(ctx); err != nil {
		return err
	}
	if threadID == "" {
		return fmt.Errorf("%w: thread ID is empty", checkpoint.ErrInvalidConfig)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.storage {
		if key.threadID == threadID {
			delete(s.storage, key)
			delete(s.writes, key)
		}
	}
	for key := range s.blobs {
		if key.threadID == threadID {
			delete(s.blobs, key)
		}
	}
	return nil
}

func (s *Saver) findKeyLocked(config checkpoint.Config) (checkpointKey, bool) {
	if config.CheckpointID != "" {
		key := checkpointKey{
			threadID:  config.ThreadID,
			namespace: config.Namespace,
			id:        config.CheckpointID,
		}
		_, exists := s.storage[key]
		return key, exists
	}

	var latest checkpointKey
	found := false
	for key := range s.storage {
		if key.threadID != config.ThreadID || key.namespace != config.Namespace {
			continue
		}
		if !found || key.id > latest.id {
			latest = key
			found = true
		}
	}
	return latest, found
}

func (s *Saver) tupleLocked(key checkpointKey) (checkpoint.Tuple, error) {
	stored, exists := s.storage[key]
	if !exists {
		return checkpoint.Tuple{}, fmt.Errorf("checkpoint %q disappeared", key.id)
	}
	value := checkpoint.CloneCheckpoint(stored.checkpoint)
	value.Values = make(map[string]checkpoint.EncodedValue)
	for channel, version := range value.ChannelVersions {
		storedBlob, exists := s.blobs[blobKey{
			threadID:  key.threadID,
			namespace: key.namespace,
			channel:   channel,
			version:   version,
		}]
		if exists && storedBlob.present {
			value.Values[channel] = checkpoint.CloneEncodedValue(storedBlob.value)
		}
	}
	metadata, err := checkpoint.CloneMetadata(stored.metadata)
	if err != nil {
		return checkpoint.Tuple{}, err
	}

	writesByKey := s.writes[key]
	writeKeys := make([]writeKey, 0, len(writesByKey))
	for key := range writesByKey {
		writeKeys = append(writeKeys, key)
	}
	sort.Slice(writeKeys, func(i, j int) bool {
		if writeKeys[i].taskID != writeKeys[j].taskID {
			return writeKeys[i].taskID < writeKeys[j].taskID
		}
		return writeKeys[i].index < writeKeys[j].index
	})
	pending := make([]checkpoint.PendingWrite, 0, len(writeKeys))
	for _, writeKey := range writeKeys {
		write := writesByKey[writeKey]
		write.Value = checkpoint.CloneEncodedValue(write.Value)
		pending = append(pending, write)
	}

	var parent *checkpoint.Config
	if stored.parentID != "" {
		parent = &checkpoint.Config{
			ThreadID:     key.threadID,
			Namespace:    key.namespace,
			CheckpointID: stored.parentID,
		}
	}
	return checkpoint.Tuple{
		Config: checkpoint.Config{
			ThreadID:     key.threadID,
			Namespace:    key.namespace,
			CheckpointID: key.id,
		},
		Checkpoint:    value,
		Metadata:      metadata,
		ParentConfig:  parent,
		PendingWrites: pending,
	}, nil
}

func metadataMatches(metadata, filter checkpoint.Metadata) bool {
	for key, expected := range filter {
		actual, exists := metadata[key]
		if !exists || !reflect.DeepEqual(actual, expected) {
			return false
		}
	}
	return true
}

func ctxError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", checkpoint.ErrInvalidConfig)
	}
	return ctx.Err()
}

var _ checkpoint.Saver = (*Saver)(nil)
