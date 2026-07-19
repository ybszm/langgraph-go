// Package checkpoint defines durable graph-state storage contracts.
package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// CurrentVersion is the current Go checkpoint envelope version.
	CurrentVersion = 1
	// StateChannel is the initial runtime channel containing the typed graph state.
	StateChannel = "__state__"
	// TaskResultChannel stores a completed node result as a pending write.
	TaskResultChannel = "__task_result__"
	// NodeErrorChannel stores a retry-exhausted source failure before its
	// configured error handler starts.
	NodeErrorChannel = "__node_error__"
	// InterruptChannel stores task-scoped interrupt prompts and resume values.
	InterruptChannel = "__interrupt__"
	// SubgraphChannel links an interrupted parent task to the child checkpoint
	// namespace that owns the actual interrupt controls.
	SubgraphChannel = "__subgraph__"
	// StaticInterruptChannel stores compile-time interrupt-before gates.
	StaticInterruptChannel = "__static_interrupt__"
)

var (
	// ErrInvalidConfig indicates an incomplete or invalid checkpoint config.
	ErrInvalidConfig = errors.New("invalid checkpoint config")
	// ErrInvalidCheckpoint indicates a malformed checkpoint envelope.
	ErrInvalidCheckpoint = errors.New("invalid checkpoint")
	// ErrCodecMismatch indicates that encoded data does not match a codec.
	ErrCodecMismatch = errors.New("checkpoint codec mismatch")
	// ErrEncryption indicates invalid encryption configuration, entropy
	// failure, malformed ciphertext, or failed authenticated decryption.
	ErrEncryption = errors.New("checkpoint encryption failure")
	// ErrNotFound indicates that an explicitly requested checkpoint is absent.
	ErrNotFound = errors.New("checkpoint not found")
)

// Config identifies a checkpoint thread, namespace, and optional exact ID.
type Config struct {
	ThreadID     string
	Namespace    string
	CheckpointID string
}

// Validate checks the fields required for saver operations.
func (c Config) Validate() error {
	if c.ThreadID == "" {
		return fmt.Errorf("%w: thread ID is empty", ErrInvalidConfig)
	}
	return nil
}

// EncodedValue is a versioned, typed serialized value.
type EncodedValue struct {
	Type    string
	Version int
	Data    []byte
}

// Task identifies a node scheduled from a checkpoint.
type Task struct {
	ID           string
	Name         string
	Input        *EncodedValue
	Triggers     []string
	ReadChannels []string
}

// Checkpoint is a state snapshot at one super-step boundary.
type Checkpoint struct {
	Version         int
	ID              string
	Timestamp       time.Time
	Step            int
	Values          map[string]EncodedValue
	ChannelVersions map[string]string
	VersionsSeen    map[string]map[string]string
	UpdatedChannels []string
	Next            []Task
	Waiting         map[string][]string
}

// Validate checks the persistent checkpoint envelope.
func (c Checkpoint) Validate() error {
	if c.Version != CurrentVersion {
		return fmt.Errorf(
			"%w: version=%d, want=%d",
			ErrInvalidCheckpoint,
			c.Version,
			CurrentVersion,
		)
	}
	if c.ID == "" {
		return fmt.Errorf("%w: checkpoint ID is empty", ErrInvalidCheckpoint)
	}
	if c.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is zero", ErrInvalidCheckpoint)
	}
	for _, task := range c.Next {
		if task.ID == "" || task.Name == "" {
			return fmt.Errorf("%w: next task has an empty ID or name", ErrInvalidCheckpoint)
		}
		if task.Input != nil && (task.Input.Type == "" || task.Input.Version <= 0) {
			return fmt.Errorf("%w: next task has an invalid input envelope", ErrInvalidCheckpoint)
		}
		for _, trigger := range task.Triggers {
			if trigger == "" {
				return fmt.Errorf("%w: next task has an empty trigger", ErrInvalidCheckpoint)
			}
		}
		for _, channel := range task.ReadChannels {
			if channel == "" {
				return fmt.Errorf("%w: next task has an empty read channel", ErrInvalidCheckpoint)
			}
		}
	}
	return nil
}

// Source describes how a checkpoint was created.
type Source string

const (
	SourceInput  Source = "input"
	SourceLoop   Source = "loop"
	SourceUpdate Source = "update"
	SourceFork   Source = "fork"
)

// Metadata contains JSON-compatible checkpoint metadata. The standard keys
// are source, step, parents, and run_id; savers must preserve custom keys.
type Metadata map[string]any

// PendingWrite is an intermediate task result associated with a checkpoint.
type PendingWrite struct {
	TaskID   string
	TaskPath string
	Index    int
	Channel  string
	Value    EncodedValue
}

// Tuple contains a checkpoint and all storage metadata needed to resume it.
type Tuple struct {
	Config        Config
	Checkpoint    Checkpoint
	Metadata      Metadata
	ParentConfig  *Config
	PendingWrites []PendingWrite
}

// ListOptions controls checkpoint history queries. A nil Config searches all
// threads and namespaces. A non-empty Config.CheckpointID selects an exact ID.
type ListOptions struct {
	Config        *Config
	AllNamespaces bool
	Filter        Metadata
	Before        *Config
	Limit         int
}

// Saver is the synchronous Go checkpoint storage contract. Context provides
// cancellation for both in-memory and external implementations.
type Saver interface {
	GetTuple(ctx context.Context, config Config) (Tuple, bool, error)
	List(ctx context.Context, options ListOptions) ([]Tuple, error)
	Put(
		ctx context.Context,
		parent Config,
		checkpoint Checkpoint,
		metadata Metadata,
		newVersions map[string]string,
	) (Config, error)
	PutWrites(ctx context.Context, config Config, writes []PendingWrite) error
	DeleteThread(ctx context.Context, threadID string) error
}

// Codec serializes one typed state or delta into a versioned value.
type Codec[T any] interface {
	Encode(value T) (EncodedValue, error)
	Decode(value EncodedValue) (T, error)
}

// Clock supplies checkpoint timestamps.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now implements Clock.
func (f ClockFunc) Now() time.Time {
	return f()
}

// SystemClock returns the current UTC time.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}

// IDGenerator creates sortable checkpoint IDs.
type IDGenerator interface {
	NewID(now time.Time) (string, error)
}

// IDGeneratorFunc adapts a function to IDGenerator.
type IDGeneratorFunc func(time.Time) (string, error)

// NewID implements IDGenerator.
func (f IDGeneratorFunc) NewID(now time.Time) (string, error) {
	return f(now)
}
