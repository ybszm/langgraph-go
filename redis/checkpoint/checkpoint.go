// Package checkpoint implements durable graph checkpoints with Redis.
package checkpoint

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	redis "github.com/redis/go-redis/v9"
	lgcheckpoint "github.com/wahanbo/langgraph-go/checkpoint"
)

type Options struct{ Prefix string }

type Saver struct {
	client redis.UniversalClient
	prefix string
}

type storedRecord struct {
	Config       lgcheckpoint.Config     `json:"config"`
	Checkpoint   lgcheckpoint.Checkpoint `json:"checkpoint"`
	Metadata     json.RawMessage         `json:"metadata,omitempty"`
	ParentConfig *lgcheckpoint.Config    `json:"parent_config,omitempty"`
}

func New(client redis.UniversalClient, options Options) (*Saver, error) {
	if client == nil {
		return nil, errors.New("redis checkpoint client is nil")
	}
	prefix := strings.TrimSuffix(options.Prefix, ":")
	if prefix == "" {
		prefix = "langgraph"
	}
	return &Saver{client: client, prefix: prefix + ":checkpoint"}, nil
}

func (s *Saver) Put(ctx context.Context, parent lgcheckpoint.Config, value lgcheckpoint.Checkpoint, metadata lgcheckpoint.Metadata, _ map[string]string) (lgcheckpoint.Config, error) {
	if err := contextError(ctx); err != nil {
		return lgcheckpoint.Config{}, err
	}
	if err := parent.Validate(); err != nil {
		return lgcheckpoint.Config{}, err
	}
	if err := value.Validate(); err != nil {
		return lgcheckpoint.Config{}, err
	}
	clonedMetadata, err := lgcheckpoint.CloneMetadata(metadata)
	if err != nil {
		return lgcheckpoint.Config{}, err
	}
	metadataData, err := json.Marshal(clonedMetadata)
	if err != nil {
		return lgcheckpoint.Config{}, err
	}
	config := lgcheckpoint.Config{ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID}
	var parentConfig *lgcheckpoint.Config
	if parent.CheckpointID != "" {
		copy := parent
		parentConfig = &copy
	}
	record := storedRecord{Config: config, Checkpoint: lgcheckpoint.CloneCheckpoint(value), Metadata: metadataData, ParentConfig: parentConfig}
	data, err := json.Marshal(record)
	if err != nil {
		return lgcheckpoint.Config{}, fmt.Errorf("encode Redis checkpoint: %w", err)
	}
	key := s.recordKey(config)
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, key, data, 0)
		pipe.SAdd(ctx, s.indexKey(), key)
		pipe.SAdd(ctx, s.threadKey(config.ThreadID), key)
		pipe.SAdd(ctx, s.namespaceKey(config.ThreadID, config.Namespace), key)
		return nil
	})
	if err != nil {
		return lgcheckpoint.Config{}, fmt.Errorf("put Redis checkpoint: %w", err)
	}
	return config, nil
}

func (s *Saver) PutWrites(ctx context.Context, config lgcheckpoint.Config, writes []lgcheckpoint.PendingWrite) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if config.CheckpointID == "" {
		return fmt.Errorf("%w: checkpoint ID is empty for pending writes", lgcheckpoint.ErrInvalidConfig)
	}
	exists, err := s.client.Exists(ctx, s.recordKey(config)).Result()
	if err != nil {
		return fmt.Errorf("check Redis checkpoint: %w", err)
	}
	if exists == 0 {
		return fmt.Errorf("%w: pending-write checkpoint %q does not exist", lgcheckpoint.ErrInvalidConfig, config.CheckpointID)
	}
	type encodedWrite struct {
		field   string
		data    []byte
		replace bool
	}
	prepared := make([]encodedWrite, len(writes))
	for i, write := range writes {
		if write.TaskID == "" || write.Channel == "" {
			return fmt.Errorf("%w: pending write has an empty task or channel", lgcheckpoint.ErrInvalidCheckpoint)
		}
		data, err := json.Marshal(write)
		if err != nil {
			return fmt.Errorf("encode pending write: %w", err)
		}
		prepared[i] = encodedWrite{field: writeField(write.TaskID, write.Index), data: data, replace: write.Index < 0}
	}
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, write := range prepared {
			if write.replace {
				pipe.HSet(ctx, s.writesKey(config), write.field, write.data)
			} else {
				pipe.HSetNX(ctx, s.writesKey(config), write.field, write.data)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("put Redis pending writes: %w", err)
	}
	return nil
}

func (s *Saver) GetTuple(ctx context.Context, config lgcheckpoint.Config) (lgcheckpoint.Tuple, bool, error) {
	if err := contextError(ctx); err != nil {
		return lgcheckpoint.Tuple{}, false, err
	}
	if err := config.Validate(); err != nil {
		return lgcheckpoint.Tuple{}, false, err
	}
	if config.CheckpointID == "" {
		members, err := s.client.SMembers(ctx, s.namespaceKey(config.ThreadID, config.Namespace)).Result()
		if err != nil {
			return lgcheckpoint.Tuple{}, false, fmt.Errorf("list Redis checkpoints: %w", err)
		}
		var latest string
		for _, key := range members {
			decoded, ok := decodeRecordKey(s.prefix, key)
			if ok && (latest == "" || decoded.CheckpointID > latest) {
				latest = decoded.CheckpointID
			}
		}
		if latest == "" {
			return lgcheckpoint.Tuple{}, false, nil
		}
		config.CheckpointID = latest
	}
	return s.getExact(ctx, config)
}

func (s *Saver) getExact(ctx context.Context, config lgcheckpoint.Config) (lgcheckpoint.Tuple, bool, error) {
	data, err := s.client.Get(ctx, s.recordKey(config)).Bytes()
	if errors.Is(err, redis.Nil) {
		return lgcheckpoint.Tuple{}, false, nil
	}
	if err != nil {
		return lgcheckpoint.Tuple{}, false, fmt.Errorf("get Redis checkpoint: %w", err)
	}
	var record storedRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return lgcheckpoint.Tuple{}, false, fmt.Errorf("decode Redis checkpoint: %w", err)
	}
	metadata, err := decodeMetadata(record.Metadata)
	if err != nil {
		return lgcheckpoint.Tuple{}, false, err
	}
	encodedWrites, err := s.client.HVals(ctx, s.writesKey(config)).Result()
	if err != nil {
		return lgcheckpoint.Tuple{}, false, fmt.Errorf("get Redis pending writes: %w", err)
	}
	writes := make([]lgcheckpoint.PendingWrite, 0, len(encodedWrites))
	for _, encoded := range encodedWrites {
		var write lgcheckpoint.PendingWrite
		if err := json.Unmarshal([]byte(encoded), &write); err != nil {
			return lgcheckpoint.Tuple{}, false, fmt.Errorf("decode Redis pending write: %w", err)
		}
		writes = append(writes, write)
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].TaskID != writes[j].TaskID {
			return writes[i].TaskID < writes[j].TaskID
		}
		return writes[i].Index < writes[j].Index
	})
	return lgcheckpoint.Tuple{Config: record.Config, Checkpoint: lgcheckpoint.CloneCheckpoint(record.Checkpoint), Metadata: metadata, ParentConfig: record.ParentConfig, PendingWrites: writes}, true, nil
}

func (s *Saver) List(ctx context.Context, options lgcheckpoint.ListOptions) ([]lgcheckpoint.Tuple, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit < 0 {
		return nil, fmt.Errorf("%w: list limit cannot be negative", lgcheckpoint.ErrInvalidConfig)
	}
	if options.Config != nil && options.Config.ThreadID == "" {
		return nil, fmt.Errorf("%w: list thread ID is empty", lgcheckpoint.ErrInvalidConfig)
	}
	index := s.indexKey()
	if options.Config != nil {
		if options.AllNamespaces {
			index = s.threadKey(options.Config.ThreadID)
		} else {
			index = s.namespaceKey(options.Config.ThreadID, options.Config.Namespace)
		}
	}
	members, err := s.client.SMembers(ctx, index).Result()
	if err != nil {
		return nil, fmt.Errorf("list Redis checkpoint index: %w", err)
	}
	configs := make([]lgcheckpoint.Config, 0, len(members))
	for _, key := range members {
		config, ok := decodeRecordKey(s.prefix, key)
		if !ok {
			continue
		}
		if options.Config != nil && options.Config.CheckpointID != "" && config.CheckpointID != options.Config.CheckpointID {
			continue
		}
		if options.Before != nil && options.Before.CheckpointID != "" && config.CheckpointID >= options.Before.CheckpointID {
			continue
		}
		configs = append(configs, config)
	}
	sort.Slice(configs, func(i, j int) bool {
		if configs[i].CheckpointID != configs[j].CheckpointID {
			return configs[i].CheckpointID > configs[j].CheckpointID
		}
		if configs[i].ThreadID != configs[j].ThreadID {
			return configs[i].ThreadID < configs[j].ThreadID
		}
		return configs[i].Namespace < configs[j].Namespace
	})
	result := make([]lgcheckpoint.Tuple, 0, len(configs))
	for _, config := range configs {
		tuple, found, err := s.getExact(ctx, config)
		if err != nil {
			return nil, err
		}
		if !found || !metadataMatches(tuple.Metadata, options.Filter) {
			continue
		}
		result = append(result, tuple)
		if options.Limit > 0 && len(result) == options.Limit {
			break
		}
	}
	return result, nil
}

func (s *Saver) DeleteThread(ctx context.Context, threadID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if threadID == "" {
		return fmt.Errorf("%w: thread ID is empty", lgcheckpoint.ErrInvalidConfig)
	}
	threadIndex := s.threadKey(threadID)
	members, err := s.client.SMembers(ctx, threadIndex).Result()
	if err != nil {
		return fmt.Errorf("list Redis thread checkpoints: %w", err)
	}
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range members {
			config, ok := decodeRecordKey(s.prefix, key)
			if !ok {
				continue
			}
			pipe.Del(ctx, key, s.writesKey(config))
			pipe.SRem(ctx, s.indexKey(), key)
			pipe.SRem(ctx, s.namespaceKey(config.ThreadID, config.Namespace), key)
		}
		pipe.Del(ctx, threadIndex)
		return nil
	})
	if err != nil {
		return fmt.Errorf("delete Redis checkpoint thread: %w", err)
	}
	return nil
}

func (s *Saver) recordKey(c lgcheckpoint.Config) string {
	return s.prefix + ":record:" + encode(c.ThreadID) + ":" + encode(c.Namespace) + ":" + encode(c.CheckpointID)
}
func (s *Saver) writesKey(c lgcheckpoint.Config) string { return s.recordKey(c) + ":writes" }
func (s *Saver) indexKey() string                       { return s.prefix + ":index" }
func (s *Saver) threadKey(thread string) string         { return s.prefix + ":thread:" + encode(thread) }
func (s *Saver) namespaceKey(thread, namespace string) string {
	return s.prefix + ":namespace:" + encode(thread) + ":" + encode(namespace)
}
func encode(v string) string { return base64.RawURLEncoding.EncodeToString([]byte(v)) }
func decode(v string) (string, bool) {
	data, err := base64.RawURLEncoding.DecodeString(v)
	return string(data), err == nil
}
func decodeRecordKey(prefix, key string) (lgcheckpoint.Config, bool) {
	parts := strings.Split(strings.TrimPrefix(key, prefix+":record:"), ":")
	if len(parts) != 3 || !strings.HasPrefix(key, prefix+":record:") {
		return lgcheckpoint.Config{}, false
	}
	thread, ok1 := decode(parts[0])
	namespace, ok2 := decode(parts[1])
	id, ok3 := decode(parts[2])
	return lgcheckpoint.Config{ThreadID: thread, Namespace: namespace, CheckpointID: id}, ok1 && ok2 && ok3
}
func writeField(task string, index int) string { return encode(task) + ":" + strconv.Itoa(index) }
func metadataMatches(metadata, filter lgcheckpoint.Metadata) bool {
	for key, expected := range filter {
		actual, ok := metadata[key]
		if !ok || !reflect.DeepEqual(actual, expected) {
			return false
		}
	}
	return true
}
func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", lgcheckpoint.ErrInvalidConfig)
	}
	return ctx.Err()
}

func decodeMetadata(data []byte) (lgcheckpoint.Metadata, error) {
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode Redis checkpoint metadata: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode Redis checkpoint metadata trailing data")
	}
	return lgcheckpoint.Metadata(normalizeNumbers(value).(map[string]any)), nil
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
		number, err := typed.Float64()
		if err == nil {
			return number
		}
		return text
	case map[string]any:
		for key, item := range typed {
			typed[key] = normalizeNumbers(item)
		}
		return typed
	case []any:
		for i, item := range typed {
			typed[i] = normalizeNumbers(item)
		}
		return typed
	default:
		return value
	}
}

var _ lgcheckpoint.Saver = (*Saver)(nil)
