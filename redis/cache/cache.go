// Package cache implements the graph task-result cache contract with Redis.
package cache

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	redis "github.com/redis/go-redis/v9"
	lgcache "github.com/wahanbo/langgraph-go/cache"
)

type Options struct{ Prefix string }

type Store struct {
	client redis.UniversalClient
	prefix string
}

func New(client redis.UniversalClient, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("redis cache client is nil")
	}
	prefix := strings.TrimSuffix(options.Prefix, ":")
	if prefix == "" {
		prefix = "langgraph"
	}
	return &Store{client: client, prefix: prefix + ":cache"}, nil
}

func (s *Store) Get(ctx context.Context, keys []lgcache.Key) (map[lgcache.Key][]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return map[lgcache.Key][]byte{}, nil
	}
	redisKeys := make([]string, len(keys))
	for i, key := range keys {
		if err := validateKey(key); err != nil {
			return nil, err
		}
		redisKeys[i] = s.itemKey(key)
	}
	values, err := s.client.MGet(ctx, redisKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis cache get: %w", err)
	}
	result := make(map[lgcache.Key][]byte)
	for i, value := range values {
		if value != nil {
			result[keys[i]] = []byte(value.(string))
		}
	}
	return result, nil
}

func (s *Store) Set(ctx context.Context, items map[lgcache.Key]lgcache.Item) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	for key, item := range items {
		if err := validateKey(key); err != nil {
			return err
		}
		if item.TTL < 0 {
			return errors.New("cache TTL cannot be negative")
		}
	}
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, item := range items {
			itemKey := s.itemKey(key)
			pipe.Set(ctx, itemKey, item.Data, item.TTL)
			pipe.SAdd(ctx, s.namespaceKey(key.Namespace), itemKey)
			pipe.SAdd(ctx, s.namespaceIndexesKey(), s.namespaceKey(key.Namespace))
			pipe.SAdd(ctx, s.indexKey(), itemKey)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("redis cache set: %w", err)
	}
	return nil
}

func (s *Store) Clear(ctx context.Context, namespaces []string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if namespaces == nil {
		members, err := s.client.SMembers(ctx, s.indexKey()).Result()
		if err != nil {
			return fmt.Errorf("redis cache list: %w", err)
		}
		namespaceIndexes, indexErr := s.client.SMembers(ctx, s.namespaceIndexesKey()).Result()
		if indexErr != nil {
			return fmt.Errorf("redis cache namespace indexes: %w", indexErr)
		}
		targets := append(members, namespaceIndexes...)
		targets = append(targets, s.indexKey(), s.namespaceIndexesKey())
		if len(targets) > 0 {
			if err := s.client.Del(ctx, targets...).Err(); err != nil {
				return fmt.Errorf("redis cache clear: %w", err)
			}
		}
		return nil
	}
	for _, namespace := range namespaces {
		index := s.namespaceKey(namespace)
		members, err := s.client.SMembers(ctx, index).Result()
		if err != nil {
			return fmt.Errorf("redis cache namespace list: %w", err)
		}
		if len(members) == 0 {
			_ = s.client.Del(ctx, index).Err()
			continue
		}
		_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, members...)
			pipe.SRem(ctx, s.indexKey(), stringsToAny(members)...)
			pipe.SRem(ctx, s.namespaceIndexesKey(), index)
			pipe.Del(ctx, index)
			return nil
		})
		if err != nil {
			return fmt.Errorf("redis cache clear namespace: %w", err)
		}
	}
	return nil
}

func (s *Store) itemKey(key lgcache.Key) string {
	return s.prefix + ":item:" + encode(key.Namespace) + ":" + encode(key.Key)
}
func (s *Store) namespaceKey(namespace string) string {
	return s.prefix + ":namespace:" + encode(namespace)
}
func (s *Store) indexKey() string            { return s.prefix + ":index" }
func (s *Store) namespaceIndexesKey() string { return s.prefix + ":namespace-indexes" }
func encode(value string) string             { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
func stringsToAny(values []string) []any {
	result := make([]any, len(values))
	for i := range values {
		result[i] = values[i]
	}
	return result
}
func validateKey(key lgcache.Key) error {
	if key.Namespace == "" || key.Key == "" {
		return errors.New("cache namespace and key cannot be empty")
	}
	return nil
}
func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("cache context is nil")
	}
	return ctx.Err()
}

var _ lgcache.Store = (*Store)(nil)
