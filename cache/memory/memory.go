// Package memory provides an in-process task cache.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ybszm/langgraph-go/cache"
)

type entry struct {
	data      []byte
	expiresAt time.Time
}

// Store is a concurrency-safe in-memory cache with lazy TTL eviction.
type Store struct {
	mu    sync.Mutex
	now   func() time.Time
	items map[cache.Key]entry
}

// New creates an empty cache.
func New() *Store {
	return NewWithClock(time.Now)
}

// NewWithClock creates a cache with an injectable clock for tests.
func NewWithClock(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{now: now, items: make(map[cache.Key]entry)}
}

// Get implements cache.Store.
func (s *Store) Get(ctx context.Context, keys []cache.Key) (map[cache.Key][]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	result := make(map[cache.Key][]byte)
	for _, key := range keys {
		stored, exists := s.items[key]
		if !exists {
			continue
		}
		if !stored.expiresAt.IsZero() && !now.Before(stored.expiresAt) {
			delete(s.items, key)
			continue
		}
		result[key] = append([]byte(nil), stored.data...)
	}
	return result, nil
}

// Set implements cache.Store.
func (s *Store) Set(ctx context.Context, items map[cache.Key]cache.Item) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for key, item := range items {
		stored := entry{data: append([]byte(nil), item.Data...)}
		if item.TTL < 0 {
			return fmt.Errorf("cache TTL cannot be negative")
		}
		if item.TTL > 0 {
			stored.expiresAt = now.Add(item.TTL)
		}
		s.items[key] = stored
	}
	return nil
}

// Clear implements cache.Store. Nil namespaces clears every entry.
func (s *Store) Clear(ctx context.Context, namespaces []string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if namespaces == nil {
		clear(s.items)
		return nil
	}
	selected := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		selected[namespace] = struct{}{}
	}
	for key := range s.items {
		if _, exists := selected[key.Namespace]; exists {
			delete(s.items, key)
		}
	}
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("cache context is nil")
	}
	return ctx.Err()
}

var _ cache.Store = (*Store)(nil)
