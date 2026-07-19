// Package cache defines task-result cache contracts independent from graph
// checkpoints.
package cache

import (
	"context"
	"time"
)

// Key identifies one cached task result.
type Key struct {
	Namespace string
	Key       string
}

// Item contains opaque serialized data and its TTL. A zero TTL never expires.
type Item struct {
	Data []byte
	TTL  time.Duration
}

// Store is a concurrency-safe task cache.
type Store interface {
	Get(ctx context.Context, keys []Key) (map[Key][]byte, error)
	Set(ctx context.Context, items map[Key]Item) error
	Clear(ctx context.Context, namespaces []string) error
}
