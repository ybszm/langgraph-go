package checkpoint

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// MonotonicIDGenerator creates lexically sortable IDs within one generator.
// A random process suffix prevents practical collisions across compiled graphs.
type MonotonicIDGenerator struct {
	mu     sync.Mutex
	lastNS int64
	suffix string
}

// NewMonotonicIDGenerator creates a checkpoint ID generator.
func NewMonotonicIDGenerator() (*MonotonicIDGenerator, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("create checkpoint ID generator: %w", err)
	}
	return &MonotonicIDGenerator{suffix: hex.EncodeToString(random)}, nil
}

// NewID implements IDGenerator.
func (g *MonotonicIDGenerator) NewID(now time.Time) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	nanoseconds := now.UTC().UnixNano()
	if nanoseconds <= g.lastNS {
		nanoseconds = g.lastNS + 1
	}
	g.lastNS = nanoseconds
	return fmt.Sprintf("%020d-%s", nanoseconds, g.suffix), nil
}
