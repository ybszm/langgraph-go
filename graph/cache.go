package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	cachepkg "github.com/wahanbo/langgraph-go/cache"
	"github.com/wahanbo/langgraph-go/checkpoint"
)

// CachePolicy controls task-result caching for a node.
type CachePolicy[S any] struct {
	KeyFunc func(S) (string, error)
	TTL     time.Duration
}

type nodeCachePolicy struct {
	key func(any) (string, error)
	ttl time.Duration
}

// WithCachePolicy enables result caching for one node.
func WithCachePolicy[S any](policy CachePolicy[S]) NodeOption {
	return func(options *nodeOptions) error {
		if policy.TTL < 0 {
			return fmt.Errorf("%w: cache TTL cannot be negative", ErrInvalidGraph)
		}
		keyFunc := policy.KeyFunc
		if keyFunc == nil {
			keyFunc = func(state S) (string, error) {
				data, err := json.Marshal(state)
				if err != nil {
					return "", err
				}
				digest := sha256.Sum256(data)
				return hex.EncodeToString(digest[:]), nil
			}
		}
		options.cachePolicy = &nodeCachePolicy{
			ttl: policy.TTL,
			key: func(value any) (string, error) {
				state, ok := value.(S)
				if !ok {
					return "", fmt.Errorf("cache state has type %T", value)
				}
				return keyFunc(state)
			},
		}
		return nil
	}
}

// TaskCacheConfig supplies the cache backend and delta codec.
type TaskCacheConfig[D any] struct {
	Store      cachepkg.Store
	Namespace  string
	DeltaCodec checkpoint.Codec[D]
	// StateCodec is optional for ordinary commands and required when cached
	// task results contain Command Sends.
	StateCodec any
}

type cacheRuntime[S, D any] struct {
	store      cachepkg.Store
	namespace  string
	deltaCodec checkpoint.Codec[D]
	stateCodec checkpoint.Codec[S]
}

// WithTaskCache enables compiled-graph task caching.
func WithTaskCache[S, D any](config TaskCacheConfig[D]) CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		if config.Store == nil || config.DeltaCodec == nil || config.Namespace == "" {
			return fmt.Errorf("%w: task cache requires store, namespace, and delta codec", ErrInvalidGraph)
		}
		if target.cache != nil {
			return fmt.Errorf("%w: task cache configured more than once", ErrInvalidGraph)
		}
		var stateCodec checkpoint.Codec[S]
		if config.StateCodec != nil {
			var ok bool
			stateCodec, ok = config.StateCodec.(checkpoint.Codec[S])
			if !ok {
				return fmt.Errorf("%w: task cache state codec has incompatible type", ErrInvalidGraph)
			}
		}
		target.cache = &cacheRuntime[S, D]{
			store: config.Store, namespace: config.Namespace, deltaCodec: config.DeltaCodec,
			stateCodec: stateCodec,
		}
		return nil
	}
}

func (g *CompiledGraph[S, D]) cacheKey(node NodeID, state S) (cachepkg.Key, bool, error) {
	policy := g.cachePolicies[node]
	if g.cache == nil || policy == nil {
		return cachepkg.Key{}, false, nil
	}
	key, err := policy.key(any(state))
	if err != nil {
		return cachepkg.Key{}, false, err
	}
	if key == "" {
		return cachepkg.Key{}, false, fmt.Errorf("cache key for node %q is empty", node)
	}
	return cachepkg.Key{Namespace: g.cache.namespace + ":" + string(node), Key: key}, true, nil
}

type prefetchedCacheResult[D any] struct {
	result taskResult[D]
	found  bool
}

func (g *CompiledGraph[S, D]) getCachedBatch(
	ctx context.Context,
	tasks []scheduledTask,
	states []S,
	recovered map[string]taskResult[D],
) ([]prefetchedCacheResult[D], error) {
	result := make([]prefetchedCacheResult[D], len(tasks))
	if g.cache == nil {
		return result, nil
	}
	keys := make([]cachepkg.Key, len(tasks))
	enabled := make([]bool, len(tasks))
	unique := make([]cachepkg.Key, 0, len(tasks))
	seen := make(map[cachepkg.Key]struct{}, len(tasks))
	for index, task := range tasks {
		if _, restored := recovered[task.taskID]; restored {
			continue
		}
		key, active, err := g.cacheKey(task.node, states[index])
		if err != nil {
			return nil, fmt.Errorf("cache key for node %q: %w", task.node, err)
		}
		keys[index], enabled[index] = key, active
		if active {
			if _, duplicate := seen[key]; !duplicate {
				seen[key] = struct{}{}
				unique = append(unique, key)
			}
		}
	}
	if len(unique) == 0 {
		return result, nil
	}
	values, err := g.cache.store.Get(ctx, unique)
	if err != nil {
		return nil, err
	}
	for index, task := range tasks {
		if !enabled[index] {
			continue
		}
		data, exists := values[keys[index]]
		if !exists {
			continue
		}
		cached, err := g.decodeCached(task.node, task.taskID, data)
		if err != nil {
			return nil, fmt.Errorf("cache decode for node %q: %w", task.node, err)
		}
		result[index] = prefetchedCacheResult[D]{result: cached, found: true}
	}
	return result, nil
}

func (g *CompiledGraph[S, D]) decodeCached(node NodeID, taskID string, data []byte) (taskResult[D], error) {
	var encoded checkpoint.EncodedValue
	if err := json.Unmarshal(data, &encoded); err != nil {
		return taskResult[D]{}, err
	}
	persistence := persistenceRuntime[S, D]{deltaCodec: g.cache.deltaCodec, stateCodec: g.cache.stateCodec}
	result, err := persistence.decodeTaskResult(taskID, encoded)
	if err != nil {
		return taskResult[D]{}, err
	}
	if result.node != node {
		return taskResult[D]{}, fmt.Errorf("cached node %q does not match %q", result.node, node)
	}
	result.cached = true
	return result, nil
}

func (g *CompiledGraph[S, D]) prepareCached(
	state S, result taskResult[D],
) (cachepkg.Key, cachepkg.Item, bool, error) {
	key, enabled, err := g.cacheKey(result.node, state)
	if err != nil || !enabled {
		return cachepkg.Key{}, cachepkg.Item{}, false, err
	}
	persistence := persistenceRuntime[S, D]{deltaCodec: g.cache.deltaCodec, stateCodec: g.cache.stateCodec}
	encoded, err := persistence.encodeTaskResult(result)
	if err != nil {
		return cachepkg.Key{}, cachepkg.Item{}, false, err
	}
	data, err := json.Marshal(encoded)
	if err != nil {
		return cachepkg.Key{}, cachepkg.Item{}, false, err
	}
	return key, cachepkg.Item{Data: data, TTL: g.cachePolicies[result.node].ttl}, true, nil
}
