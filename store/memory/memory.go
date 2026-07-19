// Package memory provides a concurrency-safe, process-local Store.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wahanbo/langgraph-go/store"
)

type itemKey struct {
	namespace string
	key       string
}

type Store struct {
	mu    sync.RWMutex
	items map[itemKey]store.Item
	now   func() time.Time
}

func New() *Store {
	return &Store{items: make(map[itemKey]store.Item), now: func() time.Time { return time.Now().UTC() }}
}

func (s *Store) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return nil, err
	}
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *Store) Search(ctx context.Context, prefix store.Namespace, options store.SearchOptions) ([]store.SearchItem, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit == 0 {
		options.Limit = 10
	}
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: prefix, Filter: options.Filter, Limit: options.Limit,
		Offset: options.Offset, Query: options.Query,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *Store) Put(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	if value == nil {
		return fmt.Errorf("%w: Put value is nil; use Delete", store.ErrInvalidOperation)
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value}})
	return err
}

func (s *Store) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key}})
	return err
}

func (s *Store) ListNamespaces(ctx context.Context, options store.ListNamespacesOptions) ([]store.Namespace, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit == 0 {
		options.Limit = 100
	}
	conditions := make([]store.MatchCondition, 0, 2)
	if len(options.Prefix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchPrefix, Path: options.Prefix})
	}
	if len(options.Suffix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchSuffix, Path: options.Suffix})
	}
	results, err := s.Batch(ctx, []store.Operation{store.ListNamespacesOp{
		MatchConditions: conditions, MaxDepth: options.MaxDepth, Limit: options.Limit, Offset: options.Offset,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Namespaces, nil
}

// Batch evaluates reads against one locked pre-write snapshot, deduplicates
// writes by namespace/key with last-write-wins, then applies writes. Result
// order always matches operation order.
func (s *Store) Batch(ctx context.Context, operations []store.Operation) ([]store.Result, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	results := make([]store.Result, len(operations))
	puts := make(map[itemKey]store.PutOp)
	putOrder := make([]itemKey, 0)

	s.mu.Lock()
	defer s.mu.Unlock()
	for index, operation := range operations {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		switch op := operation.(type) {
		case store.GetOp:
			if item, exists := s.items[makeKey(op.Namespace, op.Key)]; exists {
				clone, err := cloneItem(item)
				if err != nil {
					return nil, err
				}
				results[index].Item = &clone
			}
		case store.SearchOp:
			items, err := s.searchLocked(op)
			if err != nil {
				return nil, err
			}
			results[index].Items = items
		case store.ListNamespacesOp:
			namespaces, err := s.listLocked(op)
			if err != nil {
				return nil, err
			}
			results[index].Namespaces = namespaces
		case store.PutOp:
			if op.TTL != 0 {
				return nil, fmt.Errorf("%w: MemoryStore", store.ErrUnsupportedTTL)
			}
			key := makeKey(op.Namespace, op.Key)
			if _, exists := puts[key]; !exists {
				putOrder = append(putOrder, key)
			}
			puts[key] = op
		default:
			return nil, fmt.Errorf("%w: %T", store.ErrInvalidOperation, operation)
		}
	}
	prepared := make(map[itemKey]store.Value, len(puts))
	for key, op := range puts {
		if op.Value == nil {
			continue
		}
		value, err := cloneValue(op.Value)
		if err != nil {
			return nil, err
		}
		prepared[key] = value
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	for _, key := range putOrder {
		op := puts[key]
		if op.Value == nil {
			delete(s.items, key)
			continue
		}
		now := s.now().UTC()
		createdAt := now
		if existing, exists := s.items[key]; exists {
			createdAt = existing.CreatedAt
		}
		s.items[key] = store.Item{
			Namespace: cloneNamespace(op.Namespace), Key: op.Key, Value: prepared[key],
			CreatedAt: createdAt, UpdatedAt: now,
		}
	}
	return results, nil
}

func (s *Store) searchLocked(op store.SearchOp) ([]store.SearchItem, error) {
	if op.Limit < 0 || op.Offset < 0 {
		return nil, fmt.Errorf("%w: negative search pagination", store.ErrInvalidOperation)
	}
	// Without an embedding index, query is intentionally scoreless and does
	// not change structured filtering, matching upstream InMemoryStore.
	candidates := make([]store.Item, 0)
	for _, item := range s.items {
		if !hasPrefix(item.Namespace, op.NamespacePrefix) {
			continue
		}
		matched, err := matchesFilter(item.Value, op.Filter)
		if err != nil {
			return nil, err
		}
		if matched {
			candidates = append(candidates, item)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := namespaceString(candidates[i].Namespace), namespaceString(candidates[j].Namespace)
		if a != b {
			return a < b
		}
		return candidates[i].Key < candidates[j].Key
	})
	limit := op.Limit
	if limit == 0 {
		limit = 10
	}
	start := min(op.Offset, len(candidates))
	end := min(start+limit, len(candidates))
	result := make([]store.SearchItem, 0, end-start)
	for _, item := range candidates[start:end] {
		clone, err := cloneItem(item)
		if err != nil {
			return nil, err
		}
		result = append(result, store.SearchItem{Item: clone})
	}
	return result, nil
}

func (s *Store) listLocked(op store.ListNamespacesOp) ([]store.Namespace, error) {
	if op.Limit < 0 || op.Offset < 0 || op.MaxDepth < 0 {
		return nil, fmt.Errorf("%w: negative namespace pagination/depth", store.ErrInvalidOperation)
	}
	set := make(map[string]store.Namespace)
	for _, item := range s.items {
		namespace := item.Namespace
		matched := true
		for _, condition := range op.MatchConditions {
			ok, err := matchNamespace(namespace, condition)
			if err != nil {
				return nil, err
			}
			if !ok {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if op.MaxDepth > 0 && len(namespace) > op.MaxDepth {
			namespace = namespace[:op.MaxDepth]
		}
		set[namespaceString(namespace)] = cloneNamespace(namespace)
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	limit := op.Limit
	if limit == 0 {
		limit = 100
	}
	start := min(op.Offset, len(keys))
	end := min(start+limit, len(keys))
	result := make([]store.Namespace, 0, end-start)
	for _, key := range keys[start:end] {
		result = append(result, set[key])
	}
	return result, nil
}

func matchesFilter(value, filter store.Value) (bool, error) {
	for key, expected := range filter {
		matched, err := compareValues(value[key], expected)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func compareValues(actual, expected any) (bool, error) {
	if object, ok := objectValue(expected); ok {
		hasOperator := false
		for key := range object {
			if strings.HasPrefix(key, "$") {
				hasOperator = true
				break
			}
		}
		if hasOperator {
			for operator, operand := range object {
				matched, err := applyOperator(actual, operator, operand)
				if err != nil || !matched {
					return matched, err
				}
			}
			return true, nil
		}
		actualObject, ok := objectValue(actual)
		if !ok {
			return false, nil
		}
		for key, nested := range object {
			matched, err := compareValues(actualObject[key], nested)
			if err != nil || !matched {
				return matched, err
			}
		}
		return true, nil
	}
	return reflect.DeepEqual(actual, expected), nil
}

func objectValue(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case store.Value:
		return map[string]any(typed), true
	default:
		return nil, false
	}
}

func applyOperator(actual any, operator string, operand any) (bool, error) {
	switch operator {
	case "$eq":
		return reflect.DeepEqual(actual, operand), nil
	case "$ne":
		return !reflect.DeepEqual(actual, operand), nil
	case "$gt", "$gte", "$lt", "$lte":
		a, err := numeric(actual)
		if err != nil {
			return false, err
		}
		b, err := numeric(operand)
		if err != nil {
			return false, err
		}
		switch operator {
		case "$gt":
			return a > b, nil
		case "$gte":
			return a >= b, nil
		case "$lt":
			return a < b, nil
		default:
			return a <= b, nil
		}
	default:
		return false, fmt.Errorf("%w: filter operator %q", store.ErrUnsupportedQuery, operator)
	}
}

func numeric(value any) (float64, error) {
	ref := reflect.ValueOf(value)
	if !ref.IsValid() {
		return 0, fmt.Errorf("%w: nil is not numeric", store.ErrUnsupportedQuery)
	}
	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(ref.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(ref.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return ref.Float(), nil
	case reflect.String:
		result, err := strconv.ParseFloat(ref.String(), 64)
		if err == nil {
			return result, nil
		}
	}
	return 0, fmt.Errorf("%w: %T is not numeric", store.ErrUnsupportedQuery, value)
}

func matchNamespace(namespace store.Namespace, condition store.MatchCondition) (bool, error) {
	if len(namespace) < len(condition.Path) {
		return false, nil
	}
	start := 0
	switch condition.Type {
	case store.MatchPrefix:
	case store.MatchSuffix:
		start = len(namespace) - len(condition.Path)
	default:
		return false, fmt.Errorf("%w: namespace match type %q", store.ErrUnsupportedQuery, condition.Type)
	}
	for index, part := range condition.Path {
		if part != "*" && namespace[start+index] != part {
			return false, nil
		}
	}
	return true, nil
}

func cloneItem(item store.Item) (store.Item, error) {
	value, err := cloneValue(item.Value)
	if err != nil {
		return store.Item{}, err
	}
	item.Namespace, item.Value = cloneNamespace(item.Namespace), value
	return item, nil
}

func cloneValue(value store.Value) (store.Value, error) {
	_, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: value is not JSON serializable: %v", store.ErrInvalidOperation, err)
	}
	result := make(store.Value, len(value))
	for key, item := range value {
		result[key] = cloneJSON(item)
	}
	return result, nil
}

func cloneJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = cloneJSON(item)
		}
		return result
	case store.Value:
		result := make(store.Value, len(typed))
		for key, item := range typed {
			result[key] = cloneJSON(item)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneJSON(item)
		}
		return result
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}

func validatePublic(ctx context.Context, namespace store.Namespace, key string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := store.ValidateNamespace(namespace); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: key is empty", store.ErrInvalidOperation)
	}
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", store.ErrInvalidOperation)
	}
	return ctx.Err()
}

func makeKey(namespace store.Namespace, key string) itemKey {
	return itemKey{namespace: namespaceString(namespace), key: key}
}
func namespaceString(namespace store.Namespace) string { return strings.Join(namespace, "\x00") }
func cloneNamespace(namespace store.Namespace) store.Namespace {
	return append(store.Namespace(nil), namespace...)
}
func hasPrefix(namespace, prefix store.Namespace) bool {
	if len(namespace) < len(prefix) {
		return false
	}
	for index := range prefix {
		if namespace[index] != prefix[index] {
			return false
		}
	}
	return true
}

var _ store.Store = (*Store)(nil)
