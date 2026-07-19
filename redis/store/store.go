// Package store implements long-term graph memory with Redis.
package store

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
	"time"

	redis "github.com/redis/go-redis/v9"
	lgstore "github.com/wahanbo/langgraph-go/store"
)

type Options struct {
	Prefix string
	Clock  func() time.Time
}
type Store struct {
	client redis.UniversalClient
	key    string
	now    func() time.Time
}
type record struct {
	Namespace lgstore.Namespace `json:"namespace"`
	Key       string            `json:"key"`
	Value     json.RawMessage   `json:"value"`
	CreatedAt int64             `json:"created_at"`
	UpdatedAt int64             `json:"updated_at"`
	ExpiresAt int64             `json:"expires_at,omitempty"`
	TTLNanos  int64             `json:"ttl_nanos,omitempty"`
}

func New(client redis.UniversalClient, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("redis store client is nil")
	}
	prefix := strings.TrimSuffix(options.Prefix, ":")
	if prefix == "" {
		prefix = "langgraph"
	}
	now := options.Clock
	if now == nil {
		now = time.Now
	}
	return &Store{client: client, key: prefix + ":store:items", now: now}, nil
}

func (s *Store) Get(ctx context.Context, namespace lgstore.Namespace, key string) (*lgstore.Item, error) {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return nil, err
	}
	return s.get(ctx, namespace, key, false)
}
func (s *Store) Search(ctx context.Context, namespace lgstore.Namespace, options lgstore.SearchOptions) ([]lgstore.SearchItem, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit == 0 {
		options.Limit = 10
	}
	results, err := s.Batch(ctx, []lgstore.Operation{lgstore.SearchOp{NamespacePrefix: namespace, Filter: options.Filter, Query: options.Query, Limit: options.Limit, Offset: options.Offset}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}
func (s *Store) Put(ctx context.Context, namespace lgstore.Namespace, key string, value lgstore.Value) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	if value == nil {
		return fmt.Errorf("%w: Put value is nil; use Delete", lgstore.ErrInvalidOperation)
	}
	_, err := s.Batch(ctx, []lgstore.Operation{lgstore.PutOp{Namespace: namespace, Key: key, Value: value, TTLSet: true}})
	return err
}
func (s *Store) Delete(ctx context.Context, namespace lgstore.Namespace, key string) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	_, err := s.Batch(ctx, []lgstore.Operation{lgstore.PutOp{Namespace: namespace, Key: key}})
	return err
}
func (s *Store) ListNamespaces(ctx context.Context, options lgstore.ListNamespacesOptions) ([]lgstore.Namespace, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.Limit == 0 {
		options.Limit = 100
	}
	conditions := make([]lgstore.MatchCondition, 0, 2)
	if len(options.Prefix) > 0 {
		conditions = append(conditions, lgstore.MatchCondition{Type: lgstore.MatchPrefix, Path: options.Prefix})
	}
	if len(options.Suffix) > 0 {
		conditions = append(conditions, lgstore.MatchCondition{Type: lgstore.MatchSuffix, Path: options.Suffix})
	}
	results, err := s.Batch(ctx, []lgstore.Operation{lgstore.ListNamespacesOp{MatchConditions: conditions, MaxDepth: options.MaxDepth, Limit: options.Limit, Offset: options.Offset}})
	if err != nil {
		return nil, err
	}
	return results[0].Namespaces, nil
}

func (s *Store) Batch(ctx context.Context, operations []lgstore.Operation) ([]lgstore.Result, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	prepared, err := prepareOperations(operations)
	if err != nil {
		return nil, err
	}
	var result []lgstore.Result
	for attempt := 0; attempt < 256; attempt++ {
		err = s.client.Watch(ctx, func(tx *redis.Tx) error {
			encoded, err := tx.HGetAll(ctx, s.key).Result()
			if err != nil {
				return err
			}
			now := s.now().UTC()
			records, items, err := decodeSnapshot(encoded, now)
			if err != nil {
				return err
			}
			results := make([]lgstore.Result, len(operations))
			writes := make(map[string]*record)
			for index, operation := range operations {
				switch op := operation.(type) {
				case lgstore.GetOp:
					field := itemField(op.Namespace, op.Key)
					if item, ok := items[field]; ok {
						copy := cloneItem(item)
						results[index].Item = &copy
						if op.RefreshTTL && records[field].TTLNanos > 0 {
							refreshed := records[field]
							refreshed.ExpiresAt = now.Add(time.Duration(refreshed.TTLNanos)).UnixNano()
							writes[field] = &refreshed
						}
					}
				case lgstore.SearchOp:
					found, err := searchItems(items, op)
					if err != nil {
						return err
					}
					results[index].Items = found
					if op.RefreshTTL {
						for _, found := range found {
							field := itemField(found.Namespace, found.Key)
							current := records[field]
							if current.TTLNanos > 0 {
								current.ExpiresAt = now.Add(time.Duration(current.TTLNanos)).UnixNano()
								writes[field] = &current
							}
						}
					}
				case lgstore.ListNamespacesOp:
					listed, err := listNamespaces(items, op)
					if err != nil {
						return err
					}
					results[index].Namespaces = listed
				case lgstore.PutOp:
					// Applied after all reads so Batch observes one pre-write snapshot.
				default:
					return fmt.Errorf("%w: %T", lgstore.ErrInvalidOperation, operation)
				}
			}
			for index, operation := range operations {
				op, ok := operation.(lgstore.PutOp)
				if !ok {
					continue
				}
				field := itemField(op.Namespace, op.Key)
				if op.Value == nil {
					writes[field] = nil
					continue
				}
				current, exists := records[field]
				created := now.UnixNano()
				if exists {
					created = current.CreatedAt
				}
				next := record{Namespace: cloneNamespace(op.Namespace), Key: op.Key, Value: prepared[index], CreatedAt: created, UpdatedAt: now.UnixNano()}
				if op.TTLSet && op.TTL > 0 {
					next.TTLNanos = int64(op.TTL)
					next.ExpiresAt = now.Add(op.TTL).UnixNano()
				}
				writes[field] = &next
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				for field, next := range writes {
					if next == nil {
						pipe.HDel(ctx, s.key, field)
						continue
					}
					data, err := json.Marshal(next)
					if err != nil {
						return err
					}
					pipe.HSet(ctx, s.key, field, data)
				}
				return nil
			})
			if err == nil {
				result = results
			}
			return err
		}, s.key)
		if !errors.Is(err, redis.TxFailedErr) {
			break
		}
		if waitErr := waitForRetry(ctx, attempt); waitErr != nil {
			return nil, waitErr
		}
	}
	if err != nil {
		return nil, fmt.Errorf("Redis store batch: %w", err)
	}
	return result, nil
}

func (s *Store) PutWithTTL(ctx context.Context, namespace lgstore.Namespace, key string, value lgstore.Value, ttl time.Duration) error {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("%w: TTL must be positive", lgstore.ErrInvalidTTL)
	}
	if value == nil {
		return fmt.Errorf("%w: value is nil", lgstore.ErrInvalidOperation)
	}
	_, err := s.Batch(ctx, []lgstore.Operation{lgstore.PutOp{Namespace: namespace, Key: key, Value: value, TTL: ttl, TTLSet: true}})
	return err
}
func (s *Store) GetWithTTLRefresh(ctx context.Context, namespace lgstore.Namespace, key string, refresh bool) (*lgstore.Item, error) {
	if err := validatePublic(ctx, namespace, key); err != nil {
		return nil, err
	}
	return s.get(ctx, namespace, key, refresh)
}

func (s *Store) get(ctx context.Context, namespace lgstore.Namespace, key string, refresh bool) (*lgstore.Item, error) {
	field := itemField(namespace, key)
	if !refresh {
		data, err := s.client.HGet(ctx, s.key, field).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("Redis store get: %w", err)
		}
		item, _, found, err := decodeRecord(data, s.now().UTC())
		if err != nil || !found {
			return nil, err
		}
		return &item, nil
	}
	var result *lgstore.Item
	var err error
	for attempt := 0; attempt < 256; attempt++ {
		err = s.client.Watch(ctx, func(tx *redis.Tx) error {
			data, getErr := tx.HGet(ctx, s.key, field).Bytes()
			if errors.Is(getErr, redis.Nil) {
				result = nil
				return nil
			}
			if getErr != nil {
				return getErr
			}
			item, stored, found, decodeErr := decodeRecord(data, s.now().UTC())
			if decodeErr != nil {
				return decodeErr
			}
			if !found {
				result = nil
				return nil
			}
			result = &item
			if stored.TTLNanos <= 0 {
				return nil
			}
			stored.ExpiresAt = s.now().UTC().Add(time.Duration(stored.TTLNanos)).UnixNano()
			encoded, encodeErr := json.Marshal(stored)
			if encodeErr != nil {
				return encodeErr
			}
			_, transactionErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error { pipe.HSet(ctx, s.key, field, encoded); return nil })
			return transactionErr
		}, s.key)
		if !errors.Is(err, redis.TxFailedErr) {
			break
		}
		if waitErr := waitForRetry(ctx, attempt); waitErr != nil {
			return nil, waitErr
		}
	}
	if err != nil {
		return nil, fmt.Errorf("Redis store get with TTL refresh: %w", err)
	}
	return result, nil
}
func (s *Store) SearchWithTTLRefresh(ctx context.Context, namespace lgstore.Namespace, options lgstore.SearchOptions, refresh bool) ([]lgstore.SearchItem, error) {
	if options.Limit == 0 {
		options.Limit = 10
	}
	results, err := s.Batch(ctx, []lgstore.Operation{lgstore.SearchOp{NamespacePrefix: namespace, Filter: options.Filter, Query: options.Query, Limit: options.Limit, Offset: options.Offset, RefreshTTL: refresh, RefreshTTLSet: true}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}
func (s *Store) SweepExpired(ctx context.Context) (int64, error) {
	items, err := s.SweepExpiredItems(ctx)
	return int64(len(items)), err
}
func (s *Store) SweepExpiredItems(ctx context.Context) ([]lgstore.ItemIdentity, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	var removed []lgstore.ItemIdentity
	var err error
	for attempt := 0; attempt < 256; attempt++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			encoded, err := tx.HGetAll(ctx, s.key).Result()
			if err != nil {
				return err
			}
			now := s.now().UTC().UnixNano()
			fields := make([]string, 0)
			removed = nil
			for field, data := range encoded {
				var item record
				if err := json.Unmarshal([]byte(data), &item); err != nil {
					return err
				}
				if item.ExpiresAt > 0 && item.ExpiresAt <= now {
					fields = append(fields, field)
					removed = append(removed, lgstore.ItemIdentity{Namespace: cloneNamespace(item.Namespace), Key: item.Key})
				}
			}
			sort.Slice(removed, func(i, j int) bool {
				a, b := namespaceString(removed[i].Namespace), namespaceString(removed[j].Namespace)
				if a != b {
					return a < b
				}
				return removed[i].Key < removed[j].Key
			})
			if len(fields) == 0 {
				return nil
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error { pipe.HDel(ctx, s.key, fields...); return nil })
			return err
		}, s.key)
		if !errors.Is(err, redis.TxFailedErr) {
			break
		}
		if waitErr := waitForRetry(ctx, attempt); waitErr != nil {
			return nil, waitErr
		}
	}
	if err != nil {
		return nil, fmt.Errorf("Redis store sweep: %w", err)
	}
	return removed, nil
}

func prepareOperations(operations []lgstore.Operation) (map[int]json.RawMessage, error) {
	prepared := make(map[int]json.RawMessage)
	for index, operation := range operations {
		switch op := operation.(type) {
		case lgstore.GetOp:
			if op.Key == "" {
				return nil, fmt.Errorf("%w: key is empty", lgstore.ErrInvalidOperation)
			}
		case lgstore.SearchOp:
			if op.Limit < 0 || op.Offset < 0 {
				return nil, fmt.Errorf("%w: negative search pagination", lgstore.ErrInvalidOperation)
			}
		case lgstore.ListNamespacesOp:
			if op.Limit < 0 || op.Offset < 0 || op.MaxDepth < 0 {
				return nil, fmt.Errorf("%w: negative namespace pagination/depth", lgstore.ErrInvalidOperation)
			}
		case lgstore.PutOp:
			if op.Key == "" {
				return nil, fmt.Errorf("%w: key is empty", lgstore.ErrInvalidOperation)
			}
			if op.TTL < 0 {
				return nil, fmt.Errorf("%w: negative TTL", lgstore.ErrInvalidTTL)
			}
			if op.Value != nil {
				data, err := json.Marshal(op.Value)
				if err != nil {
					return nil, fmt.Errorf("%w: value is not JSON serializable: %v", lgstore.ErrInvalidOperation, err)
				}
				prepared[index] = data
			}
		default:
			return nil, fmt.Errorf("%w: %T", lgstore.ErrInvalidOperation, operation)
		}
	}
	return prepared, nil
}
func decodeSnapshot(encoded map[string]string, now time.Time) (map[string]record, map[string]lgstore.Item, error) {
	records := make(map[string]record)
	items := make(map[string]lgstore.Item)
	for field, data := range encoded {
		var stored record
		if err := json.Unmarshal([]byte(data), &stored); err != nil {
			return nil, nil, fmt.Errorf("decode Redis store record: %w", err)
		}
		if stored.ExpiresAt > 0 && stored.ExpiresAt <= now.UnixNano() {
			continue
		}
		value, err := decodeValue(stored.Value)
		if err != nil {
			return nil, nil, err
		}
		records[field] = stored
		items[field] = lgstore.Item{Namespace: cloneNamespace(stored.Namespace), Key: stored.Key, Value: value, CreatedAt: time.Unix(0, stored.CreatedAt).UTC(), UpdatedAt: time.Unix(0, stored.UpdatedAt).UTC()}
	}
	return records, items, nil
}

func decodeRecord(data []byte, now time.Time) (lgstore.Item, record, bool, error) {
	var stored record
	if err := json.Unmarshal(data, &stored); err != nil {
		return lgstore.Item{}, record{}, false, fmt.Errorf("decode Redis store record: %w", err)
	}
	if stored.ExpiresAt > 0 && stored.ExpiresAt <= now.UnixNano() {
		return lgstore.Item{}, stored, false, nil
	}
	value, err := decodeValue(stored.Value)
	if err != nil {
		return lgstore.Item{}, record{}, false, err
	}
	return lgstore.Item{Namespace: cloneNamespace(stored.Namespace), Key: stored.Key, Value: value, CreatedAt: time.Unix(0, stored.CreatedAt).UTC(), UpdatedAt: time.Unix(0, stored.UpdatedAt).UTC()}, stored, true, nil
}
func decodeValue(data []byte) (lgstore.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected trailing JSON value")
	}
	return lgstore.Value(normalizeNumbers(value).(map[string]any)), nil
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

func searchItems(items map[string]lgstore.Item, op lgstore.SearchOp) ([]lgstore.SearchItem, error) {
	candidates := make([]lgstore.Item, 0)
	for _, item := range items {
		if !hasPrefix(item.Namespace, op.NamespacePrefix) {
			continue
		}
		matched, err := matchesFilter(item.Value, op.Filter)
		if err != nil {
			return nil, err
		}
		if matched {
			candidates = append(candidates, cloneItem(item))
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
	result := make([]lgstore.SearchItem, 0, end-start)
	for _, item := range candidates[start:end] {
		result = append(result, lgstore.SearchItem{Item: item})
	}
	return result, nil
}
func listNamespaces(items map[string]lgstore.Item, op lgstore.ListNamespacesOp) ([]lgstore.Namespace, error) {
	set := make(map[string]lgstore.Namespace)
	for _, item := range items {
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
	result := make([]lgstore.Namespace, 0, end-start)
	for _, key := range keys[start:end] {
		result = append(result, set[key])
	}
	return result, nil
}
func matchesFilter(value, filter lgstore.Value) (bool, error) {
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
	case lgstore.Value:
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
		return false, fmt.Errorf("%w: filter operator %q", lgstore.ErrUnsupportedQuery, operator)
	}
}
func numeric(value any) (float64, error) {
	ref := reflect.ValueOf(value)
	if !ref.IsValid() {
		return 0, fmt.Errorf("%w: nil is not numeric", lgstore.ErrUnsupportedQuery)
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
	return 0, fmt.Errorf("%w: %T is not numeric", lgstore.ErrUnsupportedQuery, value)
}
func matchNamespace(namespace lgstore.Namespace, condition lgstore.MatchCondition) (bool, error) {
	if len(namespace) < len(condition.Path) {
		return false, nil
	}
	start := 0
	switch condition.Type {
	case lgstore.MatchPrefix:
	case lgstore.MatchSuffix:
		start = len(namespace) - len(condition.Path)
	default:
		return false, fmt.Errorf("%w: namespace match type %q", lgstore.ErrUnsupportedQuery, condition.Type)
	}
	for index, part := range condition.Path {
		if part != "*" && namespace[start+index] != part {
			return false, nil
		}
	}
	return true, nil
}
func hasPrefix(namespace, prefix lgstore.Namespace) bool {
	if len(namespace) < len(prefix) {
		return false
	}
	for i := range prefix {
		if namespace[i] != prefix[i] {
			return false
		}
	}
	return true
}
func itemField(namespace lgstore.Namespace, key string) string {
	data, _ := json.Marshal(namespace)
	return base64.RawURLEncoding.EncodeToString(data) + ":" + base64.RawURLEncoding.EncodeToString([]byte(key))
}
func namespaceString(namespace lgstore.Namespace) string { return strings.Join(namespace, "\x00") }
func cloneNamespace(namespace lgstore.Namespace) lgstore.Namespace {
	return append(lgstore.Namespace(nil), namespace...)
}
func cloneItem(item lgstore.Item) lgstore.Item {
	item.Namespace = cloneNamespace(item.Namespace)
	data, _ := json.Marshal(item.Value)
	item.Value, _ = decodeValue(data)
	return item
}
func validatePublic(ctx context.Context, namespace lgstore.Namespace, key string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := lgstore.ValidateNamespace(namespace); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: key is empty", lgstore.ErrInvalidOperation)
	}
	return nil
}
func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", lgstore.ErrInvalidOperation)
	}
	return ctx.Err()
}

func waitForRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(min(attempt+1, 10)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ lgstore.ExpiredItemStore = (*Store)(nil)
