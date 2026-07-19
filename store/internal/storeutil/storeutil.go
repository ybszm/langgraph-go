// Package storeutil contains backend-neutral Store persistence helpers.
package storeutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/ybszm/langgraph-go/store"
)

func EncodeNamespace(namespace store.Namespace) (string, error) {
	data, err := json.Marshal(namespace)
	if err != nil {
		return "", fmt.Errorf("%w: encode namespace: %v", store.ErrInvalidOperation, err)
	}
	return string(data), nil
}

func DecodeNamespace(data string) (store.Namespace, error) {
	var namespace store.Namespace
	if err := json.Unmarshal([]byte(data), &namespace); err != nil {
		return nil, fmt.Errorf("decode store namespace: %w", err)
	}
	return namespace, nil
}

func DecodeValue(data []byte) (store.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode store value: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unexpected trailing JSON value")
	}
	return store.Value(normalizeNumbers(value).(map[string]any)), nil
}

func Search(items []store.Item, op store.SearchOp) ([]store.SearchItem, error) {
	if op.Limit < 0 || op.Offset < 0 {
		return nil, fmt.Errorf("%w: negative search pagination", store.ErrInvalidOperation)
	}
	candidates := make([]store.Item, 0)
	for _, item := range items {
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
		a, b := NamespaceString(candidates[i].Namespace), NamespaceString(candidates[j].Namespace)
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
		result = append(result, store.SearchItem{Item: item})
	}
	return result, nil
}

func ListNamespaces(items []store.Item, op store.ListNamespacesOp) ([]store.Namespace, error) {
	if op.Limit < 0 || op.Offset < 0 || op.MaxDepth < 0 {
		return nil, fmt.Errorf("%w: negative namespace pagination/depth", store.ErrInvalidOperation)
	}
	set := make(map[string]store.Namespace)
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
		set[NamespaceString(namespace)] = CloneNamespace(namespace)
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

func NamespaceString(namespace store.Namespace) string { return strings.Join(namespace, "\x00") }

func CloneNamespace(namespace store.Namespace) store.Namespace {
	return append(store.Namespace(nil), namespace...)
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
		if number, err := typed.Float64(); err == nil {
			return number
		}
		return text
	case map[string]any:
		for key, item := range typed {
			typed[key] = normalizeNumbers(item)
		}
		return typed
	case []any:
		for index, item := range typed {
			typed[index] = normalizeNumbers(item)
		}
		return typed
	default:
		return value
	}
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
