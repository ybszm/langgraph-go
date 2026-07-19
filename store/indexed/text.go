package indexed

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

func tokenizePath(path string) ([]string, error) {
	if path == "$" {
		return nil, nil
	}
	var tokens []string
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	for index := 0; index < len(path); {
		switch path[index] {
		case '.':
			flush()
			index++
		case '[', '{':
			flush()
			open := path[index]
			close := byte(']')
			if open == '{' {
				close = '}'
			}
			start, depth := index, 0
			for index < len(path) {
				if path[index] == open {
					depth++
				}
				if path[index] == close {
					depth--
					if depth == 0 {
						index++
						break
					}
				}
				index++
			}
			if depth != 0 {
				return nil, fmt.Errorf("unclosed %q", string(open))
			}
			tokens = append(tokens, path[start:index])
		default:
			current.WriteByte(path[index])
			index++
		}
	}
	flush()
	if len(tokens) == 0 {
		return nil, fmt.Errorf("path has no tokens")
	}
	return tokens, nil
}

func extractTexts(value any, tokens []string) ([]string, error) {
	if len(tokens) == 0 {
		encoded, err := canonicalJSON(value)
		if err != nil {
			return nil, err
		}
		return []string{encoded}, nil
	}
	return extractAt(value, tokens, 0)
}

func extractAt(value any, tokens []string, position int) ([]string, error) {
	if position == len(tokens) {
		return leafText(value)
	}
	token := tokens[position]
	if strings.HasPrefix(token, "[") {
		if !strings.HasSuffix(token, "]") {
			return nil, fmt.Errorf("invalid array token %q", token)
		}
		values, ok := sliceValues(value)
		if !ok {
			return nil, nil
		}
		indexText := token[1 : len(token)-1]
		if indexText == "*" {
			var result []string
			for _, item := range values {
				texts, err := extractAt(item, tokens, position+1)
				if err != nil {
					return nil, err
				}
				result = append(result, texts...)
			}
			return result, nil
		}
		index, err := strconv.Atoi(indexText)
		if err != nil {
			return nil, fmt.Errorf("invalid array index %q", indexText)
		}
		if index < 0 {
			index += len(values)
		}
		if index < 0 || index >= len(values) {
			return nil, nil
		}
		return extractAt(values[index], tokens, position+1)
	}
	if strings.HasPrefix(token, "{") {
		if !strings.HasSuffix(token, "}") {
			return nil, fmt.Errorf("invalid selection token %q", token)
		}
		object, ok := objectValue(value)
		if !ok {
			return nil, nil
		}
		var result []string
		for _, field := range strings.Split(token[1:len(token)-1], ",") {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			fieldTokens, err := tokenizePath(field)
			if err != nil {
				return nil, err
			}
			selected, err := extractAt(object, append(fieldTokens, tokens[position+1:]...), 0)
			if err != nil {
				return nil, err
			}
			result = append(result, selected...)
		}
		return result, nil
	}
	if token == "*" {
		if object, ok := objectValue(value); ok {
			keys := make([]string, 0, len(object))
			for key := range object {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			var result []string
			for _, key := range keys {
				texts, err := extractAt(object[key], tokens, position+1)
				if err != nil {
					return nil, err
				}
				result = append(result, texts...)
			}
			return result, nil
		}
		if values, ok := sliceValues(value); ok {
			var result []string
			for _, item := range values {
				texts, err := extractAt(item, tokens, position+1)
				if err != nil {
					return nil, err
				}
				result = append(result, texts...)
			}
			return result, nil
		}
		return nil, nil
	}
	object, ok := objectValue(value)
	if !ok {
		return nil, nil
	}
	next, exists := object[token]
	if !exists {
		return nil, nil
	}
	return extractAt(next, tokens, position+1)
}

func leafText(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	switch value := value.(type) {
	case string:
		return []string{value}, nil
	case bool:
		return []string{strconv.FormatBool(value)}, nil
	case json.Number:
		return []string{value.String()}, nil
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return []string{fmt.Sprint(value)}, nil
	case reflect.Map, reflect.Slice, reflect.Array:
		encoded, err := canonicalJSON(value)
		if err != nil {
			return nil, err
		}
		return []string{encoded}, nil
	default:
		return nil, nil
	}
}

func canonicalJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func objectValue(value any) (map[string]any, bool) {
	if object, ok := value.(map[string]any); ok {
		return object, true
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.Kind() != reflect.Map || reflected.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	result := make(map[string]any, reflected.Len())
	iterator := reflected.MapRange()
	for iterator.Next() {
		result[iterator.Key().String()] = iterator.Value().Interface()
	}
	return result, true
}

func sliceValues(value any) ([]any, bool) {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, false
	}
	result := make([]any, reflected.Len())
	for index := range result {
		result[index] = reflected.Index(index).Interface()
	}
	return result, true
}
