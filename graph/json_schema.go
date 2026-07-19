package graph

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"
)

var (
	timeType       = reflect.TypeOf(time.Time{})
	rawMessageType = reflect.TypeOf(json.RawMessage{})
)

func jsonSchemaFor(valueType reflect.Type) json.RawMessage {
	if valueType == nil {
		return nil
	}
	builder := jsonSchemaBuilder{visiting: make(map[reflect.Type]bool)}
	document := builder.schema(valueType)
	document["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	document["x-go-type"] = valueType.String()
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil
	}
	return encoded
}

type jsonSchemaBuilder struct{ visiting map[reflect.Type]bool }

func (b *jsonSchemaBuilder) schema(valueType reflect.Type) map[string]any {
	if valueType == timeType {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	if valueType == rawMessageType {
		return map[string]any{}
	}
	if valueType.Kind() == reflect.Pointer {
		return map[string]any{"anyOf": []any{b.schema(valueType.Elem()), map[string]any{"type": "null"}}}
	}
	switch valueType.Kind() {
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Slice:
		if valueType.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string", "contentEncoding": "base64"}
		}
		return map[string]any{"type": "array", "items": b.schema(valueType.Elem())}
	case reflect.Array:
		return map[string]any{
			"type": "array", "items": b.schema(valueType.Elem()),
			"minItems": valueType.Len(), "maxItems": valueType.Len(),
		}
	case reflect.Map:
		result := map[string]any{"type": "object", "additionalProperties": b.schema(valueType.Elem())}
		if valueType.Key().Kind() != reflect.String {
			result["x-go-map-key-type"] = valueType.Key().String()
		}
		return result
	case reflect.Struct:
		if b.visiting[valueType] {
			return map[string]any{"type": "object", "x-go-recursive-type": valueType.String()}
		}
		b.visiting[valueType] = true
		defer delete(b.visiting, valueType)
		properties := make(map[string]any)
		required := make([]string, 0, valueType.NumField())
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			if field.PkgPath != "" {
				continue
			}
			name, omitEmpty, skip := jsonField(field)
			if skip {
				continue
			}
			fieldSchema := b.schema(field.Type)
			applyJSONSchemaTag(fieldSchema, field.Tag.Get("jsonschema"))
			properties[name] = fieldSchema
			if !omitEmpty {
				required = append(required, name)
			}
		}
		result := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
		if len(required) > 0 {
			result["required"] = required
		}
		return result
	case reflect.Interface:
		return map[string]any{}
	default:
		return map[string]any{"x-go-type": valueType.String()}
	}
}

func jsonField(field reflect.StructField) (name string, omitEmpty, skip bool) {
	name = field.Name
	parts := strings.Split(field.Tag.Get("json"), ",")
	if parts[0] == "-" {
		return "", false, true
	}
	if parts[0] != "" {
		name = parts[0]
	}
	for _, option := range parts[1:] {
		if option == "omitempty" || option == "omitzero" {
			omitEmpty = true
		}
	}
	return name, omitEmpty, false
}

func applyJSONSchemaTag(schema map[string]any, tag string) {
	for _, entry := range strings.Split(tag, ",") {
		key, value, found := strings.Cut(entry, "=")
		if !found || value == "" {
			continue
		}
		switch key {
		case "title", "description", "format", "pattern":
			schema[key] = value
		case "enum":
			schema[key] = strings.Split(value, "|")
		}
	}
}
