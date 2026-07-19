package checkpoint

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// CloneEncodedValue returns an isolated encoded value.
func CloneEncodedValue(value EncodedValue) EncodedValue {
	value.Data = append([]byte(nil), value.Data...)
	return value
}

// CloneCheckpoint returns an isolated checkpoint value.
func CloneCheckpoint(source Checkpoint) Checkpoint {
	result := source
	result.Values = make(map[string]EncodedValue, len(source.Values))
	for channel, value := range source.Values {
		result.Values[channel] = CloneEncodedValue(value)
	}
	result.ChannelVersions = cloneStringMap(source.ChannelVersions)
	result.VersionsSeen = make(map[string]map[string]string, len(source.VersionsSeen))
	for node, versions := range source.VersionsSeen {
		result.VersionsSeen[node] = cloneStringMap(versions)
	}
	result.UpdatedChannels = append([]string(nil), source.UpdatedChannels...)
	result.Next = append([]Task(nil), source.Next...)
	for index := range result.Next {
		result.Next[index].Triggers = append([]string(nil), source.Next[index].Triggers...)
		result.Next[index].ReadChannels = append([]string(nil), source.Next[index].ReadChannels...)
		if source.Next[index].Input != nil {
			input := CloneEncodedValue(*source.Next[index].Input)
			result.Next[index].Input = &input
		}
	}
	result.Waiting = make(map[string][]string, len(source.Waiting))
	for key, values := range source.Waiting {
		result.Waiting[key] = append([]string(nil), values...)
	}
	return result
}

// CloneMetadata validates and deep-copies JSON-compatible metadata while
// preserving the concrete scalar types supplied by the caller.
func CloneMetadata(source Metadata) (Metadata, error) {
	if source == nil {
		return nil, nil
	}
	if _, err := json.Marshal(source); err != nil {
		return nil, fmt.Errorf("checkpoint metadata is not JSON-compatible: %w", err)
	}
	cloned, err := cloneJSONCompatible(reflect.ValueOf(map[string]any(source)))
	if err != nil {
		return nil, err
	}
	return Metadata(cloned.Interface().(map[string]any)), nil
}

func cloneJSONCompatible(value reflect.Value) (reflect.Value, error) {
	if !value.IsValid() {
		return value, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneJSONCompatible(value.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result, nil
	}

	switch value.Kind() {
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("metadata map key must be a string")
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			cloned, err := cloneJSONCompatible(iterator.Value())
			if err != nil {
				return reflect.Value{}, err
			}
			result.SetMapIndex(iterator.Key(), cloned)
		}
		return result, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := range value.Len() {
			cloned, err := cloneJSONCompatible(value.Index(index))
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(index).Set(cloned)
		}
		return result, nil
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for index := range value.Len() {
			cloned, err := cloneJSONCompatible(value.Index(index))
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(index).Set(cloned)
		}
		return result, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneJSONCompatible(value.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloned)
		return result, nil
	default:
		return value, nil
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
