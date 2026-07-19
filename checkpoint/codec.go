package checkpoint

import (
	"encoding/json"
	"fmt"
)

// JSONCodec is a strict, versioned JSON codec for one Go type.
type JSONCodec[T any] struct {
	typeName string
	version  int
}

// NewJSONCodec creates a JSON codec. typeName must be stable across process
// restarts and version must be positive.
func NewJSONCodec[T any](typeName string, version int) (*JSONCodec[T], error) {
	if typeName == "" {
		return nil, fmt.Errorf("%w: codec type is empty", ErrCodecMismatch)
	}
	if version <= 0 {
		return nil, fmt.Errorf("%w: codec version must be positive", ErrCodecMismatch)
	}
	return &JSONCodec[T]{typeName: typeName, version: version}, nil
}

// MustJSONCodec creates a JSON codec and panics if its static definition is
// invalid. It is intended for package-level codec declarations.
func MustJSONCodec[T any](typeName string, version int) *JSONCodec[T] {
	codec, err := NewJSONCodec[T](typeName, version)
	if err != nil {
		panic(err)
	}
	return codec
}

// Encode implements Codec.
func (c *JSONCodec[T]) Encode(value T) (EncodedValue, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return EncodedValue{}, fmt.Errorf("encode %s v%d: %w", c.typeName, c.version, err)
	}
	return EncodedValue{
		Type:    c.typeName,
		Version: c.version,
		Data:    data,
	}, nil
}

// Decode implements Codec.
func (c *JSONCodec[T]) Decode(value EncodedValue) (T, error) {
	var result T
	if value.Type != c.typeName || value.Version != c.version {
		return result, fmt.Errorf(
			"%w: got %s v%d, want %s v%d",
			ErrCodecMismatch,
			value.Type,
			value.Version,
			c.typeName,
			c.version,
		)
	}
	if err := json.Unmarshal(value.Data, &result); err != nil {
		return result, fmt.Errorf("decode %s v%d: %w", c.typeName, c.version, err)
	}
	return result, nil
}
