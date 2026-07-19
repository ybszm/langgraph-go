package checkpoint

import "fmt"

// BinaryMarshalFunc serializes one typed value using an application-selected
// MessagePack, Protobuf, CBOR, or other binary implementation.
type BinaryMarshalFunc[T any] func(T) ([]byte, error)

// BinaryUnmarshalFunc deserializes one typed value from owned input bytes.
type BinaryUnmarshalFunc[T any] func([]byte) (T, error)

// BinaryCodec binds a stable type/version identity to typed binary functions
// without imposing one third-party serialization runtime on applications.
type BinaryCodec[T any] struct {
	typeName  string
	version   int
	marshal   BinaryMarshalFunc[T]
	unmarshal BinaryUnmarshalFunc[T]
}

// NewBinaryCodec constructs a strict typed binary codec.
func NewBinaryCodec[T any](
	typeName string,
	version int,
	marshal BinaryMarshalFunc[T],
	unmarshal BinaryUnmarshalFunc[T],
) (*BinaryCodec[T], error) {
	if typeName == "" || version <= 0 || marshal == nil || unmarshal == nil {
		return nil, fmt.Errorf("%w: binary codec requires type, positive version, marshal, and unmarshal", ErrCodecMismatch)
	}
	return &BinaryCodec[T]{typeName: typeName, version: version, marshal: marshal, unmarshal: unmarshal}, nil
}

// NewMessagePackCodec constructs a MessagePack-identified typed codec using
// the caller's preferred implementation.
func NewMessagePackCodec[T any](
	typeName string, version int,
	marshal BinaryMarshalFunc[T], unmarshal BinaryUnmarshalFunc[T],
) (*BinaryCodec[T], error) {
	return NewBinaryCodec("msgpack/"+typeName, version, marshal, unmarshal)
}

// NewProtobufCodec constructs a Protobuf-identified typed codec using the
// caller's generated-message runtime.
func NewProtobufCodec[T any](
	typeName string, version int,
	marshal BinaryMarshalFunc[T], unmarshal BinaryUnmarshalFunc[T],
) (*BinaryCodec[T], error) {
	return NewBinaryCodec("protobuf/"+typeName, version, marshal, unmarshal)
}

// Encode implements Codec and takes ownership of a copy of serializer output.
func (c *BinaryCodec[T]) Encode(value T) (EncodedValue, error) {
	data, err := c.marshal(value)
	if err != nil {
		return EncodedValue{}, fmt.Errorf("encode %s v%d: %w", c.typeName, c.version, err)
	}
	if data == nil {
		return EncodedValue{}, fmt.Errorf("%w: %s v%d serializer returned nil data", ErrCodecMismatch, c.typeName, c.version)
	}
	return EncodedValue{Type: c.typeName, Version: c.version, Data: append([]byte(nil), data...)}, nil
}

// Decode implements Codec and protects EncodedValue ownership from custom
// unmarshaler mutation.
func (c *BinaryCodec[T]) Decode(value EncodedValue) (T, error) {
	var zero T
	if value.Type != c.typeName || value.Version != c.version {
		return zero, fmt.Errorf(
			"%w: got %s v%d, want %s v%d", ErrCodecMismatch,
			value.Type, value.Version, c.typeName, c.version,
		)
	}
	result, err := c.unmarshal(append([]byte(nil), value.Data...))
	if err != nil {
		return zero, fmt.Errorf("decode %s v%d: %w", c.typeName, c.version, err)
	}
	return result, nil
}
