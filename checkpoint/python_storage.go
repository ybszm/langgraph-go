package checkpoint

import (
	"encoding/json"
	"fmt"
)

// PythonTasksChannel stores pending sends migrated from a parent checkpoint by
// the upstream PostgreSQL saver.
const PythonTasksChannel = "__pregel_tasks"

// PythonCheckpointSerializer bridges the typed checkpoint column used by
// Python SQL savers. Applications can extend it for private Python MessagePack
// extension objects.
type PythonCheckpointSerializer interface {
	Decode(typeName string, data []byte) (PythonCheckpoint, error)
	Encode(value PythonCheckpoint) (typeName string, data []byte, err error)
}

// PythonValueSerializer bridges independently typed channel blobs and pending
// writes. The default serializer intentionally accepts only portable JSON and
// standard MessagePack values.
type PythonValueSerializer interface {
	DecodeValue(typeName string, data []byte) (json.RawMessage, error)
	EncodeValue(value json.RawMessage) (typeName string, data []byte, err error)
}

// DefaultPythonSerializer implements both PythonCheckpointSerializer and
// PythonValueSerializer for portable JSON and standard MessagePack values.
type DefaultPythonSerializer struct{}

func (DefaultPythonSerializer) Decode(typeName string, data []byte) (PythonCheckpoint, error) {
	switch typeName {
	case "msgpack":
		return DecodePythonMessagePackCheckpoint(data)
	case "json":
		return DecodePythonCheckpoint(data)
	default:
		return PythonCheckpoint{}, fmt.Errorf("%w: unsupported Python checkpoint serializer %q", ErrCodecMismatch, typeName)
	}
}

func (DefaultPythonSerializer) Encode(value PythonCheckpoint) (string, []byte, error) {
	data, err := EncodePythonMessagePackCheckpoint(value)
	return "msgpack", data, err
}

// DecodeValue implements PythonValueSerializer.
func (DefaultPythonSerializer) DecodeValue(typeName string, data []byte) (json.RawMessage, error) {
	switch typeName {
	case "msgpack":
		return DecodePythonMessagePackValue(data)
	case "json":
		if !json.Valid(data) {
			return nil, fmt.Errorf("%w: invalid Python JSON typed value", ErrCodecMismatch)
		}
		return append(json.RawMessage(nil), data...), nil
	case "null":
		return json.RawMessage("null"), nil
	default:
		return nil, fmt.Errorf("%w: unsupported Python value serializer %q", ErrCodecMismatch, typeName)
	}
}

// EncodeValue implements PythonValueSerializer.
func (DefaultPythonSerializer) EncodeValue(value json.RawMessage) (string, []byte, error) {
	data, err := EncodePythonMessagePackValue(value)
	return "msgpack", data, err
}

// PythonTypedValue is an opaque typed payload from an upstream blob or writes
// table.
type PythonTypedValue struct {
	Type string
	Data []byte
}

// PythonPendingWrite preserves upstream task path and index ordering.
type PythonPendingWrite struct {
	TaskID   string
	TaskPath string
	Index    int
	Channel  string
	Value    PythonTypedValue
}

// PythonTuple is a physical-schema-compatible Python checkpoint record.
type PythonTuple struct {
	Config        Config
	Checkpoint    PythonCheckpoint
	Metadata      Metadata
	ParentConfig  *Config
	PendingWrites []PythonPendingWrite
}
