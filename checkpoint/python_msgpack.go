package checkpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

const (
	pythonExtConstructorSingleArg int8 = iota
	pythonExtConstructorPosArgs
	pythonExtConstructorKWArgs
	pythonExtMethodSingleArg
	pythonExtPydanticV1
	pythonExtPydanticV2
	pythonExtNumPyArray
	pythonExtDeltaSnapshot
)

// PythonMessagePackExtension is one detached upstream private Ext value. It
// preserves the exact extension code and nested MessagePack payload without
// importing or constructing a Python class.
type PythonMessagePackExtension struct {
	Code    int8
	Payload []byte
}

// DecodePythonMessagePackExtension decodes one root Ext value without using
// process-global MessagePack extension registration.
func DecodePythonMessagePackExtension(data []byte) (PythonMessagePackExtension, error) {
	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	code, size, err := decoder.DecodeExtHeader()
	if err != nil {
		return PythonMessagePackExtension{}, fmt.Errorf("%w: decode Python extension header: %v", ErrCodecMismatch, err)
	}
	payload := make([]byte, size)
	if err := decoder.ReadFull(payload); err != nil {
		return PythonMessagePackExtension{}, fmt.Errorf("%w: decode Python extension payload: %v", ErrCodecMismatch, err)
	}
	if _, err := decoder.PeekCode(); err != io.EOF {
		if err == nil {
			return PythonMessagePackExtension{}, fmt.Errorf("%w: Python extension has trailing data", ErrCodecMismatch)
		}
		return PythonMessagePackExtension{}, fmt.Errorf("%w: inspect Python extension trailing data: %v", ErrCodecMismatch, err)
	}
	return PythonMessagePackExtension{Code: code, Payload: payload}, nil
}

// EncodePythonMessagePackExtension encodes one exact private Ext envelope.
func EncodePythonMessagePackExtension(extension PythonMessagePackExtension) ([]byte, error) {
	if extension.Code < pythonExtConstructorSingleArg || extension.Code > pythonExtDeltaSnapshot {
		return nil, fmt.Errorf("%w: unsupported Python extension code %d", ErrCodecMismatch, extension.Code)
	}
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	if err := encoder.EncodeExtHeader(extension.Code, len(extension.Payload)); err != nil {
		return nil, fmt.Errorf("%w: encode Python extension header: %v", ErrCodecMismatch, err)
	}
	if _, err := encoder.Writer().Write(extension.Payload); err != nil {
		return nil, fmt.Errorf("%w: encode Python extension payload: %v", ErrCodecMismatch, err)
	}
	return buffer.Bytes(), nil
}

// PortableValue safely projects the private extension to JSON-compatible
// data using LangGraph 1.2.9's ext-hook-to-JSON semantics. It never imports or
// invokes the named Python constructor.
func (extension PythonMessagePackExtension) PortableValue() (json.RawMessage, error) {
	value, err := portablePythonExtension(extension)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode Python extension portable value: %v", ErrCodecMismatch, err)
	}
	return encoded, nil
}

type pythonMessagePackEnvelope struct {
	V               int                       `msgpack:"v"`
	ID              string                    `msgpack:"id"`
	TS              string                    `msgpack:"ts"`
	ChannelValues   map[string]any            `msgpack:"channel_values"`
	ChannelVersions map[string]any            `msgpack:"channel_versions"`
	VersionsSeen    map[string]map[string]any `msgpack:"versions_seen"`
	PendingSends    []any                     `msgpack:"pending_sends"`
	UpdatedChannels []string                  `msgpack:"updated_channels"`
}

// DecodePythonMessagePackValue decodes a JSON-compatible typed value from an
// upstream checkpoint blob or pending write without constructing Python
// classes represented by private extension codes.
func DecodePythonMessagePackValue(data []byte) (json.RawMessage, error) {
	value, err := decodePythonMessagePack(data)
	if err != nil {
		return nil, fmt.Errorf("%w: decode Python MessagePack value: %v", ErrCodecMismatch, err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: convert Python MessagePack value to portable JSON: %v", ErrCodecMismatch, err)
	}
	return json.RawMessage(encoded), nil
}

// EncodePythonMessagePackValue encodes one JSON-compatible channel value or
// pending write using standard MessagePack types.
func EncodePythonMessagePackValue(value json.RawMessage) ([]byte, error) {
	if !json.Valid(value) {
		return nil, fmt.Errorf("%w: invalid portable JSON value", ErrCodecMismatch)
	}
	decoded, err := decodePortableJSON(value)
	if err != nil {
		return nil, fmt.Errorf("%w: decode portable JSON value: %v", ErrCodecMismatch, err)
	}
	encoded, err := msgpack.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf("%w: encode Python MessagePack value: %v", ErrCodecMismatch, err)
	}
	return encoded, nil
}

// DecodePythonMessagePackCheckpoint decodes the default typed "msgpack"
// checkpoint payload emitted by langgraph-checkpoint 1.2.9. JSON-compatible
// values are portable. Python constructor/extension objects are rejected until
// an application explicitly registers an object codec at the SQL boundary.
func DecodePythonMessagePackCheckpoint(data []byte) (PythonCheckpoint, error) {
	portable, err := decodePythonMessagePack(data)
	if err != nil {
		return PythonCheckpoint{}, fmt.Errorf("%w: decode Python MessagePack checkpoint extensions: %v", ErrInvalidCheckpoint, err)
	}
	normalized, err := msgpack.Marshal(portable)
	if err != nil {
		return PythonCheckpoint{}, fmt.Errorf("%w: normalize Python MessagePack checkpoint extensions: %v", ErrInvalidCheckpoint, err)
	}
	var raw pythonMessagePackEnvelope
	if err := msgpack.Unmarshal(normalized, &raw); err != nil {
		return PythonCheckpoint{}, fmt.Errorf("%w: decode Python MessagePack checkpoint: %v", ErrInvalidCheckpoint, err)
	}
	value := PythonCheckpoint{
		V:               raw.V,
		ID:              raw.ID,
		TS:              raw.TS,
		ChannelValues:   make(map[string]json.RawMessage, len(raw.ChannelValues)),
		ChannelVersions: make(map[string]PythonChannelVersion, len(raw.ChannelVersions)),
		VersionsSeen:    make(map[string]map[string]PythonChannelVersion, len(raw.VersionsSeen)),
		PendingSends:    make([]json.RawMessage, len(raw.PendingSends)),
		UpdatedChannels: append([]string(nil), raw.UpdatedChannels...),
	}
	for channel, item := range raw.ChannelValues {
		encoded, err := json.Marshal(item)
		if err != nil {
			return PythonCheckpoint{}, fmt.Errorf("%w: encode Python channel %q as portable JSON: %v", ErrInvalidCheckpoint, channel, err)
		}
		value.ChannelValues[channel] = encoded
	}
	for channel, item := range raw.ChannelVersions {
		version, err := pythonVersionFromMessagePack(item)
		if err != nil {
			return PythonCheckpoint{}, fmt.Errorf("Python channel version %q: %w", channel, err)
		}
		value.ChannelVersions[channel] = version
	}
	for node, versions := range raw.VersionsSeen {
		converted := make(map[string]PythonChannelVersion, len(versions))
		for channel, item := range versions {
			version, err := pythonVersionFromMessagePack(item)
			if err != nil {
				return PythonCheckpoint{}, fmt.Errorf("Python versions_seen %q/%q: %w", node, channel, err)
			}
			converted[channel] = version
		}
		value.VersionsSeen[node] = converted
	}
	for i, send := range raw.PendingSends {
		encoded, err := json.Marshal(send)
		if err != nil {
			return PythonCheckpoint{}, fmt.Errorf("%w: encode Python pending send %d as portable JSON: %v", ErrInvalidCheckpoint, i, err)
		}
		value.PendingSends[i] = encoded
	}
	if err := value.Validate(); err != nil {
		return PythonCheckpoint{}, err
	}
	return value, nil
}

func decodePythonMessagePack(data []byte) (any, error) {
	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	value, err := decodePythonMessagePackItem(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.PeekCode(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("MessagePack value has trailing data")
		}
		return nil, err
	}
	return value, nil
}

func decodePythonMessagePackItem(decoder *msgpack.Decoder) (any, error) {
	code, err := decoder.PeekCode()
	if err != nil {
		return nil, err
	}
	switch {
	case msgpcode.IsExt(code):
		extensionCode, size, err := decoder.DecodeExtHeader()
		if err != nil {
			return nil, err
		}
		payload := make([]byte, size)
		if err := decoder.ReadFull(payload); err != nil {
			return nil, err
		}
		return portablePythonExtension(PythonMessagePackExtension{Code: extensionCode, Payload: payload})
	case msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32:
		size, err := decoder.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		items := make([]any, size)
		for index := range items {
			items[index], err = decodePythonMessagePackItem(decoder)
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	case msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32:
		size, err := decoder.DecodeMapLen()
		if err != nil {
			return nil, err
		}
		items := make(map[string]any, size)
		for index := 0; index < size; index++ {
			key, err := decodePythonMessagePackItem(decoder)
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("non-string MessagePack map key has type %T", key)
			}
			items[name], err = decodePythonMessagePackItem(decoder)
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		return decoder.DecodeInterface()
	}
}

func portablePythonExtension(extension PythonMessagePackExtension) (any, error) {
	if extension.Code < pythonExtConstructorSingleArg || extension.Code > pythonExtDeltaSnapshot {
		return nil, fmt.Errorf("%w: unsupported Python extension code %d", ErrCodecMismatch, extension.Code)
	}
	payload, err := decodePythonMessagePack(extension.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode Python extension %d payload: %v", ErrCodecMismatch, extension.Code, err)
	}
	if extension.Code == pythonExtDeltaSnapshot {
		return payload, nil
	}
	if extension.Code == pythonExtNumPyArray {
		return nil, fmt.Errorf("%w: NumPy extension requires an application array codec", ErrCodecMismatch)
	}
	tuple, ok := payload.([]any)
	if !ok || len(tuple) < 3 {
		return nil, fmt.Errorf("%w: Python extension %d payload is not a constructor tuple", ErrCodecMismatch, extension.Code)
	}
	module, moduleOK := tuple[0].(string)
	name, nameOK := tuple[1].(string)
	if !moduleOK || !nameOK || module == "" || name == "" {
		return nil, fmt.Errorf("%w: Python extension %d has invalid constructor identity", ErrCodecMismatch, extension.Code)
	}
	argument := tuple[2]
	switch extension.Code {
	case pythonExtConstructorSingleArg:
		if module == "uuid" && name == "UUID" {
			hexValue, ok := argument.(string)
			if !ok || len(hexValue) != 32 {
				return nil, fmt.Errorf("%w: Python UUID extension has invalid hex value", ErrCodecMismatch)
			}
			return hexValue[:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:], nil
		}
		return argument, nil
	case pythonExtConstructorPosArgs:
		if module == "langgraph.types" && name == "Send" {
			args, ok := argument.([]any)
			if !ok || len(args) < 2 || len(args) > 3 {
				return nil, fmt.Errorf("%w: Python Send extension has invalid args", ErrCodecMismatch)
			}
			node, ok := args[0].(string)
			if !ok || node == "" {
				return nil, fmt.Errorf("%w: Python Send extension has invalid node", ErrCodecMismatch)
			}
			send := map[string]any{"node": node, "arg": args[1]}
			if len(args) == 3 {
				send["timeout"] = args[2]
			}
			return send, nil
		}
		return argument, nil
	case pythonExtConstructorKWArgs, pythonExtPydanticV1, pythonExtPydanticV2:
		return argument, nil
	case pythonExtMethodSingleArg:
		if len(tuple) != 4 {
			return nil, fmt.Errorf("%w: Python method extension has invalid tuple", ErrCodecMismatch)
		}
		method, ok := tuple[3].(string)
		if !ok || method == "" {
			return nil, fmt.Errorf("%w: Python method extension has invalid method", ErrCodecMismatch)
		}
		return argument, nil
	default:
		return nil, fmt.Errorf("%w: unsupported Python extension code %d", ErrCodecMismatch, extension.Code)
	}
}

// EncodePythonMessagePackCheckpoint encodes JSON-compatible portable values
// using the default Python checkpoint MessagePack field layout.
func EncodePythonMessagePackCheckpoint(value PythonCheckpoint) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	raw := pythonMessagePackEnvelope{
		V:               value.V,
		ID:              value.ID,
		TS:              value.TS,
		ChannelValues:   make(map[string]any, len(value.ChannelValues)),
		ChannelVersions: make(map[string]any, len(value.ChannelVersions)),
		VersionsSeen:    make(map[string]map[string]any, len(value.VersionsSeen)),
		PendingSends:    make([]any, len(value.PendingSends)),
		UpdatedChannels: append([]string(nil), value.UpdatedChannels...),
	}
	for channel, encoded := range value.ChannelValues {
		item, err := decodePortableJSON(encoded)
		if err != nil {
			return nil, fmt.Errorf("%w: decode portable channel %q: %v", ErrInvalidCheckpoint, channel, err)
		}
		raw.ChannelValues[channel] = item
	}
	for channel, version := range value.ChannelVersions {
		item, err := pythonVersionMessagePackValue(version)
		if err != nil {
			return nil, fmt.Errorf("Python channel version %q: %w", channel, err)
		}
		raw.ChannelVersions[channel] = item
	}
	for node, versions := range value.VersionsSeen {
		converted := make(map[string]any, len(versions))
		for channel, version := range versions {
			item, err := pythonVersionMessagePackValue(version)
			if err != nil {
				return nil, fmt.Errorf("Python versions_seen %q/%q: %w", node, channel, err)
			}
			converted[channel] = item
		}
		raw.VersionsSeen[node] = converted
	}
	for i, encoded := range value.PendingSends {
		item, err := decodePortableJSON(encoded)
		if err != nil {
			return nil, fmt.Errorf("%w: decode portable pending send %d: %v", ErrInvalidCheckpoint, i, err)
		}
		raw.PendingSends[i] = item
	}
	data, err := msgpack.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: encode Python MessagePack checkpoint: %v", ErrInvalidCheckpoint, err)
	}
	return data, nil
}

func pythonVersionFromMessagePack(value any) (PythonChannelVersion, error) {
	switch value := value.(type) {
	case string:
		return NewPythonStringVersion(value), nil
	case int:
		return NewPythonIntegerVersion(int64(value)), nil
	case int8:
		return NewPythonIntegerVersion(int64(value)), nil
	case int16:
		return NewPythonIntegerVersion(int64(value)), nil
	case int32:
		return NewPythonIntegerVersion(int64(value)), nil
	case int64:
		return NewPythonIntegerVersion(value), nil
	case uint:
		return PythonChannelVersion{kind: PythonVersionInteger, raw: strconv.FormatUint(uint64(value), 10)}, nil
	case uint8:
		return PythonChannelVersion{kind: PythonVersionInteger, raw: strconv.FormatUint(uint64(value), 10)}, nil
	case uint16:
		return PythonChannelVersion{kind: PythonVersionInteger, raw: strconv.FormatUint(uint64(value), 10)}, nil
	case uint32:
		return PythonChannelVersion{kind: PythonVersionInteger, raw: strconv.FormatUint(uint64(value), 10)}, nil
	case uint64:
		return PythonChannelVersion{kind: PythonVersionInteger, raw: strconv.FormatUint(value, 10)}, nil
	case float32:
		return NewPythonFloatVersion(float64(value)), nil
	case float64:
		return NewPythonFloatVersion(value), nil
	default:
		return PythonChannelVersion{}, fmt.Errorf("%w: MessagePack channel version has type %T", ErrInvalidCheckpoint, value)
	}
}

func pythonVersionMessagePackValue(value PythonChannelVersion) (any, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	switch value.kind {
	case PythonVersionString:
		return value.String(), nil
	case PythonVersionInteger:
		if signed, err := strconv.ParseInt(value.raw, 10, 64); err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(value.raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: integer channel version exceeds MessagePack range", ErrInvalidCheckpoint)
		}
		return unsigned, nil
	case PythonVersionFloat:
		floating, err := strconv.ParseFloat(value.raw, 64)
		if err != nil || math.IsInf(floating, 0) || math.IsNaN(floating) {
			return nil, fmt.Errorf("%w: invalid floating-point channel version", ErrInvalidCheckpoint)
		}
		return floating, nil
	default:
		return nil, fmt.Errorf("%w: unknown Python channel version kind %q", ErrInvalidCheckpoint, value.kind)
	}
}

func decodePortableJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return normalizeJSONNumbers(value)
}

func normalizeJSONNumbers(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		if strings.ContainsAny(value.String(), ".eE") {
			return value.Float64()
		}
		if signed, err := strconv.ParseInt(value.String(), 10, 64); err == nil {
			return signed, nil
		}
		return strconv.ParseUint(value.String(), 10, 64)
	case []any:
		for i, item := range value {
			converted, err := normalizeJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			value[i] = converted
		}
		return value, nil
	case map[string]any:
		for key, item := range value {
			converted, err := normalizeJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			value[key] = converted
		}
		return value, nil
	default:
		return value, nil
	}
}
