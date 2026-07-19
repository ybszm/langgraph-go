package checkpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// PythonCheckpointVersion is the portable checkpoint envelope version used by
// langgraph-checkpoint 1.2.9. It is deliberately independent from
// CurrentVersion, which versions the richer Go runtime envelope.
const PythonCheckpointVersion = 2

// PythonVersionKind identifies one of the scalar channel-version forms
// accepted by the Python checkpoint contract.
type PythonVersionKind string

const (
	PythonVersionString  PythonVersionKind = "string"
	PythonVersionInteger PythonVersionKind = "integer"
	PythonVersionFloat   PythonVersionKind = "float"
)

// PythonChannelVersion preserves the JSON scalar spelling and kind of a
// Python channel version. Python permits string, integer, and float versions;
// using interface{} or float64 here would lose large integers and scalar kind.
type PythonChannelVersion struct {
	kind PythonVersionKind
	raw  string
}

// NewPythonStringVersion constructs a string channel version.
func NewPythonStringVersion(value string) PythonChannelVersion {
	data, _ := json.Marshal(value)
	return PythonChannelVersion{kind: PythonVersionString, raw: string(data)}
}

// NewPythonIntegerVersion constructs an integer channel version.
func NewPythonIntegerVersion(value int64) PythonChannelVersion {
	return PythonChannelVersion{kind: PythonVersionInteger, raw: strconv.FormatInt(value, 10)}
}

// NewPythonFloatVersion constructs a floating-point channel version. Non-finite
// values are rejected when the containing checkpoint is encoded or validated.
func NewPythonFloatVersion(value float64) PythonChannelVersion {
	raw := strconv.FormatFloat(value, 'g', -1, 64)
	if !strings.ContainsAny(raw, ".eE") && !math.IsInf(value, 0) && !math.IsNaN(value) {
		raw += ".0"
	}
	return PythonChannelVersion{kind: PythonVersionFloat, raw: raw}
}

// Kind returns the preserved JSON scalar kind.
func (v PythonChannelVersion) Kind() PythonVersionKind { return v.kind }

// String returns the scalar value without JSON quoting. Numeric values retain
// their original JSON spelling.
func (v PythonChannelVersion) String() string {
	if v.kind != PythonVersionString {
		return v.raw
	}
	var value string
	if json.Unmarshal([]byte(v.raw), &value) != nil {
		return ""
	}
	return value
}

// MarshalJSON implements json.Marshaler without coercing numeric versions to
// float64.
func (v PythonChannelVersion) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	return []byte(v.raw), nil
}

// UnmarshalJSON implements json.Unmarshaler and rejects booleans, null,
// arrays, and objects, matching ChannelVersions in langgraph-checkpoint 1.2.9.
func (v *PythonChannelVersion) UnmarshalJSON(data []byte) error {
	if v == nil {
		return fmt.Errorf("%w: nil Python channel version", ErrInvalidCheckpoint)
	}
	raw := bytes.TrimSpace(data)
	if !json.Valid(raw) {
		return fmt.Errorf("%w: invalid Python channel version JSON", ErrInvalidCheckpoint)
	}
	if len(raw) > 0 && raw[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("%w: decode Python string channel version: %v", ErrInvalidCheckpoint, err)
		}
		v.kind, v.raw = PythonVersionString, string(raw)
		return nil
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return fmt.Errorf("%w: Python channel version must be a string or number", ErrInvalidCheckpoint)
	}
	if _, err := strconv.ParseFloat(number.String(), 64); err != nil {
		return fmt.Errorf("%w: invalid Python numeric channel version", ErrInvalidCheckpoint)
	}
	v.raw = number.String()
	if strings.ContainsAny(v.raw, ".eE") {
		v.kind = PythonVersionFloat
	} else {
		v.kind = PythonVersionInteger
	}
	return nil
}

func (v PythonChannelVersion) validate() error {
	var decoded PythonChannelVersion
	if err := decoded.UnmarshalJSON([]byte(v.raw)); err != nil {
		return err
	}
	if decoded.kind != v.kind {
		return fmt.Errorf("%w: Python channel version kind %q does not match %q", ErrInvalidCheckpoint, v.kind, decoded.kind)
	}
	return nil
}

// PythonCheckpoint is the portable checkpoint dictionary serialized by
// langgraph-checkpoint 1.2.9. Channel values and pending sends remain raw JSON
// so callers can bridge them to application-specific Go codecs without losing
// number precision or Python serializer details.
type PythonCheckpoint struct {
	V               int                                        `json:"v"`
	ID              string                                     `json:"id"`
	TS              string                                     `json:"ts"`
	ChannelValues   map[string]json.RawMessage                 `json:"channel_values"`
	ChannelVersions map[string]PythonChannelVersion            `json:"channel_versions"`
	VersionsSeen    map[string]map[string]PythonChannelVersion `json:"versions_seen"`
	PendingSends    []json.RawMessage                          `json:"pending_sends"`
	UpdatedChannels []string                                   `json:"updated_channels"`
}

// Validate checks the portable Python checkpoint envelope.
func (c PythonCheckpoint) Validate() error {
	if c.V != PythonCheckpointVersion {
		return fmt.Errorf("%w: Python checkpoint version=%d, want=%d", ErrInvalidCheckpoint, c.V, PythonCheckpointVersion)
	}
	if c.ID == "" {
		return fmt.Errorf("%w: Python checkpoint ID is empty", ErrInvalidCheckpoint)
	}
	if _, err := time.Parse(time.RFC3339Nano, c.TS); err != nil {
		return fmt.Errorf("%w: invalid Python checkpoint timestamp: %v", ErrInvalidCheckpoint, err)
	}
	if c.ChannelValues == nil || c.ChannelVersions == nil || c.VersionsSeen == nil {
		return fmt.Errorf("%w: Python checkpoint maps must not be null", ErrInvalidCheckpoint)
	}
	for channel, value := range c.ChannelValues {
		if channel == "" || !json.Valid(value) {
			return fmt.Errorf("%w: invalid Python channel value for %q", ErrInvalidCheckpoint, channel)
		}
	}
	for channel, version := range c.ChannelVersions {
		if channel == "" {
			return fmt.Errorf("%w: empty Python channel-version name", ErrInvalidCheckpoint)
		}
		if err := version.validate(); err != nil {
			return fmt.Errorf("Python channel %q: %w", channel, err)
		}
	}
	for node, versions := range c.VersionsSeen {
		if node == "" || versions == nil {
			return fmt.Errorf("%w: invalid Python versions_seen node %q", ErrInvalidCheckpoint, node)
		}
		for channel, version := range versions {
			if channel == "" {
				return fmt.Errorf("%w: empty Python versions_seen channel", ErrInvalidCheckpoint)
			}
			if err := version.validate(); err != nil {
				return fmt.Errorf("Python versions_seen %q/%q: %w", node, channel, err)
			}
		}
	}
	for i, send := range c.PendingSends {
		if !json.Valid(send) {
			return fmt.Errorf("%w: invalid Python pending send at index %d", ErrInvalidCheckpoint, i)
		}
	}
	for _, channel := range c.UpdatedChannels {
		if channel == "" {
			return fmt.Errorf("%w: empty Python updated channel", ErrInvalidCheckpoint)
		}
	}
	return nil
}

// DecodePythonCheckpoint decodes and validates a Python 1.2.9 portable
// checkpoint dictionary.
func DecodePythonCheckpoint(data []byte) (PythonCheckpoint, error) {
	var value PythonCheckpoint
	if err := json.Unmarshal(data, &value); err != nil {
		return PythonCheckpoint{}, fmt.Errorf("%w: decode Python checkpoint: %v", ErrInvalidCheckpoint, err)
	}
	if err := value.Validate(); err != nil {
		return PythonCheckpoint{}, err
	}
	return value, nil
}

// EncodePythonCheckpoint validates and encodes a Python 1.2.9 portable
// checkpoint dictionary.
func EncodePythonCheckpoint(value PythonCheckpoint) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode Python checkpoint: %v", ErrInvalidCheckpoint, err)
	}
	return data, nil
}
