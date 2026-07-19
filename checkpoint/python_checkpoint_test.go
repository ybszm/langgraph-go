package checkpoint_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

func TestPythonCheckpointFixture12_9RoundTrip(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "python-1.2.9-checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := checkpoint.DecodePythonCheckpoint(fixture)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if value.V != checkpoint.PythonCheckpointVersion || value.ID != "1f000000-0000-6000-8000-000000000001" {
		t.Fatalf("unexpected envelope: %#v", value)
	}
	if got := value.ChannelVersions["text"]; got.Kind() != checkpoint.PythonVersionString || got.String() != "00000000000000000000000000000001.0.1" {
		t.Fatalf("string channel version = %#v", got)
	}
	if got := value.ChannelVersions["count"]; got.Kind() != checkpoint.PythonVersionInteger || got.String() != "2" {
		t.Fatalf("integer channel version = %#v", got)
	}
	if got := value.ChannelVersions["ratio"]; got.Kind() != checkpoint.PythonVersionFloat || got.String() != "1.5" {
		t.Fatalf("float channel version = %#v", got)
	}
	if got := string(value.ChannelValues["text"]); got != `"hello"` {
		t.Fatalf("raw channel value = %s", got)
	}

	encoded, err := checkpoint.EncodePythonCheckpoint(value)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	assertJSONEqual(t, fixture, encoded)
}

func TestPythonCheckpointRejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"v":1,"id":"id","ts":"2026-07-19T08:30:45Z","channel_values":{},"channel_versions":{},"versions_seen":{},"pending_sends":[],"updated_channels":null}`,
		`{"v":2,"id":"id","ts":"not-a-time","channel_values":{},"channel_versions":{},"versions_seen":{},"pending_sends":[],"updated_channels":null}`,
		`{"v":2,"id":"id","ts":"2026-07-19T08:30:45Z","channel_values":{},"channel_versions":{"bad":true},"versions_seen":{},"pending_sends":[],"updated_channels":null}`,
		`{"v":2,"id":"id","ts":"2026-07-19T08:30:45Z","channel_values":{"bad":},"channel_versions":{},"versions_seen":{},"pending_sends":[],"updated_channels":null}`,
	}
	for _, input := range tests {
		_, err := checkpoint.DecodePythonCheckpoint([]byte(input))
		if !errors.Is(err, checkpoint.ErrInvalidCheckpoint) {
			t.Fatalf("DecodePythonCheckpoint(%s) error = %v", input, err)
		}
	}
}

func TestPythonCheckpointVersionConstructors(t *testing.T) {
	t.Parallel()

	versions := []struct {
		value checkpoint.PythonChannelVersion
		kind  checkpoint.PythonVersionKind
		want  string
	}{
		{checkpoint.NewPythonStringVersion("v1"), checkpoint.PythonVersionString, `"v1"`},
		{checkpoint.NewPythonIntegerVersion(42), checkpoint.PythonVersionInteger, `42`},
		{checkpoint.NewPythonFloatVersion(1.25), checkpoint.PythonVersionFloat, `1.25`},
	}
	for _, tt := range versions {
		if tt.value.Kind() != tt.kind {
			t.Fatalf("kind = %q, want %q", tt.value.Kind(), tt.kind)
		}
		got, err := json.Marshal(tt.value)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Fatalf("encoded version = %s, want %s", got, tt.want)
		}
	}
}

func assertJSONEqual(t *testing.T, left, right []byte) {
	t.Helper()
	var l, r any
	if err := json.Unmarshal(left, &l); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(right, &r); err != nil {
		t.Fatal(err)
	}
	lb, _ := json.Marshal(l)
	rb, _ := json.Marshal(r)
	if string(lb) != string(rb) {
		t.Fatalf("JSON differs:\nleft:  %s\nright: %s", lb, rb)
	}
}
