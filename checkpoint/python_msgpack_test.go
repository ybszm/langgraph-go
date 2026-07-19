package checkpoint_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
)

func TestPythonMessagePackPrivateExtensionFixtures12_9(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "python-1.2.9-msgpack-ext.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Values     map[string]string `json:"values"`
		Checkpoint string            `json:"checkpoint"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		"uuid":     `"12345678-1234-5678-1234-567812345678"`,
		"datetime": `"2026-07-19T08:30:45.123456+00:00"`,
		"set":      `[1,2,3]`,
		"send":     `{"arg":{"x":1},"node":"worker"}`,
	}
	for name, want := range expected {
		payload, err := base64.StdEncoding.DecodeString(fixture.Values[name])
		if err != nil {
			t.Fatal(err)
		}
		extension, err := checkpoint.DecodePythonMessagePackExtension(payload)
		if err != nil {
			t.Fatalf("%s extension: %v", name, err)
		}
		reencoded, err := checkpoint.EncodePythonMessagePackExtension(extension)
		if err != nil || !bytes.Equal(reencoded, payload) {
			t.Fatalf("%s exact round trip equal=%v err=%v", name, bytes.Equal(reencoded, payload), err)
		}
		portable, err := extension.PortableValue()
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqual(t, []byte(want), portable)
		decoded, err := checkpoint.DecodePythonMessagePackValue(payload)
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqual(t, portable, decoded)
	}

	nested, _ := base64.StdEncoding.DecodeString(fixture.Values["nested"])
	decoded, err := checkpoint.DecodePythonMessagePackValue(nested)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, []byte(`{
		"uuid":"12345678-1234-5678-1234-567812345678",
		"datetime":"2026-07-19T08:30:45.123456+00:00",
		"set":[1,2,3],
		"send":{"node":"worker","arg":{"x":1}}
	}`), decoded)

	checkpointBlob, _ := base64.StdEncoding.DecodeString(fixture.Checkpoint)
	decodedCheckpoint, err := checkpoint.DecodePythonMessagePackCheckpoint(checkpointBlob)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, []byte(`"12345678-1234-5678-1234-567812345678"`), decodedCheckpoint.ChannelValues["id"])
	assertJSONEqual(t, []byte(`{"node":"worker","arg":{"x":1}}`), decodedCheckpoint.PendingSends[0])
}

func TestPythonMessagePackExtensionRejectsUnknownAndNumPyCodes(t *testing.T) {
	if _, err := checkpoint.EncodePythonMessagePackExtension(checkpoint.PythonMessagePackExtension{
		Code: 99, Payload: []byte{0xc0},
	}); err == nil {
		t.Fatal("unknown extension code accepted")
	}
	numpy, err := checkpoint.EncodePythonMessagePackExtension(checkpoint.PythonMessagePackExtension{
		Code: 6, Payload: []byte{0x94, 0xa3, '<', 'i', '4', 0x91, 0x02, 0xa1, 'C', 0xc4, 0x00},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkpoint.DecodePythonMessagePackValue(numpy); err == nil {
		t.Fatal("NumPy extension decoded without an application array codec")
	}
}

func TestPythonMessagePackCheckpointFixture12_9(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("testdata", "python-1.2.9-sqlite-row.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Checkpoint []any `json:"checkpoint"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	payload, err := base64.StdEncoding.DecodeString(fixture.Checkpoint[5].(string))
	if err != nil {
		t.Fatal(err)
	}
	value, err := checkpoint.DecodePythonMessagePackCheckpoint(payload)
	if err != nil {
		t.Fatalf("decode Python msgpack fixture: %v", err)
	}
	if value.V != 2 || value.ID != "1f000000-0000-6000-8000-000000000001" {
		t.Fatalf("unexpected checkpoint: %#v", value)
	}
	if got := value.ChannelVersions["count"]; got.Kind() != checkpoint.PythonVersionInteger || got.String() != "2" {
		t.Fatalf("count version = %#v", got)
	}
	if got := string(value.ChannelValues["text"]); got != `"hello"` {
		t.Fatalf("text channel = %s", got)
	}

	reencoded, err := checkpoint.EncodePythonMessagePackCheckpoint(value)
	if err != nil {
		t.Fatalf("encode Python msgpack fixture: %v", err)
	}
	roundTrip, err := checkpoint.DecodePythonMessagePackCheckpoint(reencoded)
	if err != nil {
		t.Fatalf("decode reencoded checkpoint: %v", err)
	}
	left, err := checkpoint.EncodePythonCheckpoint(value)
	if err != nil {
		t.Fatal(err)
	}
	right, err := checkpoint.EncodePythonCheckpoint(roundTrip)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, left, right)
}

func TestPythonMessagePackTypedWriteFixtures12_9(t *testing.T) {
	t.Parallel()

	fixtures := map[string]string{
		"gaZwcm9tcHSpY29udGludWU/": `{"prompt":"continue?"}`,
		"gaJva8M=":                 `{"ok":true}`,
	}
	for encoded, want := range fixtures {
		payload, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		value, err := checkpoint.DecodePythonMessagePackValue(payload)
		if err != nil {
			t.Fatalf("decode typed write: %v", err)
		}
		assertJSONEqual(t, []byte(want), value)
		roundTrip, err := checkpoint.EncodePythonMessagePackValue(value)
		if err != nil {
			t.Fatalf("encode typed write: %v", err)
		}
		decoded, err := checkpoint.DecodePythonMessagePackValue(roundTrip)
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqual(t, value, decoded)
	}
}
