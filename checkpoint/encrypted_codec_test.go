package checkpoint_test

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
)

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestEncryptedCodecRoundTripTamperAndIdentityBinding(t *testing.T) {
	base := checkpoint.MustJSONCodec[codecValue]("tests.secret", 3)
	key := bytes.Repeat([]byte{0x42}, 32)
	codec, err := checkpoint.NewEncryptedCodec(base, key, checkpoint.WithEncryptionRandom(bytes.NewReader(bytes.Repeat([]byte{0x11}, 64))))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(codecValue{Name: "alpha", Count: 7})
	if err != nil {
		t.Fatal(err)
	}
	if encoded.Type != checkpoint.EncryptedTypePrefix+"tests.secret" || encoded.Version != 3 || bytes.Contains(encoded.Data, []byte("alpha")) {
		t.Fatalf("encoded=%+v", encoded)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil || decoded != (codecValue{Name: "alpha", Count: 7}) {
		t.Fatalf("decoded=%#v error=%v", decoded, err)
	}

	tampered := checkpoint.CloneEncodedValue(encoded)
	tampered.Data[len(tampered.Data)-1] ^= 0xff
	if _, err := codec.Decode(tampered); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("tamper error=%v", err)
	}
	wrongKey, _ := checkpoint.NewEncryptedCodec(base, bytes.Repeat([]byte{0x24}, 32))
	if _, err := wrongKey.Decode(encoded); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("wrong-key error=%v", err)
	}
	identitySwap := checkpoint.CloneEncodedValue(encoded)
	identitySwap.Version++
	if _, err := codec.Decode(identitySwap); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("identity error=%v", err)
	}
}

func TestEncryptedCodecRejectsInvalidDefinitionAndEntropyFailure(t *testing.T) {
	base := checkpoint.MustJSONCodec[codecValue]("tests.secret", 1)
	if _, err := checkpoint.NewEncryptedCodec[codecValue](nil, bytes.Repeat([]byte{1}, 32)); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("nil codec error=%v", err)
	}
	if _, err := checkpoint.NewEncryptedCodec(base, []byte("short")); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("key error=%v", err)
	}
	if _, err := checkpoint.NewEncryptedCodec(base, bytes.Repeat([]byte{1}, 32), nil); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("nil option error=%v", err)
	}
	entropy := errors.New("entropy unavailable")
	codec, err := checkpoint.NewEncryptedCodec(base, bytes.Repeat([]byte{1}, 32), checkpoint.WithEncryptionRandom(failingReader{err: entropy}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(codecValue{}); !errors.Is(err, entropy) || !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("entropy error=%v", err)
	}
	if _, err := codec.Decode(checkpoint.EncodedValue{Type: checkpoint.EncryptedTypePrefix + "tests.secret", Version: 1, Data: []byte{1}}); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("truncated error=%v", err)
	}
}

func TestEncryptedCodecDefaultUsesFreshNonces(t *testing.T) {
	base := checkpoint.MustJSONCodec[codecValue]("tests.secret", 1)
	codec, err := checkpoint.NewEncryptedCodec(base, bytes.Repeat([]byte{0x55}, 32))
	if err != nil {
		t.Fatal(err)
	}
	first, err := codec.Encode(codecValue{Name: "same"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.Encode(codecValue{Name: "same"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Data, second.Data) {
		t.Fatal("two encryptions reused a nonce/ciphertext")
	}
	plainType := first
	plainType.Type = "tests.secret"
	if _, err := codec.Decode(plainType); !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("plaintext identity error=%v", err)
	}
}

func TestRotatingEncryptedCodecReadsOldKeyAndWritesActiveKey(t *testing.T) {
	base := checkpoint.MustJSONCodec[codecValue]("tests.secret", 1)
	oldKey := bytes.Repeat([]byte{0x10}, 32)
	newKey := bytes.Repeat([]byte{0x20}, 32)
	oldCodec, err := checkpoint.NewRotatingEncryptedCodec(base, "old", map[string][]byte{"old": oldKey})
	if err != nil {
		t.Fatal(err)
	}
	oldValue, err := oldCodec.Encode(codecValue{Name: "before rotation"})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := checkpoint.NewRotatingEncryptedCodec(base, "new", map[string][]byte{"old": oldKey, "new": newKey})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := rotated.Decode(oldValue)
	if err != nil || decoded.Name != "before rotation" {
		t.Fatalf("decoded=%#v error=%v", decoded, err)
	}
	newValue, err := rotated.Encode(codecValue{Name: "after rotation"})
	if err != nil {
		t.Fatal(err)
	}
	if newValue.Type != checkpoint.EncryptedTypePrefix+"new/tests.secret" {
		t.Fatalf("type=%q", newValue.Type)
	}
	withoutOld, _ := checkpoint.NewRotatingEncryptedCodec(base, "new", map[string][]byte{"new": newKey})
	if _, err := withoutOld.Decode(oldValue); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("removed-key error=%v", err)
	}
	tampered := checkpoint.CloneEncodedValue(oldValue)
	tampered.Type = strings.Replace(tampered.Type, "/old/", "/new/", 1)
	if _, err := rotated.Decode(tampered); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("key-id tamper error=%v", err)
	}
}

func TestRotatingEncryptedCodecRejectsInvalidKeyring(t *testing.T) {
	base := checkpoint.MustJSONCodec[codecValue]("tests.secret", 1)
	key := bytes.Repeat([]byte{1}, 32)
	for name, test := range map[string]struct {
		active string
		keys   map[string][]byte
	}{
		"empty active":         {keys: map[string][]byte{"key": key}},
		"missing active":       {active: "missing", keys: map[string][]byte{"key": key}},
		"invalid key ID":       {active: "bad/id", keys: map[string][]byte{"bad/id": key}},
		"invalid inactive key": {active: "good", keys: map[string][]byte{"good": key, "bad/id": key}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := checkpoint.NewRotatingEncryptedCodec(base, test.active, test.keys); !errors.Is(err, checkpoint.ErrEncryption) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestResolvingEncryptedCodecUsesActiveAndHistoricalKeys(t *testing.T) {
	base := checkpoint.MustJSONCodec[codecValue]("tests.secret", 3)
	oldKey := bytes.Repeat([]byte{0x31}, 32)
	newKey := bytes.Repeat([]byte{0x32}, 32)
	oldCodec, err := checkpoint.NewEncryptedCodec(base, oldKey,
		checkpoint.WithEncryptionKeyID("old"),
		checkpoint.WithEncryptionRandom(bytes.NewReader(bytes.Repeat([]byte{0x11}, 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	oldValue, err := oldCodec.Encode(codecValue{Name: "historical"})
	if err != nil {
		t.Fatal(err)
	}
	resolved := make([]string, 0)
	resolverErr := errors.New("kms unavailable")
	resolver := checkpoint.EncryptionKeyResolverFunc(func(keyID string) ([]byte, error) {
		resolved = append(resolved, keyID)
		switch keyID {
		case "old":
			return oldKey, nil
		case "new":
			return newKey, nil
		default:
			return nil, resolverErr
		}
	})
	codec, err := checkpoint.NewResolvingEncryptedCodec(base, "new", resolver,
		checkpoint.WithEncryptionRandom(bytes.NewReader(bytes.Repeat([]byte{0x22}, 32))),
	)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(oldValue)
	if err != nil || decoded.Name != "historical" {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	newValue, err := codec.Encode(codecValue{Name: "active"})
	if err != nil || newValue.Type != checkpoint.EncryptedTypePrefix+"new/tests.secret" {
		t.Fatalf("encoded=%+v err=%v", newValue, err)
	}
	if !reflect.DeepEqual(resolved, []string{"old", "new"}) {
		t.Fatalf("resolved=%v", resolved)
	}
	unknown := oldValue
	unknown.Type = checkpoint.EncryptedTypePrefix + "missing/tests.secret"
	if _, err := codec.Decode(unknown); !errors.Is(err, checkpoint.ErrEncryption) || !errors.Is(err, resolverErr) {
		t.Fatalf("unknown-key err=%v", err)
	}
	if oldKey[0] != 0x31 || newKey[0] != 0x32 {
		t.Fatal("resolver-owned key material was mutated")
	}
}
