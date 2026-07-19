package checkpoint_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

type binaryFixture struct {
	Name  string
	Count int
}

func TestMessagePackAndProtobufCodecFixtures(t *testing.T) {
	messagePackFixture := []byte{0x82, 0xa4, 'n', 'a', 'm', 'e', 0xa5, 'a', 'l', 'p', 'h', 'a', 0xa5, 'c', 'o', 'u', 'n', 't', 0x07}
	protobufFixture := []byte{0x0a, 0x05, 'a', 'l', 'p', 'h', 'a', 0x10, 0x07}
	want := binaryFixture{Name: "alpha", Count: 7}
	for _, test := range []struct {
		name     string
		newCodec func(checkpoint.BinaryMarshalFunc[binaryFixture], checkpoint.BinaryUnmarshalFunc[binaryFixture]) (*checkpoint.BinaryCodec[binaryFixture], error)
		fixture  []byte
		wantType string
	}{
		{name: "messagepack", fixture: messagePackFixture, wantType: "msgpack/tests.fixture", newCodec: func(m checkpoint.BinaryMarshalFunc[binaryFixture], u checkpoint.BinaryUnmarshalFunc[binaryFixture]) (*checkpoint.BinaryCodec[binaryFixture], error) {
			return checkpoint.NewMessagePackCodec("tests.fixture", 1, m, u)
		}},
		{name: "protobuf", fixture: protobufFixture, wantType: "protobuf/tests.fixture", newCodec: func(m checkpoint.BinaryMarshalFunc[binaryFixture], u checkpoint.BinaryUnmarshalFunc[binaryFixture]) (*checkpoint.BinaryCodec[binaryFixture], error) {
			return checkpoint.NewProtobufCodec("tests.fixture", 1, m, u)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			serializerBuffer := append([]byte(nil), test.fixture...)
			decodeErr := errors.New("invalid fixture")
			codec, err := test.newCodec(
				func(value binaryFixture) ([]byte, error) {
					if value != want {
						return nil, errors.New("unexpected value")
					}
					return serializerBuffer, nil
				},
				func(data []byte) (binaryFixture, error) {
					if !bytes.Equal(data, test.fixture) {
						return binaryFixture{}, decodeErr
					}
					if len(data) > 0 {
						data[0] ^= 0xff
					}
					return want, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := codec.Encode(want)
			if err != nil || encoded.Type != test.wantType || !bytes.Equal(encoded.Data, test.fixture) {
				t.Fatalf("encoded=%+v err=%v", encoded, err)
			}
			encoded.Data[0] ^= 0xff
			if serializerBuffer[0] != test.fixture[0] {
				t.Fatal("Encode aliases serializer buffer")
			}
			encoded.Data = append([]byte(nil), test.fixture...)
			decoded, err := codec.Decode(encoded)
			if err != nil || decoded != want || !bytes.Equal(encoded.Data, test.fixture) {
				t.Fatalf("decoded=%+v encoded=%x err=%v", decoded, encoded.Data, err)
			}
			bad := encoded
			bad.Data = []byte{0xff}
			if _, err := codec.Decode(bad); !errors.Is(err, decodeErr) {
				t.Fatalf("decode err=%v", err)
			}
		})
	}
}

func TestBinaryCodecRejectsInvalidDefinitionAndIdentity(t *testing.T) {
	if _, err := checkpoint.NewBinaryCodec[binaryFixture]("", 0, nil, nil); !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("definition err=%v", err)
	}
	codec, _ := checkpoint.NewProtobufCodec("tests.fixture", 1,
		func(binaryFixture) ([]byte, error) { return []byte{}, nil },
		func([]byte) (binaryFixture, error) { return binaryFixture{}, nil },
	)
	if _, err := codec.Decode(checkpoint.EncodedValue{Type: "protobuf/other", Version: 1}); !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("identity err=%v", err)
	}
}
