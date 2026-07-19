package checkpoint_test

import (
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
)

type codecValue struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func TestJSONCodecRoundTripAndIdentityValidation(t *testing.T) {
	codec := checkpoint.MustJSONCodec[codecValue]("tests.codec-value", 2)
	encoded, err := codec.Encode(codecValue{Name: "alpha", Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != (codecValue{Name: "alpha", Count: 3}) {
		t.Fatalf("decoded = %#v", decoded)
	}

	encoded.Version = 1
	if _, err := codec.Decode(encoded); !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("error = %v, want ErrCodecMismatch", err)
	}
}

func TestJSONCodecRejectsInvalidDefinition(t *testing.T) {
	if _, err := checkpoint.NewJSONCodec[codecValue]("", 1); !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("empty type error = %v", err)
	}
	if _, err := checkpoint.NewJSONCodec[codecValue]("type", 0); !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("zero version error = %v", err)
	}
}
