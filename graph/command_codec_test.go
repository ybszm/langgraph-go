package graph_test

import (
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
)

func TestCommandCodecRoundTripsTypedUpdateGotoTargetAndSends(t *testing.T) {
	codec, err := graph.NewCommandCodec[testState, testDelta](checkpoint.MustJSONCodec[testState]("state", 1), checkpoint.MustJSONCodec[testDelta]("delta", 1))
	if err != nil {
		t.Fatal(err)
	}
	emptyGoto := []graph.NodeID{}
	command := graph.Command[testDelta]{
		Update: testDelta{Add: 2, Label: "x"}, HasUpdate: true, Goto: emptyGoto,
		Sends:  []graph.TaskSend{graph.SendTo("worker", testState{Total: 7, Path: []string{"sent"}})},
		Target: graph.CommandParent,
	}
	encoded, err := codec.Encode(command)
	if err != nil {
		t.Fatal(err)
	}
	command.Sends[0].State.(testState).Path[0] = "mutated"
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	state, ok := decoded.Sends[0].State.(testState)
	if !decoded.HasUpdate || decoded.Update.Add != 2 || decoded.Goto == nil || len(decoded.Goto) != 0 ||
		decoded.Target != graph.CommandParent || !ok || state.Total != 7 || state.Path[0] != "sent" {
		t.Fatalf("decoded=%+v send=%+v", decoded, state)
	}
}

func TestCommandCodecRejectsWrongEnvelope(t *testing.T) {
	codec, _ := graph.NewCommandCodec[testState, testDelta](checkpoint.MustJSONCodec[testState]("state", 1), checkpoint.MustJSONCodec[testDelta]("delta", 1))
	_, err := codec.Decode(checkpoint.EncodedValue{Type: "wrong", Version: 1})
	if !errors.Is(err, checkpoint.ErrCodecMismatch) {
		t.Fatalf("err=%v", err)
	}
}

var _ checkpoint.Codec[graph.Command[testDelta]] = mustCommandCodec()

func mustCommandCodec() *graph.CommandCodec[testState, testDelta] {
	codec, _ := graph.NewCommandCodec[testState, testDelta](checkpoint.MustJSONCodec[testState]("state", 1), checkpoint.MustJSONCodec[testDelta]("delta", 1))
	return codec
}
