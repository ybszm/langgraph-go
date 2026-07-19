package sse_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/wahanbo/langgraph-go/providers/internal/sse"
)

func TestScanCommentsMultilineAndFinalEvent(t *testing.T) {
	var got [][2]string
	err := sse.Scan(context.Background(), strings.NewReader(": ping\nevent: chunk\ndata: one\ndata: two\n\ndata: final"), 1024, func(event, data string) error { got = append(got, [2]string{event, data}); return nil })
	want := [][2]string{{"chunk", "one\ntwo"}, {"", "final"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestScanBoundsAndCancellation(t *testing.T) {
	if err := sse.Scan(context.Background(), strings.NewReader("data: too-long\n\n"), 4, func(string, string) error { return nil }); err == nil {
		t.Fatal("size limit accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sse.Scan(ctx, strings.NewReader("data: x\n\n"), 100, func(string, string) error { return nil }); err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
}
