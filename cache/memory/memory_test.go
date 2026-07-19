package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/cache"
	"github.com/wahanbo/langgraph-go/cache/memory"
)

func TestStoreTTLIsolationAndClear(t *testing.T) {
	now := time.Unix(100, 0)
	store := memory.NewWithClock(func() time.Time { return now })
	keyA := cache.Key{Namespace: "a", Key: "1"}
	keyB := cache.Key{Namespace: "b", Key: "1"}
	if err := store.Set(context.Background(), map[cache.Key]cache.Item{
		keyA: {Data: []byte("a"), TTL: time.Second},
		keyB: {Data: []byte("b")},
	}); err != nil {
		t.Fatal(err)
	}
	values, err := store.Get(context.Background(), []cache.Key{keyA, keyB})
	if err != nil || string(values[keyA]) != "a" || string(values[keyB]) != "b" {
		t.Fatalf("values=%#v err=%v", values, err)
	}
	values[keyB][0] = 'x'
	again, _ := store.Get(context.Background(), []cache.Key{keyB})
	if string(again[keyB]) != "b" {
		t.Fatal("Get returned mutable internal bytes")
	}
	now = now.Add(time.Second)
	expired, _ := store.Get(context.Background(), []cache.Key{keyA, keyB})
	if _, exists := expired[keyA]; exists || string(expired[keyB]) != "b" {
		t.Fatalf("expired values=%#v", expired)
	}
	if err := store.Clear(context.Background(), []string{"b"}); err != nil {
		t.Fatal(err)
	}
	cleared, _ := store.Get(context.Background(), []cache.Key{keyB})
	if len(cleared) != 0 {
		t.Fatalf("clear result=%#v", cleared)
	}
}
