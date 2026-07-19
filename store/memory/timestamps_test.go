package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/store"
)

func TestUpdatePreservesCreatedAt(t *testing.T) {
	created := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	updated := created.Add(time.Minute)
	times := []time.Time{created, updated}
	s := New()
	s.now = func() time.Time {
		value := times[0]
		times = times[1:]
		return value
	}
	namespace := store.Namespace{"timestamps"}
	if err := s.Put(context.Background(), namespace, "key", store.Value{"version": 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), namespace, "key", store.Value{"version": 2}); err != nil {
		t.Fatal(err)
	}
	item, err := s.Get(context.Background(), namespace, "key")
	if err != nil {
		t.Fatal(err)
	}
	if item.CreatedAt != created || item.UpdatedAt != updated {
		t.Fatalf("timestamps created=%v updated=%v", item.CreatedAt, item.UpdatedAt)
	}
}
