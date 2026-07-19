package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	storeredis "github.com/wahanbo/langgraph-go/redis/store"
	"github.com/wahanbo/langgraph-go/store"
	"github.com/wahanbo/langgraph-go/store/storetest"
)

func TestStoreContract(t *testing.T) {
	server := miniredis.RunT(t)
	storetest.Run(t, func(t *testing.T) store.Store {
		server.FlushAll()
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		value, err := storeredis.New(client, storeredis.Options{Prefix: "contract"})
		if err != nil {
			t.Fatal(err)
		}
		return value
	})
}

func TestTTLRefreshAndSweep(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	value, _ := storeredis.New(client, storeredis.Options{Prefix: "ttl", Clock: func() time.Time { return now }})
	namespace := store.Namespace{"ttl"}
	if err := value.PutWithTTL(context.Background(), namespace, "refresh", store.Value{"value": 1}, time.Minute); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if _, err := value.GetWithTTLRefresh(context.Background(), namespace, "refresh", true); err != nil {
		t.Fatal(err)
	}
	now = now.Add(40 * time.Second)
	count, err := value.SweepExpired(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("early count=%d err=%v", count, err)
	}
	now = now.Add(51 * time.Second)
	count, err = value.SweepExpired(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("expired count=%d err=%v", count, err)
	}
}
