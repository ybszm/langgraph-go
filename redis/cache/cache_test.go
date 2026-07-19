package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	lgcache "github.com/wahanbo/langgraph-go/cache"
	cacheredis "github.com/wahanbo/langgraph-go/redis/cache"
)

func TestStoreSetGetTTLAndClear(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := cacheredis.New(client, cacheredis.Options{Prefix: "test"})
	if err != nil {
		t.Fatal(err)
	}
	stable := lgcache.Key{Namespace: "worker", Key: "stable"}
	expiring := lgcache.Key{Namespace: "worker", Key: "expiring"}
	other := lgcache.Key{Namespace: "other", Key: "value"}
	if err := store.Set(context.Background(), map[lgcache.Key]lgcache.Item{stable: {Data: []byte("stable")}, expiring: {Data: []byte("short"), TTL: time.Second}, other: {Data: []byte("other")}}); err != nil {
		t.Fatal(err)
	}
	values, err := store.Get(context.Background(), []lgcache.Key{stable, expiring})
	if err != nil || string(values[stable]) != "stable" || string(values[expiring]) != "short" {
		t.Fatalf("values=%v err=%v", values, err)
	}
	server.FastForward(time.Second)
	values, _ = store.Get(context.Background(), []lgcache.Key{stable, expiring})
	if len(values) != 1 {
		t.Fatalf("expired values=%v", values)
	}
	if err := store.Clear(context.Background(), []string{"worker"}); err != nil {
		t.Fatal(err)
	}
	values, _ = store.Get(context.Background(), []lgcache.Key{stable, other})
	if len(values) != 1 || string(values[other]) != "other" {
		t.Fatalf("remaining=%v", values)
	}
	if err := store.Clear(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	values, _ = store.Get(context.Background(), []lgcache.Key{other})
	if len(values) != 0 {
		t.Fatalf("global clear=%v", values)
	}
}

func TestSetValidationIsAtomic(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, _ := cacheredis.New(client, cacheredis.Options{})
	good := lgcache.Key{Namespace: "atomic", Key: "good"}
	bad := lgcache.Key{Namespace: "atomic", Key: "bad"}
	err := store.Set(context.Background(), map[lgcache.Key]lgcache.Item{good: {Data: []byte("no")}, bad: {Data: []byte("bad"), TTL: -time.Second}})
	if err == nil {
		t.Fatal("negative TTL accepted")
	}
	values, _ := store.Get(context.Background(), []lgcache.Key{good, bad})
	if len(values) != 0 {
		t.Fatalf("partial write=%v", values)
	}
}
