package redis_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	lgcache "github.com/wahanbo/langgraph-go/cache"
	"github.com/wahanbo/langgraph-go/checkpoint"
	cachebackend "github.com/wahanbo/langgraph-go/redis/cache"
	checkpointbackend "github.com/wahanbo/langgraph-go/redis/checkpoint"
	storebackend "github.com/wahanbo/langgraph-go/redis/store"
	"github.com/wahanbo/langgraph-go/store"
)

func TestRealRedisBackendsShareCommittedData(t *testing.T) {
	address := os.Getenv("LANGGRAPH_REDIS_ADDR")
	if address == "" {
		t.Skip("LANGGRAPH_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	first := redis.NewClient(&redis.Options{Addr: address})
	second := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	if err := first.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("langgraph-integration-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		keys, _ := first.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			_ = first.Del(context.Background(), keys...).Err()
		}
	})

	firstCache, _ := cachebackend.New(first, cachebackend.Options{Prefix: prefix})
	secondCache, _ := cachebackend.New(second, cachebackend.Options{Prefix: prefix})
	cacheKey := lgcache.Key{Namespace: "integration", Key: "result"}
	if err := firstCache.Set(ctx, map[lgcache.Key]lgcache.Item{cacheKey: {Data: []byte("committed")}}); err != nil {
		t.Fatal(err)
	}
	values, err := secondCache.Get(ctx, []lgcache.Key{cacheKey})
	if err != nil || string(values[cacheKey]) != "committed" {
		t.Fatalf("cache=%v err=%v", values, err)
	}

	firstStore, _ := storebackend.New(first, storebackend.Options{Prefix: prefix})
	secondStore, _ := storebackend.New(second, storebackend.Options{Prefix: prefix})
	namespace := store.Namespace{"integration"}
	if err := firstStore.Put(ctx, namespace, "memory", store.Value{"answer": 42}); err != nil {
		t.Fatal(err)
	}
	item, err := secondStore.Get(ctx, namespace, "memory")
	if err != nil || item == nil || item.Value["answer"] != 42 {
		t.Fatalf("store=%+v err=%v", item, err)
	}

	firstSaver, _ := checkpointbackend.New(first, checkpointbackend.Options{Prefix: prefix})
	secondSaver, _ := checkpointbackend.New(second, checkpointbackend.Options{Prefix: prefix})
	value := checkpoint.Checkpoint{Version: checkpoint.CurrentVersion, ID: "0001", Timestamp: time.Now().UTC(), Values: map[string]checkpoint.EncodedValue{}, ChannelVersions: map[string]string{}, VersionsSeen: map[string]map[string]string{}}
	config, err := firstSaver.Put(ctx, checkpoint.Config{ThreadID: "integration"}, value, checkpoint.Metadata{"source": "integration"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tuple, found, err := secondSaver.GetTuple(ctx, config)
	if err != nil || !found || tuple.Checkpoint.ID != "0001" {
		t.Fatalf("checkpoint=%+v found=%v err=%v", tuple, found, err)
	}
}
