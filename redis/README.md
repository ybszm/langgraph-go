# Redis backends

This optional module provides Redis implementations of the core checkpoint,
long-term store, and task-cache contracts without adding Redis dependencies to
the graph runtime.

```go
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

saver, err := checkpointredis.New(client, checkpointredis.Options{Prefix: "my-app"})
memoryStore, err := storeredis.New(client, storeredis.Options{Prefix: "my-app"})
taskCache, err := cacheredis.New(client, cacheredis.Options{Prefix: "my-app"})
```

All constructors accept `redis.UniversalClient`, so standalone, Sentinel, and
Cluster clients can be supplied by the application. The caller owns and closes
the client. Prefixes isolate applications sharing one Redis deployment.

The checkpoint saver stores one complete, versioned checkpoint envelope per
checkpoint ID and keeps retry-safe pending writes separately. The task cache
uses Redis-native TTLs. The long-term store supports structured filters,
namespace listing, optimistic transactional batch snapshots, TTL refresh, and
explicit expiration sweeps.

Store items for one prefix share a Redis hash. Point reads use `HGET`, while
Search, ListNamespaces, and Batch intentionally read a consistent logical
snapshot with `HGETALL`. Use separate prefixes to partition large data sets;
semantic/vector search remains the responsibility of `store/vector`.

Set `LANGGRAPH_REDIS_ADDR` to run the optional integration tests against a real
Redis server. Unit and contract tests use miniredis and require no service.
