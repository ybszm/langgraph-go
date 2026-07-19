import json
import sys

from langgraph.store.postgres import PostgresStore


mode, dsn, scope = sys.argv[1:4]
namespace = ("interop", scope)

with PostgresStore.from_conn_string(dsn) as store:
    store.setup()
    if mode == "write":
        store.put(namespace, "python-item", {"writer": "python", "kind": "shared", "count": 2})
        item = store.get(namespace, "python-item")
        print(json.dumps({
            "namespace": list(item.namespace),
            "key": item.key,
            "value": item.value,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat(),
        }))
    elif mode == "read":
        item = store.get(namespace, "go-item")
        matches = store.search(namespace, filter={"kind": "shared"}, limit=10)
        namespaces = store.list_namespaces(prefix=namespace, limit=10)
        print(json.dumps({
            "item": None if item is None else {
                "namespace": list(item.namespace), "key": item.key, "value": item.value
            },
            "matches": [{"key": value.key, "value": value.value} for value in matches],
            "namespaces": [list(value) for value in namespaces],
        }))
    else:
        raise ValueError(mode)
