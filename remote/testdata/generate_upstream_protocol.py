"""Capture fixed LangGraph 1.2.9 Python SDK HTTP requests.

Run with PYTHONPATH pointing at the pinned checkout's libs/sdk-py directory.
The JSON output is copied to upstream_protocol_1_2_9.json and reviewed in git.
"""
import json
import httpx

from langgraph_sdk.client import SyncHttpClient, SyncRunsClient, SyncThreadsClient

captured = []

def handler(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content) if request.content else None
    captured.append({"method": request.method, "path": request.url.path,
                     "query": request.url.query.decode(), "body": body})
    path = request.url.path
    if path.endswith("/runs") and request.method == "GET": response = []
    elif path.endswith("/history"): response = []
    elif path.endswith("/join"): response = {"value": 9}
    elif "/state" in path and request.method == "POST": response = {"checkpoint": {"thread_id":"thread-1","checkpoint_ns":"","checkpoint_id":"cp-1"}}
    elif "/state" in path: response = {"values":{},"next":[],"checkpoint":{"thread_id":"thread-1","checkpoint_ns":"","checkpoint_id":"cp-1"},"metadata":{},"created_at":None,"parent_checkpoint":None,"tasks":[],"interrupts":[]}
    elif "/runs/" in path: response = {"run_id":"run-1"}
    elif path.endswith("/runs"): response = {"run_id":"run-1"}
    else: response = {"thread_id":"thread-1"}
    return httpx.Response(200, json=response)

with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://fixture.invalid") as raw:
    http = SyncHttpClient(raw); threads = SyncThreadsClient(http); runs = SyncRunsClient(http)
    threads.create(thread_id="thread-1", metadata={"tenant":"acme"}, if_exists="raise")
    threads.get("thread-1")
    runs.create("thread-1", "graph-1", input={"value":3}, config={"configurable":{"mode":"fixture"}}, stream_mode=["values","updates"])
    runs.list("thread-1", limit=5, offset=2)
    runs.get("thread-1", "run-1")
    runs.cancel("thread-1", "run-1", wait=False, action="interrupt")
    runs.join("thread-1", "run-1")
    threads.get_state("thread-1", subgraphs=True)
    threads.update_state("thread-1", {"value":7}, as_node="worker")
    threads.get_history("thread-1", limit=4, before="cp-0")

print(json.dumps({"upstream":"langgraph 1.2.9","commit":"95af6a00718588e7b7ce17310e8006d267896a77","requests":captured}, indent=2, sort_keys=True))
