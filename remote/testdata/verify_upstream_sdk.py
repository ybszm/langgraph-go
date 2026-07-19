"""End-to-end smoke test for the pinned LangGraph Python SDK against Go."""
import sys
from langgraph_sdk import get_sync_client

client = get_sync_client(url=sys.argv[1], api_key=None)
thread = client.threads.create(thread_id="python-sdk-thread", metadata={"tenant": "acme"})
assert thread["thread_id"] == "python-sdk-thread"
assert thread["metadata"] == {"tenant": "acme"}
loaded = client.threads.get("python-sdk-thread")
assert loaded["metadata"] == {"tenant": "acme"}
run = client.runs.create("python-sdk-thread", "graph-1", input={"value": 3})
assert run["assistant_id"] == "graph-1"
output = client.runs.join("python-sdk-thread", run["run_id"])
assert output == {"value": 9}
state = client.threads.get_state("python-sdk-thread")
assert state["values"] == {"Count": 3}
updated = client.threads.update_state("python-sdk-thread", {"Add": 2}, as_node="next")
assert updated["checkpoint"]["checkpoint_id"] == "cp-3"
history = client.threads.get_history("python-sdk-thread", limit=4)
assert len(history) == 1
client.close()
