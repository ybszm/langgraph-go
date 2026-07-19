"""LangGraph 1.2.9 / checkpoint-postgres 3.1.0 physical interop fixture."""

from __future__ import annotations

import json
import os
import sys

from langgraph.checkpoint.postgres import PostgresSaver


def main() -> None:
    mode, dsn, thread_id = sys.argv[1:4]
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    with PostgresSaver.from_conn_string(dsn) as saver:
        saver.setup()
        if mode == "write":
            checkpoint = {
                "v": 2,
                "id": "1f000000-0000-6000-8000-000000000201",
                "ts": "2026-07-19T08:30:45+00:00",
                "channel_values": {
                    "text": "python",
                    "object": {"writer": "python", "count": 2},
                },
                "channel_versions": {"text": "v1", "object": "v2"},
                "versions_seen": {"worker": {"object": "v2"}},
                "pending_sends": [],
                "updated_channels": ["text", "object"],
            }
            stored = saver.put(
                config,
                checkpoint,
                {"source": "loop", "step": 2, "fixture": "python"},
                {"text": "v1", "object": "v2"},
            )
            saver.put_writes(
                stored,
                [("result", {"ok": True, "writer": "python"})],
                "python-task",
                task_path="pull/python",
            )
            print(json.dumps({"checkpoint_id": stored["configurable"]["checkpoint_id"]}))
            return
        if mode == "read":
            value = saver.get_tuple(config)
            if value is None:
                raise RuntimeError("Go checkpoint was not found")
            checkpoint = value.checkpoint
            writes = [
                {
                    "task_id": task_id,
                    "channel": channel,
                    "value": item,
                }
                for task_id, channel, item in value.pending_writes
            ]
            print(
                json.dumps(
                    {
                        "checkpoint": checkpoint,
                        "metadata": value.metadata,
                        "writes": writes,
                    },
                    default=str,
                    sort_keys=True,
                )
            )
            return
        raise ValueError(f"unknown mode: {mode}")


if __name__ == "__main__":
    # Avoid accidentally inheriting application-specific serializer settings.
    os.environ.pop("LANGGRAPH_STRICT_MSGPACK", None)
    main()
