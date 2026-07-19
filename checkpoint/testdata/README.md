# Checkpoint compatibility fixtures

`python-1.2.9-checkpoint.json` 由固定上游提交
`95af6a00718588e7b7ce17310e8006d267896a77` 的
`langgraph.checkpoint.base.empty_checkpoint()` 生成，再固定 ID、时间、channel
values、混合标量 channel versions、versions_seen 与 updated_channels。

它用于验证 Go codec 接受并保留 Python `langgraph-checkpoint` 1.2.9 的
portable checkpoint 字典；它不表示 Go saver 可以直接打开上游 SQLite 或
PostgreSQL 数据库。物理表布局与 typed serializer/blob 协议由独立的 SQL
interop/migration 契约覆盖。

`python-1.2.9-sqlite-row.json` 由同一固定提交的真实 `SqliteSaver` 数据库生成，
保留默认 msgpack checkpoint、JSON metadata，以及 interrupt/普通 pending writes
的 type、index 和 base64 payload，用于物理 SQLite adapter 的逐层契约。
