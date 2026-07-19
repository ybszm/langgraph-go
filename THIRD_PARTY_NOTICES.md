# Third-party notices

LangGraph Go is an independent behavioral reimplementation informed by the
public API, documentation, tests, and MIT-licensed source of
[`langchain-ai/langgraph`](https://github.com/langchain-ai/langgraph).

The compatibility reference is LangGraph `1.2.9`, commit
`95af6a00718588e7b7ce17310e8006d267896a77`. No upstream source tree is
vendored in this repository. LangGraph and LangChain are trademarks of their
respective owners; this project is not affiliated with or endorsed by them.

Direct Go dependencies and their license information include:

- `github.com/jackc/pgx/v5` — MIT
- `github.com/vmihailenco/msgpack/v5` — BSD-2-Clause
- `go.temporal.io/api` and `go.temporal.io/sdk` — MIT
- `golang.org/x/sync` — BSD-3-Clause
- `modernc.org/sqlite` — BSD-3-Clause

Test-only dependencies and transitive dependencies are recorded in `go.mod`
and `go.sum`; their full license texts are distributed by their respective
projects.
