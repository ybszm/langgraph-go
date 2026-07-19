# MCP client

This optional Go 1.25 module uses the official Model Context Protocol Go SDK.
It connects over Streamable HTTP or a child process, discovers paginated tool
definitions, and converts them into schema-bearing `prebuilt.Tool` values.

```go
client, err := mcpclient.ConnectHTTP(ctx, "http://localhost:3000/mcp", nil, mcpclient.Options{})
if err != nil {
    return err
}
defer client.Close()

tools, err := mcpclient.Tools[prebuilt.AgentState, prebuilt.AgentDelta](ctx, client)
```

Protocol failures are returned as Go errors. A successful MCP response marked
`isError` becomes an error-status tool message, allowing the model to repair its
arguments. Structured results are retained as tool artifacts.
