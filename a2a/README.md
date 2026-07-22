# A2A subset (langgraph-go)

Minimal Agent-to-Agent HTTP protocol for services that already use
langgraph-go agents. This is **not** a full multi-vendor A2A implementation.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/a2a/v1/card` | Agent card (name, skills, metadata) |
| `POST` | `/a2a/v1/message:send` | Send a user/agent message, receive agent reply |

Header: `X-A2A-Protocol: langgraph-go-a2a/0.1`

## Server

```go
srv := &a2a.Server{
    Card: a2a.AgentCard{Name: "ops-agent", Skills: []string{"diagnose"}},
    Handler: func(ctx context.Context, req a2a.SendRequest) (a2a.SendResponse, error) {
        // Call prebuilt.QuickAgent / MultiAgentCoordinator here.
        return a2a.SendResponse{
            SessionID: req.SessionID,
            Message:   a2a.Message{Role: "agent", Content: "ok"},
        }, nil
    },
}
http.Handle("/a2a/", srv)
```

## Client

```go
client := &a2a.Client{BaseURL: "http://127.0.0.1:8080"}
card, _ := client.GetCard(ctx)
resp, _ := client.Send(ctx, a2a.SendRequest{
    SessionID: "s1",
    Message:   a2a.Message{Role: "user", Content: "check redis"},
})
```

## Security

Put TLS, authentication, and rate limits in front of these handlers. See the
`httpware` package for bearer auth and rate limiting helpers.
