package remote

import (
	"fmt"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
)

// DevelopmentServerOptions returns ServerOptions suitable for local demos:
// in-memory durable EventLog (enables /threads/{id}/runs/{id}/stream) and
// sensible clocks/IDs. Production deployments should supply Postgres-backed
// ControlStore/EventLog implementations instead.
func DevelopmentServerOptions() (ServerOptions, error) {
	eventLog, err := distributed.NewMemoryEventLog(distributed.EventLogOptions{
		Clock: time.Now,
		IDGenerator: func() string {
			return randomID()
		},
	})
	if err != nil {
		return ServerOptions{}, fmt.Errorf("development event log: %w", err)
	}
	return ServerOptions{
		EventLog:            eventLog,
		ControlPollInterval: 50 * time.Millisecond,
		MaxBodyBytes:        1 << 20,
	}, nil
}
