package distributed

import "sync/atomic"

// Metrics is a process-local counter set for distributed workers and queues.
// Callers may export Snapshot values to Prometheus or logging without importing
// a metrics SDK into the core module.
type Metrics struct {
	TasksEnqueued   atomic.Uint64
	TasksLeased     atomic.Uint64
	TasksCompleted  atomic.Uint64
	TasksFailed     atomic.Uint64
	LeaseTimeouts   atomic.Uint64
	FenceConflicts  atomic.Uint64
	OutboxPublished atomic.Uint64
	OutboxRetried   atomic.Uint64
	EventsAppended  atomic.Uint64
	EventsTailed    atomic.Uint64
}

// Snapshot is a point-in-time copy of Metrics counters.
type Snapshot struct {
	TasksEnqueued   uint64 `json:"tasks_enqueued"`
	TasksLeased     uint64 `json:"tasks_leased"`
	TasksCompleted  uint64 `json:"tasks_completed"`
	TasksFailed     uint64 `json:"tasks_failed"`
	LeaseTimeouts   uint64 `json:"lease_timeouts"`
	FenceConflicts  uint64 `json:"fence_conflicts"`
	OutboxPublished uint64 `json:"outbox_published"`
	OutboxRetried   uint64 `json:"outbox_retried"`
	EventsAppended  uint64 `json:"events_appended"`
	EventsTailed    uint64 `json:"events_tailed"`
}

// Snapshot returns the current counter values.
func (m *Metrics) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{
		TasksEnqueued:   m.TasksEnqueued.Load(),
		TasksLeased:     m.TasksLeased.Load(),
		TasksCompleted:  m.TasksCompleted.Load(),
		TasksFailed:     m.TasksFailed.Load(),
		LeaseTimeouts:   m.LeaseTimeouts.Load(),
		FenceConflicts:  m.FenceConflicts.Load(),
		OutboxPublished: m.OutboxPublished.Load(),
		OutboxRetried:   m.OutboxRetried.Load(),
		EventsAppended:  m.EventsAppended.Load(),
		EventsTailed:    m.EventsTailed.Load(),
	}
}

// DefaultMetrics is an optional process-wide registry for simple demos.
var DefaultMetrics = &Metrics{}
