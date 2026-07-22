package distributed

import "testing"

func TestMetricsSnapshot(t *testing.T) {
	var m Metrics
	m.TasksEnqueued.Add(2)
	m.TasksCompleted.Add(1)
	snap := m.Snapshot()
	if snap.TasksEnqueued != 2 || snap.TasksCompleted != 1 {
		t.Fatalf("%+v", snap)
	}
}
