package graph_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointsqlite "github.com/ybszm/langgraph-go/checkpoint/sqlite"
	"github.com/ybszm/langgraph-go/graph"
)

const recoveryThreadID = "sqlite-worker-kill-recovery"

func appendRecoveryMarker(path, value string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(value + "\n"); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func compileWorkerKillGraph(
	t *testing.T,
	saver checkpoint.Saver,
	markerPath string,
	helper bool,
) *graph.CompiledGraph[testState, testDelta] {
	t.Helper()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "fast", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		if err := appendRecoveryMarker(markerPath, "fast"); err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: "fast"}), nil
	})
	addNode(t, builder, "slow", func(
		ctx context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		if helper {
			if err := appendRecoveryMarker(markerPath, "slow-start"); err != nil {
				return graph.NoCommand[testDelta](), err
			}
			<-ctx.Done()
			return graph.NoCommand[testDelta](), ctx.Err()
		}
		if err := appendRecoveryMarker(markerPath, "slow-recovered"); err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 2, Label: "slow"}), nil
	})
	addEdge(t, builder, graph.START, "fast")
	addEdge(t, builder, graph.START, "slow")
	addEdge(t, builder, "fast", graph.END)
	addEdge(t, builder, "slow", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[testState]("tests.worker-kill-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.worker-kill-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestSQLiteWorkerKillRecoveryPreservesSuccessfulPeer(t *testing.T) {
	databasePath := os.Getenv("LANGGRAPH_RECOVERY_DB")
	markerPath := os.Getenv("LANGGRAPH_RECOVERY_MARKERS")
	if os.Getenv("LANGGRAPH_RECOVERY_HELPER") == "1" {
		saver, err := checkpointsqlite.Open(context.Background(), databasePath)
		if err != nil {
			t.Fatal(err)
		}
		defer saver.Close()
		compiled := compileWorkerKillGraph(t, saver, markerPath, true)
		_, err = compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
			ThreadID: recoveryThreadID, MaxConcurrency: 2,
		})
		t.Fatalf("helper Invoke unexpectedly returned: %v", err)
	}

	directory := t.TempDir()
	databasePath = filepath.Join(directory, "checkpoints.db")
	markerPath = filepath.Join(directory, "markers.log")
	command := exec.Command(os.Args[0], "-test.run=^TestSQLiteWorkerKillRecoveryPreservesSuccessfulPeer$")
	command.Env = append(os.Environ(),
		"LANGGRAPH_RECOVERY_HELPER=1",
		"LANGGRAPH_RECOVERY_DB="+databasePath,
		"LANGGRAPH_RECOVERY_MARKERS="+markerPath,
	)
	var processOutput bytes.Buffer
	command.Stdout = &processOutput
	command.Stderr = &processOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- command.Wait()
	}()
	terminated := false
	defer func() {
		if !terminated && command.Process != nil {
			_ = command.Process.Kill()
			select {
			case <-waitDone:
			case <-time.After(time.Second):
			}
		}
	}()

	var pollingSaver *checkpointsqlite.Saver
	foundPendingResult := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pollingSaver == nil {
			opened, err := checkpointsqlite.Open(context.Background(), databasePath)
			if err == nil {
				pollingSaver = opened
			}
		}
		if pollingSaver != nil {
			tuple, found, err := pollingSaver.GetTuple(context.Background(), checkpoint.Config{ThreadID: recoveryThreadID})
			if err == nil && found {
				for _, write := range tuple.PendingWrites {
					if write.Channel == checkpoint.TaskResultChannel {
						foundPendingResult = true
						if err := command.Process.Kill(); err != nil {
							select {
							case waitErr := <-waitDone:
								terminated = true
								t.Fatalf(
									"helper exited before kill: %v; output=%s",
									waitErr, processOutput.String(),
								)
							case <-time.After(100 * time.Millisecond):
								t.Fatalf("kill helper: %v", err)
							}
						}
						select {
						case <-waitDone:
							terminated = true
						case <-time.After(time.Second):
							t.Fatal("timed out waiting for killed helper process")
						}
						break
					}
				}
			}
		}
		if foundPendingResult {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pollingSaver != nil {
		_ = pollingSaver.Close()
	}
	if !foundPendingResult {
		t.Fatalf("timed out waiting for durable pending result; helper output=%s", processOutput.String())
	}

	recoverySaver, err := checkpointsqlite.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer recoverySaver.Close()
	recoveredGraph := compileWorkerKillGraph(t, recoverySaver, markerPath, false)
	result, err := recoveredGraph.Invoke(context.Background(), testState{Total: 100}, graph.RunConfig{
		ThreadID: recoveryThreadID, MaxConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 || !reflect.DeepEqual(result.Path, []string{"fast", "slow"}) {
		t.Fatalf("result=%+v", result)
	}
	rawMarkers, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	markers := strings.Fields(string(rawMarkers))
	counts := make(map[string]int)
	for _, marker := range markers {
		counts[marker]++
	}
	if counts["fast"] != 1 || counts["slow-start"] != 1 || counts["slow-recovered"] != 1 {
		t.Fatalf("markers=%v raw=%q", counts, rawMarkers)
	}
}
