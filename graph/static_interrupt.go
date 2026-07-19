package graph

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

const staticInterruptType = "langgraph.go/static-interrupt"

type staticInterruptGate struct {
	Nodes    []string `json:"nodes"`
	Kind     string   `json:"kind"`
	Consumed bool     `json:"consumed"`
}

func (g *CompiledGraph[S, D]) matchesInterruptAfter(results []taskResult[D]) bool {
	for _, result := range results {
		if _, exists := g.interruptAfter[result.outputNode()]; exists {
			return true
		}
	}
	return false
}

func (g *CompiledGraph[S, D]) matchesInterruptBefore(tasks []scheduledTask) bool {
	for _, task := range tasks {
		if _, exists := g.interruptBefore[task.node]; exists {
			return true
		}
	}
	return false
}

func (g *CompiledGraph[S, D]) putStaticInterrupt(
	ctx context.Context,
	config checkpoint.Config,
	tasks []scheduledTask,
	kind string,
	consumed bool,
) error {
	nodes := make([]string, len(tasks))
	for index, task := range tasks {
		nodes[index] = string(task.node)
	}
	encoded, err := encodeStaticInterrupt(staticInterruptGate{Nodes: nodes, Kind: kind, Consumed: consumed})
	if err != nil {
		return persistenceError("encode-static-interrupt", config, err)
	}
	if err := g.persistence.saver.PutWrites(ctx, config, []checkpoint.PendingWrite{{
		TaskID: "__static_interrupt__", TaskPath: "__static_interrupt__", Index: -3,
		Channel: checkpoint.StaticInterruptChannel, Value: encoded,
	}}); err != nil {
		return persistenceError("put-static-interrupt", config, err)
	}
	return nil
}

func encodeStaticInterrupt(gate staticInterruptGate) (checkpoint.EncodedValue, error) {
	data, err := json.Marshal(gate)
	if err != nil {
		return checkpoint.EncodedValue{}, err
	}
	return checkpoint.EncodedValue{Type: staticInterruptType, Version: 1, Data: data}, nil
}

func decodeStaticInterrupt(value checkpoint.EncodedValue) (staticInterruptGate, error) {
	if value.Type != staticInterruptType || value.Version != 1 {
		return staticInterruptGate{}, fmt.Errorf("%w: static interrupt has %s v%d", checkpoint.ErrCodecMismatch, value.Type, value.Version)
	}
	var gate staticInterruptGate
	if err := json.Unmarshal(value.Data, &gate); err != nil {
		return staticInterruptGate{}, err
	}
	return gate, nil
}
