// Package sdk binds the provider-neutral temporal adapters to the official
// Temporal Go SDK. Core graph packages do not import this package.
package sdk

import (
	"context"
	"fmt"

	temporaladapter "github.com/wahanbo/langgraph-go/backend/temporal"
	"github.com/wahanbo/langgraph-go/graph"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// SerializableRunConfig is the durable subset safe for Temporal payloads.
type SerializableRunConfig struct {
	Context                                                                  any
	NewRun                                                                   bool
	RecursionLimit, MaxConcurrency                                           int
	ThreadID, CheckpointNamespace, CheckpointID, RunID, ParentRunID, RunName string
	Tags                                                                     []string
	Metadata                                                                 map[string]any
	StreamSubgraphs                                                          bool
}

// WorkflowRequest is the official SDK workflow input produced by Client.
type WorkflowRequest[I any] struct {
	WorkflowID string
	Input      I
	Config     SerializableRunConfig
}

func serializableConfig(config graph.RunConfig) SerializableRunConfig {
	return SerializableRunConfig{Context: config.Context, NewRun: config.NewRun, RecursionLimit: config.RecursionLimit, MaxConcurrency: config.MaxConcurrency, ThreadID: config.ThreadID, CheckpointNamespace: config.CheckpointNamespace, CheckpointID: config.CheckpointID, RunID: config.RunID, ParentRunID: config.ParentRunID, RunName: config.RunName, Tags: append([]string(nil), config.Tags...), Metadata: cloneMap(config.Metadata), StreamSubgraphs: config.StreamSubgraphs}
}

type workflowStarter interface {
	ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error)
}

// Client implements temporal.Client using ExecuteWorkflow.
type Client[I, O any] struct {
	client    workflowStarter
	taskQueue string
	workflow  any
	options   func(temporaladapter.StartRequest[I]) client.StartWorkflowOptions
}

func NewClient[I, O any](sdkClient client.Client, taskQueue string, workflowType any) (*Client[I, O], error) {
	return newClient[I, O](sdkClient, taskQueue, workflowType, nil)
}
func newClient[I, O any](sdkClient workflowStarter, taskQueue string, workflowType any, options func(temporaladapter.StartRequest[I]) client.StartWorkflowOptions) (*Client[I, O], error) {
	if sdkClient == nil || taskQueue == "" || workflowType == nil {
		return nil, fmt.Errorf("%w: SDK client, task queue, and workflow are required", temporaladapter.ErrInvalidConfiguration)
	}
	return &Client[I, O]{client: sdkClient, taskQueue: taskQueue, workflow: workflowType, options: options}, nil
}
func (c *Client[I, O]) StartWorkflow(ctx context.Context, request temporaladapter.StartRequest[I]) (temporaladapter.Handle[O], error) {
	options := client.StartWorkflowOptions{ID: request.WorkflowID, TaskQueue: c.taskQueue}
	if c.options != nil {
		options = c.options(request)
		if options.ID == "" {
			options.ID = request.WorkflowID
		}
		if options.TaskQueue == "" {
			options.TaskQueue = c.taskQueue
		}
	}
	run, err := c.client.ExecuteWorkflow(ctx, options, c.workflow, WorkflowRequest[I]{WorkflowID: request.WorkflowID, Input: request.Input, Config: serializableConfig(request.Config)})
	if err != nil {
		return nil, err
	}
	return workflowHandle[O]{run: run}, nil
}

type workflowHandle[O any] struct{ run client.WorkflowRun }

func (h workflowHandle[O]) WorkflowID() string { return h.run.GetID() }
func (h workflowHandle[O]) RunID() string      { return h.run.GetRunID() }
func (h workflowHandle[O]) Get(ctx context.Context) (O, error) {
	var output O
	err := h.run.Get(ctx, &output)
	return output, err
}

type controlClient interface {
	SignalWorkflow(context.Context, string, string, string, interface{}) error
	UpdateWorkflow(context.Context, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error)
	QueryWorkflow(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error)
}

// ControlClient implements the provider-neutral signal/update/query boundary.
type ControlClient[SP, UP, UR, QA, QR any] struct{ client controlClient }

func NewControlClient[SP, UP, UR, QA, QR any](sdkClient client.Client) (*ControlClient[SP, UP, UR, QA, QR], error) {
	if sdkClient == nil {
		return nil, fmt.Errorf("%w: SDK control client is required", temporaladapter.ErrInvalidConfiguration)
	}
	return &ControlClient[SP, UP, UR, QA, QR]{client: sdkClient}, nil
}
func (c *ControlClient[SP, UP, UR, QA, QR]) SignalWorkflow(ctx context.Context, request temporaladapter.SignalRequest[SP]) error {
	return c.client.SignalWorkflow(ctx, request.Ref.WorkflowID, request.Ref.RunID, request.Name, request)
}
func (c *ControlClient[SP, UP, UR, QA, QR]) UpdateWorkflow(ctx context.Context, request temporaladapter.UpdateRequest[UP]) (UR, error) {
	var result UR
	handle, err := c.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{UpdateID: request.RequestID, WorkflowID: request.Ref.WorkflowID, RunID: request.Ref.RunID, UpdateName: request.Name, Args: []interface{}{request}, WaitForStage: client.WorkflowUpdateStageCompleted})
	if err != nil {
		return result, err
	}
	err = handle.Get(ctx, &result)
	return result, err
}
func (c *ControlClient[SP, UP, UR, QA, QR]) QueryWorkflow(ctx context.Context, request temporaladapter.QueryRequest[QA]) (QR, error) {
	var result QR
	encoded, err := c.client.QueryWorkflow(ctx, request.Ref.WorkflowID, request.Ref.RunID, request.Name, request)
	if err != nil {
		return result, err
	}
	err = encoded.Get(&result)
	return result, err
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
