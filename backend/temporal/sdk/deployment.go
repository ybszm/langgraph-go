package sdk

import (
	"context"
	"errors"
	"fmt"

	temporaladapter "github.com/wahanbo/langgraph-go/backend/temporal"
	"go.temporal.io/sdk/client"
)

// ErrVersionNotDrained prevents unsafe deletion of a deployment version.
var ErrVersionNotDrained = errors.New("temporal worker deployment version is not drained")

type deploymentHandle interface {
	Describe(context.Context, client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error)
	SetCurrentVersion(context.Context, client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error)
	SetRampingVersion(context.Context, client.WorkerDeploymentSetRampingVersionOptions) (client.WorkerDeploymentSetRampingVersionResponse, error)
	DescribeVersion(context.Context, client.WorkerDeploymentDescribeVersionOptions) (client.WorkerDeploymentVersionDescription, error)
	DeleteVersion(context.Context, client.WorkerDeploymentDeleteVersionOptions) (client.WorkerDeploymentDeleteVersionResponse, error)
}

// DeploymentManager performs conflict-safe promotion and drainage-safe
// deprecation through the current Worker Deployment API.
type DeploymentManager struct{ handle func(string) deploymentHandle }

func NewDeploymentManager(sdkClient client.Client) (*DeploymentManager, error) {
	if sdkClient == nil {
		return nil, fmt.Errorf("%w: SDK client is required", temporaladapter.ErrInvalidConfiguration)
	}
	deployments := sdkClient.WorkerDeploymentClient()
	return newDeploymentManager(func(name string) deploymentHandle { return deployments.GetHandle(name) })
}

func newDeploymentManager(handle func(string) deploymentHandle) (*DeploymentManager, error) {
	if handle == nil {
		return nil, fmt.Errorf("%w: deployment handle provider is required", temporaladapter.ErrInvalidConfiguration)
	}
	return &DeploymentManager{handle: handle}, nil
}

// Promote makes buildID current while retaining server protections for missing
// pollers and task queues.
func (m *DeploymentManager) Promote(ctx context.Context, deploymentName, buildID string) error {
	h, err := m.validHandle(ctx, deploymentName, buildID)
	if err != nil {
		return err
	}
	description, err := h.Describe(ctx, client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		return err
	}
	_, err = h.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{BuildID: buildID, ConflictToken: description.ConflictToken})
	return err
}

// Ramp routes percentage of eligible traffic to buildID using optimistic concurrency.
func (m *DeploymentManager) Ramp(ctx context.Context, deploymentName, buildID string, percentage float32) error {
	if percentage < 0 || percentage > 100 {
		return fmt.Errorf("%w: ramp percentage must be in [0,100]", temporaladapter.ErrInvalidConfiguration)
	}
	h, err := m.validHandle(ctx, deploymentName, buildID)
	if err != nil {
		return err
	}
	description, err := h.Describe(ctx, client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		return err
	}
	_, err = h.SetRampingVersion(ctx, client.WorkerDeploymentSetRampingVersionOptions{BuildID: buildID, Percentage: percentage, ConflictToken: description.ConflictToken})
	return err
}

// Deprecate deletes only a fully drained, non-current server version and never
// bypasses the SDK's drainage protection.
func (m *DeploymentManager) Deprecate(ctx context.Context, deploymentName, buildID string) error {
	h, err := m.validHandle(ctx, deploymentName, buildID)
	if err != nil {
		return err
	}
	description, err := h.DescribeVersion(ctx, client.WorkerDeploymentDescribeVersionOptions{BuildID: buildID})
	if err != nil {
		return err
	}
	if description.Info.DrainageInfo == nil || description.Info.DrainageInfo.DrainageStatus != client.WorkerDeploymentVersionDrainageStatusDrained {
		return ErrVersionNotDrained
	}
	_, err = h.DeleteVersion(ctx, client.WorkerDeploymentDeleteVersionOptions{BuildID: buildID, SkipDrainage: false})
	return err
}

func (m *DeploymentManager) validHandle(ctx context.Context, deploymentName, buildID string) (deploymentHandle, error) {
	if ctx == nil || deploymentName == "" || buildID == "" {
		return nil, fmt.Errorf("%w: context, deployment name, and build ID are required", temporaladapter.ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h := m.handle(deploymentName)
	if h == nil {
		return nil, fmt.Errorf("%w: deployment handle is nil", temporaladapter.ErrInvalidConfiguration)
	}
	return h, nil
}
