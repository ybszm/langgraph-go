package sdk

import (
	"context"
	"fmt"
	"sync"

	temporaladapter "github.com/wahanbo/langgraph-go/backend/temporal"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type sdkWorker interface {
	RegisterWorkflowWithOptions(interface{}, workflow.RegisterOptions)
	RegisterActivityWithOptions(interface{}, activity.RegisterOptions)
	Start() error
	Run(<-chan interface{}) error
	Stop()
}

type workerFactory func(string, worker.Options) sdkWorker

// Registrar binds WorkerPlan to an official Temporal Worker Deployment worker.
// ConfigureVersioning creates the worker; registrations and lifecycle operations
// are intentionally kept separate so a complete plan is installed before polling.
type Registrar struct {
	mu             sync.Mutex
	factory        workerFactory
	deploymentName string
	baseOptions    worker.Options
	worker         sdkWorker
	spec           temporaladapter.VersioningSpec
}

// NewRegistrar constructs an SDK registrar using current Worker Deployment
// versioning. The deployment name is stable across Build IDs.
func NewRegistrar(sdkClient client.Client, deploymentName string, options worker.Options) (*Registrar, error) {
	if sdkClient == nil {
		return nil, fmt.Errorf("%w: SDK client is required", temporaladapter.ErrInvalidConfiguration)
	}
	return newRegistrar(func(taskQueue string, options worker.Options) sdkWorker {
		return worker.New(sdkClient, taskQueue, options)
	}, deploymentName, options)
}

func newRegistrar(factory workerFactory, deploymentName string, options worker.Options) (*Registrar, error) {
	if factory == nil || deploymentName == "" {
		return nil, fmt.Errorf("%w: worker factory and deployment name are required", temporaladapter.ErrInvalidConfiguration)
	}
	return &Registrar{factory: factory, deploymentName: deploymentName, baseOptions: options}, nil
}

func (r *Registrar) ConfigureVersioning(ctx context.Context, spec temporaladapter.VersioningSpec) error {
	if ctx == nil || spec.TaskQueue == "" || spec.BuildID == "" || spec.Compatibility > temporaladapter.VersionCompatible {
		return fmt.Errorf("%w: valid context, task queue, build ID, and compatibility are required", temporaladapter.ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	behavior := workflow.VersioningBehaviorPinned
	if spec.Compatibility == temporaladapter.VersionCompatible {
		behavior = workflow.VersioningBehaviorAutoUpgrade
	}
	options := r.baseOptions
	options.DisableRegistrationAliasing = true
	options.DeploymentOptions = worker.DeploymentOptions{
		UseVersioning:             true,
		Version:                   worker.WorkerDeploymentVersion{DeploymentName: r.deploymentName, BuildID: spec.BuildID},
		DefaultVersioningBehavior: behavior,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker != nil {
		return fmt.Errorf("%w: registrar is already configured", temporaladapter.ErrInvalidConfiguration)
	}
	r.worker = r.factory(spec.TaskQueue, options)
	r.spec = spec
	if r.worker == nil {
		return fmt.Errorf("%w: worker factory returned nil", temporaladapter.ErrInvalidConfiguration)
	}
	return nil
}

func (r *Registrar) RegisterWorkflow(ctx context.Context, registration temporaladapter.Registration) error {
	return r.register(ctx, registration, true)
}

func (r *Registrar) RegisterActivity(ctx context.Context, registration temporaladapter.Registration) error {
	return r.register(ctx, registration, false)
}

func (r *Registrar) register(ctx context.Context, registration temporaladapter.Registration, isWorkflow bool) (err error) {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", temporaladapter.ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker == nil || registration.TaskQueue != r.spec.TaskQueue || registration.BuildID != r.spec.BuildID || registration.Name == "" || registration.Handler == nil {
		return fmt.Errorf("%w: registration does not match configured worker", temporaladapter.ErrInvalidConfiguration)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("temporal SDK registration panic: %v", recovered)
		}
	}()
	if isWorkflow {
		behavior := workflow.VersioningBehaviorPinned
		if r.spec.Compatibility == temporaladapter.VersionCompatible {
			behavior = workflow.VersioningBehaviorAutoUpgrade
		}
		r.worker.RegisterWorkflowWithOptions(registration.Handler, workflow.RegisterOptions{Name: registration.Name, VersioningBehavior: behavior})
	} else {
		r.worker.RegisterActivityWithOptions(registration.Handler, activity.RegisterOptions{Name: registration.Name})
	}
	return nil
}

// Start begins polling after the plan has been applied.
func (r *Registrar) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker == nil {
		return fmt.Errorf("%w: registrar is not configured", temporaladapter.ErrInvalidConfiguration)
	}
	return r.worker.Start()
}

// Run starts polling and blocks until interrupted.
func (r *Registrar) Run(interrupt <-chan interface{}) error {
	r.mu.Lock()
	w := r.worker
	r.mu.Unlock()
	if w == nil {
		return fmt.Errorf("%w: registrar is not configured", temporaladapter.ErrInvalidConfiguration)
	}
	return w.Run(interrupt)
}

// Stop stops the configured worker. It is a no-op before configuration.
func (r *Registrar) Stop() {
	r.mu.Lock()
	w := r.worker
	r.mu.Unlock()
	if w != nil {
		w.Stop()
	}
}
