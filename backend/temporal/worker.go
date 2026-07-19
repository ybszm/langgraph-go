package temporal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ErrDuplicateRegistration identifies a repeated workflow or activity name.
var ErrDuplicateRegistration = errors.New("duplicate temporal worker registration")

// VersionCompatibility describes how a worker build relates to existing builds.
type VersionCompatibility uint8

const (
	// VersionPinned admits only workflows explicitly assigned to this build.
	VersionPinned VersionCompatibility = iota
	// VersionCompatible declares this build compatible with the queue's prior build.
	VersionCompatible
)

// WorkerSpec identifies one provider worker deployment.
type WorkerSpec struct {
	TaskQueue     string
	BuildID       string
	Compatibility VersionCompatibility
}

// VersioningSpec is passed to an SDK binding before handler registration.
type VersioningSpec struct {
	TaskQueue     string
	BuildID       string
	Compatibility VersionCompatibility
}

// Registration is one named workflow or activity handler. Handler is
// intentionally type-erased only at the optional SDK registration boundary.
type Registration struct {
	TaskQueue string
	BuildID   string
	Name      string
	Handler   any
}

// Registrar is implemented by a concrete Temporal worker SDK binding.
type Registrar interface {
	ConfigureVersioning(context.Context, VersioningSpec) error
	RegisterWorkflow(context.Context, Registration) error
	RegisterActivity(context.Context, Registration) error
}

// WorkerPlan collects an immutable-at-apply registration snapshot.
type WorkerPlan struct {
	mu         sync.RWMutex
	spec       WorkerSpec
	workflow   *Registration
	activities map[string]Registration
}

// NewWorkerPlan validates and constructs a worker registration plan.
func NewWorkerPlan(spec WorkerSpec) (*WorkerPlan, error) {
	if !validWorkerName(spec.TaskQueue) || !validWorkerName(spec.BuildID) {
		return nil, fmt.Errorf("%w: task queue and build ID are required", ErrInvalidConfiguration)
	}
	if spec.Compatibility > VersionCompatible {
		return nil, fmt.Errorf("%w: unknown version compatibility", ErrInvalidConfiguration)
	}
	return &WorkerPlan{spec: spec, activities: make(map[string]Registration)}, nil
}

// SetWorkflow registers the single graph workflow type in this plan.
func (p *WorkerPlan) SetWorkflow(name string, handler any) error {
	if !validWorkerName(name) || isNil(handler) {
		return fmt.Errorf("%w: workflow name and handler are required", ErrInvalidConfiguration)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.workflow != nil {
		return fmt.Errorf("%w: workflow %q", ErrDuplicateRegistration, name)
	}
	registration := Registration{TaskQueue: p.spec.TaskQueue, BuildID: p.spec.BuildID, Name: name, Handler: handler}
	p.workflow = &registration
	return nil
}

// AddActivity registers one stable node activity name.
func (p *WorkerPlan) AddActivity(name string, handler any) error {
	if !validWorkerName(name) || isNil(handler) {
		return fmt.Errorf("%w: activity name and handler are required", ErrInvalidConfiguration)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.activities[name]; exists {
		return fmt.Errorf("%w: activity %q", ErrDuplicateRegistration, name)
	}
	p.activities[name] = Registration{TaskQueue: p.spec.TaskQueue, BuildID: p.spec.BuildID, Name: name, Handler: handler}
	return nil
}

// Apply configures versioning, then registers the workflow and activities in
// deterministic lexical order. It never starts a network worker itself.
func (p *WorkerPlan) Apply(ctx context.Context, registrar Registrar) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNil(registrar) {
		return fmt.Errorf("%w: registrar is nil", ErrInvalidConfiguration)
	}
	p.mu.RLock()
	spec := p.spec
	var workflow *Registration
	if p.workflow != nil {
		copy := *p.workflow
		workflow = &copy
	}
	activities := make([]Registration, 0, len(p.activities))
	for _, registration := range p.activities {
		activities = append(activities, registration)
	}
	p.mu.RUnlock()
	if workflow == nil {
		return fmt.Errorf("%w: workflow registration is required", ErrInvalidConfiguration)
	}
	sort.Slice(activities, func(i, j int) bool { return activities[i].Name < activities[j].Name })
	if err := registrar.ConfigureVersioning(ctx, VersioningSpec(spec)); err != nil {
		return &Error{Operation: "configure-versioning", Err: err}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := registrar.RegisterWorkflow(ctx, *workflow); err != nil {
		return &Error{Operation: "register-workflow", WorkflowID: workflow.Name, Err: err}
	}
	for _, registration := range activities {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := registrar.RegisterActivity(ctx, registration); err != nil {
			return &Error{Operation: "register-activity", WorkflowID: registration.Name, Err: err}
		}
	}
	return nil
}

func validWorkerName(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}
