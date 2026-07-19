// Package ttl adds default expiration, default read refresh, and an explicit
// context-managed sweeper lifecycle to any TTLStore.
package ttl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ybszm/langgraph-go/store"
)

var ErrSweeperRunning = errors.New("store TTL sweeper is already running")

const defaultSweepInterval = 5 * time.Minute

// Config controls TTL defaults and background sweeping. A nil RefreshOnRead
// defaults to true, matching the fixed upstream contract.
type Config struct {
	DefaultTTL    time.Duration
	RefreshOnRead *bool
	SweepInterval time.Duration
	OnSweepError  func(error)
}

type sweepRun struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Manager decorates a TTLStore without taking ownership of the backend.
type Manager struct {
	base          store.TTLStore
	defaultTTL    time.Duration
	refreshOnRead bool
	sweepInterval time.Duration
	onSweepError  func(error)

	lifecycleMu sync.Mutex
	run         *sweepRun
}

// New validates config and constructs a stopped Manager.
func New(base store.TTLStore, config Config) (*Manager, error) {
	if base == nil {
		return nil, fmt.Errorf("%w: TTL store is nil", store.ErrInvalidOperation)
	}
	if config.DefaultTTL < 0 {
		return nil, fmt.Errorf("%w: default TTL must not be negative", store.ErrInvalidTTL)
	}
	if config.SweepInterval < 0 {
		return nil, fmt.Errorf("%w: sweep interval must not be negative", store.ErrInvalidTTL)
	}
	refresh := true
	if config.RefreshOnRead != nil {
		refresh = *config.RefreshOnRead
	}
	interval := config.SweepInterval
	if interval == 0 {
		interval = defaultSweepInterval
	}
	return &Manager{
		base: base, defaultTTL: config.DefaultTTL, refreshOnRead: refresh,
		sweepInterval: interval, onSweepError: config.OnSweepError,
	}, nil
}

// Batch applies configured defaults only where the corresponding explicit-set
// flag is false, then delegates one ordered batch to the backend.
func (m *Manager) Batch(ctx context.Context, operations []store.Operation) ([]store.Result, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", store.ErrInvalidOperation)
	}
	prepared := make([]store.Operation, len(operations))
	for index, operation := range operations {
		switch operation := operation.(type) {
		case store.PutOp:
			if operation.Value != nil {
				if operation.TTL > 0 {
					operation.TTLSet = true
				} else if !operation.TTLSet && m.defaultTTL > 0 {
					operation.TTL = m.defaultTTL
					operation.TTLSet = true
				}
			}
			prepared[index] = operation
		case store.GetOp:
			if !operation.RefreshTTLSet {
				operation.RefreshTTL = m.refreshOnRead
				operation.RefreshTTLSet = true
			}
			prepared[index] = operation
		case store.SearchOp:
			if !operation.RefreshTTLSet {
				operation.RefreshTTL = m.refreshOnRead
				operation.RefreshTTLSet = true
			}
			prepared[index] = operation
		default:
			prepared[index] = operation
		}
	}
	return m.base.Batch(ctx, prepared)
}

func (m *Manager) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	results, err := m.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (m *Manager) Search(ctx context.Context, namespace store.Namespace, options store.SearchOptions) ([]store.SearchItem, error) {
	results, err := m.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: namespace, Query: options.Query, Filter: options.Filter,
		Limit: options.Limit, Offset: options.Offset,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (m *Manager) Put(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	_, err := m.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value}})
	return err
}

func (m *Manager) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	_, err := m.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key}})
	return err
}

func (m *Manager) ListNamespaces(ctx context.Context, options store.ListNamespacesOptions) ([]store.Namespace, error) {
	conditions := make([]store.MatchCondition, 0, 2)
	if len(options.Prefix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchPrefix, Path: options.Prefix})
	}
	if len(options.Suffix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchSuffix, Path: options.Suffix})
	}
	results, err := m.Batch(ctx, []store.Operation{store.ListNamespacesOp{
		MatchConditions: conditions, MaxDepth: options.MaxDepth,
		Limit: options.Limit, Offset: options.Offset,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Namespaces, nil
}

func (m *Manager) PutWithTTL(ctx context.Context, namespace store.Namespace, key string, value store.Value, duration time.Duration) error {
	if duration <= 0 {
		return fmt.Errorf("%w: TTL must be positive", store.ErrInvalidTTL)
	}
	_, err := m.Batch(ctx, []store.Operation{store.PutOp{
		Namespace: namespace, Key: key, Value: value, TTL: duration, TTLSet: true,
	}})
	return err
}

// PutWithoutTTL writes an item while explicitly bypassing DefaultTTL. It also
// clears an existing backend TTL.
func (m *Manager) PutWithoutTTL(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	_, err := m.Batch(ctx, []store.Operation{store.PutOp{
		Namespace: namespace, Key: key, Value: value, TTLSet: true,
	}})
	return err
}

func (m *Manager) GetWithTTLRefresh(ctx context.Context, namespace store.Namespace, key string, refresh bool) (*store.Item, error) {
	results, err := m.Batch(ctx, []store.Operation{store.GetOp{
		Namespace: namespace, Key: key, RefreshTTL: refresh, RefreshTTLSet: true,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (m *Manager) SearchWithTTLRefresh(ctx context.Context, namespace store.Namespace, options store.SearchOptions, refresh bool) ([]store.SearchItem, error) {
	results, err := m.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: namespace, Query: options.Query, Filter: options.Filter,
		Limit: options.Limit, Offset: options.Offset,
		RefreshTTL: refresh, RefreshTTLSet: true,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (m *Manager) SweepExpired(ctx context.Context) (int64, error) {
	return m.base.SweepExpired(ctx)
}

// StartSweeper starts one background sweep loop. The first sweep occurs after
// SweepInterval, matching the upstream lifecycle.
func (m *Manager) StartSweeper(parent context.Context) error {
	if parent == nil {
		return fmt.Errorf("%w: sweeper context is nil", store.ErrInvalidOperation)
	}
	if err := parent.Err(); err != nil {
		return err
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.run != nil {
		return ErrSweeperRunning
	}
	ctx, cancel := context.WithCancel(parent)
	run := &sweepRun{cancel: cancel, done: make(chan struct{})}
	m.run = run
	go m.runSweeper(ctx, run)
	return nil
}

func (m *Manager) runSweeper(ctx context.Context, run *sweepRun) {
	ticker := time.NewTicker(m.sweepInterval)
	defer func() {
		ticker.Stop()
		m.lifecycleMu.Lock()
		if m.run == run {
			m.run = nil
		}
		close(run.done)
		m.lifecycleMu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := m.base.SweepExpired(ctx); err != nil && ctx.Err() == nil && m.onSweepError != nil {
				m.onSweepError(err)
			}
		}
	}
}

// StopSweeper is idempotent and waits until the worker exits or ctx expires.
func (m *Manager) StopSweeper(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: stop context is nil", store.ErrInvalidOperation)
	}
	m.lifecycleMu.Lock()
	run := m.run
	if run == nil {
		m.lifecycleMu.Unlock()
		return nil
	}
	run.cancel()
	done := run.done
	m.lifecycleMu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SweeperRunning reports whether a background worker is currently active.
func (m *Manager) SweeperRunning() bool {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	return m.run != nil
}

var _ store.TTLStore = (*Manager)(nil)
