// Package store defines long-term, cross-thread memory contracts. Store data
// is deliberately separate from graph execution checkpoints.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidNamespace = errors.New("invalid store namespace")
	ErrInvalidOperation = errors.New("invalid store operation")
	ErrUnsupportedQuery = errors.New("unsupported store query")
	ErrInvalidTTL       = errors.New("invalid store TTL")
	ErrUnsupportedTTL   = errors.New("store TTL is unsupported")
)

type Namespace []string
type Value map[string]any

type Item struct {
	Namespace Namespace
	Key       string
	Value     Value
	CreatedAt time.Time
	UpdatedAt time.Time
}

type SearchItem struct {
	Item
	Score *float64
}

type MatchType string

const (
	MatchPrefix MatchType = "prefix"
	MatchSuffix MatchType = "suffix"
)

type MatchCondition struct {
	Type MatchType
	Path Namespace
}

type Operation interface{ storeOperation() }

type GetOp struct {
	Namespace Namespace
	Key       string
	// RefreshTTL extends the item's expiration from the read time when the
	// backend supports TTL and the item has a positive TTL.
	RefreshTTL bool
	// RefreshTTLSet distinguishes an explicit false from an unspecified value
	// when a managed TTL wrapper supplies a default.
	RefreshTTLSet bool
}

func (GetOp) storeOperation() {}

type SearchOp struct {
	NamespacePrefix Namespace
	Filter          Value
	Limit           int
	Offset          int
	Query           string
	// RefreshTTL extends expiration for returned items on TTL-capable stores.
	RefreshTTL bool
	// RefreshTTLSet distinguishes an explicit false from a configured default.
	RefreshTTLSet bool
}

func (SearchOp) storeOperation() {}

type PutOp struct {
	Namespace Namespace
	Key       string
	// A nil Value deletes the item.
	Value Value
	// Index controls semantic fields on indexing-capable stores. Nil uses the
	// configured defaults, an empty slice disables indexing for this item, and
	// a non-empty slice contains JSON paths such as "text" or "parts[*].body".
	Index []string
	// TTL controls expiration on TTL-capable stores. A positive duration sets
	// or refreshes expiration; zero clears any existing TTL.
	TTL time.Duration
	// TTLSet distinguishes an explicit zero (clear TTL) from an unspecified TTL
	// when a managed wrapper supplies a default.
	TTLSet bool
}

func (PutOp) storeOperation() {}

type ListNamespacesOp struct {
	MatchConditions []MatchCondition
	MaxDepth        int
	Limit           int
	Offset          int
}

func (ListNamespacesOp) storeOperation() {}

// Result has exactly one populated field for read operations. Put/delete
// results have every field nil, preserving one result per input operation.
type Result struct {
	Item       *Item
	Items      []SearchItem
	Namespaces []Namespace
}

type SearchOptions struct {
	Query  string
	Filter Value
	Limit  int
	Offset int
}

type ListNamespacesOptions struct {
	Prefix   Namespace
	Suffix   Namespace
	MaxDepth int
	Limit    int
	Offset   int
}

type Store interface {
	Batch(context.Context, []Operation) ([]Result, error)
	Get(context.Context, Namespace, string) (*Item, error)
	Search(context.Context, Namespace, SearchOptions) ([]SearchItem, error)
	Put(context.Context, Namespace, string, Value) error
	Delete(context.Context, Namespace, string) error
	ListNamespaces(context.Context, ListNamespacesOptions) ([]Namespace, error)
}

// TTLStore is the optional expiration capability implemented by durable Store
// backends. Existing Store implementations need not support it.
type TTLStore interface {
	Store
	PutWithTTL(context.Context, Namespace, string, Value, time.Duration) error
	GetWithTTLRefresh(context.Context, Namespace, string, bool) (*Item, error)
	SearchWithTTLRefresh(context.Context, Namespace, SearchOptions, bool) ([]SearchItem, error)
	SweepExpired(context.Context) (int64, error)
}

// ItemIdentity is the durable identity of one Store item without its value.
type ItemIdentity struct {
	Namespace Namespace
	Key       string
}

// ExpiredItemStore exposes identities removed by one atomic TTL sweep so
// composed external indexes can actively remove their corresponding data.
type ExpiredItemStore interface {
	TTLStore
	SweepExpiredItems(context.Context) ([]ItemIdentity, error)
}

func ValidateNamespace(namespace Namespace) error {
	if len(namespace) == 0 {
		return fmt.Errorf("%w: namespace cannot be empty", ErrInvalidNamespace)
	}
	for _, label := range namespace {
		if label == "" {
			return fmt.Errorf("%w: labels cannot be empty", ErrInvalidNamespace)
		}
		if strings.Contains(label, ".") {
			return fmt.Errorf("%w: label %q contains a period", ErrInvalidNamespace, label)
		}
	}
	if namespace[0] == "langgraph" {
		return fmt.Errorf("%w: root label %q is reserved", ErrInvalidNamespace, namespace[0])
	}
	return nil
}
