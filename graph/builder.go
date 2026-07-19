package graph

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/wahanbo/langgraph-go/managed"
)

type conditionalBranch[S any] struct {
	name    string
	router  Router[S]
	targets []NodeID
	allowed map[NodeID]struct{}
}

type waitingEdge struct {
	id      string
	sources []NodeID
	target  NodeID
}

type sendBranch[S any] struct {
	router  SendRouter[S]
	targets []NodeID
	allowed map[NodeID]struct{}
}

// StateGraph is a mutable, typed graph builder. Compile creates an immutable
// CompiledGraph that can be invoked concurrently.
//
// A StateGraph is not safe for concurrent mutation.
type StateGraph[S, D any] struct {
	reducer                 Reducer[S, D]
	cloner                  StateCloner[S]
	managedValues           managed.Projector[S]
	checkpointChannels      CheckpointChannelProjector[S]
	inputMerger             InputMerger[S]
	deltaNormalizer         DeltaNormalizer[D]
	messageExtractor        MessageExtractor[D]
	inputMessageExtractor   InputMessageExtractor[S]
	nodes                   map[NodeID]Node[S, D]
	edges                   map[NodeID][]NodeID
	branches                map[NodeID][]conditionalBranch[S]
	commandDestinations     map[NodeID][]NodeID
	retryPolicies           map[NodeID][]RetryPolicy
	defaultRetryPolicies    []RetryPolicy
	defaultCachePolicy      *nodeCachePolicy
	defaultTimeoutPolicy    *NodeTimeoutPolicy
	errorHandlers           map[NodeID]ErrorHandler[S, D]
	defaultErrorHandler     ErrorHandler[S, D]
	cachePolicies           map[NodeID]*nodeCachePolicy
	timeoutPolicies         map[NodeID]NodeTimeoutPolicy
	waitingEdges            []waitingEdge
	sendBranches            map[NodeID]sendBranch[S]
	subgraphs               map[NodeID]subgraphSpec[S, D]
	nodeSchemas             map[NodeID]nodeSchemaInfo
	checkpointChannelSchema map[string]CheckpointChannelBinding[S]
	nodeChannelReads        map[NodeID][]string
	nodeChannelTriggers     map[NodeID][]string
	dynamicInterruptNodes   map[NodeID]struct{}
}

// NewStateGraph creates an empty graph using reducer for super-step updates.
func NewStateGraph[S, D any](reducer Reducer[S, D]) *StateGraph[S, D] {
	return &StateGraph[S, D]{
		reducer:                 reducer,
		nodes:                   make(map[NodeID]Node[S, D]),
		edges:                   make(map[NodeID][]NodeID),
		branches:                make(map[NodeID][]conditionalBranch[S]),
		commandDestinations:     make(map[NodeID][]NodeID),
		retryPolicies:           make(map[NodeID][]RetryPolicy),
		cachePolicies:           make(map[NodeID]*nodeCachePolicy),
		timeoutPolicies:         make(map[NodeID]NodeTimeoutPolicy),
		errorHandlers:           make(map[NodeID]ErrorHandler[S, D]),
		sendBranches:            make(map[NodeID]sendBranch[S]),
		subgraphs:               make(map[NodeID]subgraphSpec[S, D]),
		nodeSchemas:             make(map[NodeID]nodeSchemaInfo),
		checkpointChannelSchema: make(map[string]CheckpointChannelBinding[S]),
		nodeChannelReads:        make(map[NodeID][]string),
		nodeChannelTriggers:     make(map[NodeID][]string),
		dynamicInterruptNodes:   make(map[NodeID]struct{}),
	}
}

// AddSendEdges registers a dynamic fan-out router. Targets declare every node
// the router may schedule and make compile-time reachability deterministic.
func (g *StateGraph[S, D]) AddSendEdges(source NodeID, router SendRouter[S], targets ...NodeID) error {
	if source == "" || source == END || router == nil || len(targets) == 0 {
		return fmt.Errorf("%w: invalid Send edge for source %q", ErrInvalidGraph, source)
	}
	if _, exists := g.sendBranches[source]; exists {
		return fmt.Errorf("%w: Send edge for %q already exists", ErrInvalidGraph, source)
	}
	allowed := make(map[NodeID]struct{}, len(targets))
	ordered := make([]NodeID, 0, len(targets))
	for _, target := range targets {
		if target == "" || target == START || target == END {
			return fmt.Errorf("%w: invalid Send target %q", ErrInvalidGraph, target)
		}
		if _, duplicate := allowed[target]; duplicate {
			return fmt.Errorf("%w: duplicate Send target %q", ErrInvalidGraph, target)
		}
		allowed[target] = struct{}{}
		ordered = append(ordered, target)
	}
	g.sendBranches[source] = sendBranch[S]{router: router, targets: ordered, allowed: allowed}
	return nil
}

// SetStateCloner configures per-node state isolation. Passing nil disables
// cloning and relies on the documented immutable-state node contract.
func (g *StateGraph[S, D]) SetStateCloner(cloner StateCloner[S]) {
	g.cloner = cloner
}

// SetManagedValues configures a typed projection from canonical state to the
// node-visible state. Managed fields participate in task cache identity but
// are excluded from reducers and checkpoints. Passing nil disables projection.
func (g *StateGraph[S, D]) SetManagedValues(projector managed.Projector[S]) {
	g.managedValues = projector
}

// SetCheckpointChannels configures additional observable checkpoint channels
// derived from canonical state. Values are content-compared at commit so only
// changed channel blobs receive new versions. Passing nil disables projection.
func (g *StateGraph[S, D]) SetCheckpointChannels(projector CheckpointChannelProjector[S]) {
	g.checkpointChannels = projector
}

// SetInputMerger controls how NewRun combines previous durable state and new
// input. A nil merger makes the new input replace previous state.
func (g *StateGraph[S, D]) SetInputMerger(merger InputMerger[S]) {
	g.inputMerger = merger
}

// SetDeltaNormalizer configures update canonicalization at the node-result
// boundary. Passing nil disables normalization.
func (g *StateGraph[S, D]) SetDeltaNormalizer(normalizer DeltaNormalizer[D]) {
	g.deltaNormalizer = normalizer
}

// SetMessageExtractor enables automatic messages-stream emission from node
// deltas. Passing nil disables extraction.
func (g *StateGraph[S, D]) SetMessageExtractor(extractor MessageExtractor[D]) {
	g.messageExtractor = extractor
}

// SetInputMessageExtractor configures discovery of messages already present
// in each node input. Passing nil disables input-ID deduplication seeding.
func (g *StateGraph[S, D]) SetInputMessageExtractor(extractor InputMessageExtractor[S]) {
	g.inputMessageExtractor = extractor
}

// AddNode registers a node under id.
func (g *StateGraph[S, D]) AddNode(id NodeID, node Node[S, D], options ...NodeOption) error {
	if id == "" || id == START || id == END {
		return fmt.Errorf("%w: reserved or empty node ID %q", ErrInvalidGraph, id)
	}
	if node == nil {
		return fmt.Errorf("%w: node %q has a nil implementation", ErrInvalidGraph, id)
	}
	if _, exists := g.nodes[id]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateNode, id)
	}
	configured := nodeOptions{}
	for _, option := range options {
		if option == nil {
			return fmt.Errorf("%w: node %q has a nil option", ErrInvalidGraph, id)
		}
		if err := option(&configured); err != nil {
			return err
		}
	}
	g.nodes[id] = node
	if len(configured.retryPolicies) > 0 {
		g.retryPolicies[id] = configured.retryPolicies
	}
	if configured.cachePolicy != nil {
		g.cachePolicies[id] = configured.cachePolicy
	}
	if configured.timeoutPolicy != nil {
		g.timeoutPolicies[id] = *configured.timeoutPolicy
	}
	if len(configured.channelReads) > 0 {
		g.nodeChannelReads[id] = append([]string(nil), configured.channelReads...)
	}
	if len(configured.channelTriggers) > 0 {
		g.nodeChannelTriggers[id] = append([]string(nil), configured.channelTriggers...)
	}
	if configured.dynamicInterrupt {
		g.dynamicInterruptNodes[id] = struct{}{}
	}
	if configured.errorHandler != nil {
		handler, ok := configured.errorHandler.(ErrorHandler[S, D])
		if !ok {
			delete(g.nodes, id)
			delete(g.retryPolicies, id)
			delete(g.cachePolicies, id)
			delete(g.timeoutPolicies, id)
			return fmt.Errorf("%w: node %q error handler has incompatible state or delta type", ErrInvalidGraph, id)
		}
		g.errorHandlers[id] = handler
	}
	return nil
}

// SetDefaultRetryPolicies sets policies inherited by nodes without an
// explicit WithRetryPolicies option.
func (g *StateGraph[S, D]) SetDefaultRetryPolicies(policies ...RetryPolicy) error {
	options := nodeOptions{}
	if err := WithRetryPolicies(policies...)(&options); err != nil {
		return err
	}
	g.defaultRetryPolicies = options.retryPolicies
	return nil
}

// SetDefaultCachePolicy sets the cache policy inherited at Compile by nodes
// without an explicit WithCachePolicy option.
func (g *StateGraph[S, D]) SetDefaultCachePolicy(policy CachePolicy[S]) error {
	options := nodeOptions{}
	if err := WithCachePolicy(policy)(&options); err != nil {
		return err
	}
	clone := *options.cachePolicy
	g.defaultCachePolicy = &clone
	return nil
}

// SetDefaultNodeTimeout sets the timeout policy inherited at Compile by
// nodes without an explicit WithNodeTimeout option.
func (g *StateGraph[S, D]) SetDefaultNodeTimeout(policy NodeTimeoutPolicy) error {
	options := nodeOptions{}
	if err := WithNodeTimeout(policy)(&options); err != nil {
		return err
	}
	clone := *options.timeoutPolicy
	g.defaultTimeoutPolicy = &clone
	return nil
}

// SetDefaultErrorHandler sets the handler inherited at Compile by regular
// nodes without an explicit WithErrorHandler option.
func (g *StateGraph[S, D]) SetDefaultErrorHandler(handler ErrorHandler[S, D]) error {
	if handler == nil {
		return fmt.Errorf("%w: default node error handler is nil", ErrInvalidGraph)
	}
	g.defaultErrorHandler = handler
	return nil
}

// AddWaitingEdge schedules target after every source has completed since the
// barrier was last consumed.
func (g *StateGraph[S, D]) AddWaitingEdge(sources []NodeID, target NodeID) error {
	if len(sources) < 2 {
		return fmt.Errorf("%w: waiting edge requires at least two sources", ErrInvalidGraph)
	}
	if target == "" || target == START {
		return fmt.Errorf("%w: invalid waiting-edge target %q", ErrInvalidGraph, target)
	}
	seen := make(map[NodeID]struct{}, len(sources))
	ordered := make([]NodeID, 0, len(sources))
	for _, source := range sources {
		if source == "" || source == START || source == END {
			return fmt.Errorf("%w: invalid waiting-edge source %q", ErrInvalidGraph, source)
		}
		if _, duplicate := seen[source]; duplicate {
			return fmt.Errorf("%w: duplicate waiting-edge source %q", ErrInvalidGraph, source)
		}
		seen[source] = struct{}{}
		ordered = append(ordered, source)
	}
	g.waitingEdges = append(g.waitingEdges, waitingEdge{
		id: fmt.Sprintf("waiting:%d", len(g.waitingEdges)), sources: ordered, target: target,
	})
	return nil
}

// AddEdge adds a static directed edge. Endpoints are fully validated during
// Compile so nodes and edges may be declared in either order.
func (g *StateGraph[S, D]) AddEdge(from, to NodeID) error {
	if from == "" || to == "" {
		return fmt.Errorf("%w: edge endpoints cannot be empty", ErrInvalidGraph)
	}
	if from == END {
		return fmt.Errorf("%w: END cannot have outgoing edges", ErrInvalidGraph)
	}
	if to == START {
		return fmt.Errorf("%w: START cannot have incoming edges", ErrInvalidGraph)
	}
	for _, existing := range g.edges[from] {
		if existing == to {
			return fmt.Errorf("%w: %q -> %q", ErrDuplicateEdge, from, to)
		}
	}
	g.edges[from] = append(g.edges[from], to)
	return nil
}

// AddConditionalEdges registers one conditional branch for source. Targets
// declare every node that the router may return and are used for validation,
// graph inspection, and reachability analysis.
func (g *StateGraph[S, D]) AddConditionalEdges(
	source NodeID,
	router Router[S],
	targets ...NodeID,
) error {
	return g.AddNamedConditionalEdges(source, "condition", router, targets...)
}

// AddNamedConditionalEdges registers an independently named branch. Multiple
// branches on the same source run in declaration order and merge their
// destinations before task de-duplication.
func (g *StateGraph[S, D]) AddNamedConditionalEdges(
	source NodeID,
	name string,
	router Router[S],
	targets ...NodeID,
) error {
	if source == "" || source == END {
		return fmt.Errorf("%w: invalid conditional source %q", ErrInvalidGraph, source)
	}
	if router == nil {
		return fmt.Errorf("%w: conditional router for %q is nil", ErrInvalidGraph, source)
	}
	if len(targets) == 0 {
		return fmt.Errorf("%w: conditional router for %q has no declared targets", ErrInvalidGraph, source)
	}
	if name == "" {
		return fmt.Errorf("%w: conditional branch for %q has an empty name", ErrInvalidGraph, source)
	}
	for _, branch := range g.branches[source] {
		if branch.name == name {
			return fmt.Errorf("%w: conditional branch %q for %q already exists", ErrInvalidGraph, name, source)
		}
	}

	allowed := make(map[NodeID]struct{}, len(targets))
	ordered := make([]NodeID, 0, len(targets))
	for _, target := range targets {
		if target == "" || target == START {
			return fmt.Errorf("%w: invalid conditional target %q", ErrInvalidGraph, target)
		}
		if _, duplicate := allowed[target]; duplicate {
			return fmt.Errorf("%w: duplicate conditional target %q", ErrInvalidGraph, target)
		}
		allowed[target] = struct{}{}
		ordered = append(ordered, target)
	}

	g.branches[source] = append(g.branches[source], conditionalBranch[S]{
		name:    name,
		router:  router,
		targets: ordered,
		allowed: allowed,
	})
	return nil
}

// AddCommandDestinations declares nodes that source may target with Command
// routing. Go cannot infer routing destinations from return type annotations
// the way Python can, so this metadata makes command-only paths visible to
// compile-time validation and graph inspection. Runtime routing still checks
// that every returned destination exists.
func (g *StateGraph[S, D]) AddCommandDestinations(
	source NodeID,
	targets ...NodeID,
) error {
	if source == "" || source == START || source == END {
		return fmt.Errorf("%w: invalid command source %q", ErrInvalidGraph, source)
	}
	if len(targets) == 0 {
		return fmt.Errorf("%w: command source %q has no declared targets", ErrInvalidGraph, source)
	}
	if _, exists := g.commandDestinations[source]; exists {
		return fmt.Errorf("%w: command destinations for %q already exist", ErrInvalidGraph, source)
	}

	seen := make(map[NodeID]struct{}, len(targets))
	ordered := make([]NodeID, 0, len(targets))
	for _, target := range targets {
		if target == "" || target == START {
			return fmt.Errorf("%w: invalid command target %q", ErrInvalidGraph, target)
		}
		if _, duplicate := seen[target]; duplicate {
			return fmt.Errorf("%w: duplicate command target %q", ErrInvalidGraph, target)
		}
		seen[target] = struct{}{}
		ordered = append(ordered, target)
	}
	g.commandDestinations[source] = ordered
	return nil
}

// Compile validates the builder and returns an immutable execution plan.
func (g *StateGraph[S, D]) Compile(
	options ...CompileOption[S, D],
) (*CompiledGraph[S, D], error) {
	compileOptions := compileConfig[S, D]{}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: compile option is nil", ErrInvalidGraph)
		}
		if err := option(&compileOptions); err != nil {
			return nil, err
		}
	}
	if g.reducer == nil {
		return nil, fmt.Errorf("%w: reducer is nil", ErrInvalidGraph)
	}
	if (len(compileOptions.interruptBefore) > 0 || len(compileOptions.interruptAfter) > 0) && compileOptions.persistence == nil {
		return nil, fmt.Errorf("%w: static interrupts require persistence", ErrInvalidGraph)
	}
	if len(g.dynamicInterruptNodes) > 0 && compileOptions.persistence == nil {
		return nil, fmt.Errorf("%w: declared dynamic interrupts require persistence: %w", ErrInvalidGraph, ErrCheckpointerRequired)
	}
	for node := range compileOptions.interruptBefore {
		if _, exists := g.nodes[node]; !exists {
			return nil, fmt.Errorf("%w: interrupt-before: %w: %q", ErrInvalidGraph, ErrUnknownNode, node)
		}
	}
	for node := range compileOptions.interruptAfter {
		if _, exists := g.nodes[node]; !exists {
			return nil, fmt.Errorf("%w: interrupt-after: %w: %q", ErrInvalidGraph, ErrUnknownNode, node)
		}
	}
	if len(g.nodes) == 0 {
		return nil, fmt.Errorf("%w: graph has no nodes", ErrInvalidGraph)
	}

	validateEndpoint := func(id NodeID) error {
		if id == START || id == END {
			return nil
		}
		if _, exists := g.nodes[id]; !exists {
			return fmt.Errorf("%w: %q", ErrUnknownNode, id)
		}
		return nil
	}

	for from, targets := range g.edges {
		if err := validateEndpoint(from); err != nil {
			return nil, fmt.Errorf("%w: edge source: %w", ErrInvalidGraph, err)
		}
		for _, to := range targets {
			if err := validateEndpoint(to); err != nil {
				return nil, fmt.Errorf("%w: edge %q target: %w", ErrInvalidGraph, from, err)
			}
		}
	}

	for source, branches := range g.branches {
		if err := validateEndpoint(source); err != nil {
			return nil, fmt.Errorf("%w: conditional source: %w", ErrInvalidGraph, err)
		}
		for _, branch := range branches {
			for _, target := range branch.targets {
				if err := validateEndpoint(target); err != nil {
					return nil, fmt.Errorf(
						"%w: conditional source %q branch %q target: %w",
						ErrInvalidGraph, source, branch.name, err,
					)
				}
			}
		}
	}
	for source, targets := range g.commandDestinations {
		if err := validateEndpoint(source); err != nil {
			return nil, fmt.Errorf("%w: command source: %w", ErrInvalidGraph, err)
		}
		for _, target := range targets {
			if err := validateEndpoint(target); err != nil {
				return nil, fmt.Errorf(
					"%w: command source %q target: %w",
					ErrInvalidGraph,
					source,
					err,
				)
			}
		}
	}
	for _, edge := range g.waitingEdges {
		for _, source := range edge.sources {
			if err := validateEndpoint(source); err != nil {
				return nil, fmt.Errorf("%w: waiting-edge source: %w", ErrInvalidGraph, err)
			}
		}
		if err := validateEndpoint(edge.target); err != nil {
			return nil, fmt.Errorf("%w: waiting-edge target: %w", ErrInvalidGraph, err)
		}
	}
	for source, branch := range g.sendBranches {
		if err := validateEndpoint(source); err != nil {
			return nil, fmt.Errorf("%w: Send source: %w", ErrInvalidGraph, err)
		}
		for _, target := range branch.targets {
			if err := validateEndpoint(target); err != nil {
				return nil, fmt.Errorf("%w: Send target: %w", ErrInvalidGraph, err)
			}
		}
	}

	if len(g.edges[START]) == 0 {
		if _, hasConditionalEntry := g.branches[START]; !hasConditionalEntry {
			return nil, fmt.Errorf("%w: graph has no START edge or conditional entry", ErrInvalidGraph)
		}
	}

	reachable := map[NodeID]struct{}{START: {}}
	queue := []NodeID{START}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visit := func(target NodeID) {
			if _, seen := reachable[target]; seen {
				return
			}
			reachable[target] = struct{}{}
			if target != END {
				queue = append(queue, target)
			}
		}
		for _, target := range g.edges[current] {
			visit(target)
		}
		if branches, exists := g.branches[current]; exists {
			for _, branch := range branches {
				for _, target := range branch.targets {
					visit(target)
				}
			}
		}
		for _, target := range g.commandDestinations[current] {
			visit(target)
		}
		for _, edge := range g.waitingEdges {
			for _, source := range edge.sources {
				if source == current {
					visit(edge.target)
					break
				}
			}
		}
		if branch, exists := g.sendBranches[current]; exists {
			for _, target := range branch.targets {
				visit(target)
			}
		}
	}

	for id := range g.nodes {
		if _, ok := reachable[id]; !ok {
			return nil, fmt.Errorf("%w: node %q is unreachable from START", ErrInvalidGraph, id)
		}
	}
	for node, reads := range g.nodeChannelReads {
		for _, channel := range reads {
			if _, exists := g.checkpointChannelSchema[channel]; !exists {
				return nil, fmt.Errorf("%w: node %q: %w: unknown read channel %q", ErrInvalidGraph, node, ErrCheckpointChannel, channel)
			}
		}
	}
	for node, triggers := range g.nodeChannelTriggers {
		for _, channel := range triggers {
			if _, exists := g.checkpointChannelSchema[channel]; !exists {
				return nil, fmt.Errorf("%w: node %q: %w: unknown trigger channel %q", ErrInvalidGraph, node, ErrCheckpointChannel, channel)
			}
		}
	}
	if _, endReachable := reachable[END]; !endReachable {
		return nil, fmt.Errorf("%w: END is unreachable from START", ErrInvalidGraph)
	}
	var contextType reflect.Type
	if compileOptions.contextSchema != nil {
		contextType = compileOptions.contextSchema.typeOf
	}
	if err := validateSubgraphBuilders(g.subgraphs, compileOptions.persistence != nil, contextType); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidGraph, err)
	}

	resolvedErrorHandlers, err := resolveErrorHandlers(g.errorHandlers, g.nodes, g.defaultErrorHandler)
	if err != nil {
		return nil, err
	}
	retryPolicies := cloneRetryPolicies(g.retryPolicies, g.nodes, g.defaultRetryPolicies)
	timeoutPolicies := resolveNodeTimeoutPolicies(g.timeoutPolicies, g.nodes, g.defaultTimeoutPolicy)
	for _, handler := range resolvedErrorHandlers {
		if len(g.defaultRetryPolicies) > 0 {
			retryPolicies[handler.node] = append([]RetryPolicy(nil), g.defaultRetryPolicies...)
		}
		if g.defaultTimeoutPolicy != nil {
			timeoutPolicies[handler.node] = *g.defaultTimeoutPolicy
		}
	}
	return &CompiledGraph[S, D]{
		reducer:                 g.reducer,
		cloner:                  g.cloner,
		managedValues:           g.managedValues,
		checkpointChannels:      g.checkpointChannels,
		contextSchema:           compileOptions.contextSchema,
		inputMerger:             g.inputMerger,
		deltaNormalizer:         g.deltaNormalizer,
		messageExtractor:        g.messageExtractor,
		inputMessageExtractor:   g.inputMessageExtractor,
		nodes:                   cloneNodes(g.nodes),
		edges:                   cloneEdges(g.edges),
		branches:                cloneBranches(g.branches),
		commandDestinations:     cloneEdges(g.commandDestinations),
		retryPolicies:           retryPolicies,
		errorHandlers:           resolvedErrorHandlers,
		persistence:             compileOptions.persistence,
		cache:                   compileOptions.cache,
		store:                   compileOptions.store,
		interruptBefore:         cloneNodeSet(compileOptions.interruptBefore),
		interruptAfter:          cloneNodeSet(compileOptions.interruptAfter),
		cachePolicies:           resolveCachePolicies(g.cachePolicies, g.nodes, g.defaultCachePolicy),
		timeoutPolicies:         timeoutPolicies,
		waitingEdges:            cloneWaitingEdges(g.waitingEdges),
		sendBranches:            cloneSendBranches(g.sendBranches),
		subgraphs:               cloneSubgraphs(g.subgraphs),
		nodeSchemas:             cloneNodeSchemas(g.nodeSchemas),
		checkpointChannelSchema: cloneCheckpointChannelSchema(g.checkpointChannelSchema),
		nodeChannelReads:        cloneNodeChannelReads(g.nodeChannelReads),
		nodeChannelTriggers:     cloneNodeChannelReads(g.nodeChannelTriggers),
		dynamicInterruptNodes:   cloneNodeSet(g.dynamicInterruptNodes),
		runLocks:                newRunLockSet(),
	}, nil
}

func validateSubgraphBuilders[S, D any](subgraphs map[NodeID]subgraphSpec[S, D], parentPersistent bool, parentContext reflect.Type) error {
	ids := make([]NodeID, 0, len(subgraphs))
	for id := range subgraphs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := subgraphs[id].validateCompile(parentPersistent, parentContext); err != nil {
			return &SubgraphValidationError{Node: id, Err: err}
		}
	}
	return nil
}

func cloneSendBranches[S any](source map[NodeID]sendBranch[S]) map[NodeID]sendBranch[S] {
	result := make(map[NodeID]sendBranch[S], len(source))
	for id, branch := range source {
		allowed := make(map[NodeID]struct{}, len(branch.allowed))
		for target := range branch.allowed {
			allowed[target] = struct{}{}
		}
		result[id] = sendBranch[S]{router: branch.router, targets: cloneNodeIDs(branch.targets), allowed: allowed}
	}
	return result
}

func cloneWaitingEdges(source []waitingEdge) []waitingEdge {
	result := make([]waitingEdge, len(source))
	for index, edge := range source {
		result[index] = waitingEdge{id: edge.id, sources: cloneNodeIDs(edge.sources), target: edge.target}
	}
	return result
}

func resolveCachePolicies[S, D any](
	explicit map[NodeID]*nodeCachePolicy,
	nodes map[NodeID]Node[S, D],
	defaultPolicy *nodeCachePolicy,
) map[NodeID]*nodeCachePolicy {
	result := make(map[NodeID]*nodeCachePolicy, len(nodes))
	for id := range nodes {
		policy := explicit[id]
		if policy == nil {
			policy = defaultPolicy
		}
		if policy != nil {
			clone := *policy
			result[id] = &clone
		}
	}
	return result
}

func cloneRetryPolicies[S, D any](
	explicit map[NodeID][]RetryPolicy,
	nodes map[NodeID]Node[S, D],
	defaults []RetryPolicy,
) map[NodeID][]RetryPolicy {
	result := make(map[NodeID][]RetryPolicy, len(nodes))
	for id := range nodes {
		policies := explicit[id]
		if len(policies) == 0 {
			policies = defaults
		}
		if len(policies) > 0 {
			result[id] = append([]RetryPolicy(nil), policies...)
		}
	}
	return result
}

func cloneNodes[S, D any](source map[NodeID]Node[S, D]) map[NodeID]Node[S, D] {
	result := make(map[NodeID]Node[S, D], len(source))
	for id, node := range source {
		result[id] = node
	}
	return result
}

func cloneEdges(source map[NodeID][]NodeID) map[NodeID][]NodeID {
	result := make(map[NodeID][]NodeID, len(source))
	for id, targets := range source {
		result[id] = cloneNodeIDs(targets)
	}
	return result
}

func cloneNodeSet(source map[NodeID]struct{}) map[NodeID]struct{} {
	result := make(map[NodeID]struct{}, len(source))
	for node := range source {
		result[node] = struct{}{}
	}
	return result
}

func cloneBranches[S any](
	source map[NodeID][]conditionalBranch[S],
) map[NodeID][]conditionalBranch[S] {
	result := make(map[NodeID][]conditionalBranch[S], len(source))
	for id, branches := range source {
		cloned := make([]conditionalBranch[S], len(branches))
		for index, branch := range branches {
			allowed := make(map[NodeID]struct{}, len(branch.allowed))
			for target := range branch.allowed {
				allowed[target] = struct{}{}
			}
			cloned[index] = conditionalBranch[S]{
				name: branch.name, router: branch.router,
				targets: cloneNodeIDs(branch.targets), allowed: allowed,
			}
		}
		result[id] = cloned
	}
	return result
}

func routeConditional[S any](
	ctx context.Context,
	step int,
	source NodeID,
	state S,
	branch conditionalBranch[S],
) ([]NodeID, error) {
	targets, err := branch.router(ctx, state)
	if err != nil {
		return nil, &RouterError{Step: step, Source: source, Branch: branch.name, Err: err}
	}
	for _, target := range targets {
		if _, allowed := branch.allowed[target]; !allowed {
			return nil, &RouterError{
				Step:   step,
				Source: source,
				Branch: branch.name,
				Err: fmt.Errorf(
					"%w: router returned undeclared target %q",
					ErrUnknownNode,
					target,
				),
			}
		}
	}
	return cloneNodeIDs(targets), nil
}
