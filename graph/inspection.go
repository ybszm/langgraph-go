package graph

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// GraphEdgeKind identifies how a declared topology edge is scheduled.
type GraphEdgeKind string

const (
	GraphEdgeStatic      GraphEdgeKind = "static"
	GraphEdgeConditional GraphEdgeKind = "conditional"
	GraphEdgeCommand     GraphEdgeKind = "command"
	GraphEdgeSend        GraphEdgeKind = "send"
	GraphEdgeWaiting     GraphEdgeKind = "waiting"
)

// GraphNodeInfo describes one node in an immutable compiled plan.
type GraphNodeInfo struct {
	ID               NodeID
	Virtual          bool
	IsSubgraph       bool
	InputSchema      string
	OutputSchema     string
	DynamicInterrupt bool
	InputJSONSchema  json.RawMessage
	OutputJSONSchema json.RawMessage
}

// GraphEdgeInfo describes one declared edge. Waiting edges retain all Sources
// as one hyperedge; every other kind has exactly one source.
type GraphEdgeInfo struct {
	Sources []NodeID
	Target  NodeID
	Kind    GraphEdgeKind
	Branch  string
}

// GraphDescription is a deterministic, detached view of a compiled plan.
type GraphDescription struct {
	Nodes             []GraphNodeInfo
	Edges             []GraphEdgeInfo
	Subgraphs         []GraphSubgraphInfo
	ContextSchema     string
	StateSchema       string
	DeltaSchema       string
	ContextJSONSchema json.RawMessage
	StateJSONSchema   json.RawMessage
	DeltaJSONSchema   json.RawMessage
}

// GraphSubgraphInfo attaches one recursively inspected child plan to its
// parent node boundary.
type GraphSubgraphInfo struct {
	Node  NodeID
	Graph GraphDescription
}

// Inspect returns declared graph topology without executing routers or nodes.
// Conditional, Command, and Send targets are the target sets registered at
// build time and therefore describe possible rather than observed routes.
func (g *CompiledGraph[S, D]) Inspect() GraphDescription {
	return g.inspect(false)
}

// InspectRecursive returns a detached topology tree including every compiled
// child graph without flattening namespace or type boundaries.
func (g *CompiledGraph[S, D]) InspectRecursive() GraphDescription {
	return g.inspect(true)
}

func (g *CompiledGraph[S, D]) inspect(recursive bool) GraphDescription {
	nodeIDs := make([]NodeID, 0, len(g.nodes))
	for id := range g.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(left, right int) bool { return nodeIDs[left] < nodeIDs[right] })
	nodes := make([]GraphNodeInfo, 0, len(nodeIDs)+2)
	nodes = append(nodes, GraphNodeInfo{ID: START, Virtual: true})
	for _, id := range nodeIDs {
		_, subgraph := g.subgraphs[id]
		schema := g.nodeSchemas[id]
		nodes = append(nodes, GraphNodeInfo{
			ID: id, IsSubgraph: subgraph, InputSchema: schema.input, OutputSchema: schema.output,
			DynamicInterrupt: hasNode(g.dynamicInterruptNodes, id),
			InputJSONSchema:  jsonSchemaFor(schema.inputType), OutputJSONSchema: jsonSchemaFor(schema.outputType),
		})
	}
	nodes = append(nodes, GraphNodeInfo{ID: END, Virtual: true})

	edges := make([]GraphEdgeInfo, 0)
	appendSingle := func(source, target NodeID, kind GraphEdgeKind) {
		edges = append(edges, GraphEdgeInfo{Sources: []NodeID{source}, Target: target, Kind: kind})
	}
	for source, targets := range g.edges {
		for _, target := range targets {
			appendSingle(source, target, GraphEdgeStatic)
		}
	}
	for source, branches := range g.branches {
		for _, branch := range branches {
			for _, target := range branch.targets {
				edges = append(edges, GraphEdgeInfo{
					Sources: []NodeID{source}, Target: target, Kind: GraphEdgeConditional, Branch: branch.name,
				})
			}
		}
	}
	for source, targets := range g.commandDestinations {
		for _, target := range targets {
			appendSingle(source, target, GraphEdgeCommand)
		}
	}
	for source, branch := range g.sendBranches {
		for _, target := range branch.targets {
			appendSingle(source, target, GraphEdgeSend)
		}
	}
	for _, edge := range g.waitingEdges {
		edges = append(edges, GraphEdgeInfo{
			Sources: cloneNodeIDs(edge.sources), Target: edge.target, Kind: GraphEdgeWaiting,
		})
	}
	sort.SliceStable(edges, func(left, right int) bool {
		leftSources := inspectionSourcesKey(edges[left].Sources)
		rightSources := inspectionSourcesKey(edges[right].Sources)
		if leftSources != rightSources {
			return leftSources < rightSources
		}
		if edges[left].Target != edges[right].Target {
			return edges[left].Target < edges[right].Target
		}
		if edges[left].Branch != edges[right].Branch {
			return edges[left].Branch < edges[right].Branch
		}
		return edges[left].Kind < edges[right].Kind
	})
	description := GraphDescription{
		Nodes: nodes, Edges: edges,
		StateSchema:     reflect.TypeOf((*S)(nil)).Elem().String(),
		DeltaSchema:     reflect.TypeOf((*D)(nil)).Elem().String(),
		StateJSONSchema: jsonSchemaFor(reflect.TypeOf((*S)(nil)).Elem()),
		DeltaJSONSchema: jsonSchemaFor(reflect.TypeOf((*D)(nil)).Elem()),
	}
	if g.contextSchema != nil {
		description.ContextSchema = g.contextSchema.typeOf.String()
		description.ContextJSONSchema = jsonSchemaFor(g.contextSchema.typeOf)
	}
	if recursive {
		for _, id := range nodeIDs {
			if child, exists := g.subgraphs[id]; exists {
				description.Subgraphs = append(description.Subgraphs, GraphSubgraphInfo{
					Node: id, Graph: child.inspectRecursive(),
				})
			}
		}
	}
	return description
}

func hasNode(nodes map[NodeID]struct{}, id NodeID) bool {
	_, exists := nodes[id]
	return exists
}

func inspectionSourcesKey(sources []NodeID) string {
	values := make([]string, len(sources))
	for index, source := range sources {
		values[index] = string(source)
	}
	return strings.Join(values, "\x00")
}

// Mermaid renders this detached description as a stable Mermaid flowchart.
// Waiting hyperedges use explicit diamond barrier nodes; recursive subgraphs
// remain nested clusters linked to their parent boundary node.
func (description GraphDescription) Mermaid() string {
	renderer := mermaidRenderer{}
	renderer.builder.WriteString("flowchart TD\n")
	renderer.renderGraph(description, "  ")
	return renderer.builder.String()
}

type mermaidRenderer struct {
	builder strings.Builder
	nextID  int
}

func (renderer *mermaidRenderer) id(prefix string) string {
	id := prefix + "_" + fmt.Sprintf("%d", renderer.nextID)
	renderer.nextID++
	return id
}

func (renderer *mermaidRenderer) renderGraph(description GraphDescription, indent string) map[NodeID]string {
	ids := make(map[NodeID]string, len(description.Nodes))
	for _, node := range description.Nodes {
		id := renderer.id("n")
		ids[node.ID] = id
		label := mermaidLabel(string(node.ID))
		if node.Virtual {
			renderer.builder.WriteString(fmt.Sprintf("%s%s((\"%s\"))\n", indent, id, label))
		} else {
			renderer.builder.WriteString(fmt.Sprintf("%s%s[\"%s\"]\n", indent, id, label))
		}
	}
	for _, edge := range description.Edges {
		if edge.Kind == GraphEdgeWaiting {
			barrier := renderer.id("waiting")
			renderer.builder.WriteString(fmt.Sprintf("%s%s{\"waiting\"}\n", indent, barrier))
			for _, source := range edge.Sources {
				renderer.builder.WriteString(fmt.Sprintf("%s%s -->|waiting| %s\n", indent, ids[source], barrier))
			}
			renderer.builder.WriteString(fmt.Sprintf("%s%s -->|waiting| %s\n", indent, barrier, ids[edge.Target]))
			continue
		}
		for _, source := range edge.Sources {
			renderer.builder.WriteString(fmt.Sprintf(
				"%s%s -->|%s| %s\n", indent, ids[source], mermaidLabel(string(edge.Kind)), ids[edge.Target],
			))
		}
	}
	for _, subgraph := range description.Subgraphs {
		cluster := renderer.id("subgraph")
		renderer.builder.WriteString(fmt.Sprintf("%ssubgraph %s[\"%s\"]\n", indent, cluster, mermaidLabel(string(subgraph.Node))))
		childIDs := renderer.renderGraph(subgraph.Graph, indent+"  ")
		renderer.builder.WriteString(indent + "end\n")
		if parentID, exists := ids[subgraph.Node]; exists {
			if startID := childIDs[START]; startID != "" {
				renderer.builder.WriteString(fmt.Sprintf("%s%s -. contains .-> %s\n", indent, parentID, startID))
			}
		}
	}
	return ids
}

func mermaidLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	value = strings.ReplaceAll(value, "\r", "")
	return strings.ReplaceAll(value, "\n", "\\n")
}
