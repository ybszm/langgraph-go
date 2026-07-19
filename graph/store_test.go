package graph_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/store"
	storememory "github.com/wahanbo/langgraph-go/store/memory"
)

type storeState struct{ User, Memory string }
type storeDelta struct{ Written bool }

func TestRuntimeStoreIsSharedAcrossThreadsAndConcurrentNodes(t *testing.T) {
	memories := storememory.New()
	builder := graph.NewStateGraph(func(_ context.Context, state storeState, _ []storeDelta) (storeState, error) {
		return state, nil
	})
	if err := builder.AddNode("remember", func(ctx context.Context, state storeState, runtime graph.Runtime) (graph.Command[storeDelta], error) {
		if runtime.Store == nil {
			return graph.Command[storeDelta]{}, fmt.Errorf("runtime store is nil")
		}
		err := runtime.Store.Put(ctx, store.Namespace{"users", state.User}, "memory", store.Value{"text": state.Memory})
		if err != nil {
			return graph.Command[storeDelta]{}, err
		}
		return graph.Update(storeDelta{Written: true}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "remember"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("remember", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithStore[storeState, storeDelta](memories))
	if err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	for index := 0; index < 20; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			_, invokeErr := compiled.Invoke(context.Background(), storeState{
				User: fmt.Sprintf("user-%02d", index), Memory: fmt.Sprintf("memory-%02d", index),
			}, graph.RunConfig{})
			if invokeErr != nil {
				t.Errorf("Invoke: %v", invokeErr)
			}
		}()
	}
	group.Wait()
	items, err := memories.Search(context.Background(), store.Namespace{"users"}, store.SearchOptions{Limit: 100})
	if err != nil || len(items) != 20 {
		t.Fatalf("shared memories=%d err=%v", len(items), err)
	}
}

type storeChildState struct{ User string }
type storeChildDelta struct{}

func TestSubgraphInheritsParentRuntimeStore(t *testing.T) {
	memories := storememory.New()
	childBuilder := graph.NewStateGraph(func(_ context.Context, state storeChildState, _ []storeChildDelta) (storeChildState, error) {
		return state, nil
	})
	if err := childBuilder.AddNode("write", func(ctx context.Context, state storeChildState, runtime graph.Runtime) (graph.Command[storeChildDelta], error) {
		if runtime.Store == nil {
			return graph.Command[storeChildDelta]{}, fmt.Errorf("child runtime store is nil")
		}
		return graph.NoCommand[storeChildDelta](), runtime.Store.Put(ctx, store.Namespace{"child"}, state.User, store.Value{"seen": true})
	}); err != nil {
		t.Fatal(err)
	}
	_ = childBuilder.AddEdge(graph.START, "write")
	_ = childBuilder.AddEdge("write", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	parentBuilder := graph.NewStateGraph(func(_ context.Context, state storeState, _ []storeDelta) (storeState, error) {
		return state, nil
	})
	err = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[storeState, storeDelta, storeChildState, storeChildDelta]{
		Input: func(_ context.Context, parent storeState) (storeChildState, error) {
			return storeChildState{User: parent.User}, nil
		},
		Output: func(_ context.Context, _ storeState, _ storeChildState) (graph.Command[storeDelta], error) {
			return graph.NoCommand[storeDelta](), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	parent, err := parentBuilder.Compile(graph.WithStore[storeState, storeDelta](memories))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Invoke(context.Background(), storeState{User: "alice"}, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	item, err := memories.Get(context.Background(), store.Namespace{"child"}, "alice")
	if err != nil || item == nil || item.Value["seen"] != true {
		t.Fatalf("child memory=%+v err=%v", item, err)
	}
}
