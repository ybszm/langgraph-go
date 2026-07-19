package main

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/functional"
)

func main() {
	double, err := functional.NewTask("double", func(_ context.Context, input int) (int, error) { return input * 2, nil })
	must(err)
	entry, err := functional.NewEntrypoint("map", func(ctx context.Context, inputs []int) ([]int, error) {
		futures := make([]*functional.Future[int], len(inputs))
		for i, input := range inputs {
			futures[i] = double.Call(ctx, input)
		}
		result := make([]int, len(inputs))
		for i, future := range futures {
			value, err := future.Await(ctx)
			if err != nil {
				return nil, err
			}
			result[i] = value
		}
		return result, nil
	}, functional.EntrypointOptions{MaxConcurrency: 3})
	must(err)
	result, err := entry.Invoke(context.Background(), []int{1, 2, 3})
	must(err)
	fmt.Println(result)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
