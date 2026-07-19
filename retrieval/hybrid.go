package retrieval

import (
	"context"
	"fmt"
	"sort"

	"golang.org/x/sync/errgroup"
)

// WeightedRetriever assigns a positive fusion weight to one member.
type WeightedRetriever struct {
	Retriever Retriever
	Weight    float64
}

// HybridRetriever combines rankings with weighted reciprocal-rank fusion.
type HybridRetriever struct {
	members      []WeightedRetriever
	rankConstant float64
}

// NewHybrid creates a parallel reciprocal-rank fusion retriever. A zero rank
// constant selects the conventional value 60.
func NewHybrid(members []WeightedRetriever, rankConstant float64) (*HybridRetriever, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("%w: hybrid retrievers are empty", ErrInvalidQuery)
	}
	if rankConstant == 0 {
		rankConstant = 60
	}
	if rankConstant < 0 {
		return nil, fmt.Errorf("%w: rank constant is negative", ErrInvalidQuery)
	}
	cloned := append([]WeightedRetriever(nil), members...)
	for i, member := range cloned {
		if member.Retriever == nil || member.Weight <= 0 {
			return nil, fmt.Errorf("%w: hybrid member %d has invalid retriever or weight", ErrInvalidQuery, i)
		}
	}
	return &HybridRetriever{members: cloned, rankConstant: rankConstant}, nil
}

func (hybrid *HybridRetriever) Retrieve(ctx context.Context, query Query) ([]Result, error) {
	if err := validateQuery(ctx, query); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit == 0 {
		limit = 10
	}
	memberQuery := query
	memberQuery.Limit = max(limit*2, 20)
	rankings := make([][]Result, len(hybrid.members))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, member := range hybrid.members {
		index, member := index, member
		group.Go(func() error {
			results, err := member.Retriever.Retrieve(groupCtx, memberQuery)
			if err != nil {
				return fmt.Errorf("hybrid retriever %d: %w", index, err)
			}
			rankings[index] = results
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	type fused struct {
		document               Document
		score                  float64
		firstMember, firstRank int
	}
	combined := make(map[string]fused)
	for memberIndex, ranking := range rankings {
		for rank, result := range ranking {
			if result.Document.ID == "" {
				return nil, fmt.Errorf("%w: hybrid result has empty document ID", ErrInvalidDocument)
			}
			current, exists := combined[result.Document.ID]
			if !exists {
				current = fused{document: result.Document.Clone(), firstMember: memberIndex, firstRank: rank}
			}
			current.score += hybrid.members[memberIndex].Weight / (hybrid.rankConstant + float64(rank+1))
			combined[result.Document.ID] = current
		}
	}
	ordered := make([]fused, 0, len(combined))
	for _, result := range combined {
		ordered = append(ordered, result)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].score != ordered[j].score {
			return ordered[i].score > ordered[j].score
		}
		if ordered[i].firstMember != ordered[j].firstMember {
			return ordered[i].firstMember < ordered[j].firstMember
		}
		if ordered[i].firstRank != ordered[j].firstRank {
			return ordered[i].firstRank < ordered[j].firstRank
		}
		return ordered[i].document.ID < ordered[j].document.ID
	})
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	results := make([]Result, len(ordered))
	for i, result := range ordered {
		results[i] = Result{Document: result.document, Score: result.score}
	}
	return results, nil
}

var _ Retriever = (*HybridRetriever)(nil)
