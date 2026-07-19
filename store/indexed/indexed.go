// Package indexed composes a Store, an Embedder, and a VectorIndex into a
// semantic-search-capable Store.
package indexed

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/wahanbo/langgraph-go/store"
)

var ErrIndexSync = errors.New("store committed but vector index synchronization failed")
var ErrOutbox = errors.New("durable vector outbox failed")

// VectorMutation is one durable intent to make a vector identity reflect the
// final base-store value written by a batch.
type VectorMutation struct {
	Namespace store.Namespace        `json:"namespace"`
	Key       string                 `json:"key"`
	ValueHash string                 `json:"value_hash,omitempty"`
	Missing   bool                   `json:"missing,omitempty"`
	Documents []store.VectorDocument `json:"documents,omitempty"`
}

// PendingVectorMutation adds the durable journal identity.
type PendingVectorMutation struct {
	ID string
	VectorMutation
}

// VectorOutbox is the durability boundary between base Store commit and
// vector-index synchronization.
type VectorOutbox interface {
	Enqueue(context.Context, []VectorMutation) ([]string, error)
	List(context.Context, int) ([]PendingVectorMutation, error)
	Ack(context.Context, []string) error
}

// Config controls semantic indexing. Nil or empty Fields defaults to ["$"].
type Config struct {
	Embedder store.Embedder
	Index    store.VectorIndex
	Fields   []string
	Outbox   VectorOutbox
}

type configuredField struct {
	raw    string
	tokens []string
}

// Store serializes orchestration calls so the base Store snapshot and vector
// index snapshot advance together for callers that use this wrapper.
type Store struct {
	mu       sync.Mutex
	base     store.Store
	embedder store.Embedder
	index    store.VectorIndex
	fields   []configuredField
	outbox   VectorOutbox
}

// New constructs a semantic Store over provider-neutral dependencies.
func New(base store.Store, config Config) (*Store, error) {
	if base == nil {
		return nil, fmt.Errorf("%w: base store is nil", store.ErrInvalidOperation)
	}
	if config.Embedder == nil {
		return nil, fmt.Errorf("%w: embedder is nil", store.ErrInvalidEmbedding)
	}
	if config.Index == nil {
		return nil, fmt.Errorf("%w: vector index is nil", store.ErrInvalidVector)
	}
	fields := config.Fields
	if len(fields) == 0 {
		fields = []string{"$"}
	}
	configured, err := configureFields(fields)
	if err != nil {
		return nil, err
	}
	return &Store{
		base: base, embedder: config.Embedder, index: config.Index,
		fields: configured, outbox: config.Outbox,
	}, nil
}

// Batch preserves the base Store's pre-write read snapshot, then synchronizes
// the final write for each item into the vector index.
func (s *Store) Batch(ctx context.Context, operations []store.Operation) ([]store.Result, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", store.ErrInvalidOperation)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	prepared, err := s.prepare(ctx, operations)
	if err != nil {
		return nil, err
	}
	var outboxIDs []string
	if s.outbox != nil && len(prepared.writes) > 0 {
		mutations, mutationErr := vectorMutations(prepared.writes, prepared.documents)
		if mutationErr != nil {
			return nil, mutationErr
		}
		outboxIDs, err = s.outbox.Enqueue(ctx, mutations)
		if err != nil {
			return nil, fmt.Errorf("%w: enqueue: %v", ErrOutbox, err)
		}
	}
	baseResults, err := s.base.Batch(ctx, prepared.baseOperations)
	if err != nil {
		if len(outboxIDs) > 0 {
			_ = s.outbox.Ack(context.WithoutCancel(ctx), outboxIDs)
		}
		return nil, err
	}
	for operationIndex, semantic := range prepared.searches {
		baseResults[operationIndex].Items = mergeSearch(
			baseResults[operationIndex].Items,
			semantic.matches,
			semantic.operation.Limit,
			semantic.operation.Offset,
		)
	}
	if err := s.synchronize(context.WithoutCancel(ctx), prepared.writes, prepared.documents); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIndexSync, err)
	}
	if len(outboxIDs) > 0 {
		if err := s.outbox.Ack(context.WithoutCancel(ctx), outboxIDs); err != nil {
			return nil, fmt.Errorf("%w: acknowledge synchronized mutations: %v", ErrOutbox, err)
		}
	}
	return baseResults, nil
}

// RepairPending reconciles durable outbox intents against the current base
// Store snapshot. It is safe after process restart and idempotent.
func (s *Store) RepairPending(ctx context.Context, limit int) (int, error) {
	if s.outbox == nil {
		return 0, fmt.Errorf("%w: outbox is not configured", ErrOutbox)
	}
	if limit < 0 {
		return 0, fmt.Errorf("%w: negative repair limit", ErrOutbox)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, err := s.outbox.List(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("%w: list: %v", ErrOutbox, err)
	}
	repaired := 0
	for _, mutation := range pending {
		item, err := s.base.Get(ctx, mutation.Namespace, mutation.Key)
		if err != nil {
			return repaired, fmt.Errorf("%w: inspect %s: %v", ErrOutbox, mutation.ID, err)
		}
		matches := mutation.Missing && item == nil
		if !mutation.Missing && item != nil {
			hash, hashErr := valueHash(item.Value)
			if hashErr != nil {
				return repaired, hashErr
			}
			matches = hash == mutation.ValueHash
		}
		if matches || item == nil {
			if err := s.index.Delete(ctx, mutation.Namespace, mutation.Key); err != nil {
				return repaired, fmt.Errorf("%w: delete %s: %v", ErrIndexSync, mutation.ID, err)
			}
			if matches && len(mutation.Documents) > 0 {
				if err := s.index.Upsert(ctx, mutation.Documents); err != nil {
					return repaired, fmt.Errorf("%w: upsert %s: %v", ErrIndexSync, mutation.ID, err)
				}
			}
		}
		if err := s.outbox.Ack(ctx, []string{mutation.ID}); err != nil {
			return repaired, fmt.Errorf("%w: acknowledge %s: %v", ErrOutbox, mutation.ID, err)
		}
		repaired++
	}
	return repaired, nil
}

func (s *Store) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key, RefreshTTL: true}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *Store) Search(ctx context.Context, prefix store.Namespace, options store.SearchOptions) ([]store.SearchItem, error) {
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: prefix, Query: options.Query, Filter: options.Filter,
		Limit: options.Limit, Offset: options.Offset, RefreshTTL: true,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *Store) Put(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value}})
	return err
}

func (s *Store) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key}})
	return err
}

func (s *Store) ListNamespaces(ctx context.Context, options store.ListNamespacesOptions) ([]store.Namespace, error) {
	conditions := make([]store.MatchCondition, 0, 2)
	if len(options.Prefix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchPrefix, Path: options.Prefix})
	}
	if len(options.Suffix) > 0 {
		conditions = append(conditions, store.MatchCondition{Type: store.MatchSuffix, Path: options.Suffix})
	}
	results, err := s.Batch(ctx, []store.Operation{store.ListNamespacesOp{
		MatchConditions: conditions, MaxDepth: options.MaxDepth, Limit: options.Limit, Offset: options.Offset,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Namespaces, nil
}

func (s *Store) PutWithTTL(ctx context.Context, namespace store.Namespace, key string, value store.Value, ttl time.Duration) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value, TTL: ttl}})
	return err
}

func (s *Store) GetWithTTLRefresh(ctx context.Context, namespace store.Namespace, key string, refresh bool) (*store.Item, error) {
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key, RefreshTTL: refresh}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *Store) SearchWithTTLRefresh(ctx context.Context, prefix store.Namespace, options store.SearchOptions, refresh bool) ([]store.SearchItem, error) {
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{
		NamespacePrefix: prefix, Query: options.Query, Filter: options.Filter,
		Limit: options.Limit, Offset: options.Offset, RefreshTTL: refresh,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *Store) SweepExpired(ctx context.Context) (int64, error) {
	ttlStore, ok := s.base.(store.TTLStore)
	if !ok {
		return 0, store.ErrUnsupportedTTL
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	identityStore, ok := s.base.(store.ExpiredItemStore)
	if !ok {
		return ttlStore.SweepExpired(ctx)
	}
	items, err := identityStore.SweepExpiredItems(ctx)
	if err != nil {
		return 0, err
	}
	mutations := make([]VectorMutation, len(items))
	for index, item := range items {
		mutations[index] = VectorMutation{Namespace: append(store.Namespace(nil), item.Namespace...), Key: item.Key, Missing: true}
	}
	var outboxIDs []string
	if s.outbox != nil && len(mutations) > 0 {
		outboxIDs, err = s.outbox.Enqueue(context.WithoutCancel(ctx), mutations)
		if err != nil {
			return int64(len(items)), fmt.Errorf("%w: enqueue expired cleanup: %v", ErrOutbox, err)
		}
	}
	for _, item := range items {
		if err := s.index.Delete(context.WithoutCancel(ctx), item.Namespace, item.Key); err != nil {
			return int64(len(items)), fmt.Errorf("%w: clean expired vector: %v", ErrIndexSync, err)
		}
	}
	if len(outboxIDs) > 0 {
		if err := s.outbox.Ack(context.WithoutCancel(ctx), outboxIDs); err != nil {
			return int64(len(items)), fmt.Errorf("%w: acknowledge expired cleanup: %v", ErrOutbox, err)
		}
	}
	return int64(len(items)), nil
}

type semanticSearch struct {
	operation store.SearchOp
	matches   []store.VectorMatch
}

type finalWrite struct {
	operation store.PutOp
	position  int
}

type preparedBatch struct {
	baseOperations []store.Operation
	searches       map[int]semanticSearch
	writes         []finalWrite
	documents      []store.VectorDocument
}

func (s *Store) prepare(ctx context.Context, operations []store.Operation) (preparedBatch, error) {
	prepared := preparedBatch{
		baseOperations: append([]store.Operation(nil), operations...),
		searches:       make(map[int]semanticSearch),
	}
	writeByIdentity := make(map[string]finalWrite)
	queryVectors := make(map[string][]float32)
	for index, operation := range operations {
		if err := ctx.Err(); err != nil {
			return preparedBatch{}, err
		}
		switch operation := operation.(type) {
		case store.SearchOp:
			if operation.Limit < 0 || operation.Offset < 0 {
				return preparedBatch{}, fmt.Errorf("%w: negative search pagination", store.ErrInvalidOperation)
			}
			if operation.Query == "" {
				continue
			}
			vector, ok := queryVectors[operation.Query]
			if !ok {
				var err error
				vector, err = s.embedder.EmbedQuery(ctx, operation.Query)
				if err != nil {
					return preparedBatch{}, fmt.Errorf("embed query: %w", err)
				}
				queryVectors[operation.Query] = append([]float32(nil), vector...)
			}
			matches, err := s.index.Search(ctx, store.VectorQuery{
				NamespacePrefix: operation.NamespacePrefix,
				Vector:          vector,
				Limit:           maxInt,
			})
			if err != nil {
				return preparedBatch{}, fmt.Errorf("search vector index: %w", err)
			}
			prepared.searches[index] = semanticSearch{operation: operation, matches: matches}
			operation.Query = ""
			operation.Limit = maxInt
			operation.Offset = 0
			prepared.baseOperations[index] = operation
		case store.PutOp:
			if err := validatePut(operation); err != nil {
				return preparedBatch{}, err
			}
			identity, err := itemIdentity(operation.Namespace, operation.Key)
			if err != nil {
				return preparedBatch{}, err
			}
			writeByIdentity[identity] = finalWrite{operation: operation, position: index}
		}
	}
	prepared.writes = make([]finalWrite, 0, len(writeByIdentity))
	for _, write := range writeByIdentity {
		prepared.writes = append(prepared.writes, write)
	}
	sort.Slice(prepared.writes, func(left, right int) bool {
		return prepared.writes[left].position < prepared.writes[right].position
	})
	documents, err := s.embedWrites(ctx, prepared.writes)
	if err != nil {
		return preparedBatch{}, err
	}
	prepared.documents = documents
	return prepared, nil
}

type documentText struct {
	namespace store.Namespace
	key       string
	field     string
	text      string
}

func (s *Store) embedWrites(ctx context.Context, writes []finalWrite) ([]store.VectorDocument, error) {
	var assignments []documentText
	for _, write := range writes {
		operation := write.operation
		if operation.Value == nil || operation.Index != nil && len(operation.Index) == 0 {
			continue
		}
		fields := s.fields
		if operation.Index != nil {
			var err error
			fields, err = configureFields(operation.Index)
			if err != nil {
				return nil, err
			}
		}
		for _, field := range fields {
			texts, err := extractTexts(operation.Value, field.tokens)
			if err != nil {
				return nil, fmt.Errorf("%w: extract field %q: %v", store.ErrInvalidEmbedding, field.raw, err)
			}
			for index, value := range texts {
				name := field.raw
				if len(texts) > 1 {
					name += "." + strconv.Itoa(index)
				}
				assignments = append(assignments, documentText{
					namespace: operation.Namespace, key: operation.Key, field: name, text: value,
				})
			}
		}
	}
	if len(assignments) == 0 {
		return nil, nil
	}
	uniqueTexts := make([]string, 0, len(assignments))
	textIndex := make(map[string]int, len(assignments))
	for _, assignment := range assignments {
		if _, exists := textIndex[assignment.text]; exists {
			continue
		}
		textIndex[assignment.text] = len(uniqueTexts)
		uniqueTexts = append(uniqueTexts, assignment.text)
	}
	vectors, err := s.embedder.EmbedDocuments(ctx, uniqueTexts)
	if err != nil {
		return nil, fmt.Errorf("embed documents: %w", err)
	}
	if len(vectors) != len(uniqueTexts) {
		return nil, fmt.Errorf("%w: embeddings=%d texts=%d", store.ErrInvalidEmbedding, len(vectors), len(uniqueTexts))
	}
	documents := make([]store.VectorDocument, 0, len(assignments))
	for _, assignment := range assignments {
		documents = append(documents, store.VectorDocument{
			Namespace: append(store.Namespace(nil), assignment.namespace...),
			Key:       assignment.key,
			Field:     assignment.field,
			Vector:    append([]float32(nil), vectors[textIndex[assignment.text]]...),
		})
	}
	return documents, nil
}

func (s *Store) synchronize(ctx context.Context, writes []finalWrite, documents []store.VectorDocument) error {
	for _, write := range writes {
		if err := s.index.Delete(ctx, write.operation.Namespace, write.operation.Key); err != nil {
			return err
		}
	}
	if len(documents) == 0 {
		return nil
	}
	return s.index.Upsert(ctx, documents)
}

func vectorMutations(writes []finalWrite, documents []store.VectorDocument) ([]VectorMutation, error) {
	byIdentity := make(map[string][]store.VectorDocument)
	for _, document := range documents {
		identity, err := itemIdentity(document.Namespace, document.Key)
		if err != nil {
			return nil, err
		}
		byIdentity[identity] = append(byIdentity[identity], document)
	}
	result := make([]VectorMutation, len(writes))
	for index, write := range writes {
		identity, err := itemIdentity(write.operation.Namespace, write.operation.Key)
		if err != nil {
			return nil, err
		}
		mutation := VectorMutation{
			Namespace: append(store.Namespace(nil), write.operation.Namespace...), Key: write.operation.Key,
			Missing: write.operation.Value == nil, Documents: cloneVectorDocuments(byIdentity[identity]),
		}
		if !mutation.Missing {
			mutation.ValueHash, err = valueHash(write.operation.Value)
			if err != nil {
				return nil, err
			}
		}
		result[index] = mutation
	}
	return result, nil
}

func valueHash(value store.Value) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("%w: hash store value: %v", store.ErrInvalidOperation, err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func cloneVectorDocuments(source []store.VectorDocument) []store.VectorDocument {
	result := make([]store.VectorDocument, len(source))
	for index, document := range source {
		result[index] = document
		result[index].Namespace = append(store.Namespace(nil), document.Namespace...)
		result[index].Vector = append([]float32(nil), document.Vector...)
	}
	return result
}

func mergeSearch(candidates []store.SearchItem, matches []store.VectorMatch, limit, offset int) []store.SearchItem {
	if limit == 0 {
		limit = 10
	}
	candidateByID := make(map[string]store.SearchItem, len(candidates))
	for _, candidate := range candidates {
		identity, _ := itemIdentity(candidate.Namespace, candidate.Key)
		candidateByID[identity] = candidate
	}
	ordered := make([]store.SearchItem, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, match := range matches {
		identity, _ := itemIdentity(match.Namespace, match.Key)
		candidate, exists := candidateByID[identity]
		if !exists {
			continue
		}
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		score := match.Score
		candidate.Score = &score
		ordered = append(ordered, candidate)
	}
	for _, candidate := range candidates {
		identity, _ := itemIdentity(candidate.Namespace, candidate.Key)
		if _, exists := seen[identity]; exists {
			continue
		}
		candidate.Score = nil
		ordered = append(ordered, candidate)
	}
	start := min(offset, len(ordered))
	end := min(start+limit, len(ordered))
	return ordered[start:end]
}

func validatePut(operation store.PutOp) error {
	if err := store.ValidateNamespace(operation.Namespace); err != nil {
		return err
	}
	if operation.Key == "" {
		return fmt.Errorf("%w: key cannot be empty", store.ErrInvalidOperation)
	}
	return nil
}

func configureFields(fields []string) ([]configuredField, error) {
	configured := make([]configuredField, len(fields))
	for index, field := range fields {
		if field == "" {
			return nil, fmt.Errorf("%w: index field cannot be empty", store.ErrInvalidEmbedding)
		}
		tokens, err := tokenizePath(field)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", store.ErrInvalidEmbedding, field, err)
		}
		configured[index] = configuredField{raw: field, tokens: tokens}
	}
	return configured, nil
}

func itemIdentity(namespace store.Namespace, key string) (string, error) {
	encoded, err := json.Marshal(namespace)
	if err != nil {
		return "", fmt.Errorf("%w: encode namespace: %v", store.ErrInvalidOperation, err)
	}
	return string(encoded) + "\x00" + key, nil
}

const maxInt = int(^uint(0) >> 1)

var _ store.TTLStore = (*Store)(nil)
