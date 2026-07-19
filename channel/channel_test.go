package channel_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/channel"
)

func TestLastAndAnyValue(t *testing.T) {
	last := channel.NewLastValue(channel.Snapshot[int]{})
	if _, err := last.Get(); !errors.Is(err, channel.ErrEmpty) {
		t.Fatalf("Get error=%v", err)
	}
	if _, err := last.Update([]int{1, 2}); !errors.Is(err, channel.ErrInvalidUpdate) {
		t.Fatalf("Update error=%v", err)
	}
	if changed, err := last.Update([]int{3}); err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	restored := channel.NewLastValue(last.Checkpoint())
	if value, _ := restored.Get(); value != 3 {
		t.Fatalf("restored=%d", value)
	}

	anyValue := channel.NewAnyValue(channel.Snapshot[int]{Value: 1, Present: true})
	if changed, err := anyValue.Update(nil); err != nil || !changed || anyValue.Available() {
		t.Fatalf("AnyValue clear changed=%v available=%v err=%v", changed, anyValue.Available(), err)
	}
	if changed, err := anyValue.Update(nil); err != nil || changed {
		t.Fatalf("second clear=%v err=%v", changed, err)
	}
	if _, err := anyValue.Update([]int{4, 5}); err != nil {
		t.Fatal(err)
	}
	if value, _ := anyValue.Get(); value != 5 {
		t.Fatalf("AnyValue=%d", value)
	}
}

func TestEphemeralAndUntrackedValue(t *testing.T) {
	ephemeral := channel.NewEphemeralValue(channel.Snapshot[string]{}, true)
	if _, err := ephemeral.Update([]string{"a", "b"}); !errors.Is(err, channel.ErrInvalidUpdate) {
		t.Fatalf("error=%v", err)
	}
	if _, err := ephemeral.Update([]string{"a"}); err != nil {
		t.Fatal(err)
	}
	if changed, _ := ephemeral.Update(nil); !changed || ephemeral.Available() {
		t.Fatal("ephemeral did not clear")
	}

	untracked := channel.NewUntrackedValue[string](false)
	if _, err := untracked.Update([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if value, _ := untracked.Get(); value != "b" {
		t.Fatalf("value=%q", value)
	}
	if untracked.Checkpoint().Present {
		t.Fatal("untracked value was checkpointed")
	}
	restored := channel.NewUntrackedValue[string](false)
	if _, err := restored.Get(); !errors.Is(err, channel.ErrEmpty) {
		t.Fatalf("restored error=%v", err)
	}
}

func TestBinaryOperatorAggregateOverwrite(t *testing.T) {
	aggregate := channel.NewBinaryOperatorAggregate(func(left, right int) int { return left + right })
	updates := []channel.AggregateUpdate[int]{
		channel.AggregateValue(1), channel.AggregateValue(2), channel.AggregateValue(3),
	}
	if _, err := aggregate.Update(updates); err != nil {
		t.Fatal(err)
	}
	if value, _ := aggregate.Get(); value != 6 {
		t.Fatalf("value=%d", value)
	}
	if _, err := aggregate.Update([]channel.AggregateUpdate[int]{
		channel.AggregateValue(4), channel.AggregateOverwrite(10), channel.AggregateValue(100),
	}); err != nil {
		t.Fatal(err)
	}
	if value, _ := aggregate.Get(); value != 10 {
		t.Fatalf("overwrite value=%d", value)
	}
	if _, err := aggregate.Update([]channel.AggregateUpdate[int]{
		channel.AggregateOverwrite(1), channel.AggregateOverwrite(2),
	}); !errors.Is(err, channel.ErrInvalidUpdate) {
		t.Fatalf("duplicate overwrite error=%v", err)
	}
	restored := channel.RestoreBinaryOperatorAggregate(aggregate.Checkpoint(), func(a, b int) int { return a + b })
	if value, _ := restored.Get(); value != 1 {
		t.Fatalf("restored=%d", value)
	}
}

func TestTopicReplaceAndAccumulate(t *testing.T) {
	topic := channel.NewTopic[string](nil, false)
	if changed, _ := topic.Update([]channel.TopicUpdate[string]{channel.TopicValue("a"), channel.TopicValues("b", "c")}); !changed {
		t.Fatal("topic unchanged")
	}
	if values, _ := topic.Get(); !reflect.DeepEqual(values, []string{"a", "b", "c"}) {
		t.Fatalf("values=%#v", values)
	}
	checkpoint := topic.Checkpoint()
	copyTopic := channel.NewTopic(checkpoint, false)
	checkpoint[0] = "mutated"
	if values, _ := copyTopic.Get(); values[0] != "a" {
		t.Fatal("topic checkpoint was not copied")
	}
	if changed, _ := copyTopic.Update(nil); !changed || copyTopic.Available() {
		t.Fatal("non-accumulating topic did not clear")
	}

	accumulating := channel.NewTopic([]string{"a"}, true)
	if changed, _ := accumulating.Update(nil); changed {
		t.Fatal("empty accumulating update changed channel")
	}
	_, _ = accumulating.Update([]channel.TopicUpdate[string]{channel.TopicValues("b", "b")})
	if values, _ := accumulating.Get(); !reflect.DeepEqual(values, []string{"a", "b", "b"}) {
		t.Fatalf("accumulated=%#v", values)
	}
}

func TestNamedBarrierAndFinishGates(t *testing.T) {
	barrier, err := channel.NewNamedBarrier([]string{"a", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.Get(); !errors.Is(err, channel.ErrEmpty) {
		t.Fatalf("Get error=%v", err)
	}
	if changed, err := barrier.Update([]string{"a", "a"}); err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if _, err := barrier.Update([]string{"unknown"}); !errors.Is(err, channel.ErrInvalidUpdate) {
		t.Fatalf("error=%v", err)
	}
	_, _ = barrier.Update([]string{"b"})
	if !barrier.Available() || !reflect.DeepEqual(barrier.Checkpoint(), []string{"a", "b"}) {
		t.Fatalf("checkpoint=%#v", barrier.Checkpoint())
	}
	if !barrier.Consume() || barrier.Available() {
		t.Fatal("barrier did not consume")
	}

	value := channel.NewLastValueAfterFinish(channel.FinishSnapshot[int]{})
	_, _ = value.Update([]int{1, 2})
	if value.Available() || !value.Finish() {
		t.Fatal("finish gate state invalid")
	}
	if got, _ := value.Get(); got != 2 {
		t.Fatalf("finished value=%d", got)
	}
	if !value.Consume() || value.Available() {
		t.Fatal("finished value did not clear")
	}

	finished, err := channel.NewNamedBarrierAfterFinish(
		[]string{"x", "y"}, channel.BarrierFinishSnapshot[string]{},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = finished.Update([]string{"x", "y"})
	if finished.Available() || !finished.Finish() || !finished.Available() {
		t.Fatal("finished barrier invalid")
	}
	snapshot := finished.Checkpoint()
	restored, err := channel.NewNamedBarrierAfterFinish([]string{"x", "y"}, snapshot)
	if err != nil || !restored.Available() {
		t.Fatalf("restored available=%v err=%v", restored.Available(), err)
	}
}

func TestDeltaChannelUpdateReplayAndSnapshotCadence(t *testing.T) {
	reducer := func(current []string, updates [][]string) ([]string, error) {
		result := append([]string(nil), current...)
		for _, update := range updates {
			result = append(result, update...)
		}
		return result, nil
	}
	delta, err := channel.NewDeltaChannel(func() []string { return []string{} }, reducer, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delta.Update([]channel.DeltaWrite[[]string, []string]{
		channel.DeltaValue[[]string]([]string{"a"}),
		channel.DeltaValue[[]string]([]string{"b"}),
	}); err != nil {
		t.Fatal(err)
	}
	if value, _ := delta.Get(); !reflect.DeepEqual(value, []string{"a", "b"}) {
		t.Fatalf("value=%#v", value)
	}
	if delta.Checkpoint().Present || delta.ShouldSnapshot(2) || !delta.ShouldSnapshot(3) {
		t.Fatal("snapshot contract invalid")
	}
	if _, err := delta.Update([]channel.DeltaWrite[[]string, []string]{
		channel.DeltaOverwrite[[]string, []string]([]string{"reset"}),
		channel.DeltaValue[[]string]([]string{"ignored"}),
	}); err != nil {
		t.Fatal(err)
	}
	if value, _ := delta.Get(); !reflect.DeepEqual(value, []string{"reset"}) {
		t.Fatalf("overwrite=%#v", value)
	}

	replayed, _ := channel.NewDeltaChannel(func() []string { return []string{"seed"} }, reducer, 3)
	err = replayed.ReplayWrites([]channel.DeltaWrite[[]string, []string]{
		channel.DeltaValue[[]string]([]string{"old"}),
		channel.DeltaOverwrite[[]string, []string]([]string{"new-base"}),
		channel.DeltaValue[[]string]([]string{"after"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := replayed.Get(); !reflect.DeepEqual(value, []string{"new-base", "after"}) {
		t.Fatalf("replayed=%#v", value)
	}
}
