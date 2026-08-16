package brdcst

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

func TestStaticConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultStaticConfig()
		require.NoError(t, cfg.Validate())
	})
}

// testQueryID is the id the broadcast state machines under test report progress under.
const testQueryID = coordt.QueryID("test")

// newStaticTest creates a Static state machine for the query id "test".
func newStaticTest(t *testing.T) *Static[tiny.Key, tiny.Node, tiny.Message] {
	t.Helper()
	msg := tiny.Message{Content: "store this"}
	return NewStatic(testQueryID, msg, DefaultStaticConfig(), coordt.NoopTracer())
}

// TestStaticContactsEverySeedOnce checks that a static broadcast asks for its record to be
// stored with each of the nodes it was seeded with, one node per advance.
func TestStaticContactsEverySeedOnce(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newStaticTest(t)

	seeds := []tiny.Node{tiny.NewNode(4), tiny.NewNode(5), tiny.NewNode(6)}

	contacted := make(map[string]bool, len(seeds))
	state := sm.Advance(ctx, now, &EventBroadcastStart[tiny.Key, tiny.Node]{Seed: seeds})
	for range seeds {
		st, ok := state.(*StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message])
		require.True(t, ok, "state is %T", state)
		require.Equal(t, testQueryID, st.QueryID)
		require.Equal(t, "store this", st.Message.Content)
		require.False(t, contacted[st.NodeID.String()], "node contacted twice")
		contacted[st.NodeID.String()] = true

		state = sm.Advance(ctx, now, &EventBroadcastPoll{})
	}

	require.Len(t, contacted, len(seeds))

	// with every seed contacted and none having replied the broadcast is waiting
	require.IsType(t, &StateBroadcastWaiting{}, state)
}

// TestStaticWaitingReportsNothingDue checks that a static broadcast waiting on store record
// requests names no time at which it could make progress, since a store carries no deadline.
func TestStaticWaitingReportsNothingDue(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newStaticTest(t)

	state := sm.Advance(ctx, now, &EventBroadcastStart[tiny.Key, tiny.Node]{
		Seed: []tiny.Node{tiny.NewNode(4)},
	})
	require.IsType(t, &StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = sm.Advance(ctx, now, &EventBroadcastPoll{})
	require.IsType(t, &StateBroadcastWaiting{}, state)
	require.True(t, state.(*StateBroadcastWaiting).NextDue.IsZero())
}

// TestStaticFinishesWhenEveryStoreIsReported checks that a static broadcast reports the nodes
// it contacted and the error from each that failed once none is left outstanding.
func TestStaticFinishesWhenEveryStoreIsReported(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newStaticTest(t)

	a := tiny.NewNode(4)
	b := tiny.NewNode(5)

	state := sm.Advance(ctx, now, &EventBroadcastStart[tiny.Key, tiny.Node]{Seed: []tiny.Node{a, b}})
	require.IsType(t, &StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)
	state = sm.Advance(ctx, now, &EventBroadcastPoll{})
	require.IsType(t, &StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	// the first node stores the record, leaving the second outstanding
	state = sm.Advance(ctx, now, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: a,
	})
	require.IsType(t, &StateBroadcastWaiting{}, state)

	// the second fails, which leaves nothing outstanding
	storeErr := fmt.Errorf("no space")
	state = sm.Advance(ctx, now, &EventBroadcastStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: b,
		Error:  storeErr,
	})

	st, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testQueryID, st.QueryID)
	require.ElementsMatch(t, []tiny.Node{a, b}, st.Contacted)
	require.Len(t, st.Errors, 1)
	require.Equal(t, b, st.Errors[b.String()].Node)
	require.Equal(t, storeErr, st.Errors[b.String()].Err)
}

// TestStaticEmptySeedFinishesImmediately checks that a static broadcast started with no nodes
// has nothing to contact and finishes without reporting any.
func TestStaticEmptySeedFinishesImmediately(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newStaticTest(t)

	state := sm.Advance(ctx, now, &EventBroadcastStart[tiny.Key, tiny.Node]{})

	st, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Empty(t, st.Contacted)
	require.Empty(t, st.Errors)
}

// TestStaticStopRecordsOutstandingNodesAsFailed checks that stopping a static broadcast
// records both the nodes it had yet to contact and those it had not heard back from.
func TestStaticStopRecordsOutstandingNodesAsFailed(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newStaticTest(t)

	a := tiny.NewNode(4)
	b := tiny.NewNode(5)

	// one node is contacted, leaving the other still to do
	state := sm.Advance(ctx, now, &EventBroadcastStart[tiny.Key, tiny.Node]{Seed: []tiny.Node{a, b}})
	require.IsType(t, &StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = sm.Advance(ctx, now, &EventBroadcastStop{})

	st, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.ElementsMatch(t, []tiny.Node{a, b}, st.Contacted)
	require.Len(t, st.Errors, 2)
	require.EqualError(t, st.Errors[a.String()].Err, "cancelled")
	require.EqualError(t, st.Errors[b.String()].Err, "cancelled")
}

// TestStaticFinishedIsSticky checks that a static broadcast polled after it has finished
// reports the same result rather than reporting that it is idle.
func TestStaticFinishedIsSticky(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newStaticTest(t)

	a := tiny.NewNode(4)

	state := sm.Advance(ctx, now, &EventBroadcastStart[tiny.Key, tiny.Node]{Seed: []tiny.Node{a}})
	require.IsType(t, &StateBroadcastStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = sm.Advance(ctx, now, &EventBroadcastStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: a,
	})
	require.IsType(t, &StateBroadcastFinished[tiny.Key, tiny.Node]{}, state)

	state = sm.Advance(ctx, now, &EventBroadcastPoll{})
	st, ok := state.(*StateBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.ElementsMatch(t, []tiny.Node{a}, st.Contacted)
}
