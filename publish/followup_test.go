package publish

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/query"
)

// newFollowUpTest creates a FollowUp state machine for the query id "test", running its query
// in a pool built from qcfg and starting from seeds. A nil qcfg uses the default query pool
// configuration.
func newFollowUpTest(t *testing.T, qcfg *query.PoolConfig, seeds ...tiny.Node) *FollowUp[tiny.Key, tiny.Node, tiny.Message] {
	t.Helper()
	if qcfg == nil {
		qcfg = query.DefaultPoolConfig()
	}
	qp, err := query.NewPool[tiny.Key, tiny.Node, tiny.Message](tiny.NewNode(0), qcfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	fu, err := NewFollowUp(testActivityID, qp, msg, seeds, DefaultFollowUpConfig())
	require.NoError(t, err)
	return fu
}

// TestFollowUpQueriesBeforeStoring checks that a follow up publish finds the nodes closest
// to its target before asking for the record to be stored with any of them.
func TestFollowUpQueriesBeforeStoring(t *testing.T) {
	ctx := context.Background()
	now := epoch

	target := tiny.Key(0b00000001)
	a := tiny.NewNode(4)

	// the first phase contacts the seed node rather than storing anything with it

	sm := newFollowUpTest(t, nil, a)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	fcState, ok := state.(*StatePublishFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testActivityID, fcState.ActivityID)
	require.Equal(t, a, fcState.NodeID)

	// the node knows of nobody closer, which ends the query and starts the second phase
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      a,
		CloserNodes: []tiny.Node{a},
	})
	srState, ok := state.(*StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testActivityID, srState.ActivityID)
	require.Equal(t, a, srState.NodeID)
	require.Equal(t, "store this", srState.Message.Content)

	// the record is stored with every node the query settled on, so the publish finishes
	state = sm.Advance(ctx, now, &EventPublishStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: a,
	})
	fnState, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.ElementsMatch(t, []tiny.Node{a}, fnState.Contacted)
	require.Empty(t, fnState.Errors)
}

// TestFollowUpReportsIdleBeforeStarted checks that a follow up publish polled before it has
// been given a target reports that it has nothing to do.
func TestFollowUpReportsIdleBeforeStarted(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newFollowUpTest(t, nil)

	state := sm.Advance(ctx, now, &EventPublishPoll{})
	require.IsType(t, &StatePublishIdle{}, state)
}

// TestFollowUpFinishesWhenQueryFindsNoNodes checks that a follow up publish whose query
// settles on no node at all skips the second phase and reports contacting nobody.
func TestFollowUpFinishesWhenQueryFindsNoNodes(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newFollowUpTest(t, nil)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: tiny.Key(0b00000001),
	})

	st, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testActivityID, st.ActivityID)
	require.Empty(t, st.Contacted)
	require.Empty(t, st.Errors)
}

// TestFollowUpWaitsAtQueryPoolCapacity checks that a follow up publish whose query pool has
// no capacity to spare reports the time at which the pool could next make progress.
func TestFollowUpWaitsAtQueryPoolCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch

	// a single slot, which the publish's own query fills
	qcfg := query.DefaultPoolConfig()
	qcfg.Concurrency = 1

	a := tiny.NewNode(4)

	sm := newFollowUpTest(t, qcfg, a)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: tiny.Key(0b00000001),
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	state = sm.Advance(ctx, now, &EventPublishPoll{})
	require.IsType(t, &StatePublishWaiting{}, state)
	require.Equal(t, testActivityID, state.(*StatePublishWaiting).ActivityID)
	require.Equal(t, now.Add(qcfg.RequestTimeout), state.(*StatePublishWaiting).NextDue)
}

// TestFollowUpFinishesWhenQueryTimesOut checks that a follow up publish whose query runs out
// of time gives up rather than storing the record with the nodes found so far.
func TestFollowUpFinishesWhenQueryTimesOut(t *testing.T) {
	ctx := context.Background()
	now := epoch

	qcfg := query.DefaultPoolConfig()
	qcfg.Timeout = 3 * time.Minute

	// the request must outlive the query so the query is still waiting when its deadline passes
	qcfg.RequestTimeout = time.Hour

	a := tiny.NewNode(4)

	sm := newFollowUpTest(t, qcfg, a)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: tiny.Key(0b00000001),
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// the node never replies, but the query has not run out of time yet
	now = now.Add(qcfg.Timeout - time.Second)
	state = sm.Advance(ctx, now, &EventPublishPoll{})
	require.IsType(t, &StatePublishWaiting{}, state)

	// once the deadline passes the query is abandoned and the publish has nobody to store with
	now = now.Add(2 * time.Second)
	state = sm.Advance(ctx, now, &EventPublishPoll{})

	st, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testActivityID, st.ActivityID)
	require.Empty(t, st.Contacted)
	require.Empty(t, st.Errors)
}

// TestFollowUpStopDuringQueryFinishes checks that stopping a follow up publish while its
// query is still running abandons the query and reports the publish as finished.
func TestFollowUpStopDuringQueryFinishes(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newFollowUpTest(t, nil, tiny.NewNode(4))

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: tiny.Key(0b00000001),
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	state = sm.Advance(ctx, now, &EventPublishStop{})

	st, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testActivityID, st.ActivityID)
	require.Empty(t, st.Contacted)
}

// TestFollowUpStopDuringStoresRecordsOutstandingNodesAsFailed checks that stopping a follow up
// publish after its query has finished records the nodes it had yet to hear from.
func TestFollowUpStopDuringStoresRecordsOutstandingNodesAsFailed(t *testing.T) {
	ctx := context.Background()
	now := epoch

	target := tiny.Key(0b00000001)
	a := tiny.NewNode(4)
	b := tiny.NewNode(5)

	sm := newFollowUpTest(t, nil, a)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// the seed reports one closer node, which the query goes on to contact
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      a,
		CloserNodes: []tiny.Node{a, b},
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// that node knows of nobody closer, ending the query and starting the second phase
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      b,
		CloserNodes: []tiny.Node{b},
	})
	require.IsType(t, &StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = sm.Advance(ctx, now, &EventPublishStop{})

	st, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Len(t, st.Errors, 2)
	for _, n := range []tiny.Node{a, b} {
		require.EqualError(t, st.Errors[n.String()].Err, "cancelled", "node %s", n)
	}
}

// TestFollowUpReportsStoreFailures checks that a follow up publish reports the error from a
// node that was asked to store the record and refused.
func TestFollowUpReportsStoreFailures(t *testing.T) {
	ctx := context.Background()
	now := epoch

	a := tiny.NewNode(4)

	sm := newFollowUpTest(t, nil, a)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: tiny.Key(0b00000001),
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      a,
		CloserNodes: []tiny.Node{a},
	})
	require.IsType(t, &StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	storeErr := fmt.Errorf("no space")
	state = sm.Advance(ctx, now, &EventPublishStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: a,
		Error:  storeErr,
	})

	st, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.ElementsMatch(t, []tiny.Node{a}, st.Contacted)
	require.Len(t, st.Errors, 1)
	require.Equal(t, storeErr, st.Errors[a.String()].Err)
}
