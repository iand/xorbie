package publish

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/mathext"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/query"
)

func TestOptimisticConfigValidate(t *testing.T) {
	testCases := []struct {
		name  string
		cfg   *OptimisticConfig
		valid bool
	}{
		{
			name:  "default",
			cfg:   DefaultOptimisticConfig(),
			valid: true,
		},
		{
			name: "zero replication factor",
			cfg: func() *OptimisticConfig {
				cfg := DefaultOptimisticConfig()
				cfg.ReplicationFactor = 0
				return cfg
			}(),
			valid: false,
		},
		{
			name: "individual certainty of one",
			cfg: func() *OptimisticConfig {
				cfg := DefaultOptimisticConfig()
				cfg.IndividualCertainty = 1
				return cfg
			}(),
			valid: false,
		},
		{
			name: "individual certainty of zero",
			cfg: func() *OptimisticConfig {
				cfg := DefaultOptimisticConfig()
				cfg.IndividualCertainty = 0
				return cfg
			}(),
			valid: false,
		},
		{
			name: "set strictness of one",
			cfg: func() *OptimisticConfig {
				cfg := DefaultOptimisticConfig()
				cfg.SetStrictness = 1
				return cfg
			}(),
			valid: false,
		},
		{
			name: "set strictness of zero",
			cfg: func() *OptimisticConfig {
				cfg := DefaultOptimisticConfig()
				cfg.SetStrictness = 0
				return cfg
			}(),
			valid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestOptimisticThresholds checks that the two distance thresholds are the quantiles the
// algorithm calls for, scaled by the network size.
func TestOptimisticThresholds(t *testing.T) {
	const networkSize = 1000

	cfg := DefaultOptimisticConfig()
	individual, set, err := cfg.thresholds(networkSize)
	require.NoError(t, err)

	require.InDelta(t, 14.525261, individual*networkSize, 0.00001)
	require.InDelta(t, 15.406641, set*networkSize, 0.00001)

	// The individual threshold is the distance below which a node is probably one of the
	// replication factor closest, so the probability of a node falling inside it must be one
	// minus the certainty asked for. The same holds for the set threshold and its strictness.
	require.InDelta(t, 1-cfg.IndividualCertainty, mathext.GammaIncReg(20, individual*networkSize), 0.00001)
	require.InDelta(t, 1-cfg.SetStrictness, mathext.GammaIncReg(11, set*networkSize), 0.00001)
}

// TestOptimisticThresholdsFallWithNetworkSize checks that a larger network narrows both
// thresholds, since the nodes closest to a key are closer the more nodes there are.
func TestOptimisticThresholdsFallWithNetworkSize(t *testing.T) {
	smallIndividual, smallSet, err := DefaultOptimisticConfig().thresholds(100)
	require.NoError(t, err)

	largeIndividual, largeSet, err := DefaultOptimisticConfig().thresholds(10000)
	require.NoError(t, err)

	require.Less(t, largeIndividual, smallIndividual)
	require.Less(t, largeSet, smallSet)
}

// newOptimisticTest creates an Optimistic state machine for the query id "test" that walks from
// seeds towards a target in a network of networkSize nodes, storing the record with
// replicationFactor of them. Its walk runs in a pool that contacts one node at a time so that the
// interleaving of the walk and the stores is visible.
func newOptimisticTest(t *testing.T, networkSize, replicationFactor int, seeds ...tiny.Node) *Optimistic[tiny.Key, tiny.Node, tiny.Message] {
	t.Helper()

	qcfg := query.DefaultPoolConfig()
	qcfg.Concurrency = 1

	qp, err := query.NewPool[tiny.Key, tiny.Node, tiny.Message](tiny.NewNode(0), qcfg)
	require.NoError(t, err)

	cfg := DefaultOptimisticConfig()
	cfg.ReplicationFactor = replicationFactor

	msg := tiny.Message{Content: "store this"}
	sm, err := NewOptimistic(testActivityID, qp, msg, seeds, networkSize, cfg, coordt.NoopTracer())
	require.NoError(t, err)

	return sm
}

// TestOptimisticStoresBeforeTheWalkEnds checks that a node discovered inside the individual
// threshold is asked to store the record while the walk is still running, which is the point of
// the strategy.
func TestOptimisticStoresBeforeTheWalkEnds(t *testing.T) {
	ctx := context.Background()
	now := epoch

	// At a replication factor of 2 in a network of 64 the individual threshold is a little
	// over two keys wide, so a node two keys from the target qualifies and one forty keys away
	// does not.
	target := tiny.Key(0)
	far := tiny.NewNode(40)
	near := tiny.NewNode(2)

	sm := newOptimisticTest(t, 64, 2, far)

	// the walk begins with the seed, which is too far away to store with
	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	fcState, ok := state.(*StatePublishFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, far, fcState.NodeID)

	// the seed knows of a node inside the individual threshold, which the walk moves on to
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      far,
		CloserNodes: []tiny.Node{near},
	})
	fcState, ok = state.(*StatePublishFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, near, fcState.NodeID)

	// with the walk waiting on that node, the record is stored with it rather than waiting for
	// the walk to settle
	state = sm.Advance(ctx, now, &EventPublishPoll{})
	srState, ok := state.(*StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, near, srState.NodeID)
	require.Equal(t, "store this", srState.Message.Content)

	// and the walk has not finished
	require.True(t, sm.walking)
}

// TestOptimisticDoesNotStoreOutsideTheIndividualThreshold checks that a node discovered too far
// from the target is left to the second phase rather than being stored with during the walk.
func TestOptimisticDoesNotStoreOutsideTheIndividualThreshold(t *testing.T) {
	ctx := context.Background()
	now := epoch

	target := tiny.Key(0)
	seed := tiny.NewNode(100)
	outside := tiny.NewNode(40)

	sm := newOptimisticTest(t, 64, 2, seed)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// the node the seed reports is nearer the target but still outside the individual
	// threshold, and the two known nodes are too far apart to satisfy the set threshold, so the
	// walk moves on to it
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      seed,
		CloserNodes: []tiny.Node{outside},
	})
	fcState, ok := state.(*StatePublishFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, outside, fcState.NodeID)

	// with the walk waiting on that node there is nothing to store, so the machine waits
	state = sm.Advance(ctx, now, &EventPublishPoll{})
	require.IsType(t, &StatePublishWaiting{}, state)
	require.True(t, sm.walking)
}

// TestOptimisticReportsWhenTheWalkIsDue checks that an optimistic publish waiting on its walk
// reports the time at which the walk could next make progress, so that a request a node does not
// answer is timed out rather than left outstanding indefinitely.
func TestOptimisticReportsWhenTheWalkIsDue(t *testing.T) {
	ctx := context.Background()
	now := epoch

	target := tiny.Key(0)
	seed := tiny.NewNode(100)

	sm := newOptimisticTest(t, 64, 2, seed)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// the walk holds a request out and there is nothing to store, so the only progress left is
	// that request timing out
	state = sm.Advance(ctx, now, &EventPublishPoll{})
	wState, ok := state.(*StatePublishWaiting)
	require.True(t, ok, "state is %T", state)
	require.Equal(t, testActivityID, wState.ActivityID)
	require.Equal(t, now.Add(query.DefaultPoolConfig().RequestTimeout), wState.NextDue)
}

// TestOptimisticAbandonsTheWalkOnTheSetThreshold checks that the walk stops once the nodes
// already found are close enough to the target that looking for better ones is not worthwhile,
// and that the record is then stored with them.
func TestOptimisticAbandonsTheWalkOnTheSetThreshold(t *testing.T) {
	ctx := context.Background()
	now := epoch

	// At a replication factor of 2 in a network of 64 the set threshold is a little over
	// fifteen keys wide, so two nodes four and eight keys away average well inside it.
	target := tiny.Key(0)
	seed := tiny.NewNode(40)
	a := tiny.NewNode(4)
	b := tiny.NewNode(8)

	sm := newOptimisticTest(t, 64, 2, seed)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// the seed answers with two nodes that between them satisfy the set threshold, so the walk
	// is abandoned and the record is stored with them instead of being carried further
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      seed,
		CloserNodes: []tiny.Node{a, b},
	})
	require.False(t, sm.walking)

	srState, ok := state.(*StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	first := srState.NodeID

	state = sm.Advance(ctx, now, &EventPublishStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: first,
	})
	srState, ok = state.(*StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	second := srState.NodeID

	require.ElementsMatch(t, []tiny.Node{a, b}, []tiny.Node{first, second})

	// with both stored the publish is finished, reporting the nodes it stored with and no
	// errors
	state = sm.Advance(ctx, now, &EventPublishStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		NodeID: second,
	})
	fState, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.ElementsMatch(t, []tiny.Node{a, b}, fState.Contacted)
	require.Empty(t, fState.Errors)
}

// TestOptimisticWalksOnWhileTooFewNodesAreKnown checks that the set threshold is not applied
// before the replication factor's worth of nodes have been found, since the mean distance of a
// smaller set is not the quantity the threshold describes.
func TestOptimisticWalksOnWhileTooFewNodesAreKnown(t *testing.T) {
	ctx := context.Background()
	now := epoch

	target := tiny.Key(0)
	seed := tiny.NewNode(40)
	a := tiny.NewNode(4)

	sm := newOptimisticTest(t, 64, 4, seed)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// two nodes are known and the replication factor is four, so however close they are the
	// walk carries on
	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      seed,
		CloserNodes: []tiny.Node{a},
	})
	require.True(t, sm.walking)
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)
}

// TestOptimisticStopAbandonsOutstandingWork checks that stopping a publish records the nodes it
// had not finished with as failures.
func TestOptimisticStopAbandonsOutstandingWork(t *testing.T) {
	ctx := context.Background()
	now := epoch

	target := tiny.Key(0)
	seed := tiny.NewNode(40)
	near := tiny.NewNode(2)

	sm := newOptimisticTest(t, 64, 2, seed)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: target,
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	state = sm.Advance(ctx, now, &EventPublishNodeResponse[tiny.Key, tiny.Node]{
		NodeID:      seed,
		CloserNodes: []tiny.Node{near},
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)

	// the store for the nearby node is outstanding when the publish is stopped
	state = sm.Advance(ctx, now, &EventPublishPoll{})
	require.IsType(t, &StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = sm.Advance(ctx, now, &EventPublishStop{})
	fState, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Contains(t, fState.Errors, near.String())

	// every node reported as a failure is one the record was asked to be stored with, which is
	// what the finished state promises
	for key := range fState.Errors {
		require.Contains(t, nodeKeys(fState.Contacted), key)
	}
}

// TestOptimisticStopWhileWalkingFinishes checks that stopping a publish whose walk is still
// running ends the operation, rather than leaving it waiting on a walk that will never be
// advanced again.
func TestOptimisticStopWhileWalkingFinishes(t *testing.T) {
	ctx := context.Background()
	now := epoch

	seed := tiny.NewNode(100)

	sm := newOptimisticTest(t, 64, 2, seed)

	state := sm.Advance(ctx, now, &EventPublishStart[tiny.Key, tiny.Node]{
		Target: tiny.Key(0),
	})
	require.IsType(t, &StatePublishFindCloser[tiny.Key, tiny.Node]{}, state)
	require.True(t, sm.walking)

	// nothing has been stored and the walk is mid-flight, so the operation finishes with
	// nothing contacted rather than reporting that it is waiting
	state = sm.Advance(ctx, now, &EventPublishStop{})
	fState, ok := state.(*StatePublishFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Empty(t, fState.Contacted)
	require.Empty(t, fState.Errors)

	// and it reports the same on every later advance
	state = sm.Advance(ctx, now, &EventPublishPoll{})
	require.IsType(t, &StatePublishFinished[tiny.Key, tiny.Node]{}, state)
}

// nodeKeys returns the string form of each node, which is how a finished publish keys the
// errors it reports.
func nodeKeys(nodes []tiny.Node) []string {
	keys := make([]string, 0, len(nodes))
	for _, n := range nodes {
		keys = append(keys, n.String())
	}
	return keys
}
