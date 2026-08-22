package publish

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

var _ coordt.StateMachine[RegionEvent, RegionState] = (*RegionPublish[tiny.Key, tiny.Node])(nil)

// testRegionID is the id a region publish under test derives its child ids from.
const testRegionID = coordt.ActivityID("region")

// newRegionTest creates a RegionPublish for the region id "region" over keys, storing each with
// its r closest nodes and running at most cap per-key publishes at once.
func newRegionTest(t *testing.T, keys []tiny.Key, nodes []tiny.Node, r, maxInFlight int) *RegionPublish[tiny.Key, tiny.Node] {
	t.Helper()
	cfg := DefaultRegionPublishConfig()
	cfg.Replication = r
	cfg.MaxInFlight = maxInFlight
	rp, err := NewRegionPublish(testRegionID, keys, nodes, cfg)
	require.NoError(t, err)
	return rp
}

// TestRegionStartsEveryKeyOnce checks that a region publish starts a per-key publish for each of
// its keys, one key per advance, and finishes once every child has reported done.
func TestRegionStartsEveryKeyOnce(t *testing.T) {
	ctx := context.Background()
	now := epoch

	keys := []tiny.Key{1, 2, 3}
	nodes := []tiny.Node{tiny.NewNode(4), tiny.NewNode(5)}

	// a cap at least as large as the key count lets every key start before any is done
	sm := newRegionTest(t, keys, nodes, 2, len(keys))

	started := make(map[tiny.Key]bool, len(keys))
	children := make([]coordt.ActivityID, 0, len(keys))

	state := sm.Advance(ctx, now, &EventRegionPoll{})
	for range keys {
		st, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
		require.True(t, ok, "state is %T", state)
		require.False(t, started[st.Target], "key started twice")
		started[st.Target] = true
		children = append(children, st.ChildID)

		state = sm.Advance(ctx, now, &EventRegionPoll{})
	}

	require.Len(t, started, len(keys))

	// every key is started but none is done, so the region is waiting on its children
	require.IsType(t, &StateRegionWaiting{}, state)

	// report each child done; the region finishes once none is outstanding
	for i, child := range children {
		state = sm.Advance(ctx, now, &EventRegionKeyDone{ChildID: child})
		if i < len(children)-1 {
			require.IsType(t, &StateRegionWaiting{}, state)
		}
	}

	require.IsType(t, &StateRegionFinished{}, state)
}

// TestRegionMintsUniqueChildIDs checks that every per-key publish a region starts is given a
// distinct child id derived from the region id.
func TestRegionMintsUniqueChildIDs(t *testing.T) {
	ctx := context.Background()
	now := epoch

	keys := []tiny.Key{1, 2, 3}
	nodes := []tiny.Node{tiny.NewNode(4)}

	sm := newRegionTest(t, keys, nodes, 1, len(keys))

	seen := make(map[coordt.ActivityID]bool, len(keys))
	state := sm.Advance(ctx, now, &EventRegionPoll{})
	for range keys {
		st, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
		require.True(t, ok, "state is %T", state)
		require.NotEqual(t, testRegionID, st.ChildID, "child id must differ from the region id")
		require.False(t, seen[st.ChildID], "child id reused")
		seen[st.ChildID] = true

		state = sm.Advance(ctx, now, &EventRegionPoll{})
	}

	require.Len(t, seen, len(keys))
}

// TestRegionCapGatesInFlightPublishes checks that a region starts no more than cap per-key
// publishes at once and waits rather than starting another when the cap is reached.
func TestRegionCapGatesInFlightPublishes(t *testing.T) {
	ctx := context.Background()
	now := epoch

	keys := []tiny.Key{1, 2, 3, 4}
	nodes := []tiny.Node{tiny.NewNode(8)}

	sm := newRegionTest(t, keys, nodes, 1, 2)

	// the first two keys start, filling the cap
	state := sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionStartKey[tiny.Key, tiny.Node]{}, state)
	state = sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionStartKey[tiny.Key, tiny.Node]{}, state)

	// with the cap full and keys still to start, a poll waits rather than starting another
	state = sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionWaiting{}, state)

	require.True(t, state.(*StateRegionWaiting).NextDue.IsZero())
}

// TestRegionKeyDoneFreesASlot checks that reporting a per-key publish done frees a slot without
// starting a key, and that the next poll starts the key the freed slot allows.
func TestRegionKeyDoneFreesASlot(t *testing.T) {
	ctx := context.Background()
	now := epoch

	keys := []tiny.Key{1, 2, 3}
	nodes := []tiny.Node{tiny.NewNode(8)}

	sm := newRegionTest(t, keys, nodes, 1, 1)

	// the cap of one lets a single key start
	state := sm.Advance(ctx, now, &EventRegionPoll{})
	first, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)

	// with the cap full a poll waits
	state = sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionWaiting{}, state)

	// the child finishing frees the slot but starts no key on its own
	state = sm.Advance(ctx, now, &EventRegionKeyDone{ChildID: first.ChildID})
	require.IsType(t, &StateRegionWaiting{}, state)

	// the next poll starts another key now that a slot is free
	state = sm.Advance(ctx, now, &EventRegionPoll{})
	second, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.NotEqual(t, first.Target, second.Target, "same key started twice")
}

// TestRegionNotFinishedWhileChildOutstanding checks that a region with its cursor exhausted keeps
// waiting until the last per-key publish reports done.
func TestRegionNotFinishedWhileChildOutstanding(t *testing.T) {
	ctx := context.Background()
	now := epoch

	keys := []tiny.Key{1}
	nodes := []tiny.Node{tiny.NewNode(8)}

	sm := newRegionTest(t, keys, nodes, 1, 1)

	state := sm.Advance(ctx, now, &EventRegionPoll{})
	only, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)

	// the only key has started, so the cursor is exhausted, but its child is still outstanding
	state = sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionWaiting{}, state)

	state = sm.Advance(ctx, now, &EventRegionKeyDone{ChildID: only.ChildID})
	require.IsType(t, &StateRegionFinished{}, state)
}

// TestRegionOffersEachKeyItsClosestNodes checks that a region offers each key only the r nodes
// closest to it, so an over-large region never over-stores.
func TestRegionOffersEachKeyItsClosestNodes(t *testing.T) {
	ctx := context.Background()
	now := epoch

	// four nodes spread across the key space and one key to publish
	nodes := []tiny.Node{tiny.NewNode(0b0000_0001), tiny.NewNode(0b0000_0010), tiny.NewNode(0b1000_0000), tiny.NewNode(0b1100_0000)}
	key := tiny.Key(0b0000_0011)

	sm := newRegionTest(t, []tiny.Key{key}, nodes, 2, 1)

	state := sm.Advance(ctx, now, &EventRegionPoll{})
	st, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)

	// the two nodes closest to 0b0000_0011 are 0b0000_0001 and 0b0000_0010
	require.Len(t, st.Nodes, 2)
	want := []tiny.Node{tiny.NewNode(0b0000_0001), tiny.NewNode(0b0000_0010)}
	require.ElementsMatch(t, want, st.Nodes)
}

// TestRegionSmallerThanReplicationOffersEveryNode checks that a region holding fewer nodes than
// the replication factor offers a key every node it has.
func TestRegionSmallerThanReplicationOffersEveryNode(t *testing.T) {
	ctx := context.Background()
	now := epoch

	nodes := []tiny.Node{tiny.NewNode(4), tiny.NewNode(5)}

	sm := newRegionTest(t, []tiny.Key{1}, nodes, 20, 1)

	state := sm.Advance(ctx, now, &EventRegionPoll{})
	st, ok := state.(*StateRegionStartKey[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.ElementsMatch(t, nodes, st.Nodes)
}

// TestRegionWithNoKeysFinishesImmediately checks that a region holding no keys has nothing to
// start and finishes on its first advance.
func TestRegionWithNoKeysFinishesImmediately(t *testing.T) {
	ctx := context.Background()
	now := epoch

	sm := newRegionTest(t, nil, []tiny.Node{tiny.NewNode(4)}, 2, 4)

	state := sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionFinished{}, state)
}

// TestRegionIgnoresUnknownChildDone checks that a done event naming a child the region never
// started leaves its in-flight accounting untouched.
func TestRegionIgnoresUnknownChildDone(t *testing.T) {
	ctx := context.Background()
	now := epoch

	keys := []tiny.Key{1, 2}
	nodes := []tiny.Node{tiny.NewNode(8)}

	sm := newRegionTest(t, keys, nodes, 1, 1)

	state := sm.Advance(ctx, now, &EventRegionPoll{})
	require.IsType(t, &StateRegionStartKey[tiny.Key, tiny.Node]{}, state)

	// a done for an id the region never handed out must not free the occupied slot
	state = sm.Advance(ctx, now, &EventRegionKeyDone{ChildID: coordt.ActivityID("bogus")})
	require.IsType(t, &StateRegionWaiting{}, state)
}
