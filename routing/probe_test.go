package routing

import (
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/internal/tiny"
)

var _ heap.Interface = (*nodeValuePendingList[tiny.Key, tiny.Node])(nil)

func TestProbeConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultProbeConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("tracer is not nil", func(t *testing.T) {
		cfg := DefaultProbeConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter is not nil", func(t *testing.T) {
		cfg := DefaultProbeConfig()
		cfg.Meter = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultProbeConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultProbeConfig()
		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("revisit interval positive", func(t *testing.T) {
		cfg := DefaultProbeConfig()
		cfg.CheckInterval = 0
		require.Error(t, cfg.Validate())
		cfg.CheckInterval = -1
		require.Error(t, cfg.Validate())
	})
}

func TestProbeStartsIdle(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultProbeConfig()

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)

	bs, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)
}

func TestProbeAddChecksPresenceInRoutingTable(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency to allow one check to run
	cfg.Concurrency = 1

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// Add node that isn't in routing table
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)

	// remains idle since probes aren't run unless node in routing table
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)
}

func TestProbeAddStartsCheckIfCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency to allow one check to run
	cfg.Concurrency = 1

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// remains idle
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the probe state machine should attempt to contact the next node
	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])

	// the connectivity check should be for the right node
	require.True(t, key.Equal(tiny.Key(4), st.NodeID.Key()))
}

func TestProbeAddManyStartsChecksIfCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency lower than the number of nodes
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))
	rt.AddNode(tiny.NewNode(3))
	rt.AddNode(tiny.NewNode(2))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// after adding second node the probe should still be idle since the
	// connectivity check will be scheduled for the future
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(3),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// after adding third node the probe should still be idle since the
	// connectivity check will be scheduled for the future
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(2),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)

	// Poll the state machine, it should now attempt to contact a node
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the connectivity check should be for the right node
	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])
	require.True(t, key.Equal(tiny.Key(4), st.NodeID.Key()))

	// Poll the state machine, it should now attempt to contact another node
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the connectivity check should be for the right node
	st = state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])
	require.True(t, key.Equal(tiny.Key(2), st.NodeID.Key()))

	// Poll the state machine, it should now be at capacity
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingAtCapacity{}, state)
}

func TestProbeAddReportsCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency to allow more than one check to run
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// remains idle
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the probe state machine should attempt to contact the next node
	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])

	// the connectivity check should be for the right node
	require.True(t, key.Equal(tiny.Key(4), st.NodeID.Key()))

	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingWithCapacity{}, state)
}

func TestProbeRemoveDeletesNodeValue(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency to allow more than one check to run
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// remove the node
	state = sm.Advance(ctx, now, &EventProbeRemove[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})

	// state indicate that node failed
	require.IsType(t, &StateProbeNodeFailure[tiny.Key, tiny.Node]{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)

	// state remains idle since there are no nodes to probe
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)
}

func TestNodeValueList(t *testing.T) {
	t.Run("put new", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}

		l.Put(nv)

		got, found := l.Get(tiny.NewNode(4))
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), tiny.Key(4)))
	})

	t.Run("put replace before", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}

		l.Put(nv1)

		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now.Add(-time.Minute),
		}
		l.Put(nv2)

		got, found := l.Get(tiny.NewNode(4))
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), tiny.Key(4)))
		require.Equal(t, nv2.NextCheckDue, got.NextCheckDue)
	})

	t.Run("put replace after", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}

		l.Put(nv1)

		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now.Add(time.Minute),
		}
		l.Put(nv2)

		got, found := l.Get(tiny.NewNode(4))
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), tiny.Key(4)))
		require.Equal(t, nv2.NextCheckDue, got.NextCheckDue)
	})

	t.Run("remove existing", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}

		l.Put(nv)

		require.Equal(t, 1, l.pendingCount())
		require.Equal(t, 1, l.nodeCount())

		_, found := l.Get(tiny.NewNode(4))
		require.True(t, found)

		l.Remove(tiny.NewNode(4))
		_, found = l.Get(tiny.NewNode(4))
		require.False(t, found)

		require.Equal(t, 0, l.pendingCount())
		require.Equal(t, 0, l.nodeCount())
	})

	t.Run("remove not-existing", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}

		l.Put(nv)

		l.Remove(tiny.NewNode(5))
		_, found := l.Get(tiny.NewNode(4))
		require.True(t, found)
	})

	t.Run("next empty list", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		got, found := l.PeekNext(now)
		require.False(t, found)
		require.Nil(t, got)
	})

	t.Run("next one entry", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}
		l.Put(nv)

		got, found := l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), tiny.Key(4)))
	})

	t.Run("next sorts by next check due", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(5),
			NextCheckDue: now.Add(-time.Minute),
		}
		l.Put(nv1)
		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now.Add(-2 * time.Minute),
		}
		l.Put(nv2)

		got, found := l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), nv2.NodeID.Key()))

		nv2.NextCheckDue = now
		l.Put(nv2)

		got, found = l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), nv1.NodeID.Key()))
	})

	t.Run("next sorts by cpl descending after time", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(5),
			Cpl:          1,
			NextCheckDue: now.Add(-time.Minute),
		}
		l.Put(nv1)
		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			Cpl:          2,
			NextCheckDue: now.Add(-time.Minute),
		}
		l.Put(nv2)

		got, found := l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), nv2.NodeID.Key()))

		nv2.NextCheckDue = now
		l.Put(nv2)

		got, found = l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), nv1.NodeID.Key()))
	})

	t.Run("next not due", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(5),
			NextCheckDue: now.Add(time.Minute),
		}
		l.Put(nv1)
		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now.Add(2 * time.Minute),
		}
		l.Put(nv2)

		got, found := l.PeekNext(now)
		require.False(t, found)
		require.Nil(t, got)
	})

	t.Run("mark ongoing", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(5),
			NextCheckDue: now.Add(time.Minute),
		}
		l.Put(nv1)
		require.Equal(t, 1, l.pendingCount())
		require.Equal(t, 0, l.ongoingCount())
		require.Equal(t, 1, l.nodeCount())

		l.MarkOngoing(nv1.NodeID, now.Add(time.Minute))
		require.Equal(t, 0, l.pendingCount())
		require.Equal(t, 1, l.ongoingCount())
		require.Equal(t, 1, l.nodeCount())
	})

	t.Run("mark ongoing changes next", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(5),
			NextCheckDue: now.Add(-2 * time.Minute),
		}
		l.Put(nv1)

		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now.Add(-1 * time.Minute),
		}
		l.Put(nv2)

		require.Equal(t, 2, l.pendingCount())
		require.Equal(t, 0, l.ongoingCount())
		require.Equal(t, 2, l.nodeCount())

		// nv1 is the next node due
		got, found := l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), nv1.NodeID.Key()))

		l.MarkOngoing(nv1.NodeID, now.Add(time.Minute))
		require.Equal(t, 1, l.pendingCount())
		require.Equal(t, 1, l.ongoingCount())
		require.Equal(t, 2, l.nodeCount())

		// nv2 is now the next node due
		got, found = l.PeekNext(now)
		require.True(t, found)
		require.True(t, key.Equal(got.NodeID.Key(), nv2.NodeID.Key()))
	})

	t.Run("put removes from ongoing", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(4),
			NextCheckDue: now,
		}
		l.Put(nv1)

		require.Equal(t, 1, l.pendingCount())
		require.Equal(t, 0, l.ongoingCount())
		require.Equal(t, 1, l.nodeCount())

		l.MarkOngoing(nv1.NodeID, now.Add(time.Minute))

		require.Equal(t, 0, l.pendingCount())
		require.Equal(t, 1, l.ongoingCount())
		require.Equal(t, 1, l.nodeCount())

		l.Put(nv1)

		require.Equal(t, 1, l.pendingCount())
		require.Equal(t, 0, l.ongoingCount())
		require.Equal(t, 1, l.nodeCount())
	})

	t.Run("mark ongoing pending mixed", func(t *testing.T) {
		t.Parallel()

		now := epoch
		l := NewNodeValueList[tiny.Key, tiny.Node]()
		nv1 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(5),
			NextCheckDue: now.Add(time.Minute),
		}
		nv2 := &nodeValue[tiny.Key, tiny.Node]{
			NodeID:       tiny.NewNode(6),
			NextCheckDue: now.Add(time.Minute),
		}

		l.Put(nv1)
		l.Put(nv2)
		l.MarkOngoing(nv1.NodeID, now.Add(time.Minute))
		l.Put(nv1)
	})
}

func TestProbeConnectivityCheckSuccess(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency to allow more than one check to run
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the probe state machine should attempt to contact the next node
	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])

	// notify that node was contacted successfully, with no closer nodes
	state = sm.Advance(ctx, now, &EventProbeConnectivityCheckSuccess[tiny.Key, tiny.Node]{
		NodeID: st.NodeID,
	})

	// node remains in routing table
	_, found := rt.GetNode(tiny.Key(4))
	require.True(t, found)

	// state machine now idle
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by another revisit interval
	now = now.Add(cfg.CheckInterval)

	// the probe state machine should attempt to contact node again, now it is time
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the connectivity check should be for the right node
	require.True(t, key.Equal(tiny.Key(4), st.NodeID.Key()))

	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingWithCapacity{}, state)
}

func TestProbeConnectivityCheckFailure(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	// Set concurrency to allow more than one check to run
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by one revisit interval
	now = now.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the probe state machine should attempt to contact the next node
	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])

	// notify that node was contacted successfully, with no closer nodes
	state = sm.Advance(ctx, now, &EventProbeConnectivityCheckFailure[tiny.Key, tiny.Node]{
		NodeID: st.NodeID,
	})

	// state machine announces node failure
	require.IsType(t, &StateProbeNodeFailure[tiny.Key, tiny.Node]{}, state)
	stf := state.(*StateProbeNodeFailure[tiny.Key, tiny.Node])

	// the failure should be for the right node
	require.True(t, key.Equal(tiny.Key(4), stf.NodeID.Key()))

	// node has been removed from routing table
	_, found := rt.GetNode(tiny.Key(4))
	require.False(t, found)

	// advance time by another revisit interval
	now = now.Add(cfg.CheckInterval)

	// state machine still idle since node was removed
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)
}

func TestProbeNotifyConnectivity(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))
	rt.AddNode(tiny.NewNode(3))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// after adding first node the probe should be idle since the
	// connectivity check will be scheduled for the future (t0+10)
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})

	// not time for a check yet
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by less than the revisit interval
	// time is now (t0+2)
	now = now.Add(2 * time.Minute)

	// add a second node, which will be second in the probe list since it's
	// time of next check will be later (t0+2+10=t0+12)
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(3),
	})

	// still not time for a check
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time past the first node's check time but before the second node's
	// time is now (t0+2+9=t0+11)
	now = now.Add(9 * time.Minute)

	// notify that the node with key 4 was connected to successfully by another process
	// this will delay the time for the next check to t0+11+10=to+21
	state = sm.Advance(ctx, now, &EventProbeNotifyConnectivity[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})

	// still not time for a check
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time past second node's check time
	// time is now (t0+2+9+4=t0+15)
	now = now.Add(4 * time.Minute)

	// Poll the state machine, it should now attempt to contact a node
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the connectivity check should be for the right node, which is the one
	// that did not get a connectivity notification
	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])
	require.True(t, key.Equal(tiny.Key(3), st.NodeID.Key()))

	// Poll the state machine, it should now waiting for a response but still have capacity
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingWithCapacity{}, state)
}

func TestProbeTimeout(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute
	cfg.Timeout = 3 * time.Minute
	cfg.Concurrency = 1 // one probe at a time, timeouts will be used to free capacity if there are more requests

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))
	rt.AddNode(tiny.NewNode(3))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// add a node
	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})

	// not time for a check yet
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time a little
	now = now.Add(time.Minute)

	// add another node
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(3),
	})

	// not time for a check yet
	require.IsType(t, &StateProbeIdle{}, state)

	// advance time by check interval
	now = now.Add(cfg.CheckInterval)

	// poll state machine
	state = sm.Advance(ctx, now, &EventProbePoll{})

	// the connectivity check should start
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)
	stm := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])
	require.True(t, key.Equal(tiny.Key(4), stm.NodeID.Key()))

	// Poll the state machine, it should now waiting for a response with no capacity
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingAtCapacity{}, state)

	// advance time past the timeout
	now = now.Add(cfg.Timeout)

	// state machine announces node failure
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeNodeFailure[tiny.Key, tiny.Node]{}, state)
	stf := state.(*StateProbeNodeFailure[tiny.Key, tiny.Node])

	// the failure should be for the right node
	require.True(t, key.Equal(tiny.Key(4), stf.NodeID.Key()))

	// node has been removed from routing table
	_, found := rt.GetNode(tiny.Key(4))
	require.False(t, found)

	// state machine starts check for next node now there is capacity
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)
	stm = state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])
	require.True(t, key.Equal(tiny.Key(3), stm.NodeID.Key()))
}

// epoch is the base instant for tests. State machines take the current time as a
// parameter, so tests move time by passing a later instant rather than by advancing a clock.
var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// TestProbeReportsNextDue checks that the probe state machine reports the earliest instant
// at which advancing it could make progress, when it is idle and when it is at capacity.
func TestProbeReportsNextDue(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute
	cfg.Timeout = time.Minute
	cfg.Concurrency = 1

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))
	rt.AddNode(tiny.NewNode(5))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	// with nothing scheduled there is nothing due
	state := sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)
	require.True(t, state.(*StateProbeIdle).NextDue.IsZero())

	// a node scheduled for its first check makes that check the next thing due
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{NodeID: tiny.NewNode(4)})
	require.IsType(t, &StateProbeIdle{}, state)
	require.Equal(t, epoch.Add(cfg.CheckInterval), state.(*StateProbeIdle).NextDue)

	// a node added later is checked later, so the earlier check remains the next thing due
	now = epoch.Add(5 * time.Minute)
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{NodeID: tiny.NewNode(5)})
	require.IsType(t, &StateProbeIdle{}, state)
	require.Equal(t, epoch.Add(cfg.CheckInterval), state.(*StateProbeIdle).NextDue)

	// the earlier check starts once it falls due, saturating the single concurrency slot
	now = epoch.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)
	require.True(t, key.Equal(tiny.Key(4), state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node]).NodeID.Key()))

	// the deadline of the in flight check is due before the second node's scheduled
	// check, because timing it out is what frees capacity to start that check
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingAtCapacity{}, state)
	require.Equal(t, now.Add(cfg.Timeout), state.(*StateProbeWaitingAtCapacity).NextDue)
}

// TestProbeReportsNextDueWithSpareCapacity checks that the probe state machine reports the
// deadline of an in flight check while it still has capacity to start further checks.
func TestProbeReportsNextDueWithSpareCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute
	cfg.Timeout = time.Minute
	cfg.Concurrency = 2

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))
	rt.AddNode(tiny.NewNode(5))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{NodeID: tiny.NewNode(4)})
	require.IsType(t, &StateProbeIdle{}, state)

	now = epoch.Add(5 * time.Minute)
	state = sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{NodeID: tiny.NewNode(5)})
	require.IsType(t, &StateProbeIdle{}, state)

	// the first node's check starts, leaving one slot free and the second node not yet due
	now = epoch.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeWaitingWithCapacity{}, state)
	require.Equal(t, now.Add(cfg.Timeout), state.(*StateProbeWaitingWithCapacity).NextDue)
}

// TestProbeConnectivityCheckDropped checks that a connectivity check which was never sent
// leaves the node in the routing table and is not left open to time out.
func TestProbeConnectivityCheckDropped(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultProbeConfig()
	cfg.CheckInterval = 10 * time.Minute

	rt, err := triert.New[tiny.Key, tiny.Node](tiny.NewNode(128), nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	sm, err := NewProbe[tiny.Key, tiny.Node](rt, cfg)
	require.NoError(t, err)

	state := sm.Advance(ctx, now, &EventProbeAdd[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(4),
	})
	require.IsType(t, &StateProbeIdle{}, state)

	now = now.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	st := state.(*StateProbeConnectivityCheck[tiny.Key, tiny.Node])

	state = sm.Advance(ctx, now, &EventProbeConnectivityCheckDropped[tiny.Key, tiny.Node]{
		NodeID: st.NodeID,
	})
	require.IsType(t, &StateProbeIdle{}, state)

	// the node keeps its place in the routing table
	_, found := rt.GetNode(tiny.Key(4))
	require.True(t, found)

	// the check that was not sent is not left to time out
	now = now.Add(cfg.Timeout * 2)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeIdle{}, state)

	_, found = rt.GetNode(tiny.Key(4))
	require.True(t, found)

	// the node is checked again once its next check falls due
	now = now.Add(cfg.CheckInterval)
	state = sm.Advance(ctx, now, &EventProbePoll{})
	require.IsType(t, &StateProbeConnectivityCheck[tiny.Key, tiny.Node]{}, state)
}
