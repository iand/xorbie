package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

// emptyRoutingTable returns a routing table that contains no nodes.
func emptyRoutingTable(t *testing.T, self tiny.Node) kad.RoutingTable[tiny.Key, tiny.Node] {
	t.Helper()
	rt, err := triert.New(self, nil)
	require.NoError(t, err)
	return rt
}

func TestBootstrapConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("tracer is not nil", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter is not nil", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Meter = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultBootstrapConfig()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

func TestBootstrapStartsIdle(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)
}

func TestBootstrapStart(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4

	// start the bootstrap
	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])

	// the query should be the one just added
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)

	// the query should attempt to contact the node it was given
	require.Equal(t, a, st.NodeID)

	// with the correct key
	require.True(t, key.Equal(self.Key(), st.Target))

	// now the bootstrap reports that it is waiting
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)
}

func TestBootstrapMessageResponse(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4

	// start the bootstrap
	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the bootstrap should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// bootstrap should respond that its query has finished
	require.IsType(t, &StateBootstrapFinished[tiny.Key, tiny.Node]{}, state)

	stf := state.(*StateBootstrapFinished[tiny.Key, tiny.Node])
	require.Equal(t, 1, stf.Stats.Requests)
	require.Equal(t, 1, stf.Stats.Success)
}

func TestBootstrapProgress(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.RequestConcurrency = 3 // 1 less than the 4 nodes to be visited

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00001000) // 8
	c := tiny.NewNode(0b00010000) // 16
	d := tiny.NewNode(0b00100000) // 32

	// ensure the order of the known nodes
	require.True(t, self.Key().Xor(a.Key()).Compare(self.Key().Xor(b.Key())) == -1)
	require.True(t, self.Key().Xor(b.Key()).Compare(self.Key().Xor(c.Key())) == -1)
	require.True(t, self.Key().Xor(c.Key()).Compare(self.Key().Xor(d.Key())) == -1)

	// start the bootstrap
	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{d, a, b, c},
	})

	// the bootstrap should attempt to contact the closest node it was given
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// next the bootstrap attempts to contact second nearest node
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, b, st.NodeID)

	// next the bootstrap attempts to contact third nearest node
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, c, st.NodeID)

	// now the bootstrap should be waiting since it is at request capacity
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// now the bootstrap has capacity to contact fourth nearest node
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, d, st.NodeID)

	// notify bootstrap that a node was contacted successfully
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: b,
	})

	// bootstrap should respond that it is waiting for messages
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that a node was contacted successfully
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: c,
	})

	// bootstrap should respond that it is waiting for last message
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// notify bootstrap that the final node was contacted successfully
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: d,
	})

	// bootstrap should respond that its query has finished
	require.IsType(t, &StateBootstrapFinished[tiny.Key, tiny.Node]{}, state)

	stf := state.(*StateBootstrapFinished[tiny.Key, tiny.Node])
	require.Equal(t, 4, stf.Stats.Requests)
	require.Equal(t, 4, stf.Stats.Success)
}

func TestBootstrapFinishesThenGoesIdle(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4

	// start the bootstrap
	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the bootstrap should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, a, st.NodeID)

	// notify bootstrap that node was contacted successfully, but no closer nodes
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// bootstrap should respond that its query has finished
	require.IsType(t, &StateBootstrapFinished[tiny.Key, tiny.Node]{}, state)

	// poll bootstrap
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})

	// bootstrap should now be idle
	require.IsType(t, &StateBootstrapIdle{}, state)
}

func TestBootstrapFinishedIgnoresLaterResponses(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(4)
	b := tiny.NewNode(8)

	// start the bootstrap
	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{b},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the bootstrap should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, b, st.NodeID)

	// notify bootstrap that node was contacted successfully with a closer node
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      b,
		CloserNodes: []tiny.Node{a},
	})

	// bootstrap should respond that it wants to contact the new node
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// poll bootstrap
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})

	// bootstrap should now be waiting
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// advance the clock past the timeout
	now = now.Add(cfg.RequestTimeout * 2)

	// poll bootstrap
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})

	// bootstrap should now be finished
	require.IsType(t, &StateBootstrapFinished[tiny.Key, tiny.Node]{}, state)

	// notify bootstrap that node was contacted successfully after the timeout
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// bootstrap should ignore late message and now be idle
	require.IsType(t, &StateBootstrapIdle{}, state)
}

func TestBootstrapFinishedIgnoresLaterFailures(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(4)
	b := tiny.NewNode(8)

	// start the bootstrap
	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{b},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the bootstrap should attempt to contact the node it was given
	st := state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, coordt.QueryID("bootstrap"), st.QueryID)
	require.Equal(t, b, st.NodeID)

	// notify bootstrap that node was contacted successfully with a closer node
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      b,
		CloserNodes: []tiny.Node{a},
	})

	// bootstrap should respond that it wants to contact the new node
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// poll bootstrap
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})

	// bootstrap should now be waiting
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// advance the clock past the timeout
	now = now.Add(cfg.RequestTimeout * 2)

	// poll bootstrap
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})

	// bootstrap should now be finished
	require.IsType(t, &StateBootstrapFinished[tiny.Key, tiny.Node]{}, state)

	// notify bootstrap that node failed to be contacted
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserFailure[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// bootstrap should ignore late message and now be idle
	require.IsType(t, &StateBootstrapIdle{}, state)
}

func TestBootstrapTimeout(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.Timeout = 3 * time.Minute

	// the request must outlive the bootstrap so its query is still waiting for a
	// response when the bootstrap deadline passes
	cfg.RequestTimeout = time.Hour

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(0b00000100) // 4

	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the node never responds, but the bootstrap has not run out of time yet
	now = now.Add(cfg.Timeout - time.Second)
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// once the deadline passes the bootstrap gives up
	now = now.Add(2 * time.Second)
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapTimeout{}, state)
}

func TestBootstrapReportsNextDue(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.Timeout = 3 * time.Minute
	cfg.RequestTimeout = time.Minute

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	// nothing running, nothing due
	state := bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)

	state = bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{tiny.NewNode(0b00000100)},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the request deadline falls before the bootstrap deadline, so it is reported
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)
	require.Equal(t, now.Add(cfg.RequestTimeout), state.(*StateBootstrapWaiting).NextDue)
}

func TestBootstrapReportsOwnDeadlineWhenSooner(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.Timeout = time.Minute
	cfg.RequestTimeout = time.Hour

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{tiny.NewNode(0b00000100)},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the bootstrap gives up before the request does, so its own deadline wins
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)
	require.Equal(t, now.Add(cfg.Timeout), state.(*StateBootstrapWaiting).NextDue)
}

// TestBootstrapTimeoutReleasesQuery checks that a bootstrap which passes its own deadline
// gives up the query it was running, so that it returns to idle and a later start begins
// a fresh bootstrap.
func TestBootstrapTimeoutReleasesQuery(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()

	// the bootstrap must give up before the request it is waiting on does, otherwise the
	// query ends by running out of nodes rather than out of time
	cfg.Timeout = time.Minute
	cfg.RequestTimeout = time.Hour

	self := tiny.NewNode(0)
	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), nil, cfg)
	require.NoError(t, err)

	a := tiny.NewNode(4)
	b := tiny.NewNode(8)

	state := bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{b},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapWaiting{}, state)

	// advance past the bootstrap deadline but not past the request deadline
	now = now.Add(2 * cfg.Timeout)

	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapTimeout{}, state)

	// the timed out query has been released, so the bootstrap is no longer running
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)

	// a later start begins a fresh bootstrap rather than resuming the timed out one
	state = bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: []tiny.Node{a},
	})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	require.Equal(t, a, state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node]).NodeID)
}

// populatedRoutingTable returns a routing table containing n nodes.
func populatedRoutingTable(t *testing.T, self tiny.Node, n int) kad.RoutingTable[tiny.Key, tiny.Node] {
	t.Helper()
	rt, err := triert.New(self, nil)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		require.True(t, rt.AddNode(tiny.NewNode(tiny.Key(i))))
	}
	return rt
}

// TestBootstrapStartsWhenPopulationLow checks that a bootstrap starts itself when the
// routing table holds fewer nodes than its minimum population.
func TestBootstrapStartsWhenPopulationLow(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.MinimumPopulation = 2

	self := tiny.NewNode(0)
	seed := tiny.NewNode(4)

	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), []tiny.Node{seed}, cfg)
	require.NoError(t, err)

	// no start event is sent, the empty routing table is the only prompt
	state := bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	require.Equal(t, seed, state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node]).NodeID)
}

// TestBootstrapDoesNotStartWhenPopulated checks that a bootstrap leaves a routing table
// alone while it holds at least its minimum population, and reports no due time for it.
func TestBootstrapDoesNotStartWhenPopulated(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.MinimumPopulation = 2

	self := tiny.NewNode(0)
	seed := tiny.NewNode(4)

	bs, err := NewBootstrap(self, populatedRoutingTable(t, self, 2), []tiny.Node{seed}, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)
	require.True(t, state.(*StateBootstrapIdle).NextDue.IsZero())
}

// TestBootstrapWaitsRetryIntervalBeforeStartingAgain checks that a bootstrap which ends
// with the routing table still short of nodes waits out its retry interval before trying
// again, and reports when that will be.
func TestBootstrapWaitsRetryIntervalBeforeStartingAgain(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.MinimumPopulation = 2
	cfg.RetryInterval = 10 * time.Minute

	self := tiny.NewNode(0)
	seed := tiny.NewNode(4)

	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), []tiny.Node{seed}, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)

	// the only seed fails, so the bootstrap runs out of nodes and ends
	state = bs.Advance(ctx, now, &EventBootstrapFindCloserFailure[tiny.Key, tiny.Node]{
		NodeID: seed,
		Error:  errors.New("failed"),
	})
	require.IsType(t, &StateBootstrapFinished[tiny.Key, tiny.Node]{}, state)

	// the table is still empty but the retry interval has not elapsed
	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)
	require.Equal(t, now.Add(cfg.RetryInterval), state.(*StateBootstrapIdle).NextDue)

	now = now.Add(cfg.RetryInterval)

	state = bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
}

// TestBootstrapZeroMinimumPopulationDisablesAutomaticStart checks that a zero minimum
// population leaves the bootstrap waiting to be asked, and that being asked without seeds
// uses the configured ones.
func TestBootstrapZeroMinimumPopulationDisablesAutomaticStart(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultBootstrapConfig()
	cfg.MinimumPopulation = 0

	self := tiny.NewNode(0)
	seed := tiny.NewNode(4)

	bs, err := NewBootstrap(self, emptyRoutingTable(t, self), []tiny.Node{seed}, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventBootstrapPoll{})
	require.IsType(t, &StateBootstrapIdle{}, state)
	require.True(t, state.(*StateBootstrapIdle).NextDue.IsZero())

	state = bs.Advance(ctx, now, &EventBootstrapStart[tiny.Key, tiny.Node]{})
	require.IsType(t, &StateBootstrapFindCloser[tiny.Key, tiny.Node]{}, state)
	require.Equal(t, seed, state.(*StateBootstrapFindCloser[tiny.Key, tiny.Node]).NodeID)
}
