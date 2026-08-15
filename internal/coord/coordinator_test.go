package coord

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/internal/coord/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
)

func TestConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		require.NoError(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter provider not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		cfg.MeterProvider = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer provider not nil", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		cfg.TracerProvider = nil
		require.Error(t, cfg.Validate())
	})
}

func TestExhaustiveQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		// A (ids[0]) is looking for D (ids[3])
		// A will first ask B, B will reply with C's address (and A's address)
		// A will then ask C, C will reply with D's address (and B's address)
		self := nodes[0].NodeID
		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](self, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		target := nodes[3].NodeID.Key()

		visited := make(map[string]int)

		// Record the nodes as they are visited
		qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
			visited[id.String()]++
			return nil
		}

		// Run a query to find the value
		_, _, err = c.QueryClosest(ctx, target, qfn, 20)
		require.NoError(t, err)

		require.Equal(t, 3, len(visited))
		require.Contains(t, visited, nodes[1].NodeID.String())
		require.Contains(t, visited, nodes[2].NodeID.String())
		require.Contains(t, visited, nodes[3].NodeID.String())
	})
}

// TestQueryReturnsClosestNodes checks that a query which runs to exhaustion hands its
// caller the closest nodes it found. The notifier closes the progress channel before it
// sends the terminal event, so a caller selecting on both can be woken by the close and
// return before the terminal event arrives.
func TestQueryReturnsClosestNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		self := nodes[0].NodeID
		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](self, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
			return nil
		}

		closest, stats, err := c.QueryClosest(ctx, nodes[3].NodeID.Key(), qfn, 20)
		require.NoError(t, err)
		require.True(t, stats.Exhausted)
		require.NotEmpty(t, closest)
	})
}

func TestRoutingUpdatedEventEmittedForCloserNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		// A (ids[0]) is looking for D (ids[3])
		// A will first ask B, B will reply with C's address (and A's address)
		// A will then ask C, C will reply with D's address (and B's address)
		self := nodes[0].NodeID
		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](self, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		rn := NewBufferedRoutingNotifier[tiny.Key, tiny.Node]()
		c.SetRoutingNotifier(rn)

		qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
			return nil
		}

		// Run a query to find the value
		target := nodes[3].NodeID.Key()
		_, _, err = c.QueryClosest(ctx, target, qfn, 20)
		require.NoError(t, err)

		// the query run by the dht should have received a response from nodes[1] with closer nodes
		// nodes[0] and nodes[2] which should trigger a routing table update since nodes[2] was
		// not in the dht's routing table.
		// the query then continues and should have received a response from nodes[2] with closer nodes
		// nodes[1] and nodes[3] which should trigger a routing table update since nodes[3] was
		// not in the dht's routing table.

		// no EventRoutingUpdated is sent for the self node

		// wait for the coordinator to finish dispatching the events emitted by the query
		synctest.Wait()

		// the order in which these events are emitted may vary depending on the
		// order in which the coordinator drained its behaviours
		ev1, err := rn.Expect(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{})
		require.NoError(t, err)
		tev1 := ev1.(*EventRoutingUpdated[tiny.Key, tiny.Node])

		ev2, err := rn.Expect(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{})
		require.NoError(t, err)
		tev2 := ev2.(*EventRoutingUpdated[tiny.Key, tiny.Node])

		if tev1.NodeID.Equal(nodes[2].NodeID) {
			require.Equal(t, nodes[3].NodeID, tev2.NodeID)
		} else if tev2.NodeID.Equal(nodes[2].NodeID) {
			require.Equal(t, nodes[3].NodeID, tev1.NodeID)
		} else {
			require.Failf(t, "did not see routing updated event for %s", nodes[2].NodeID.String())
		}
	})
}

func TestBootstrap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.Routing.BootstrapPeers = []tiny.Node{nodes[1].NodeID}

		self := nodes[0].NodeID
		d, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](self, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })

		rn := NewBufferedRoutingNotifier[tiny.Key, tiny.Node]()
		d.SetRoutingNotifier(rn)

		err = d.Bootstrap(ctx)
		require.NoError(t, err)

		// bootstrap runs in the background, wait for the coordinator to settle
		synctest.Wait()

		// the query run by the dht should have completed
		ev, err := rn.Expect(ctx, &EventBootstrapFinished{})
		require.NoError(t, err)

		require.IsType(t, &EventBootstrapFinished{}, ev)
		tevf := ev.(*EventBootstrapFinished)
		require.Equal(t, 3, tevf.Stats.Requests)
		require.Equal(t, 3, tevf.Stats.Success)
		require.Equal(t, 0, tevf.Stats.Failure)

		_, err = rn.Expect(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{})
		require.NoError(t, err)

		_, err = rn.Expect(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{})
		require.NoError(t, err)

		// coordinator will have node1 in its routing table
		require.True(t, d.IsRoutable(ctx, nodes[1].NodeID))

		// coordinator should now have node2 in its routing table
		require.True(t, d.IsRoutable(ctx, nodes[2].NodeID))

		// coordinator should now have node3 in its routing table
		require.True(t, d.IsRoutable(ctx, nodes[3].NodeID))
	})
}

func TestIncludeNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		candidate := nodes[len(nodes)-1].NodeID // not in nodes[0] routing table

		self := nodes[0].NodeID
		d, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](self, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })

		rn := NewBufferedRoutingNotifier[tiny.Key, tiny.Node]()
		d.SetRoutingNotifier(rn)

		// the routing table should not contain the node yet
		require.False(t, d.IsRoutable(ctx, candidate))

		// inject a new node
		err = d.AddNodes(ctx, []tiny.Node{candidate})
		require.NoError(t, err)

		// the include state machine runs in the background, wait for it to finish
		synctest.Wait()

		ev, err := rn.Expect(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{})
		require.NoError(t, err)

		tev := ev.(*EventRoutingUpdated[tiny.Key, tiny.Node])
		require.Equal(t, candidate, tev.NodeID)

		// the routing table should now contain the node
		require.True(t, d.IsRoutable(ctx, candidate))
	})
}

// silentRouter accepts requests but never answers them, blocking each caller until the
// request context is cancelled. It records the peers it was asked to contact.
type silentRouter struct {
	mu        sync.Mutex
	contacted []tiny.Node
}

var _ coordt.Router[tiny.Key, tiny.Node, tiny.Message] = (*silentRouter)(nil)

func (r *silentRouter) SendMessage(ctx context.Context, to tiny.Node, req tiny.Message) (tiny.Message, error) {
	r.record(to)
	<-ctx.Done()
	return tiny.Message{}, ctx.Err()
}

func (r *silentRouter) GetClosestNodes(ctx context.Context, to tiny.Node, target tiny.Key) ([]tiny.Node, error) {
	r.record(to)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *silentRouter) record(to tiny.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contacted = append(r.contacted, to)
}

func (r *silentRouter) contacts() []tiny.Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.contacted)
}

// TestCoordinatorTimesOutIdleQuery checks that a query whose only node never answers is
// ended by its request timeout rather than left waiting for a response.
func TestCoordinatorTimesOutIdleQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(2)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.Query.RequestTimeout = 2 * time.Second
		ccfg.Query.Timeout = 4 * time.Second

		rtr := &silentRouter{}

		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, rtr, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
			return nil
		}

		start := time.Now()
		_, _, err = c.QueryClosest(ctx, nodes[1].NodeID.Key(), qfn, 20)
		require.NoError(t, err)

		require.Equal(t, []tiny.Node{nodes[1].NodeID}, rtr.contacts())

		if elapsed := time.Since(start); elapsed < ccfg.Query.RequestTimeout {
			t.Errorf("query ended after %s, want at least %s", elapsed, ccfg.Query.RequestTimeout)
		}
	})
}

// TestCoordinatorExploresOnSchedule checks that a coordinator that is told to do nothing
// still explores its routing table when the first cpl in its schedule falls due.
func TestCoordinatorExploresOnSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, nodes, err := linearTopology(2)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.Routing.ExploreInterval = 2 * time.Second

		// a cpl must fall inside the key, and a tiny key is 8 bits wide
		ccfg.Routing.ExploreMaximumCpl = tiny.Key(0).BitLen() - 1

		rtr := &silentRouter{}

		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, rtr, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		// no query or bootstrap is started, so any request the router sees must come from
		// an explore of the routing table
		require.Empty(t, rtr.contacts())

		time.Sleep(2 * ccfg.Routing.ExploreInterval)
		synctest.Wait()

		require.Equal(t, []tiny.Node{nodes[1].NodeID}, rtr.contacts())
	})
}

// TestCoordinatorBootstrapTimesOut checks that a bootstrap whose seed node never answers
// ends with a timeout error rather than ending silently.
func TestCoordinatorBootstrapTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(2)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.Routing.BootstrapPeers = []tiny.Node{nodes[1].NodeID}

		// the bootstrap must give up before the request it is waiting on does, otherwise
		// the query ends by running out of nodes rather than out of time
		ccfg.Routing.BootstrapTimeout = 2 * time.Second
		ccfg.Routing.BootstrapRequestTimeout = time.Minute

		rtr := &silentRouter{}

		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, rtr, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		rn := NewBufferedRoutingNotifier[tiny.Key, tiny.Node]()
		c.SetRoutingNotifier(rn)

		err = c.Bootstrap(ctx)
		require.NoError(t, err)

		ev, err := rn.Expect(ctx, &EventBootstrapFinished{})
		require.NoError(t, err)
		require.ErrorIs(t, ev.(*EventBootstrapFinished).Err, coordt.ErrQueryTimeout)
	})
}

// TestCoordinatorBootstrapsWhenRoutingTableEmpty checks that a coordinator whose routing
// table holds no nodes bootstraps from its configured peers without being asked to.
func TestCoordinatorBootstrapsWhenRoutingTableEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		// a routing table of the coordinator's own, holding no nodes
		rt, err := triert.New[tiny.Key, tiny.Node](nodes[0].NodeID, nil)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.Routing.BootstrapPeers = []tiny.Node{nodes[1].NodeID}

		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, nodes[0].Router, rt, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		// nothing asks for a bootstrap, the empty routing table is the only prompt
		synctest.Wait()

		require.NotEmpty(t, rt.NearestNodes(nodes[0].NodeID.Key(), 20))
	})
}

// stallRouter answers for every peer but one, which it accepts requests for and never
// answers, blocking the caller until the request context is cancelled.
type stallRouter struct {
	coordt.Router[tiny.Key, tiny.Node, tiny.Message]
	stall tiny.Node
}

func (r *stallRouter) SendMessage(ctx context.Context, to tiny.Node, req tiny.Message) (tiny.Message, error) {
	if to.Equal(r.stall) {
		<-ctx.Done()
		return tiny.Message{}, ctx.Err()
	}
	return r.Router.SendMessage(ctx, to, req)
}

func (r *stallRouter) GetClosestNodes(ctx context.Context, to tiny.Node, target tiny.Key) ([]tiny.Node, error) {
	if to.Equal(r.stall) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.Router.GetClosestNodes(ctx, to, target)
}

// TestCoordinatorContinuesWhenPeerStalls checks that concurrent queries all finish when
// one of the peers they contact accepts requests and never answers. The requests that peer
// has no room for are dropped, so the event loop is never left waiting on it.
func TestCoordinatorContinuesWhenPeerStalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		rt, err := triert.New[tiny.Key, tiny.Node](nodes[0].NodeID, nil)
		require.NoError(t, err)
		for _, n := range nodes[1:] {
			require.True(t, rt.AddNode(n.NodeID))
		}

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		// one slot per peer, so the queries beyond the first to reach the stalled peer
		// have to be dropped rather than queued behind it
		ccfg.Network.NodeCapacity = 1

		// the query that does reach the stalled peer ends when its request runs out of
		// time, so that has to fall inside the test's own deadline
		ccfg.Query.RequestTimeout = 2 * time.Second

		rtr := &stallRouter{Router: nodes[0].Router, stall: nodes[1].NodeID}

		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, rtr, rt, tiny.NodeWithCpl, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
			return nil
		}

		var wg sync.WaitGroup
		for _, n := range nodes[1:] {
			wg.Go(func() {
				_, _, err := c.QueryClosest(ctx, n.NodeID.Key(), qfn, 20)
				require.NoError(t, err)
			})
		}
		wg.Wait()
	})
}
