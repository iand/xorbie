package xorbie

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/keystore"
	"github.com/iand/xorbie/netsize"
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

	t.Run("replication factor greater than zero", func(t *testing.T) {
		cfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		cfg.ReplicationFactor = 0
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
		c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
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
		c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]())
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

// TestQueryClosestWithNilQueryFunc checks that a caller wanting only the closest nodes may omit
// the query function.
func TestQueryClosestWithNilQueryFunc(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		self := nodes[0].NodeID
		c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		closest, stats, err := c.QueryClosest(ctx, nodes[3].NodeID.Key(), nil, 20)
		require.NoError(t, err)
		require.True(t, stats.Exhausted)
		require.NotEmpty(t, closest)
	})
}

// TestQueryMessageWithNilQueryFunc checks that a caller wanting only the closest nodes may omit
// the query function.
func TestQueryMessageWithNilQueryFunc(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		self := nodes[0].NodeID
		c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		msg := tiny.Message{Content: "find this", TargetKey: nodes[3].NodeID.Key()}
		closest, stats, err := c.QueryMessage(ctx, msg, nil, 20)
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
		c, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
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
		d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
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
		d, err := NewCoordinator(self, nodes[0].Router, nodes[0].RoutingTable, ccfg)
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

		c, err := NewCoordinator(nodes[0].NodeID, rtr, nodes[0].RoutingTable, ccfg)
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
		ccfg.Routing.EnableExplore = true
		ccfg.Routing.ExploreCplFunc = tiny.NodeWithCpl
		ccfg.Routing.ExploreInterval = 2 * time.Second

		// a cpl must fall inside the key, and a tiny key is 8 bits wide
		ccfg.Routing.ExploreMaximumCpl = tiny.Key(0).BitLen() - 1

		rtr := &silentRouter{}

		c, err := NewCoordinator(nodes[0].NodeID, rtr, nodes[0].RoutingTable, ccfg)
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

		c, err := NewCoordinator(nodes[0].NodeID, rtr, nodes[0].RoutingTable, ccfg)
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
		rt, err := triert.New(nodes[0].NodeID, nil)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.Routing.BootstrapPeers = []tiny.Node{nodes[1].NodeID}

		c, err := NewCoordinator(nodes[0].NodeID, nodes[0].Router, rt, ccfg)
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

		rt, err := triert.New(nodes[0].NodeID, nil)
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

		c, err := NewCoordinator(nodes[0].NodeID, rtr, rt, ccfg)
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

// TestCoordinatorPublishOptimisticNeedsAnEstimate checks that an optimistic publish started
// before the coordinator can estimate the size of the network is refused, and refused in a way
// the caller can tell apart from any other failure so that it can fall back to another strategy.
func TestCoordinatorPublishOptimisticNeedsAnEstimate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()

		c, err := NewCoordinator(nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		// nothing has been looked up, so the estimator has nothing to work from
		_, err = c.NetworkSize()
		require.ErrorIs(t, err, netsize.ErrNotEnoughData)

		msg := tiny.Message{Content: "store this", TargetKey: nodes[3].NodeID.Key()}
		_, err = c.PublishOptimistic(ctx, msg)
		require.ErrorIs(t, err, netsize.ErrNotEnoughData)
	})
}

// TestCoordinatorPublishOptimisticOnceAnEstimateExists checks that an optimistic publish runs
// once the coordinator has an estimate to work from, and that it takes its replication factor and
// certainties from the coordinator's configuration rather than from the caller.
func TestCoordinatorPublishOptimisticOnceAnEstimateExists(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.ReplicationFactor = 2

		c, err := NewCoordinator(nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		// each rank needs more than one observation before the estimator will report anything,
		// so several lookups are run to warm it
		qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
			return nil
		}
		for _, n := range nodes[1:] {
			_, _, err = c.QueryClosest(ctx, n.NodeID.Key(), qfn, 20)
			require.NoError(t, err)
		}

		_, err = c.NetworkSize()
		require.NoError(t, err)

		msg := tiny.Message{Content: "store this", TargetKey: nodes[3].NodeID.Key()}
		stats, err := c.PublishOptimistic(ctx, msg)
		require.NoError(t, err)

		require.Positive(t, stats.StoreRequests)
		require.Equal(t, stats.StoreRequests, stats.StoreSuccess+stats.StoreFailure)
		require.False(t, stats.Start.IsZero())
		require.False(t, stats.End.Before(stats.Start))
	})
}

// TestCoordinatorPublishFollowUpReportsBothPhases checks that a follow up publish reports the
// lookup that found the nodes as well as the stores that followed it.
func TestCoordinatorPublishFollowUpReportsBothPhases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		ccfg.ReplicationFactor = 2

		c, err := NewCoordinator(nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, ccfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		msg := tiny.Message{Content: "store this", TargetKey: nodes[3].NodeID.Key()}
		stats, err := c.PublishFollowUp(ctx, msg)
		require.NoError(t, err)

		// the lookup runs to completion before any store, so both phases have counts
		require.Positive(t, stats.QueryRequests)
		require.Equal(t, stats.QueryRequests, stats.QuerySuccess+stats.QueryFailure)
		require.Positive(t, stats.StoreRequests)
		require.Equal(t, stats.StoreRequests, stats.StoreSuccess+stats.StoreFailure)
	})
}

// TestCoordinatorPublishStaticReportsNoLookup checks that a static publish, which runs no
// lookup, reports zero for the query counts rather than leaving them to be guessed at.
func TestCoordinatorPublishStaticReportsNoLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		c, err := NewCoordinator(nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })

		msg := tiny.Message{Content: "store this", TargetKey: nodes[3].NodeID.Key()}
		stats, err := c.PublishStatic(ctx, msg, []tiny.Node{nodes[1].NodeID, nodes[2].NodeID})
		require.NoError(t, err)

		require.Zero(t, stats.QueryRequests)
		require.Zero(t, stats.QuerySuccess)
		require.Zero(t, stats.QueryFailure)
		require.Equal(t, 2, stats.StoreRequests)
		require.Equal(t, 2, stats.StoreSuccess)
	})
}

// regionRecordContent tags the messages a region publish sends, so the recording router can pick
// them out from the survey's find-closer traffic.
const regionRecordContent = "region-record"

// regionTarget mints a survey target for a region by placing its prefix bits in the high bits of a
// tiny key.
func regionTarget(region bitstr.Key) (tiny.Key, error) {
	var v uint8
	for i := range len(region) {
		v <<= 1
		if region.Bit(i) == 1 {
			v |= 1
		}
	}
	v <<= (8 - len(region))
	return tiny.Key(v), nil
}

// recordingRouter wraps a router and records the store messages sent through it.
type recordingRouter struct {
	inner  coordt.Router[tiny.Key, tiny.Node, tiny.Message]
	mu     sync.Mutex
	stores []regionStore
}

type regionStore struct {
	key tiny.Key
	to  tiny.Node
}

var _ coordt.Router[tiny.Key, tiny.Node, tiny.Message] = (*recordingRouter)(nil)

func (r *recordingRouter) SendMessage(ctx context.Context, to tiny.Node, req tiny.Message) (tiny.Message, error) {
	if req.Content == regionRecordContent {
		r.mu.Lock()
		r.stores = append(r.stores, regionStore{key: req.TargetKey, to: to})
		r.mu.Unlock()
	}
	return r.inner.SendMessage(ctx, to, req)
}

func (r *recordingRouter) GetClosestNodes(ctx context.Context, to tiny.Node, target tiny.Key) ([]tiny.Node, error) {
	return r.inner.GetClosestNodes(ctx, to, target)
}

func (r *recordingRouter) storedWith() map[tiny.Key][]tiny.Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[tiny.Key][]tiny.Node{}
	for _, s := range r.stores {
		out[s.key] = append(out[s.key], s.to)
	}
	return out
}

// TestCoordinatorRegionSurveyTriggersPublish drives the region publishing path end to end.
func TestCoordinatorRegionSurveyTriggersPublish(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// seed the local routing table with the other nodes so the survey has somewhere to walk from
	for _, n := range nodes[1:] {
		nodes[0].RoutingTable.AddNode(n.NodeID)
	}

	ks := keystore.New[tiny.Key]()
	keys := []tiny.Key{0b0000_0001, 0b0100_0000, 0b1000_0001}
	for _, k := range keys {
		ks.Add(k)
	}

	rtr := &recordingRouter{inner: nodes[0].Router}

	ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
	ccfg.ReplicationFactor = 2
	ccfg.Publish.EnableSurvey = true
	ccfg.Publish.SurveyTargetFunc = regionTarget
	ccfg.Publish.SurveyInitialPrefixLen = 0
	ccfg.Publish.Keystore = ks
	ccfg.Publish.RecordSource = func(k tiny.Key) tiny.Message {
		return tiny.Message{Content: regionRecordContent, TargetKey: k}
	}

	c, err := NewCoordinator(self, rtr, nodes[0].RoutingTable, ccfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	// the survey of the single region finishes and its region publish stores every provided key
	require.Eventually(t, func() bool {
		stored := rtr.storedWith()
		for _, k := range keys {
			if len(stored[k]) == 0 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "every provided key should be stored")

	// the surveyed region holds the other nodes, so every store lands on one of them
	members := map[string]bool{}
	for _, n := range nodes[1:] {
		members[n.NodeID.String()] = true
	}

	// every node a key was stored with belongs to the surveyed region and is used only once per key
	for k, tos := range rtr.storedWith() {
		require.LessOrEqual(t, len(tos), ccfg.ReplicationFactor, "a key is stored with at most the replication factor of nodes")
		seen := map[string]bool{}
		for _, to := range tos {
			require.True(t, members[to.String()], "a stored node must belong to the surveyed region")
			require.False(t, seen[to.String()], "a key must not be stored with the same node twice")
			seen[to.String()] = true
		}
		require.Contains(t, keys, k, "only provided keys are stored")
	}
}
