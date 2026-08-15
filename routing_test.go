package xorbie

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/routing"
)

// idleBootstrap returns a bootstrap state machine that is always idle
func idleBootstrap() *RecordingSM[routing.BootstrapEvent, routing.BootstrapState] {
	return NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})
}

// idleInclude returns an include state machine that is always idle
func idleInclude() *RecordingSM[routing.IncludeEvent, routing.IncludeState] {
	return NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})
}

// idleProbe returns a probe state machine that is always idle
func idleProbe() *RecordingSM[routing.ProbeEvent, routing.ProbeState] {
	return NewRecordingSM[routing.ProbeEvent, routing.ProbeState](&routing.StateProbeIdle{})
}

// idleExplore returns an explore state machine that is always idle
func idleExplore() *RecordingSM[routing.ExploreEvent, routing.ExploreState] {
	return NewRecordingSM[routing.ExploreEvent, routing.ExploreState](&routing.StateExploreIdle{})
}

func TestRoutingConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		require.NoError(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer not nil", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter is not nil", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.Meter = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("bootstrap timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.BootstrapTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.BootstrapTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("bootstrap request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.BootstrapRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.BootstrapRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("bootstrap request timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.BootstrapRequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.BootstrapRequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("connectivity check timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.ConnectivityCheckTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ConnectivityCheckTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("probe request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ProbeRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.ProbeRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("probe check interval positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.ProbeCheckInterval = 0
		require.Error(t, cfg.Validate())
		cfg.ProbeCheckInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("include request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.IncludeRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.IncludeRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("include queue capacity positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.IncludeQueueCapacity = 0
		require.Error(t, cfg.Validate())
		cfg.IncludeQueueCapacity = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore request timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreRequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreRequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore maximum cpl positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreMaximumCpl = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreMaximumCpl = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore maximum 15 or less", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreMaximumCpl = 16
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreInterval = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval multiplier at least 1", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreIntervalMultiplier = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalMultiplier = 0.9
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalMultiplier = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval between 0 and 0.05", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		cfg.ExploreIntervalJitter = 0.1
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalJitter = 0.05001
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalJitter = -0.1
		require.Error(t, cfg.Validate())
	})
}

func TestRoutingStartBootstrapSendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventStartBootstrap[tiny.Key, tiny.Node]{
		SeedNodes: []tiny.Node{nodes[1].NodeID},
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// the event that should be passed to the bootstrap state machine
	expected := &routing.EventBootstrapStart[tiny.Key, tiny.Node]{
		KnownClosestNodes: ev.SeedNodes,
	}
	require.Equal(t, expected, bootstrap.first())
}

// TestRoutingBootstrapRequestConcurrency asserts that a bootstrap with more
// seeds than its request concurrency dispatches up to that concurrency, rather
// than one request at a time.
//
// The behaviour is driven exactly as [Coordinator.eventLoop] drives it, which
// is the point: the bootstrap state machine is willing to dispatch three
// requests, but it only gets the chance if the behaviour keeps signalling that
// it is ready.
func TestRoutingBootstrapRequestConcurrency(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(6)
	require.NoError(t, err)

	self := nodes[0].NodeID

	bcfg := routing.DefaultBootstrapConfig()
	bcfg.RequestConcurrency = 3

	bootstrap, err := routing.NewBootstrap[tiny.Key](self, nodes[0].RoutingTable, nil, bcfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	routingBehaviour.Notify(ctx, &EventStartBootstrap[tiny.Key, tiny.Node]{
		SeedNodes: []tiny.Node{
			nodes[1].NodeID,
			nodes[2].NodeID,
			nodes[3].NodeID,
			nodes[4].NodeID,
			nodes[5].NodeID,
		},
	})

	evs := PerformWhileReady(t, ctx, routingBehaviour)

	var requested []tiny.Node
	for _, ev := range evs {
		if oev, ok := ev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node]); ok && oev.QueryID == routing.BootstrapQueryID {
			requested = append(requested, oev.To)
		}
	}

	require.Len(t, requested, bcfg.RequestConcurrency, "expected one outbound request per unit of request concurrency, got requests to %v", requested)
}

func TestRoutingBootstrapGetClosestNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     routing.BootstrapQueryID,
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []tiny.Node{nodes[2].NodeID},
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapFindCloserResponse[tiny.Key, tiny.Node]{}, bootstrap.first())

	rev := bootstrap.first().(*routing.EventBootstrapFindCloserResponse[tiny.Key, tiny.Node])
	require.True(t, nodes[1].NodeID.Equal(rev.NodeID))
	require.Equal(t, ev.CloserNodes, rev.CloserNodes)
}

func TestRoutingBootstrapGetClosestNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to bootstrap
	bootstrap := NewRecordingSM[routing.BootstrapEvent, routing.BootstrapState](&routing.StateBootstrapIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: routing.BootstrapQueryID,
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventBootstrapFindCloserFailure[tiny.Key, tiny.Node]{}, bootstrap.first())

	rev := bootstrap.first().(*routing.EventBootstrapFindCloserFailure[tiny.Key, tiny.Node])
	require.Equal(t, nodes[1].NodeID, rev.NodeID)
	require.Equal(t, failure, rev.Error)
}

func TestRoutingAddNodeInfoSendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventAddNode[tiny.Key, tiny.Node]{
		NodeID: nodes[2].NodeID,
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// the event that should be passed to the include state machine
	expected := &routing.EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: ev.NodeID,
	}
	require.Equal(t, expected, include.first())
}

func TestRoutingIncludeGetClosestNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     coordt.QueryID("include"),
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []tiny.Node{nodes[2].NodeID},
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeConnectivityCheckSuccess[tiny.Key, tiny.Node]{}, include.first())

	rev := include.first().(*routing.EventIncludeConnectivityCheckSuccess[tiny.Key, tiny.Node])
	require.Equal(t, nodes[1].NodeID, rev.NodeID)
}

func TestRoutingIncludeGetClosestNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to include
	include := NewRecordingSM[routing.IncludeEvent, routing.IncludeState](&routing.StateIncludeIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: coordt.QueryID("include"),
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// include should receive message response event
	require.IsType(t, &routing.EventIncludeConnectivityCheckFailure[tiny.Key, tiny.Node]{}, include.first())

	rev := include.first().(*routing.EventIncludeConnectivityCheckFailure[tiny.Key, tiny.Node])
	require.Equal(t, nodes[1].NodeID, rev.NodeID)
	require.Equal(t, failure, rev.Error)
}

func TestRoutingIncludedNodeAddToProbeList(t *testing.T) {
	// the test advances time to reach the probe check interval, so it runs in a
	// bubble where time.Sleep costs nothing
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		self := nodes[0].NodeID
		rt := nodes[0].RoutingTable

		includeCfg := routing.DefaultIncludeConfig()
		include, err := routing.NewInclude[tiny.Key, tiny.Node](rt, includeCfg)
		require.NoError(t, err)

		probeCfg := routing.DefaultProbeConfig()
		probeCfg.CheckInterval = 5 * time.Minute
		probe, err := routing.NewProbe[tiny.Key](rt, probeCfg)
		require.NoError(t, err)

		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), include, probe, idleExplore(), cfg)
		require.NoError(t, err)

		// a new node to be included
		candidate := nodes[len(nodes)-1].NodeID

		// the routing table should not contain the node yet
		_, intable := rt.GetNode(candidate.Key())
		require.False(t, intable)

		// notify that there is a new node to be included
		routingBehaviour.Notify(ctx, &EventAddNode[tiny.Key, tiny.Node]{
			NodeID: candidate,
		})

		// collect the result of the notify
		dev, ok := routingBehaviour.Perform(ctx)
		require.True(t, ok)

		// include should be asking to send a message to the node
		require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, dev)

		oev := dev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])

		// advance time a little
		time.Sleep(time.Second)

		// notify a successful response back (best to use the notify included in the event even though it will be the behaviour's Notify method)
		oev.Notify.Notify(ctx, &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
			QueryID:     oev.QueryID,
			To:          oev.To,
			Target:      oev.Target,
			CloserNodes: []tiny.Node{nodes[1].NodeID}, // must include one for include check to pass
		})
		dev, ok = routingBehaviour.Perform(ctx)

		// the routing table should now contain the node
		_, intable = rt.GetNode(candidate.Key())
		require.True(t, intable)

		// routing update event should be emitted from the include state machine
		require.True(t, ok)
		require.IsType(t, &EventRoutingUpdated[tiny.Key, tiny.Node]{}, dev)

		// drain any pending work
		DrainBehaviour[BehaviourEvent, BehaviourEvent](t, ctx, routingBehaviour)

		// advance time past the probe check interval
		time.Sleep(probeCfg.CheckInterval)

		// probe should be sent for the node
		dev, ok = routingBehaviour.Perform(ctx)
		require.True(t, ok)
		require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, dev)

		// confirm that the message is for the correct node
		oev = dev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
		require.Equal(t, coordt.QueryID("probe"), oev.QueryID)
		require.Equal(t, candidate, oev.To)
	})
}

func TestRoutingExploreSendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	exploreCfg := routing.DefaultExploreConfig()

	// a cpl must fall inside the key, and a tiny key is 8 bits wide
	maxCpl := tiny.Key(0).BitLen() - 1

	// make sure the explore starts as soon as the explore state machine is polled
	schedule := routing.NewNoWaitExploreSchedule(maxCpl)

	explore, err := routing.NewExplore[tiny.Key](self, rt, tiny.NodeWithCpl, schedule, exploreCfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	routingBehaviour.Notify(ctx, &EventRoutingPoll{})

	// collect the result of the notify
	dev, ok := routingBehaviour.Perform(ctx)
	require.True(t, ok)

	// include should be asking to send a message to the node
	require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, dev)
	gcl := dev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])

	require.Equal(t, routing.ExploreQueryID, gcl.QueryID)

	// the message should be looking for nodes closer to a key that occupies the maximum cpl
	require.Equal(t, maxCpl, self.Key().CommonPrefixLength(gcl.Target))
}

func TestRoutingExploreGetClosestNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to explore
	explore := NewRecordingSM[routing.ExploreEvent, routing.ExploreState](&routing.StateExploreIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     routing.ExploreQueryID,
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []tiny.Node{nodes[2].NodeID},
	}
	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// explore should receive message response event
	require.IsType(t, &routing.EventExploreFindCloserResponse[tiny.Key, tiny.Node]{}, explore.first())

	rev := explore.first().(*routing.EventExploreFindCloserResponse[tiny.Key, tiny.Node])
	require.True(t, nodes[1].NodeID.Equal(rev.NodeID))
	require.Equal(t, ev.CloserNodes, rev.CloserNodes)
}

func TestRoutingExploreGetClosestNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to explore
	explore := NewRecordingSM[routing.ExploreEvent, routing.ExploreState](&routing.StateExploreIdle{})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: routing.ExploreQueryID,
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// bootstrap should receive message response event
	require.IsType(t, &routing.EventExploreFindCloserFailure[tiny.Key, tiny.Node]{}, explore.first())

	rev := explore.first().(*routing.EventExploreFindCloserFailure[tiny.Key, tiny.Node])
	require.Equal(t, nodes[1].NodeID, rev.NodeID)
	require.Equal(t, failure, rev.Error)
}

// TestRoutingProbeKeepsNodeWhenCheckDropped checks that a connectivity check the network
// behaviour had no capacity for leaves the node in the routing table.
func TestRoutingProbeKeepsNodeWhenCheckDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := kadtest.CtxBubble(t)

		_, nodes, err := linearTopology(4)
		require.NoError(t, err)

		self := nodes[0].NodeID
		rt := nodes[0].RoutingTable

		probeCfg := routing.DefaultProbeConfig()

		// the check has to fall due inside the test's own deadline
		probeCfg.CheckInterval = 2 * time.Second

		probe, err := routing.NewProbe[tiny.Key](rt, probeCfg)
		require.NoError(t, err)

		routingBehaviour, err := ComposeRoutingBehaviour[tiny.Key, tiny.Node](self, idleBootstrap(), idleInclude(), probe, idleExplore(), DefaultRoutingConfig[tiny.Key, tiny.Node]())
		require.NoError(t, err)

		// the linear topology puts the second node in the first node's routing table
		checked := nodes[1].NodeID
		routingBehaviour.Notify(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{NodeID: checked})
		DrainBehaviour(t, ctx, routingBehaviour)

		// advance past the check interval so a connectivity check falls due
		time.Sleep(probeCfg.CheckInterval)

		dev, ok := routingBehaviour.Perform(ctx)
		require.True(t, ok)
		require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, dev)

		oev := dev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
		require.Equal(t, ProbeQueryID, oev.QueryID)
		require.Equal(t, checked, oev.To)

		oev.Notify.Notify(ctx, &EventGetCloserNodesFailure[tiny.Key, tiny.Node]{
			QueryID: oev.QueryID,
			To:      oev.To,
			Target:  oev.Target,
			Err:     ErrRequestDropped,
		})
		DrainBehaviour(t, ctx, routingBehaviour)

		_, found := rt.GetNode(checked.Key())
		require.True(t, found, "node was removed from the routing table")
	})
}
