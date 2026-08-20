package xorbie

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/netsize"
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

// idleSurvey returns a survey state machine that is always idle
func idleSurvey() *RecordingSM[routing.SurveyEvent, routing.SurveyState] {
	return NewRecordingSM[routing.SurveyEvent, routing.SurveyState](&routing.StateSurveyIdle{})
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
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore request concurrency positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore request timeout positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreRequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreRequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore maximum cpl positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreMaximumCpl = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreMaximumCpl = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore maximum 15 or less", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreMaximumCpl = 16
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval positive", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreInterval = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval multiplier at least 1", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreIntervalMultiplier = 0
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalMultiplier = 0.9
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalMultiplier = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore interval between 0 and 0.05", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true
		cfg.ExploreCplFunc = tiny.NodeWithCpl

		cfg.ExploreIntervalJitter = 0.1
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalJitter = 0.05001
		require.Error(t, cfg.Validate())
		cfg.ExploreIntervalJitter = -0.1
		require.Error(t, cfg.Validate())
	})

	t.Run("explore fields unchecked when explore disabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		// with the explore disabled the explore fields are not enforced
		cfg.ExploreTimeout = 0
		cfg.ExploreMaximumCpl = 0
		require.NoError(t, cfg.Validate())
	})

	t.Run("explore requires cpl function when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableExplore = true

		require.Error(t, cfg.Validate())
	})

	t.Run("survey fields unchecked when survey disabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()

		// with the survey disabled the survey fields are not enforced
		cfg.SurveyInterval = 0
		cfg.SurveyRequestConcurrency = 0
		require.NoError(t, cfg.Validate())
	})

	t.Run("survey requires target function when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableSurvey = true

		require.Error(t, cfg.Validate())
	})

	t.Run("survey interval positive when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableSurvey = true
		cfg.SurveyTargetFunc = stubTargetFn

		cfg.SurveyInterval = 0
		require.Error(t, cfg.Validate())
		cfg.SurveyInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("survey region timeout positive when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableSurvey = true
		cfg.SurveyTargetFunc = stubTargetFn

		cfg.SurveyRegionTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.SurveyRegionTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("survey request concurrency positive when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableSurvey = true
		cfg.SurveyTargetFunc = stubTargetFn

		cfg.SurveyRequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.SurveyRequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("survey request timeout positive when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableSurvey = true
		cfg.SurveyTargetFunc = stubTargetFn

		cfg.SurveyRequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.SurveyRequestTimeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("survey walk-in bound positive when enabled", func(t *testing.T) {
		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		cfg.EnableSurvey = true
		cfg.SurveyTargetFunc = stubTargetFn

		cfg.SurveyWalkInBound = 0
		require.Error(t, cfg.Validate())
		cfg.SurveyWalkInBound = -1
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), nil, cfg)
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

	bootstrap, err := routing.NewBootstrap(self, nodes[0].RoutingTable, nil, bcfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, idleProbe(), idleExplore(), nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, idleProbe(), idleExplore(), nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, idleProbe(), idleExplore(), nil, cfg)
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
		include, err := routing.NewInclude(rt, includeCfg)
		require.NoError(t, err)

		probeCfg := routing.DefaultProbeConfig()
		probeCfg.CheckInterval = 5 * time.Minute
		probe, err := routing.NewProbe(rt, probeCfg)
		require.NoError(t, err)

		cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
		routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), include, probe, idleExplore(), nil, cfg)
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
		DrainBehaviour(t, ctx, routingBehaviour)

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

	explore, err := routing.NewExplore(self, rt, tiny.NodeWithCpl, schedule, exploreCfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, nil, cfg)
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
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, nil, cfg)
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

func TestRoutingSurveyOffWhenNil(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// no survey supplied, so the behaviour must not attempt to survey any region
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), idleExplore(), nil, cfg)
	require.NoError(t, err)

	routingBehaviour.Notify(ctx, &EventRoutingPoll{})

	// polling with every child idle and no survey produces no event
	_, ok := routingBehaviour.Perform(ctx)
	require.False(t, ok)
}

func TestRoutingSurveySendsEvent(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// a survey that wants to find closer nodes for a target inside a region
	survey := NewRecordingSM[routing.SurveyEvent, routing.SurveyState](&routing.StateSurveyFindCloser[tiny.Key, tiny.Node]{
		QueryID: routing.SurveyQueryID,
		Target:  self.Key(),
		NodeID:  nodes[1].NodeID,
	})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), idleExplore(), survey, cfg)
	require.NoError(t, err)

	routingBehaviour.Notify(ctx, &EventRoutingPoll{})

	dev, ok := routingBehaviour.Perform(ctx)
	require.True(t, ok)

	// the survey should be asking to send a message to the node
	require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, dev)
	gcl := dev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
	require.Equal(t, routing.SurveyQueryID, gcl.QueryID)
	require.Equal(t, nodes[1].NodeID, gcl.To)
	require.Equal(t, self.Key(), gcl.Target)
}

func TestRoutingSurveyEmitsRegionSurveyed(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	members := []tiny.Node{nodes[1].NodeID, nodes[2].NodeID}

	// a survey that has finished surveying a region
	survey := NewRecordingSM[routing.SurveyEvent, routing.SurveyState](&routing.StateSurveyFinished[tiny.Key, tiny.Node]{
		Prefix: bitstr.Key("00"),
		Nodes:  members,
	})

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), idleExplore(), survey, cfg)
	require.NoError(t, err)

	routingBehaviour.Notify(ctx, &EventRoutingPoll{})

	dev, ok := routingBehaviour.Perform(ctx)
	require.True(t, ok)

	// a finished survey travels outwards as a region surveyed notification
	require.IsType(t, &EventRegionSurveyed[tiny.Key, tiny.Node]{}, dev)
	rs := dev.(*EventRegionSurveyed[tiny.Key, tiny.Node])
	require.Equal(t, bitstr.Key("00"), rs.Prefix)
	require.Equal(t, members, rs.Nodes)
}

func TestRoutingSurveyGetCloserNodesSuccess(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to the survey
	survey := idleSurvey()

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), idleExplore(), survey, cfg)
	require.NoError(t, err)

	ev := &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     routing.SurveyQueryID,
		To:          nodes[1].NodeID,
		Target:      nodes[0].NodeID.Key(),
		CloserNodes: []tiny.Node{nodes[2].NodeID},
	}
	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// survey should receive the message response event
	require.IsType(t, &routing.EventSurveyFindCloserResponse[tiny.Key, tiny.Node]{}, survey.first())
	rev := survey.first().(*routing.EventSurveyFindCloserResponse[tiny.Key, tiny.Node])
	require.True(t, nodes[1].NodeID.Equal(rev.NodeID))
	require.Equal(t, ev.CloserNodes, rev.CloserNodes)
}

func TestRoutingSurveyGetCloserNodesFailure(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID

	// records the event passed to the survey
	survey := idleSurvey()

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), idleExplore(), survey, cfg)
	require.NoError(t, err)

	failure := errors.New("failed")
	ev := &EventGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: routing.SurveyQueryID,
		To:      nodes[1].NodeID,
		Target:  nodes[0].NodeID.Key(),
		Err:     failure,
	}

	routingBehaviour.Notify(ctx, ev)
	routingBehaviour.Perform(ctx)

	// survey should receive the message failure event
	require.IsType(t, &routing.EventSurveyFindCloserFailure[tiny.Key, tiny.Node]{}, survey.first())
	rev := survey.first().(*routing.EventSurveyFindCloserFailure[tiny.Key, tiny.Node])
	require.Equal(t, nodes[1].NodeID, rev.NodeID)
	require.Equal(t, failure, rev.Error)
}

// stubTargetFn is a survey target function that always returns the zero key.
func stubTargetFn(bitstr.Key) (tiny.Key, error) { return tiny.Key(0), nil }

func TestNewRoutingBehaviourSurveyDisabled(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	// with no survey target function the survey is left off
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	rb, err := NewRoutingBehaviour(self, rt, cfg)
	require.NoError(t, err)
	require.Nil(t, rb.survey)
}

func TestNewRoutingBehaviourSurveyEnabled(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	// enabling the survey with a target function turns it on
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	cfg.EnableSurvey = true
	cfg.SurveyTargetFunc = stubTargetFn
	rb, err := NewRoutingBehaviour(self, rt, cfg)
	require.NoError(t, err)
	require.NotNil(t, rb.survey)
}

func TestNewRoutingBehaviourSurveyEnabledRequiresTargetFn(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	// enabling the survey without a target function is a configuration error
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	cfg.EnableSurvey = true
	_, err = NewRoutingBehaviour(self, rt, cfg)
	require.Error(t, err)
}

func TestNewRoutingBehaviourExploreDisabled(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	// with no cpl function the explore is left off
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	rb, err := NewRoutingBehaviour(self, rt, cfg)
	require.NoError(t, err)
	require.Nil(t, rb.explore)
}

func TestNewRoutingBehaviourExploreEnabled(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	// enabling the explore with a cpl function turns it on
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	cfg.EnableExplore = true
	cfg.ExploreCplFunc = tiny.NodeWithCpl
	rb, err := NewRoutingBehaviour(self, rt, cfg)
	require.NoError(t, err)
	require.NotNil(t, rb.explore)
}

func TestNewRoutingBehaviourExploreEnabledRequiresCplFn(t *testing.T) {
	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	self := nodes[0].NodeID
	rt := nodes[0].RoutingTable

	// enabling the explore without a cpl function is a configuration error
	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	cfg.EnableExplore = true
	_, err = NewRoutingBehaviour(self, rt, cfg)
	require.Error(t, err)
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

		probe, err := routing.NewProbe(rt, probeCfg)
		require.NoError(t, err)

		routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), probe, idleExplore(), nil, DefaultRoutingConfig[tiny.Key, tiny.Node]())
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

// onceExplore is an explore state machine that reports a state on its first advance and is
// idle from then on.
type onceExplore struct {
	state routing.ExploreState
	done  bool
}

func (o *onceExplore) Advance(ctx context.Context, now time.Time, ev routing.ExploreEvent) routing.ExploreState {
	if o.done {
		return &routing.StateExploreIdle{}
	}
	o.done = true
	return o.state
}

// TestRoutingBehaviourTracksExploreResults checks that the results of a completed explore
// reach the configured network size estimator.
func TestRoutingBehaviourTracksExploreResults(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	nse, err := netsize.New[tiny.Key, tiny.Node](nil)
	if err != nil {
		t.Fatalf("new estimator: %v", err)
	}

	explore := new(onceExplore)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	cfg.NetworkSize = nse

	self := tiny.NewNode(tiny.Key(0b11111111))
	routingBehaviour, err := ComposeRoutingBehaviour(self, idleBootstrap(), idleInclude(), idleProbe(), explore, nil, cfg)
	if err != nil {
		t.Fatalf("compose routing behaviour: %v", err)
	}

	// Each explore reports nodes at increasing distances from its own target, so that the ranks
	// the estimator files them under hold more than one distinct distance.
	explores := []struct {
		target    tiny.Key
		distances []uint8
	}{
		{target: tiny.Key(0b00000000), distances: []uint8{1, 2, 4}},
		{target: tiny.Key(0b10000000), distances: []uint8{3, 5, 9}},
		{target: tiny.Key(0b01000000), distances: []uint8{2, 6, 12}},
	}

	for _, ex := range explores {
		nodes := make([]tiny.Node, 0, len(ex.distances))
		for _, d := range ex.distances {
			nodes = append(nodes, tiny.NewNode(tiny.Key(uint8(ex.target)^d)))
		}

		explore.state = &routing.StateExploreQueryFinished[tiny.Key, tiny.Node]{
			Cpl:          1,
			Target:       ex.target,
			ClosestNodes: nodes,
		}
		explore.done = false

		routingBehaviour.Notify(ctx, &EventRoutingPoll{})
		routingBehaviour.Perform(ctx)
	}

	est, err := nse.Estimate(time.Now())
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	if want := 9; est.Samples != want {
		t.Errorf("got %d samples, want %d", est.Samples, want)
	}
}

// onceBootstrap is a bootstrap state machine that reports a state on its first advance and is
// idle from then on.
type onceBootstrap struct {
	state routing.BootstrapState
	done  bool
}

func (o *onceBootstrap) Advance(ctx context.Context, now time.Time, ev routing.BootstrapEvent) routing.BootstrapState {
	if o.done {
		return &routing.StateBootstrapIdle{}
	}
	o.done = true
	return o.state
}

// TestRoutingBehaviourTracksBootstrapResults checks that the results of a completed bootstrap
// reach the configured network size estimator.
func TestRoutingBehaviourTracksBootstrapResults(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	nse, err := netsize.New[tiny.Key, tiny.Node](nil)
	if err != nil {
		t.Fatalf("new estimator: %v", err)
	}

	bootstrap := new(onceBootstrap)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	cfg.NetworkSize = nse

	self := tiny.NewNode(tiny.Key(0b11111111))
	routingBehaviour, err := ComposeRoutingBehaviour(self, bootstrap, idleInclude(), idleProbe(), idleExplore(), nil, cfg)
	if err != nil {
		t.Fatalf("compose routing behaviour: %v", err)
	}

	bootstraps := []struct {
		target    tiny.Key
		distances []uint8
	}{
		{target: tiny.Key(0b00000000), distances: []uint8{1, 2, 4}},
		{target: tiny.Key(0b10000000), distances: []uint8{3, 5, 9}},
		{target: tiny.Key(0b01000000), distances: []uint8{2, 6, 12}},
	}

	for _, bs := range bootstraps {
		nodes := make([]tiny.Node, 0, len(bs.distances))
		for _, d := range bs.distances {
			nodes = append(nodes, tiny.NewNode(tiny.Key(uint8(bs.target)^d)))
		}

		bootstrap.state = &routing.StateBootstrapFinished[tiny.Key, tiny.Node]{
			Target:       bs.target,
			ClosestNodes: nodes,
		}
		bootstrap.done = false

		routingBehaviour.Notify(ctx, &EventRoutingPoll{})
		routingBehaviour.Perform(ctx)
	}

	est, err := nse.Estimate(time.Now())
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	if want := 9; est.Samples != want {
		t.Errorf("got %d samples, want %d", est.Samples, want)
	}
}
