package xorbie

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/publish"
	"github.com/iand/xorbie/query"
	"github.com/iand/xorbie/routing"
)

type RecordingSM[E any, S any] struct {
	State    S
	Received []E
}

func NewRecordingSM[E any, S any](response S) *RecordingSM[E, S] {
	return &RecordingSM[E, S]{
		State: response,
	}
}

func (r *RecordingSM[E, S]) Advance(ctx context.Context, now time.Time, e E) S {
	r.Received = append(r.Received, e)
	return r.State
}

func (r *RecordingSM[E, S]) first() E {
	if len(r.Received) == 0 {
		var zero E
		return zero
	}
	return r.Received[0]
}

// maxPerformIterations bounds [PerformWhileReady] so that a behaviour which
// signals ready without ever running out of work fails the test instead of
// hanging it.
const maxPerformIterations = 1000

// PerformWhileReady drives a behaviour the way [Coordinator.eventLoop] drives
// it: Perform is called only while Ready() is signalled, never speculatively.
// It returns the events the behaviour emitted, and stops once the behaviour
// stops signalling that it has work to do.
func PerformWhileReady[I BehaviourEvent, O BehaviourEvent](t *testing.T, ctx context.Context, b Behaviour[I, O]) []O {
	t.Helper()

	var evs []O
	for range maxPerformIterations {
		select {
		case <-b.Ready():
			ev, ok := b.Perform(ctx)
			if ok {
				evs = append(evs, ev)
			}
		case <-ctx.Done():
			t.Fatal("context cancelled while performing behaviour work")
		default:
			return evs
		}
	}

	t.Fatalf("behaviour still ready after %d iterations", maxPerformIterations)
	return nil
}

// testPeers returns n peers wired into a linear topology.
func testPeers(t *testing.T, n int) []*testPeer {
	t.Helper()
	_, nodes, err := linearTopology(n)
	require.NoError(t, err)
	return nodes
}

// newTestQueryBehaviour returns a query behaviour that gives up on a query after
// timeout and on an individual request after requestTimeout.
func newTestQueryBehaviour(t *testing.T, timeout, requestTimeout time.Duration, self tiny.Node) *QueryBehaviour[tiny.Key, tiny.Node, tiny.Message] {
	t.Helper()

	cfg := DefaultQueryConfig[tiny.Key, tiny.Node]()
	cfg.Timeout = timeout
	cfg.RequestTimeout = requestTimeout

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)
	return b
}

// newTestPublishBehaviour returns a publish behaviour holding an empty pool.
func newTestPublishBehaviour(t *testing.T, self tiny.Node) *PublishBehaviour[tiny.Key, tiny.Node, tiny.Message] {
	t.Helper()

	pool, err := publish.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	bcfg := DefaultPublishConfig[tiny.Key, tiny.Node, tiny.Message]()

	b, err := NewPublishBehaviour(pool, self, nil, bcfg)
	require.NoError(t, err)

	return b
}

// buildWaitingQueryBehaviour returns a builder for a query behaviour running a query
// that has contacted a node and is waiting for a response that never arrives.
func buildWaitingQueryBehaviour(timeout, requestTimeout time.Duration) func(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
	return func(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
		nodes := testPeers(t, 2)

		b := newTestQueryBehaviour(t, timeout, requestTimeout, nodes[0].NodeID)
		b.Notify(ctx, &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
			ActivityID:        "test",
			Target:            nodes[1].NodeID.Key(),
			KnownClosestNodes: []tiny.Node{nodes[1].NodeID},
		})

		return b
	}
}

// buildWaitingBootstrapBehaviour returns a routing behaviour whose bootstrap has
// contacted a seed and is waiting for a response that never arrives.
func buildWaitingBootstrapBehaviour(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
	nodes := testPeers(t, 2)

	bcfg := routing.DefaultBootstrapConfig()
	bcfg.Timeout = 10 * conformanceDeadline
	bcfg.RequestTimeout = conformanceDeadline

	bootstrap, err := routing.NewBootstrap(nodes[0].NodeID, nodes[0].RoutingTable, nil, bcfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	b, err := ComposeRoutingBehaviour(nodes[0].NodeID, bootstrap, idleInclude(), idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	b.Notify(ctx, &EventStartBootstrap[tiny.Key, tiny.Node]{
		SeedNodes: []tiny.Node{nodes[1].NodeID},
	})

	return b
}

// buildWaitingIncludeBehaviour returns a routing behaviour whose include has started a
// connectivity check for a candidate node that never answers.
func buildWaitingIncludeBehaviour(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
	nodes := testPeers(t, 4)

	icfg := routing.DefaultIncludeConfig()
	icfg.Timeout = conformanceDeadline

	include, err := routing.NewInclude(nodes[0].RoutingTable, icfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	b, err := ComposeRoutingBehaviour(nodes[0].NodeID, idleBootstrap(), include, idleProbe(), idleExplore(), cfg)
	require.NoError(t, err)

	b.Notify(ctx, &EventAddNode[tiny.Key, tiny.Node]{
		NodeID: nodes[len(nodes)-1].NodeID,
	})

	return b
}

// buildWaitingProbeBehaviour returns a routing behaviour whose probe holds a node whose
// next connectivity check is due after the check interval.
func buildWaitingProbeBehaviour(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
	nodes := testPeers(t, 2)

	pcfg := routing.DefaultProbeConfig()
	pcfg.CheckInterval = conformanceDeadline

	probe, err := routing.NewProbe(nodes[0].RoutingTable, pcfg)
	require.NoError(t, err)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	b, err := ComposeRoutingBehaviour(nodes[0].NodeID, idleBootstrap(), idleInclude(), probe, idleExplore(), cfg)
	require.NoError(t, err)

	// the linear topology puts the second node in the first node's routing table, which
	// is where the probe takes the nodes it checks from
	b.Notify(ctx, &EventRoutingUpdated[tiny.Key, tiny.Node]{
		NodeID: nodes[1].NodeID,
	})

	return b
}

// buildWaitingExploreBehaviour returns a routing behaviour whose explore has nothing to
// do until the first cpl in its schedule falls due.
func buildWaitingExploreBehaviour(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
	nodes := testPeers(t, 2)

	cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
	schedule, err := routing.NewDynamicExploreSchedule(cfg.ExploreMaximumCpl, time.Now(), conformanceDeadline, cfg.ExploreIntervalMultiplier, cfg.ExploreIntervalJitter)
	require.NoError(t, err)

	explore, err := routing.NewExplore(nodes[0].NodeID, nodes[0].RoutingTable, tiny.NodeWithCpl, schedule, routing.DefaultExploreConfig())
	require.NoError(t, err)

	b, err := ComposeRoutingBehaviour(nodes[0].NodeID, idleBootstrap(), idleInclude(), idleProbe(), explore, cfg)
	require.NoError(t, err)

	return b
}

// buildWaitingPublishBehaviour returns a publish behaviour running a follow up
// publish that has contacted a seed and is waiting for a response that never arrives.
func buildWaitingPublishBehaviour(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent] {
	nodes := testPeers(t, 2)

	// the pool the publish borrows takes no configuration, so this case depends on its
	// default request timeout matching the deadline the other cases use
	require.Equal(t, conformanceDeadline, query.DefaultPoolConfig().RequestTimeout)

	b := newTestPublishBehaviour(t, nodes[0].NodeID)

	msg := tiny.Message{Content: "store"}
	b.Notify(ctx, &EventStartFollowUpPublish[tiny.Key, tiny.Node, tiny.Message]{
		ActivityID:        "test",
		Target:            msg.Target(),
		Message:           msg,
		KnownClosestNodes: []tiny.Node{nodes[1].NodeID},
	})

	return b
}

// conformanceDeadline is the deadline every behaviour built by
// [TestBehaviourSignalsReadyAtDeadline] is waiting on, measured from its first advance.
const conformanceDeadline = time.Minute

// TestBehaviourSignalsReadyAtDeadline checks the [Behaviour] ready contract for every
// behaviour that owns a deadline: one waiting on a deadline signals ready by the time it
// falls due. Each case leaves its behaviour waiting on a response that never comes, and
// the events it emits are dropped, so only the passage of time can make work available.
func TestBehaviourSignalsReadyAtDeadline(t *testing.T) {
	testCases := []struct {
		name string
		// build returns a behaviour that will be waiting on a deadline of
		// conformanceDeadline once it has been driven until it has no more work.
		build func(t *testing.T, ctx context.Context) Behaviour[BehaviourEvent, BehaviourEvent]
	}{
		{
			name:  "query request timeout",
			build: buildWaitingQueryBehaviour(10*conformanceDeadline, conformanceDeadline),
		},
		{
			name:  "query timeout",
			build: buildWaitingQueryBehaviour(conformanceDeadline, 10*conformanceDeadline),
		},
		{
			name:  "bootstrap request timeout",
			build: buildWaitingBootstrapBehaviour,
		},
		{
			name:  "include connectivity check timeout",
			build: buildWaitingIncludeBehaviour,
		},
		{
			name:  "probe check interval",
			build: buildWaitingProbeBehaviour,
		},
		{
			name:  "explore schedule",
			build: buildWaitingExploreBehaviour,
		},
		{
			name:  "publish request timeout",
			build: buildWaitingPublishBehaviour,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := kadtest.CtxBubble(t)

				b := tc.build(t, ctx)

				// drive the behaviour until it has no more work, discarding generated events
				PerformWhileReady(t, ctx, b)

				select {
				case <-b.Ready():
					t.Fatal("behaviour signalled ready before its deadline fell due")
				default:
				}

				time.Sleep(conformanceDeadline)
				synctest.Wait()

				select {
				case <-b.Ready():
				default:
					t.Fatalf("behaviour did not signal ready by its deadline of %s", conformanceDeadline)
				}
			})
		})
	}
}

// TestBehaviourWithNoWorkArmsNoTimer checks that a behaviour holding no deadline holds
// no timer either, so an idle node goes quiet instead of waking on a schedule.
func TestBehaviourWithNoWorkArmsNoTimer(t *testing.T) {
	testCases := []struct {
		name  string
		build func(t *testing.T) (Behaviour[BehaviourEvent, BehaviourEvent], *readyTimer)
	}{
		{
			name: "query",
			build: func(t *testing.T) (Behaviour[BehaviourEvent, BehaviourEvent], *readyTimer) {
				b := newTestQueryBehaviour(t, time.Minute, time.Minute, testPeers(t, 1)[0].NodeID)
				return b, b.readyTimer
			},
		},
		{
			name: "routing",
			build: func(t *testing.T) (Behaviour[BehaviourEvent, BehaviourEvent], *readyTimer) {
				cfg := DefaultRoutingConfig[tiny.Key, tiny.Node]()
				b, err := ComposeRoutingBehaviour(testPeers(t, 1)[0].NodeID, idleBootstrap(), idleInclude(), idleProbe(), idleExplore(), cfg)
				require.NoError(t, err)
				return b, b.readyTimer
			},
		},
		{
			name: "publish",
			build: func(t *testing.T) (Behaviour[BehaviourEvent, BehaviourEvent], *readyTimer) {
				b := newTestPublishBehaviour(t, testPeers(t, 1)[0].NodeID)
				return b, b.readyTimer
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := kadtest.CtxBubble(t)

				b, timer := tc.build(t)

				PerformWhileReady(t, ctx, b)

				if timer.timer != nil {
					t.Error("behaviour armed a timer with no work outstanding")
				}

				time.Sleep(24 * time.Hour)
				synctest.Wait()

				select {
				case <-b.Ready():
					t.Error("behaviour signalled ready with no work outstanding")
				default:
				}
			})
		})
	}
}

func DrainBehaviour[I BehaviourEvent, O BehaviourEvent](t *testing.T, ctx context.Context, b Behaviour[I, O]) {
	for {
		select {
		case <-b.Ready():
			b.Perform(ctx)
		case <-ctx.Done():
			t.Fatal("context cancelled while draining behaviour")
		default:
			return
		}
	}
}

// TestInboundQueueBoundsItsLength checks that an inbound queue accepts events up to its
// capacity, refuses them beyond it, and makes room again as they are dequeued.
func TestInboundQueueBoundsItsLength(t *testing.T) {
	q := newInboundQueue(2)
	require.True(t, q.empty())

	require.True(t, q.enqueue(CtxEvent[BehaviourEvent]{Event: &EventStopQuery{ActivityID: "a"}}))
	require.True(t, q.enqueue(CtxEvent[BehaviourEvent]{Event: &EventStopQuery{ActivityID: "b"}}))
	require.False(t, q.enqueue(CtxEvent[BehaviourEvent]{Event: &EventStopQuery{ActivityID: "c"}}))

	require.False(t, q.empty())
	require.Equal(t, int64(2), q.depth.Load())

	ce, ok := q.dequeue()
	require.True(t, ok)
	require.Equal(t, coordt.ActivityID("a"), ce.Event.(*EventStopQuery).ActivityID)
	require.Equal(t, int64(1), q.depth.Load())

	// the space freed by the dequeue is available again
	require.True(t, q.enqueue(CtxEvent[BehaviourEvent]{Event: &EventStopQuery{ActivityID: "d"}}))

	ce, ok = q.dequeue()
	require.True(t, ok)
	require.Equal(t, coordt.ActivityID("b"), ce.Event.(*EventStopQuery).ActivityID)

	ce, ok = q.dequeue()
	require.True(t, ok)
	require.Equal(t, coordt.ActivityID("d"), ce.Event.(*EventStopQuery).ActivityID)

	_, ok = q.dequeue()
	require.False(t, ok)
	require.True(t, q.empty())
	require.Zero(t, q.depth.Load())
}
