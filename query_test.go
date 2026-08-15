package xorbie

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
)

func TestQueryConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		require.NoError(t, cfg.Validate())
	})

	t.Run("logger not nil", func(t *testing.T) {
		cfg := DefaultQueryConfig()
		cfg.Logger = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer not nil", func(t *testing.T) {
		cfg := DefaultQueryConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("query concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("query timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultQueryConfig()

		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

func TestQueryBehaviourBase(t *testing.T) {
	suite.Run(t, new(QueryBehaviourBaseTestSuite))
}

type QueryBehaviourBaseTestSuite struct {
	suite.Suite

	cfg   *QueryConfig
	top   *testTopology
	nodes []*testPeer
}

func (ts *QueryBehaviourBaseTestSuite) SetupTest() {
	top, nodes, err := linearTopology(4)
	ts.Require().NoError(err)

	ts.top = top
	ts.nodes = nodes

	ts.cfg = DefaultQueryConfig()
}

func (ts *QueryBehaviourBaseTestSuite) TestNotifiesNoProgress() {
	t := ts.T()
	ctx := kadtest.CtxShort(t)

	target := ts.nodes[3].NodeID.Key()
	rt := ts.nodes[0].RoutingTable
	seeds := rt.NearestNodes(target, 5)

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](ts.nodes[0].NodeID, ts.cfg)
	ts.Require().NoError(err)

	waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](5)
	cmd := &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:           "test",
		Target:            target,
		KnownClosestNodes: seeds,
		Notify:            waiter,
		NumResults:        10,
	}

	// queue the start of the query
	b.Notify(ctx, cmd)

	// behaviour should emit EventOutboundGetCloserNodes to start the query
	bev, ok := b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, bev)

	egc := bev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
	ts.Require().True(egc.To.Equal(ts.nodes[1].NodeID))

	// notify failure
	b.Notify(ctx, &EventGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: "test",
		To:      egc.To,
		Target:  target,
	})

	// query will process the response and notify that node 1 is non connective
	bev, ok = b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventNotifyNonConnectivity[tiny.Key, tiny.Node]{}, bev)

	// ensure that the waiter received query finished event
	kadtest.ReadItem[CtxEvent[*EventQueryFinished[tiny.Key, tiny.Node]]](t, ctx, waiter.Finished())
}

func (ts *QueryBehaviourBaseTestSuite) TestNotifiesQueryProgressed() {
	t := ts.T()
	ctx := kadtest.CtxShort(t)

	target := ts.nodes[3].NodeID.Key()
	rt := ts.nodes[0].RoutingTable
	seeds := rt.NearestNodes(target, 5)

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](ts.nodes[0].NodeID, ts.cfg)
	ts.Require().NoError(err)

	waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](5)
	cmd := &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:           "test",
		Target:            target,
		KnownClosestNodes: seeds,
		Notify:            waiter,
		NumResults:        10,
	}

	// queue the start of the query
	b.Notify(ctx, cmd)

	// behaviour should emit EventOutboundGetCloserNodes to start the query
	bev, ok := b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, bev)

	egc := bev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
	ts.Require().True(egc.To.Equal(ts.nodes[1].NodeID))

	// notify success
	b.Notify(ctx, &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     "test",
		To:          egc.To,
		Target:      target,
		CloserNodes: ts.nodes[1].RoutingTable.NearestNodes(target, 5),
	})

	// query will process the response and ask node 1 for closer nodes
	bev, ok = b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, bev)

	// ensure that the waiter received query progressed event
	kadtest.ReadItem[CtxEvent[*EventQueryProgressed[tiny.Key, tiny.Node, tiny.Message]]](t, ctx, waiter.Progressed())
}

func (ts *QueryBehaviourBaseTestSuite) TestNotifiesQueryFinished() {
	t := ts.T()
	ctx := kadtest.CtxShort(t)

	target := ts.nodes[3].NodeID.Key()
	rt := ts.nodes[0].RoutingTable
	seeds := rt.NearestNodes(target, 5)

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](ts.nodes[0].NodeID, ts.cfg)
	ts.Require().NoError(err)

	waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](5)
	cmd := &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:           "test",
		Target:            target,
		KnownClosestNodes: seeds,
		Notify:            waiter,
		NumResults:        10,
	}

	// queue the start of the query
	b.Notify(ctx, cmd)

	// behaviour should emit EventOutboundGetCloserNodes to start the query
	bev, ok := b.Perform(ctx)
	ts.Require().True(ok)
	ts.Require().IsType(&EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, bev)

	egc := bev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
	ts.Require().True(egc.To.Equal(ts.nodes[1].NodeID))

	// notify success
	b.Notify(ctx, &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     "test",
		To:          egc.To,
		Target:      target,
		CloserNodes: ts.nodes[1].RoutingTable.NearestNodes(target, 5),
	})

	// skip events until next EventOutboundGetCloserNodes is reached
	for {
		bev, ok = b.Perform(ctx)
		ts.Require().True(ok)

		egc, ok = bev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])
		if ok {
			break
		}
	}

	// ensure that the waiter received query progressed event
	wev := kadtest.ReadItem[CtxEvent[*EventQueryProgressed[tiny.Key, tiny.Node, tiny.Message]]](t, ctx, waiter.Progressed())
	ts.Require().True(wev.Event.NodeID.Equal(ts.nodes[1].NodeID))

	// notify success for last seen EventOutboundGetCloserNodes but supply no further nodes
	b.Notify(ctx, &EventGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID: "test",
		To:      egc.To,
		Target:  target,
	})

	// skip events until behaviour runs out of work
	for {
		_, ok = b.Perform(ctx)
		if !ok {
			break
		}
	}

	// ensure that the waiter received query progressed event
	kadtest.ReadItem[CtxEvent[*EventQueryProgressed[tiny.Key, tiny.Node, tiny.Message]]](t, ctx, waiter.Progressed())

	// ensure that the waiter received query  event
	kadtest.ReadItem[CtxEvent[*EventQueryFinished[tiny.Key, tiny.Node]]](t, ctx, waiter.Finished())
}

// TestQueryBehaviourRequestConcurrency asserts that a query with more seeds
// than its request concurrency dispatches up to that concurrency, rather than
// one request at a time.
//
// The behaviour is driven exactly as [Coordinator.eventLoop] drives it, which
// is the point: the pool is willing to dispatch three requests, but it only
// gets the chance if the behaviour keeps signalling that it is ready.
func TestQueryBehaviourRequestConcurrency(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(6)
	require.NoError(t, err)

	cfg := DefaultQueryConfig()
	cfg.Concurrency = 3
	cfg.RequestConcurrency = 3

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, cfg)
	require.NoError(t, err)

	seeds := []tiny.Node{
		nodes[1].NodeID,
		nodes[2].NodeID,
		nodes[3].NodeID,
		nodes[4].NodeID,
		nodes[5].NodeID,
	}

	b.Notify(ctx, &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:           "test",
		Target:            nodes[5].NodeID.Key(),
		KnownClosestNodes: seeds,
		NumResults:        10,
	})

	evs := PerformWhileReady(t, ctx, b)

	var requested []tiny.Node
	for _, ev := range evs {
		if oev, ok := ev.(*EventOutboundGetCloserNodes[tiny.Key, tiny.Node]); ok {
			requested = append(requested, oev.To)
		}
	}

	require.Len(t, requested, cfg.RequestConcurrency, "expected one outbound request per unit of request concurrency, got requests to %v", requested)
}

// TestQueryBehaviourNotifiesQueryTimeout checks that a query which runs out of time tells
// its waiter so and releases the notifier that was held for it.
func TestQueryBehaviourNotifiesQueryTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// the query outlives the deadline kadtest.CtxBubble would give, since the sleep
		// that expires it moves the same fake clock
		ctx := t.Context()

		_, nodes, err := linearTopology(3)
		require.NoError(t, err)

		cfg := DefaultQueryConfig()
		cfg.Timeout = time.Minute

		// one request at a time, so the query has nothing to do but wait for the response
		// that never arrives
		cfg.RequestConcurrency = 1

		// the request must outlive the query, otherwise the query ends by running out of
		// nodes to contact rather than by running out of time
		cfg.RequestTimeout = time.Hour

		b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, cfg)
		require.NoError(t, err)

		waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](5)
		b.Notify(ctx, &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
			QueryID:           "test",
			Target:            nodes[2].NodeID.Key(),
			KnownClosestNodes: []tiny.Node{nodes[1].NodeID},
			Notify:            waiter,
			NumResults:        10,
		})

		bev, ok := b.Perform(ctx)
		require.True(t, ok)
		require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, bev)
		require.Len(t, b.notifiers, 1)

		// the node never responds and the query runs out of time
		time.Sleep(2 * cfg.Timeout)
		synctest.Wait()
		_, ok = b.Perform(ctx)
		require.False(t, ok)

		wev := kadtest.ReadItem[CtxEvent[*EventQueryFinished[tiny.Key, tiny.Node]]](t, ctx, waiter.Finished())
		require.ErrorIs(t, wev.Event.Err, coordt.ErrQueryTimeout)
		require.Equal(t, coordt.QueryID("test"), wev.Event.QueryID)

		// the notifier is not retained for a query the pool has removed
		require.Empty(t, b.notifiers)
	})
}

// TestQueryTimeoutUnblocksWaitForQuery checks that a caller waiting on a query that runs
// out of time is given a timeout error rather than being left blocked until its own
// context expires.
func TestQueryTimeoutUnblocksWaitForQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// the query outlives the deadline kadtest.CtxBubble would give, since the sleep
		// that expires it moves the same fake clock
		ctx := t.Context()
		queryID := coordt.QueryID("test")

		_, nodes, err := linearTopology(3)
		require.NoError(t, err)

		cfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
		cfg.Query.Timeout = time.Minute
		cfg.Query.RequestConcurrency = 1

		// the request must outlive the query, otherwise the query ends by running out of
		// nodes to contact rather than by running out of time
		cfg.Query.RequestTimeout = time.Hour

		// the coordinator is closed immediately so the test drives the query behaviour
		// itself rather than racing the event loop
		c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, cfg)
		require.NoError(t, err)
		require.NoError(t, c.Close())

		waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](5)

		waiterDone := make(chan struct{})
		var waitErr error
		go func() {
			defer close(waiterDone)
			_, _, waitErr = c.waitForQuery(ctx, queryID, waiter, func(ctx context.Context, id tiny.Node, resp tiny.Message, stats coordt.QueryStats) error {
				return nil
			})
		}()

		c.queryBehaviour.Notify(ctx, &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
			QueryID:           queryID,
			Target:            nodes[2].NodeID.Key(),
			KnownClosestNodes: []tiny.Node{nodes[1].NodeID},
			Notify:            waiter,
			NumResults:        10,
		})

		bev, ok := c.queryBehaviour.Perform(ctx)
		require.True(t, ok)
		require.IsType(t, &EventOutboundGetCloserNodes[tiny.Key, tiny.Node]{}, bev)

		// the node never responds and the query runs out of time
		time.Sleep(2 * cfg.Query.Timeout)
		synctest.Wait()
		_, ok = c.queryBehaviour.Perform(ctx)
		require.False(t, ok)

		kadtest.AssertClosed(t, ctx, waiterDone)
		require.ErrorIs(t, waitErr, coordt.ErrQueryTimeout)
	})
}

// TestQuery_deadlock_regression checks that a waiter which is slow to consume query events
// does not deadlock against the behaviour producing them.
func TestQuery_deadlock_regression(t *testing.T) {
	ctx := kadtest.CtxShort(t)
	msg := tiny.Message{}
	queryID := coordt.QueryID("test")

	_, nodes, err := linearTopology(3)
	require.NoError(t, err)

	// it would be better to just work with the queryBehaviour in this test.
	// However, we want to test as many parts as possible and waitForQuery
	// is defined on the coordinator. Therfore, we instantiate a coordinator
	// and close it immediately to manually control state machine progression.
	c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, nil)
	require.NoError(t, err)
	require.NoError(t, c.Close()) // close immediately so that we control the state machine progression

	// define a function that produces success messages
	successMsg := func(to tiny.Node, closer ...tiny.Node) *EventSendMessageSuccess[tiny.Key, tiny.Node, tiny.Message] {
		return &EventSendMessageSuccess[tiny.Key, tiny.Node, tiny.Message]{
			QueryID:     queryID,
			Request:     msg,
			To:          to,
			Response:    tiny.Message{},
			CloserNodes: closer,
		}
	}

	// start query
	waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](5)
	wrappedWaiter := NewQueryMonitorHook[tiny.Key, tiny.Node, tiny.Message, *EventQueryFinished[tiny.Key, tiny.Node]](waiter)

	var waitErr error
	waiterDone := make(chan struct{})
	waiterMsg := make(chan struct{})
	go func() {
		defer close(waiterDone)
		defer close(waiterMsg)
		_, _, waitErr = c.waitForQuery(ctx, queryID, waiter, func(ctx context.Context, id tiny.Node, resp tiny.Message, stats coordt.QueryStats) error {
			waiterMsg <- struct{}{}
			return coordt.ErrSkipRemaining
		})
	}()

	// start the message query
	c.queryBehaviour.Notify(ctx, &EventStartMessageQuery[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:           queryID,
		Target:            msg.Target(),
		Message:           msg,
		KnownClosestNodes: []tiny.Node{nodes[1].NodeID},
		Notify:            wrappedWaiter,
		NumResults:        0,
	})

	// advance state machines and assert that the state machine
	// wants to send an outbound message to another peer
	ev, _ := c.queryBehaviour.Perform(ctx)
	require.IsType(t, &EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message]{}, ev)

	// simulate a successful response from another node that returns one new node
	// This should result in a message for the waiter
	c.queryBehaviour.Notify(ctx, successMsg(nodes[1].NodeID, nodes[2].NodeID))

	// Because we're blocking on the waiterMsg channel in the waitForQuery
	// method above, we simulate a slow receiving waiter.

	// Advance the query pool state machine. Because we returned a new node above,
	// the query pool state machine wants to send another outbound query, and the
	// behaviour has queued an event to notify the routing table of the new node.
	// The order the two are returned in does not matter here.
	var addNode, sendMessage int
	for range 2 {
		ev, ok := c.queryBehaviour.Perform(ctx)
		require.True(t, ok)
		switch ev.(type) {
		case *EventAddNode[tiny.Key, tiny.Node]:
			addNode++
		case *EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message]:
			sendMessage++
		default:
			t.Fatalf("unexpected event %T", ev)
		}
	}
	require.Equal(t, 1, addNode)
	require.Equal(t, 1, sendMessage)

	notifiedWhileBusy := make(chan struct{})
	var once sync.Once
	wrappedWaiter.BeforeProgressed = func() {
		once.Do(func() {
			close(notifiedWhileBusy)
		})
	}

	// Simulate a successful response from the new node. This node didn't return
	// any new nodes to contact, so the behaviour notifies the waiter that the
	// query progressed and then that it finished, while the waiter is still
	// blocked in the callback for the previous progress event.
	c.queryBehaviour.Notify(ctx, successMsg(nodes[2].NodeID))

	for {
		if _, ok := c.queryBehaviour.Perform(ctx); !ok {
			break
		}
	}

	// the behaviour notified the waiter without waiting for it to be ready
	kadtest.AssertClosed(t, ctx, notifiedWhileBusy)

	// release the slow waiter, whose callback returns coordt.ErrSkipRemaining and
	// so notifies the behaviour to stop the query
	kadtest.ReadItem(t, ctx, waiterMsg)

	// the waiter returns rather than deadlocking against the behaviour, which
	// would happen if either direction waited for the other
	kadtest.AssertClosed(t, ctx, waiterDone)
	require.NoError(t, waitErr)
}

// TestQueryBehaviourReportsDroppedQueryStart checks that a request to start a query that
// finds no room in the behaviour's inbound queue is reported to its caller as a finished
// query carrying ErrEventDropped. The caller waits on the monitor for a terminal event, so
// dropping the request silently would leave it waiting until its context expired.
func TestQueryBehaviourReportsDroppedQueryStart(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(2)
	require.NoError(t, err)

	cfg := DefaultQueryConfig()
	cfg.QueueCapacity = 1

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, cfg)
	require.NoError(t, err)

	// take the queue's only place
	b.Notify(ctx, &EventStopQuery{QueryID: "filler"})

	waiter := NewQueryWaiter[tiny.Key, tiny.Node, tiny.Message](1)
	b.Notify(ctx, &EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: "dropped",
		Target:  nodes[1].NodeID.Key(),
		Notify:  waiter,
	})

	select {
	case wev := <-waiter.Finished():
		require.ErrorIs(t, wev.Event.Err, ErrEventDropped)
		require.Equal(t, coordt.QueryID("dropped"), wev.Event.QueryID)
	default:
		t.Fatal("caller was not told the query had been dropped")
	}

	require.Equal(t, int64(1), b.inbound.depth.Load())
}

// TestQueryBehaviourBoundsItsInboundQueue checks that the inbound queue never grows past its
// configured capacity however many events arrive.
func TestQueryBehaviourBoundsItsInboundQueue(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(2)
	require.NoError(t, err)

	cfg := DefaultQueryConfig()
	cfg.QueueCapacity = 4

	b, err := NewQueryBehaviour[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, cfg)
	require.NoError(t, err)

	for i := range 20 {
		b.Notify(ctx, &EventStopQuery{QueryID: coordt.QueryID(fmt.Sprintf("q%d", i))})
	}

	require.Equal(t, int64(cfg.QueueCapacity), b.inbound.depth.Load())
}
