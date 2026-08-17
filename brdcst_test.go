package xorbie

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/brdcst"
	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
)

// TestBroadcastBehaviourContactsAllSeeds asserts that a static broadcast sends
// its record to every seed node without waiting for a response from each one in
// turn.
//
// The behaviour is driven exactly as [Coordinator.eventLoop] drives it, which
// is the point: the static strategy holds no concurrency limit and is willing
// to contact every seed, but it only gets the chance if the behaviour keeps
// signalling that it is ready.
func TestBroadcastBehaviourContactsAllSeeds(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(6)
	require.NoError(t, err)

	self := nodes[0].NodeID

	pool, err := brdcst.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	b, err := NewPooledBroadcastBehaviour(pool, nil)
	require.NoError(t, err)

	seeds := []tiny.Node{
		nodes[1].NodeID,
		nodes[2].NodeID,
		nodes[3].NodeID,
		nodes[4].NodeID,
		nodes[5].NodeID,
	}

	msg := tiny.Message{Content: "store"}

	b.Notify(ctx, &EventStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: "test",
		Target:  msg.Target(),
		Message: msg,
		Config:  &brdcst.StaticConfig[tiny.Key, tiny.Node]{Nodes: seeds},
		Notify:  NewBroadcastWaiter[tiny.Key, tiny.Node, tiny.Message](0),
	})

	evs := PerformWhileReady(t, ctx, b)

	var contacted []tiny.Node
	for _, ev := range evs {
		if oev, ok := ev.(*EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message]); ok {
			contacted = append(contacted, oev.To)
		}
	}

	require.ElementsMatch(t, seeds, contacted, "expected every seed to be sent the record")
}

// TestBroadcastBehaviourReportsDroppedBroadcastStart checks that a request to start a
// broadcast that finds no room in the behaviour's inbound queue is reported to its caller as
// a finished broadcast carrying ErrEventDropped. The caller waits on the monitor for a
// terminal event, so dropping the request silently would leave it waiting until its context
// expired.
func TestBroadcastBehaviourReportsDroppedBroadcastStart(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(2)
	require.NoError(t, err)

	pool, err := brdcst.NewPool[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, nil)
	require.NoError(t, err)

	cfg := DefaultBroadcastConfig[tiny.Key, tiny.Node, tiny.Message]()
	cfg.QueueCapacity = 1

	b, err := NewPooledBroadcastBehaviour(pool, cfg)
	require.NoError(t, err)

	// take the queue's only place
	b.Notify(ctx, &EventStopQuery{QueryID: "filler"})

	waiter := NewBroadcastWaiter[tiny.Key, tiny.Node, tiny.Message](1)
	b.Notify(ctx, &EventStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: "dropped",
		Target:  nodes[1].NodeID.Key(),
		Notify:  waiter,
	})

	select {
	case wev := <-waiter.Finished():
		require.ErrorIs(t, wev.Event.Err, ErrEventDropped)
		require.Equal(t, coordt.QueryID("dropped"), wev.Event.QueryID)
	default:
		t.Fatal("caller was not told the broadcast had been dropped")
	}
}
