package brdcst

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

// Assert that Pool implements the common state machine interface
var _ coordt.StateMachine[PoolEvent, PoolState] = (*Pool[tiny.Key, tiny.Node, tiny.Message])(nil)

func TestPoolStopWhenNoQueries(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	state := p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)
}

func TestPool_FollowUp_lifecycle(t *testing.T) {
	// This test attempts to cover the whole lifecycle of
	// a follow-up broadcast operation.
	//
	// We have a network of three peers: a, b, and, c
	// First, we query all three while peer c fails to respond
	// Second, we store the record with the remaining a and b, while b fails to respond

	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00000011) // 3
	c := tiny.NewNode(0b00000010) // 2

	queryID := coordt.QueryID("test")

	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{a},
		Config:  DefaultConfigFollowUp(),
	})

	// the query should attempt to contact the node it was given
	st, ok := state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)

	require.Equal(t, queryID, st.QueryID)         // the query should be the one just added
	require.Equal(t, a, st.NodeID)                // the query should attempt to contact the node it was given
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// polling the state machine returns waiting
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)

	// notify pool that the node was contacted successfully
	// with a single closer node.
	state = p.Advance(ctx, epoch, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		Target:      target,
		NodeID:      a,
		CloserNodes: []tiny.Node{a, b},
	})

	// the query should attempt to contact the single closer node it has found
	st, ok = state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)

	require.Equal(t, queryID, st.QueryID)         // the query should be the same
	require.Equal(t, b, st.NodeID)                // the query should attempt to contact the newly discovered node
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// notify pool that the node was contacted successfully
	// with no new node.
	state = p.Advance(ctx, epoch, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		Target:      target,
		NodeID:      b,
		CloserNodes: []tiny.Node{b, c}, // returns additional node
	})

	// the query should attempt to contact the newly closer node it has found
	st, ok = state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)

	require.Equal(t, queryID, st.QueryID)         // the query should be the same
	require.Equal(t, c, st.NodeID)                // the query should attempt to contact the newly discovered node
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// this last node times out -> start contacting the other two
	timeoutErr := fmt.Errorf("timeout")
	state = p.Advance(ctx, epoch, &EventPoolGetCloserNodesFailure[tiny.Key, tiny.Node]{
		QueryID: queryID,
		NodeID:  c,
		Target:  target,
		Error:   timeoutErr,
	})

	// This means we should start the follow-up phase
	srState, ok := state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)

	require.Equal(t, queryID, srState.QueryID)
	firstContactedNode := srState.NodeID
	require.True(t, a == srState.NodeID || b == srState.NodeID) // we should contact either node - there's no inherent order
	require.Equal(t, msg.Content, srState.Message.Content)

	// polling the state machine should trigger storing the record with
	// the second node
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	srState, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)

	require.Equal(t, queryID, srState.QueryID)
	require.True(t, a == srState.NodeID || b == srState.NodeID) // we should contact either node - there's no inherent order
	require.NotEqual(t, firstContactedNode, srState.NodeID)     // should be the other one now
	require.Equal(t, msg.Content, srState.Message.Content)

	// since we have two requests in-flight, polling should return a waiting state machine
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)

	// first response from storing the record comes back
	state = p.Advance(ctx, epoch, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  a,
		Request: msg,
	})
	require.IsType(t, &StatePoolWaiting{}, state)

	// second response from storing the record comes back and it failed!
	state = p.Advance(ctx, epoch, &EventPoolStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  b,
		Request: msg,
		Error:   timeoutErr,
	})

	// since we have contacted all nodes we knew, the broadcast has finished
	finishState, ok := state.(*StatePoolBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)

	require.Equal(t, queryID, finishState.QueryID)
	require.Len(t, finishState.Contacted, 2)
	require.Len(t, finishState.Errors, 1)
	require.Equal(t, finishState.Errors[b.String()].Node, b)
	require.Equal(t, finishState.Errors[b.String()].Err, timeoutErr)

	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolIdle{}, state)

	require.Nil(t, p.bcs[queryID]) // should have been removed
}

func TestPool_FollowUp_stop_during_query(t *testing.T) {
	// This test attempts to cover the case where a followup broadcast operation
	// is cancelled during the query phase

	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4

	queryID := coordt.QueryID("test")

	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{a},
		Config:  DefaultConfigFollowUp(),
	})

	// the query should attempt to contact the node it was given
	st, ok := state.(*StatePoolFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)

	require.Equal(t, queryID, st.QueryID)         // the query should be the one just added
	require.Equal(t, a, st.NodeID)                // the query should attempt to contact the node it was given
	require.True(t, key.Equal(target, st.Target)) // with the correct target

	// polling the state machine returns waiting
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)

	state = p.Advance(ctx, epoch, &EventPoolStopBroadcast{
		QueryID: queryID,
	})
	finish, ok := state.(*StatePoolBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Len(t, finish.Contacted, 0)
}

func TestPool_FollowUp_stop_during_followup_phase(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00000011) // 3

	queryID := coordt.QueryID("test")

	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{a, b},
		Config:  DefaultConfigFollowUp(),
	})

	require.IsType(t, &StatePoolFindCloser[tiny.Key, tiny.Node]{}, state)
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolFindCloser[tiny.Key, tiny.Node]{}, state)

	state = p.Advance(ctx, epoch, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		Target:      target,
		NodeID:      a,
		CloserNodes: []tiny.Node{a, b},
	})
	require.IsType(t, &StatePoolWaiting{}, state)

	state = p.Advance(ctx, epoch, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		Target:      target,
		NodeID:      b,
		CloserNodes: []tiny.Node{a, b},
	})
	require.IsType(t, &StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = p.Advance(ctx, epoch, &EventPoolStopBroadcast{
		QueryID: queryID,
	})

	st, ok := state.(*StatePoolBroadcastFinished[tiny.Key, tiny.Node])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, st.QueryID, queryID)
	require.Len(t, st.Contacted, 2)
	require.Len(t, st.Errors, 2)
}

func TestPool_empty_seed(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)

	queryID := coordt.QueryID("test")

	startEvt := &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{},
	}

	t.Run("follow up", func(t *testing.T) {
		startEvt.Config = DefaultConfigFollowUp()

		state := p.Advance(ctx, epoch, startEvt)
		require.IsType(t, &StatePoolBroadcastFinished[tiny.Key, tiny.Node]{}, state)

		state = p.Advance(ctx, epoch, &EventPoolPoll{})
		require.IsType(t, &StatePoolIdle{}, state)
	})

	t.Run("static", func(t *testing.T) {
		startEvt.Config = DefaultConfigStatic()
		state := p.Advance(ctx, epoch, startEvt)
		require.IsType(t, &StatePoolBroadcastFinished[tiny.Key, tiny.Node]{}, state)

		state = p.Advance(ctx, epoch, &EventPoolPoll{})
		require.IsType(t, &StatePoolIdle{}, state)
	})
}

func TestPool_Static_happy_path(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00000011) // 3
	c := tiny.NewNode(0b00000010) // 2

	queryID := coordt.QueryID("test")

	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{a, b, c},
		Config:  DefaultConfigStatic(),
	})
	spsr, ok := state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	first := spsr.NodeID

	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	spsr, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	second := spsr.NodeID

	state = p.Advance(ctx, epoch, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  first,
		Request: msg,
	})
	spsr, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	third := spsr.NodeID

	state = p.Advance(ctx, epoch, &EventPoolStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  second,
		Request: msg,
	})
	require.IsType(t, &StatePoolWaiting{}, state)

	state = p.Advance(ctx, epoch, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		NodeID:  third,
		Request: msg,
	})
	require.IsType(t, &StatePoolBroadcastFinished[tiny.Key, tiny.Node]{}, state)
}

func TestPool_Static_stop_mid_flight(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4
	b := tiny.NewNode(0b00000011) // 3
	c := tiny.NewNode(0b00000010) // 2

	queryID := coordt.QueryID("test")

	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{a, b, c},
		Config:  DefaultConfigStatic(),
	})
	require.IsType(t, &StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	state = p.Advance(ctx, epoch, &EventPoolStopBroadcast{QueryID: queryID})
	require.IsType(t, &StatePoolBroadcastFinished[tiny.Key, tiny.Node]{}, state)
}

func TestPoolState_interface_conformance(t *testing.T) {
	states := []PoolState{
		&StatePoolIdle{},
		&StatePoolWaiting{},
		&StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message]{},
		&StatePoolFindCloser[tiny.Key, tiny.Node]{},
		&StatePoolBroadcastFinished[tiny.Key, tiny.Node]{},
	}
	for _, st := range states {
		st.poolState() // drives test coverage
	}
}

// TestPoolReportsNextDue checks that the broadcast pool reports the due time of the query
// pool it delegates to while finding closer nodes, and reports nothing scheduled once it
// moves on to storing records, which carry no deadline.
func TestPoolReportsNextDue(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)
	a := tiny.NewNode(0b00000100) // 4

	queryID := coordt.QueryID("test")

	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: queryID,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{a},
		Config:  DefaultConfigFollowUp(),
	})
	require.IsType(t, &StatePoolFindCloser[tiny.Key, tiny.Node]{}, state)

	// while finding closer nodes the pool reports the deadline of the outstanding request
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)
	require.Equal(t, epoch.Add(cfg.pCfg.RequestTimeout), state.(*StatePoolWaiting).NextDue)

	// the node responds with no closer node, ending the query and starting the
	// follow-up phase
	state = p.Advance(ctx, epoch, &EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{
		QueryID:     queryID,
		Target:      target,
		NodeID:      a,
		CloserNodes: []tiny.Node{a},
	})
	require.IsType(t, &StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message]{}, state)

	// a store record request carries no deadline, so there is nothing scheduled
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	require.IsType(t, &StatePoolWaiting{}, state)
	require.True(t, state.(*StatePoolWaiting).NextDue.IsZero())
}

// TestPoolAdvancesEveryBroadcast checks that a broadcast waiting on outstanding requests does
// not stop the pool producing work for another broadcast running alongside it.
func TestPoolAdvancesEveryBroadcast(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)

	first := coordt.QueryID("first")
	second := coordt.QueryID("second")

	// a static broadcast takes its seed nodes as work to do, with no query phase, so both
	// broadcasts can be driven to the point of waiting without involving the query pool
	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: first,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{tiny.NewNode(0b00000100), tiny.NewNode(0b00000101)},
		Config:  DefaultConfigStatic(),
	})
	srState, ok := state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, first, srState.QueryID)
	firstContacted := srState.NodeID

	// the first broadcast's other node is contacted too, leaving it waiting on both
	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	srState, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, first, srState.QueryID)

	// the second broadcast starts and contacts one of its two nodes, keeping the other to do
	state = p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: second,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{tiny.NewNode(0b00000110), tiny.NewNode(0b00000111)},
		Config:  DefaultConfigStatic(),
	})
	srState, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, second, srState.QueryID)
	secondContacted := srState.NodeID

	// one of the first broadcast's records lands, which leaves it waiting on the other and
	// with nothing of its own to do
	state = p.Advance(ctx, epoch, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: first,
		NodeID:  firstContacted,
		Request: msg,
	})

	// the pool should hand out the second broadcast's remaining node rather than report
	// that it is waiting on the first
	srState, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, second, srState.QueryID)
	require.NotEqual(t, secondContacted, srState.NodeID)
}

// TestPoolReportsEarliestNextDueAcrossBroadcasts checks that when every broadcast is waiting the
// pool reports the earliest instant any of them could make progress, ignoring those with none.
func TestPoolReportsEarliestNextDueAcrossBroadcasts(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfigPool()

	self := tiny.NewNode(0)

	p, err := NewPool[tiny.Key, tiny.Node, tiny.Message](self, cfg)
	require.NoError(t, err)

	msg := tiny.Message{Content: "store this"}
	target := tiny.Key(0b00000001)

	followUp := coordt.QueryID("followup")
	static := coordt.QueryID("static")

	// a follow up broadcast in its query phase waits on a request that carries a deadline
	state := p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: followUp,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{tiny.NewNode(0b00000100)},
		Config:  DefaultConfigFollowUp(),
	})
	require.IsType(t, &StatePoolFindCloser[tiny.Key, tiny.Node]{}, state)

	// a static broadcast waits on store record requests, which carry no deadline
	state = p.Advance(ctx, epoch, &EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: static,
		Target:  target,
		Message: msg,
		Seed:    []tiny.Node{tiny.NewNode(0b00000110), tiny.NewNode(0b00000111)},
		Config:  DefaultConfigStatic(),
	})
	srState, ok := state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	staticContacted := srState.NodeID

	state = p.Advance(ctx, epoch, &EventPoolPoll{})
	srState, ok = state.(*StatePoolStoreRecord[tiny.Key, tiny.Node, tiny.Message])
	require.True(t, ok, "state is %T", state)
	require.Equal(t, static, srState.QueryID)

	// one of the static broadcast's records lands, leaving every broadcast waiting
	state = p.Advance(ctx, epoch, &EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: static,
		NodeID:  staticContacted,
		Request: msg,
	})

	// the request deadline is the only instant at which the pool could make progress, so the
	// static broadcast having none must not mask it
	require.IsType(t, &StatePoolWaiting{}, state)
	require.Equal(t, epoch.Add(cfg.pCfg.RequestTimeout), state.(*StatePoolWaiting).NextDue)
}

func TestPoolEvent_interface_conformance(t *testing.T) {
	events := []PoolEvent{
		&EventPoolStopBroadcast{},
		&EventPoolPoll{},
		&EventPoolStartBroadcast[tiny.Key, tiny.Node, tiny.Message]{},
		&EventPoolGetCloserNodesSuccess[tiny.Key, tiny.Node]{},
		&EventPoolGetCloserNodesFailure[tiny.Key, tiny.Node]{},
		&EventPoolStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{},
		&EventPoolStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{},
	}
	for _, ev := range events {
		ev.poolEvent() // drives test coverage
	}
}

// epoch is the base instant for tests. State machines take the current time as a
// parameter, so tests move time by passing a later instant rather than by advancing a clock.
var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
