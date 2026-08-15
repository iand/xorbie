package routing

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/key"
	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/internal/tiny"
)

func TestIncludeConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
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
		cfg := DefaultIncludeConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		cfg.Concurrency = 0
		require.Error(t, cfg.Validate())
		cfg.Concurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("queue size positive", func(t *testing.T) {
		cfg := DefaultIncludeConfig()
		cfg.QueueCapacity = 0
		require.Error(t, cfg.Validate())
		cfg.QueueCapacity = -1
		require.Error(t, cfg.Validate())
	})
}

func TestIncludeStartsIdle(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	bs, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	state := bs.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeIdle{}, state)
}

func TestIncludeAddCandidateStartsCheckIfCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.Concurrency = 1

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	candidate := tiny.NewNode(0b00000100)

	// add a candidate
	state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: candidate,
	})
	// the state machine should attempt to send a message
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	st := state.(*StateIncludeConnectivityCheck[tiny.Key, tiny.Node])

	// the message should be sent to the candidate node
	require.Equal(t, candidate, st.NodeID)

	// the message should be looking for the candidate node
	require.Equal(t, candidate, st.NodeID)

	// now the include reports that it is waiting since concurrency is 1
	state = p.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)
}

func TestIncludeAddCandidateReportsCapacity(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.Concurrency = 2

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)
	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	candidate := tiny.NewNode(0b00000100)

	// add a candidate
	state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: candidate,
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// now the state machine reports that it is waiting with capacity since concurrency
	// is greater than the number of checks in flight
	state = p.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)
}

func TestIncludeAddCandidateOverQueueLength(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.QueueCapacity = 2 // only allow two candidates in the queue
	cfg.Concurrency = 3

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000100),
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// include reports that it is waiting and has capacity for more
	state = p.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)

	// add second candidate
	state = p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000010),
	})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// include reports that it is waiting and has capacity for more
	state = p.Advance(ctx, now, &EventIncludePoll{})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeWaitingWithCapacity{}, state)

	// add third candidate
	state = p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000011),
	})
	// sends a message to the candidate
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// include reports that it is waiting at capacity since 3 messages are in flight
	state = p.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// add fourth candidate
	state = p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000101),
	})

	// include reports that it is waiting at capacity since 3 messages are already in flight
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// add fifth candidate
	state = p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000110),
	})

	// include reports that it is waiting and the candidate queue is full since it
	// is configured to have 3 concurrent checks and 2 queued
	require.IsType(t, &StateIncludeWaitingFull{}, state)

	// add sixth candidate
	state = p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000111),
	})

	// include reports that it is still waiting and the candidate queue is full since it
	// is configured to have 3 concurrent checks and 2 queued
	require.IsType(t, &StateIncludeWaitingFull{}, state)
}

func TestIncludeConnectivityCheckSuccess(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.Concurrency = 2

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000100),
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// notify that node was contacted successfully, with no closer nodes
	state = p.Advance(ctx, now, &EventIncludeConnectivityCheckSuccess[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000100),
	})

	// should respond that the routing table was updated
	require.IsType(t, &StateIncludeRoutingUpdated[tiny.Key, tiny.Node]{}, state)

	st := state.(*StateIncludeRoutingUpdated[tiny.Key, tiny.Node])

	// the update is for the correct node
	require.Equal(t, tiny.NewNode(4), st.NodeID)

	// the routing table should contain the node
	foundNode, found := rt.GetNode(tiny.Key(4))
	require.True(t, found)
	require.NotNil(t, foundNode)

	require.True(t, key.Equal(foundNode.Key(), tiny.Key(4)))

	// advancing again should reports that it is idle
	state = p.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeIdle{}, state)
}

func TestIncludeConnectivityCheckFailure(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.Concurrency = 2

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	// add a candidate
	state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000100),
	})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// notify that node was not contacted successfully
	state = p.Advance(ctx, now, &EventIncludeConnectivityCheckFailure[tiny.Key, tiny.Node]{
		NodeID: tiny.NewNode(0b00000100),
	})

	// should respond that state machine is idle
	require.IsType(t, &StateIncludeIdle{}, state)

	// the routing table should not contain the node
	foundNode, found := rt.GetNode(tiny.Key(4))
	require.False(t, found)
	require.Zero(t, foundNode)
}

func TestIncludeConnectivityCheckTimeout(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.Concurrency = 2

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	// fill every check slot with a node that never answers
	for i := range cfg.Concurrency {
		state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{
			NodeID: tiny.NewNode(tiny.Key(4 << i)),
		})
		require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)
	}

	// a further candidate has to wait for a slot
	later := tiny.NewNode(0b01000000)
	state := p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{NodeID: later})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// before the deadline the checks still hold their slots
	state = p.Advance(ctx, now.Add(cfg.Timeout), &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)

	// once the deadline passes the slots are released and the waiting candidate is checked
	state = p.Advance(ctx, now.Add(cfg.Timeout+time.Nanosecond), &EventIncludePoll{})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	st := state.(*StateIncludeConnectivityCheck[tiny.Key, tiny.Node])
	require.Equal(t, later, st.NodeID)

	// none of the abandoned candidates reached the routing table
	for i := range cfg.Concurrency {
		_, found := rt.GetNode(tiny.Key(4 << i))
		require.False(t, found)
	}
}

func TestIncludeReportsNextDue(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultIncludeConfig()
	cfg.Concurrency = 2

	rt, err := triert.New(tiny.NewNode(128), nil)
	require.NoError(t, err)

	p, err := NewInclude(rt, cfg)
	require.NoError(t, err)

	// nothing in flight, so nothing is due
	state := p.Advance(ctx, now, &EventIncludePoll{})
	require.IsType(t, &StateIncludeIdle{}, state)

	// start a check, then a second one a minute later
	state = p.Advance(ctx, now, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{NodeID: tiny.NewNode(4)})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	later := now.Add(time.Minute)
	state = p.Advance(ctx, later, &EventIncludeAddCandidate[tiny.Key, tiny.Node]{NodeID: tiny.NewNode(8)})
	require.IsType(t, &StateIncludeConnectivityCheck[tiny.Key, tiny.Node]{}, state)

	// the earlier of the two check deadlines is what is reported
	state = p.Advance(ctx, later, &EventIncludePoll{})
	require.IsType(t, &StateIncludeWaitingAtCapacity{}, state)
	require.Equal(t, now.Add(cfg.Timeout), state.(*StateIncludeWaitingAtCapacity).NextDue)
}
