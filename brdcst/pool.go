package brdcst

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/query"
)

// Broadcast is the state machine interface that every broadcast strategy implements. A
// strategy decides which nodes a record is stored with and in what order they are contacted.
type Broadcast = coordt.StateMachine[BroadcastEvent, BroadcastState]

// The Pool state machine manages every running broadcast operation.
//
// A broadcast is started by one of the start events, one per strategy, and runs until it reports
// that it has finished or is stopped by the [EventPoolStopBroadcast] event. The pool imposes no
// limit on how many run at once.
//
// Advancing the pool advances each of its broadcasts in turn and returns the first
// instruction any of them produces, so a single advance emits work for one broadcast only.
// A broadcast that is merely waiting produces no instruction and does not stop the walk. If
// no broadcast has work the pool reports [StatePoolWaiting], carrying the earliest time any
// of them could make progress, and if it holds no broadcasts at all it reports
// [StatePoolIdle].
//
// The pool owns the query pool that strategies run their find closer nodes queries in, and
// passes it to each broadcast it creates. That pool is separate from the one serving
// ordinary queries, so the two can be given different limits.
type Pool[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// qp is the pool in which broadcasts run their find closer nodes queries
	qp *query.Pool[K, N, M]

	// bcs holds every running broadcast operation, keyed by its query id
	bcs map[coordt.QueryID]Broadcast

	// cfg is a copy of the optional configuration supplied to the Pool
	cfg PoolConfig

	// nextDue is the earliest time reported by a waiting broadcast during the current
	// call to Advance. It is recalculated on each call.
	nextDue time.Time
}

// PoolConfig specifies the configuration for a broadcast [Pool].
type PoolConfig struct {
	pCfg *query.PoolConfig

	// Optimistic is the configuration given to each broadcast run by the optimistic strategy.
	Optimistic *OptimisticConfig

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (cfg *PoolConfig) Validate() error {
	if cfg.pCfg == nil {
		return fmt.Errorf("query pool config must not be nil")
	}

	if cfg.Optimistic == nil {
		return fmt.Errorf("optimistic config must not be nil")
	} else if err := cfg.Optimistic.Validate(); err != nil {
		return fmt.Errorf("optimistic config: %w", err)
	}

	if cfg.Tracer == nil {
		return fmt.Errorf("tracer must not be nil")
	}

	return nil
}

// DefaultPoolConfig returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		pCfg:       query.DefaultPoolConfig(),
		Optimistic: DefaultOptimisticConfig(),
		Tracer:     coordt.NoopTracer(),
	}
}

// NewPool creates a broadcast pool along with the query pool its broadcasts run their find
// closer nodes queries in. The default configuration is used if cfg is nil. The query pool
// is created here rather than shared with the one serving ordinary queries so that the two
// can be given different concurrency limits and neither has to coordinate with the other.
func NewPool[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, cfg *PoolConfig) (*Pool[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate pool config: %w", err)
	}

	// The query pool traces through the same tracer as the broadcast pool that owns it.
	qpCfg := *cfg.pCfg
	qpCfg.Tracer = cfg.Tracer

	qp, err := query.NewPool[K, N, M](self, &qpCfg)
	if err != nil {
		return nil, fmt.Errorf("new query pool: %w", err)
	}

	return &Pool[K, N, M]{
		qp:  qp,
		bcs: map[coordt.QueryID]Broadcast{},
		cfg: *cfg,
	}, nil
}

// Advance advances the state of the broadcast [Pool] by advancing its broadcasts.
//
// An event naming a broadcast is translated and delivered to that broadcast first, and the
// rest are then polled in turn until one of them produces an instruction to return. An event
// that names no broadcast, such as [EventPoolPoll], goes straight to the walk.
func (p *Pool[K, N, M]) Advance(ctx context.Context, now time.Time, ev PoolEvent) (out PoolState) {
	ctx, span := p.cfg.Tracer.Start(ctx, "Pool.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	// the earliest due time is accumulated across the broadcasts advanced by this call
	p.nextDue = time.Time{}

	sm, bev := p.handleEvent(ctx, ev)
	if sm != nil && bev != nil {
		if state, terminal := p.advanceBroadcast(ctx, now, sm, bev); terminal {
			return state
		}
	}

	if len(p.bcs) == 0 {
		return &StatePoolIdle{}
	}

	// advance the remaining broadcasts until one of them produces an instruction
	for _, bsm := range p.bcs {
		if sm == bsm {
			continue
		}

		state, terminal := p.advanceBroadcast(ctx, now, bsm, &EventBroadcastPoll{})
		if terminal {
			return state
		}
	}

	// no broadcast produced an instruction, so the pool has nothing to do but wait
	return &StatePoolWaiting{NextDue: p.nextDue}
}

// recordDue notes a time reported by a waiting broadcast, keeping the earliest seen
// during the current call to Advance. The zero time means the broadcast has nothing
// scheduled and is ignored.
func (p *Pool[K, N, M]) recordDue(due time.Time) {
	if due.IsZero() {
		return
	}
	if p.nextDue.IsZero() || due.Before(p.nextDue) {
		p.nextDue = due
	}
}

// handleEvent translates a [PoolEvent] into the broadcast it is addressed to and the event
// to deliver to it. Both return values are nil when the event names no broadcast, either
// because it carries an unknown query id or because it is not addressed to one.
func (p *Pool[K, N, M]) handleEvent(ctx context.Context, ev PoolEvent) (sm Broadcast, out BroadcastEvent) {
	_, span := p.cfg.Tracer.Start(ctx, "Pool.handleEvent", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventPoolStartFollowUp[K, N, M]:
		p.bcs[ev.QueryID] = NewFollowUp(ev.QueryID, p.qp, ev.Message, ev.Seed, p.cfg.Tracer)

		return p.bcs[ev.QueryID], &EventBroadcastStart[K, N]{
			Target: ev.Target,
		}

	case *EventPoolStartStatic[K, N, M]:
		p.bcs[ev.QueryID] = NewStatic(ev.QueryID, ev.Message, ev.Nodes, p.cfg.Tracer)

		return p.bcs[ev.QueryID], &EventBroadcastStart[K, N]{
			Target: ev.Target,
		}

	case *EventPoolStartOptimistic[K, N, M]:
		o, err := NewOptimistic(ev.QueryID, p.qp, ev.Message, ev.Seed, ev.NetworkSize, p.cfg.Optimistic, p.cfg.Tracer)
		if err != nil {
			return nil, nil
		}
		p.bcs[ev.QueryID] = o

		return p.bcs[ev.QueryID], &EventBroadcastStart[K, N]{
			Target: ev.Target,
		}

	case *EventPoolStopBroadcast:
		return p.bcs[ev.QueryID], &EventBroadcastStop{}

	case *EventPoolGetCloserNodesSuccess[K, N]:
		return p.bcs[ev.QueryID], &EventBroadcastNodeResponse[K, N]{
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}

	case *EventPoolGetCloserNodesFailure[K, N]:
		return p.bcs[ev.QueryID], &EventBroadcastNodeFailure[K, N]{
			NodeID: ev.NodeID,
			Error:  ev.Error,
		}

	case *EventPoolStoreRecordSuccess[K, N, M]:
		return p.bcs[ev.QueryID], &EventBroadcastStoreRecordSuccess[K, N, M]{
			NodeID:   ev.NodeID,
			Request:  ev.Request,
			Response: ev.Response,
		}

	case *EventPoolStoreRecordFailure[K, N, M]:
		return p.bcs[ev.QueryID], &EventBroadcastStoreRecordFailure[K, N, M]{
			NodeID:  ev.NodeID,
			Request: ev.Request,
			Error:   ev.Error,
		}

	case *EventPoolPoll:
		// no event to process

	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil, nil
}

// advanceBroadcast advances the given broadcast and translates the state it reports into a
// [PoolState]. The boolean reports whether that state is one the pool should return; when it
// is false the broadcast produced no instruction and the returned state is nil.
func (p *Pool[K, N, M]) advanceBroadcast(ctx context.Context, now time.Time, sm Broadcast, bev BroadcastEvent) (PoolState, bool) {
	ctx, span := p.cfg.Tracer.Start(ctx, "Pool.advanceBroadcast", trace.WithAttributes(coordt.AttrInEvent(bev)))
	defer span.End()

	state := sm.Advance(ctx, now, bev)
	switch st := state.(type) {
	case *StateBroadcastFindCloser[K, N]:
		return &StatePoolFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}, true
	case *StateBroadcastWaiting:
		// Waiting carries no instruction for the caller, so it does not end the walk.
		// The time recorded here is reported only if no broadcast has work to hand out.
		p.recordDue(st.NextDue)
	case *StateBroadcastStoreRecord[K, N, M]:
		return &StatePoolStoreRecord[K, N, M]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Message: st.Message,
		}, true
	case *StateBroadcastFinished[K, N]:
		delete(p.bcs, st.QueryID)
		return &StatePoolBroadcastFinished[K, N]{
			QueryID:    st.QueryID,
			Contacted:  st.Contacted,
			Errors:     st.Errors,
			QueryStats: st.QueryStats,
		}, true
	}

	return nil, false
}

// PoolState must be implemented by all states that a [Pool] can reach. A state is the output
// of advancing the pool, and carries an instruction that the caller is expected to act on.
type PoolState interface {
	poolState()
}

// StatePoolFindCloser indicates that one of the pool's broadcasts wants the given node
// queried for nodes closer to the target key.
type StatePoolFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	Target  K              // the key that the query wants to find closer nodes for
	NodeID  N              // the node to send the message to
}

// StatePoolWaiting indicates that the broadcast [Pool] holds broadcasts but none of them had
// work to hand out, so every one of them is waiting on a response.
type StatePoolWaiting struct {
	NextDue time.Time // the earliest time advancing the pool could make progress, zero if there is none
}

// StatePoolStoreRecord indicates that one of the pool's broadcasts wants the given message
// sent to the given node to store a record. The caller is expected to report the outcome
// with an [EventPoolStoreRecordSuccess] or [EventPoolStoreRecordFailure] event.
type StatePoolStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	NodeID  N              // the node to send the message to
	Message M              // the message that should be sent to the remote node
}

// StatePoolBroadcastFinished indicates that the broadcast operation with the id QueryID has
// finished. Contacted holds every node asked to store the record, and excludes the nodes
// queried to find the closest ones. Errors is keyed by the string representation of a node in
// Contacted and holds the error that node returned, so an operation in which every store
// succeeded carries an empty map.
type StatePoolBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID      // the id of the broadcast operation that has finished
	Contacted []N                 // all nodes asked to store the record, successful or not
	Errors    map[string]struct { // the error returned by any contacted node that failed
		Node N     // a node from the Contacted slice
		Err  error // the error that happened when contacting that Node
	}
	QueryStats query.QueryStats // the stats of the lookup that found the nodes, zero if none was run
}

// StatePoolIdle means that the broadcast [Pool] is not managing any broadcast
// operations at this time.
type StatePoolIdle struct{}

// poolState() ensures that only [PoolState]s can be returned by advancing the
// [Pool] state machine.
func (*StatePoolFindCloser[K, N]) poolState()        {}
func (*StatePoolWaiting) poolState()                 {}
func (*StatePoolStoreRecord[K, N, M]) poolState()    {}
func (*StatePoolBroadcastFinished[K, N]) poolState() {}
func (*StatePoolIdle) poolState()                    {}

// PoolEvent is an event intended to advance the state of the broadcast [Pool] state machine.
// An event flows into the pool and a state flows out of it.
type PoolEvent interface {
	poolEvent()
}

// EventPoolPoll is an event that signals the broadcast [Pool] state machine
// that it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// EventPoolStartFollowUp starts a broadcast that runs the [FollowUp] strategy, finding the nodes
// closest to the target key before storing the record with any of them.
type EventPoolStartFollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID // the unique id for this operation
	Target  K              // the key the record is stored under
	Message M              // the message that carries the record to the nodes it is stored with
	Seed    []N            // the closest nodes known so far, from where the query starts
}

// EventPoolStartStatic starts a broadcast that runs the [Static] strategy, storing the record
// with a fixed set of nodes.
type EventPoolStartStatic[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID // the unique id for this operation
	Target  K              // the key the record is stored under
	Message M              // the message that carries the record to the nodes it is stored with
	Nodes   []N            // the nodes the record is stored with
}

// EventPoolStartOptimistic starts a broadcast that runs the [Optimistic] strategy, storing the
// record with nodes as the walk towards the target key finds them.
type EventPoolStartOptimistic[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID     coordt.QueryID // the unique id for this operation
	Target      K              // the key the record is stored under
	Message     M              // the message that carries the record to the nodes it is stored with
	Seed        []N            // the closest nodes known so far, from where the walk starts
	NetworkSize int            // the estimated number of nodes in the network
}

// EventPoolStopBroadcast notifies broadcast [Pool] to stop a broadcast
// operation.
type EventPoolStopBroadcast struct {
	QueryID coordt.QueryID // the id of the broadcast operation that should be stopped
}

// EventPoolGetCloserNodesSuccess notifies a [Pool] that a remote node (NodeID)
// has successfully responded with closer nodes (CloserNodes) to the Target key
// for the broadcast operation with the given id (QueryID).
type EventPoolGetCloserNodesSuccess[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     coordt.QueryID // the id of the broadcast operation that this response belongs to
	NodeID      N              // the node the message was sent to and that replied
	Target      K              // the key that closer nodes are searched for
	CloserNodes []N            // the closer nodes sent by the node NodeID
}

// EventPoolGetCloserNodesFailure notifies a [Pool] that a remote node (NodeID)
// has failed responding with closer nodes to the Target key for the broadcast
// operation with the given id (QueryID).
type EventPoolGetCloserNodesFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the query that sent the message
	NodeID  N              // the node the message was sent to and that has replied
	Target  K              // the key that closer nodes are searched for
	Error   error          // the error that caused the failure, if any
}

// EventPoolStoreRecordSuccess notifies the broadcast [Pool] that storing a record with a
// remote node succeeded. Request holds the message that was sent. A response is optional,
// since a protocol need not confirm a store, so Response may be nil.
type EventPoolStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID  coordt.QueryID // the id of the query that sent the message
	NodeID   N              // the node the message was sent to
	Request  M              // the message that was sent to the remote node
	Response M              // the reply from the remote node, nil if the protocol sends none
}

// EventPoolStoreRecordFailure notifies the broadcast [Pool] that storing a record with a
// remote node failed. Request holds the message that was sent and Error the reason it failed.
type EventPoolStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID // the id of the query that sent the message
	NodeID  N              // the node the message was sent to
	Request M              // the message that was sent to the remote node
	Error   error          // the error that caused the failure
}

// poolEvent() ensures that only events accepted by a broadcast [Pool] can be
// assigned to the [PoolEvent] interface.
func (*EventPoolStopBroadcast) poolEvent()               {}
func (*EventPoolPoll) poolEvent()                        {}
func (*EventPoolStartFollowUp[K, N, M]) poolEvent()      {}
func (*EventPoolStartStatic[K, N, M]) poolEvent()        {}
func (*EventPoolStartOptimistic[K, N, M]) poolEvent()    {}
func (*EventPoolGetCloserNodesSuccess[K, N]) poolEvent() {}
func (*EventPoolGetCloserNodesFailure[K, N]) poolEvent() {}
func (*EventPoolStoreRecordSuccess[K, N, M]) poolEvent() {}
func (*EventPoolStoreRecordFailure[K, N, M]) poolEvent() {}
