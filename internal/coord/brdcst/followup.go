package brdcst

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/internal/coord/coordt"
	"github.com/iand/xorbie/internal/coord/query"
)

// FollowUp is a [Broadcast] state machine and encapsulates the logic around
// doing a "classic" put operation. This mimics the algorithm employed in the
// original go-libp2p-kad-dht v1 code base. It first queries the closest nodes
// to a certain target key, and after they were discovered, it "follows up" with
// storing the record with these closest nodes.
type FollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// the unique ID for this broadcast operation
	queryID coordt.QueryID

	// a struct holding configuration options
	cfg *ConfigFollowUp

	// a reference to the query pool in which the "get closer nodes" queries
	// will be spawned. This pool is governed by the broadcast [Pool].
	// Unfortunately, having a reference here breaks the hierarchy but it makes
	// the logic much easier to implement.
	pool *query.Pool[K, N, M]

	// the message that we will send to the closest nodes in the follow-up phase
	msg M

	// the closest nodes to the target key. This will be filled after the query
	// for the closest nodes has finished (when the query pool emits a
	// [query.StatePoolQueryFinished] event).
	closest []N

	// nodes we still need to store records with. This map will be filled with
	// all the closest nodes after the query has finished.
	todo map[string]N

	// nodes we have contacted to store the record but haven't heard a response yet
	waiting map[string]N

	// nodes that successfully hold the record for us
	success map[string]N

	// nodes that failed to hold the record for us
	failed map[string]struct {
		Node N
		Err  error
	}

	// tracer traces the execution of this state machine. It is supplied by the
	// pool that created it, since the per-broadcast configuration carries no
	// telemetry of its own.
	tracer trace.Tracer
}

// NewFollowUp initializes a new [FollowUp] struct.
func NewFollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](qid coordt.QueryID, pool *query.Pool[K, N, M], msg M, cfg *ConfigFollowUp, tracer trace.Tracer) *FollowUp[K, N, M] {
	return &FollowUp[K, N, M]{
		queryID: qid,
		cfg:     cfg,
		pool:    pool,
		tracer:  tracer,
		msg:     msg,
		todo:    map[string]N{},
		waiting: map[string]N{},
		success: map[string]N{},
		failed: map[string]struct {
			Node N
			Err  error
		}{},
	}
}

// Advance advances the state of the [FollowUp] [Broadcast] state machine. It
// first handles the event by mapping it to a potential event for the query
// pool. If the [BroadcastEvent] maps to a [query.PoolEvent], it gets forwarded
// to the query pool and handled in [FollowUp.advancePool]. If it doesn't map to
// a query pool event, we check if there are any nodes we should contact to hold
// the record for us and emit that instruction instead. Similarly, if we're
// waiting on responses or are completely finished, we return that as well.
func (f *FollowUp[K, N, M]) Advance(ctx context.Context, now time.Time, ev BroadcastEvent) (out BroadcastState) {
	ctx, span := f.tracer.Start(ctx, "FollowUp.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	pev := f.handleEvent(ctx, ev)
	if pev != nil {
		if state, terminal := f.advancePool(ctx, now, pev); terminal {
			return state
		}
	}

	_, isStopEvent := ev.(*EventBroadcastStop)
	if isStopEvent {
		for _, n := range f.todo {
			delete(f.todo, n.String())
			f.failed[n.String()] = struct {
				Node N
				Err  error
			}{Node: n, Err: fmt.Errorf("cancelled")}
		}

		for _, n := range f.waiting {
			delete(f.waiting, n.String())
			f.failed[n.String()] = struct {
				Node N
				Err  error
			}{Node: n, Err: fmt.Errorf("cancelled")}
		}
	}

	for k, n := range f.todo {
		delete(f.todo, k)
		f.waiting[k] = n
		return &StateBroadcastStoreRecord[K, N, M]{
			QueryID: f.queryID,
			NodeID:  n,
			Message: f.msg,
		}
	}

	if len(f.waiting) > 0 {
		// a store record request carries no deadline, so there is no time at which
		// advancing this state machine would make progress on its own
		return &StateBroadcastWaiting{}
	}

	if isStopEvent || (len(f.todo) == 0 && len(f.closest) != 0) {
		return &StateBroadcastFinished[K, N]{
			QueryID:   f.queryID,
			Contacted: f.closest,
			Errors:    f.failed,
		}
	}

	return &StateBroadcastIdle{}
}

// handleEvent receives a [BroadcastEvent] and returns the corresponding query
// pool event ([query.PoolEvent]). Some [BroadcastEvent] events don't map to
// a query pool event, in which case this method handles that event and returns
// nil.
func (f *FollowUp[K, N, M]) handleEvent(ctx context.Context, ev BroadcastEvent) (out query.PoolEvent) {
	_, span := f.tracer.Start(ctx, "FollowUp.handleEvent", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventBroadcastStart[K, N]:
		return &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID: f.queryID,
			Target:  ev.Target,
			Seed:    ev.Seed,
		}
	case *EventBroadcastStop:
		if f.isQueryDone() {
			return nil
		}

		return &query.EventPoolStopQuery{
			QueryID: f.queryID,
		}
	case *EventBroadcastNodeResponse[K, N]:
		return &query.EventPoolNodeResponse[K, N]{
			QueryID:     f.queryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}
	case *EventBroadcastNodeFailure[K, N]:
		return &query.EventPoolNodeFailure[K, N]{
			QueryID: f.queryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}
	case *EventBroadcastStoreRecordSuccess[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.success[ev.NodeID.String()] = ev.NodeID
	case *EventBroadcastStoreRecordFailure[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.failed[ev.NodeID.String()] = struct {
			Node N
			Err  error
		}{Node: ev.NodeID, Err: ev.Error}
	case *EventBroadcastPoll:
		// ignore, nothing to do
		return &query.EventPoolPoll{}
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil
}

// advancePool advances the query pool with the given query pool event that was
// returned by [FollowUp.handleEvent]. The additional boolean value indicates
// whether the returned [BroadcastState] should be ignored.
func (f *FollowUp[K, N, M]) advancePool(ctx context.Context, now time.Time, ev query.PoolEvent) (out BroadcastState, term bool) {
	ctx, span := f.tracer.Start(ctx, "FollowUp.advanceQuery", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	state := f.pool.Advance(ctx, now, ev)
	switch st := state.(type) {
	case *query.StatePoolFindCloser[K, N]:
		return &StateBroadcastFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}, true
	case *query.StatePoolWaitingAtCapacity:
		return &StateBroadcastWaiting{
			QueryID: f.queryID,
			NextDue: st.NextDue,
		}, true
	case *query.StatePoolWaitingWithCapacity:
		return &StateBroadcastWaiting{
			QueryID: f.queryID,
			NextDue: st.NextDue,
		}, true
	case *query.StatePoolQueryFinished[K, N]:
		if len(st.ClosestNodes) == 0 {
			return &StateBroadcastFinished[K, N]{
				QueryID:   f.queryID,
				Contacted: make([]N, 0),
				Errors: map[string]struct {
					Node N
					Err  error
				}{},
			}, true
		}

		f.closest = st.ClosestNodes

		for _, n := range st.ClosestNodes {
			f.todo[n.String()] = n
		}

	case *query.StatePoolQueryTimeout:
		return &StateBroadcastFinished[K, N]{
			QueryID:   f.queryID,
			Contacted: make([]N, 0),
			Errors: map[string]struct {
				Node N
				Err  error
			}{},
		}, true
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	return nil, false
}

// isQueryDone returns true if the DHT walk/ query phase has finished.
// This is indicated by the fact that the [FollowUp.closest] slice is filled.
func (f *FollowUp[K, N, M]) isQueryDone() bool {
	return len(f.closest) != 0
}
