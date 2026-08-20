package publish

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/query"
)

// The FollowUp state machine publishes a record in two phases, finding the nodes closest to
// a target key and then following up by storing the record with each of them.
//
// The first phase is a find closer nodes query, run in the query pool the state machine is
// given rather than one of its own. The state machine emits [StatePublishFindCloser] to
// ask for a node to be contacted, and expects the outcome as an [EventPublishNodeResponse]
// or [EventPublishNodeFailure] event.
//
// When that query finishes, every node it settled on becomes an outstanding piece of work
// for the second phase, and the state machine emits [StatePublishStoreRecord] to ask for
// the record to be stored with one of them, a single node per call. A query that finds no
// node at all skips the second phase and finishes immediately.
//
// The state machine expects to be notified of the outcome of each store with the
// [EventPublishStoreRecordSuccess] or [EventPublishStoreRecordFailure] events. A store
// request carries no deadline, so a node that never responds leaves the operation
// outstanding indefinitely. While the state machine is waiting in either phase, and has
// nothing it can hand out, it emits [StatePublishWaiting].
//
// Once no node is outstanding the state machine emits [StatePublishFinished], reporting
// every node contacted in the second phase and the error for any that failed. The nodes
// contacted to find the closest ones are not reported.
//
// The [EventPublishStop] event abandons the operation, cancelling the query if it is still
// running and recording every node not yet contacted or not yet heard from as having failed.
//
// This is the algorithm used by the original go-libp2p-kad-dht v1 code base.
type FollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// queryID is the unique id of this publish operation
	queryID coordt.QueryID

	// queryPool is the pool in which the find closer nodes query is run. It belongs to the
	// publish pool that created this state machine and is shared with its siblings, so
	// advancing it may produce work for another publish.
	queryPool *query.Pool[K, N, M]

	// msg is the message sent to each node to store the record
	msg M

	// seeds holds the nodes the find closer nodes query starts from
	seeds []N

	// closest holds the nodes the find closer nodes query settled on, and is filled when
	// that query finishes
	closest []N

	// queryStats holds the stats reported by the find closer nodes query when it ended
	queryStats query.QueryStats

	// todo holds the nodes that have yet to be asked to store the record, filled from closest
	todo map[string]N

	// waiting holds the nodes that have been asked to store the record but have yet to reply
	waiting map[string]N

	// success holds the nodes that stored the record
	success map[string]N

	// failed holds the nodes that did not store the record, with the error each returned
	failed map[string]struct {
		Node N
		Err  error
	}

	// tracer traces the execution of this state machine. It is supplied by the pool that
	// created it, since the per-publish configuration carries no telemetry of its own.
	tracer trace.Tracer
}

// NewFollowUp creates a state machine that publishes msg to the nodes closest to the target
// it is started with, querying from seeds in pool and reporting progress under the query id qid.
func NewFollowUp[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](qid coordt.QueryID, pool *query.Pool[K, N, M], msg M, seeds []N, tracer trace.Tracer) *FollowUp[K, N, M] {
	return &FollowUp[K, N, M]{
		queryID:   qid,
		queryPool: pool,
		tracer:    tracer,
		msg:       msg,
		seeds:     seeds,
		todo:      map[string]N{},
		waiting:   map[string]N{},
		success:   map[string]N{},
		failed: map[string]struct {
			Node N
			Err  error
		}{},
	}
}

// Advance advances the state of the [FollowUp] [Publish] state machine.
//
// An event belonging to the first phase is forwarded to the query pool, and whatever that
// pool reports is returned. Otherwise the state machine looks for a node still to be asked
// to store the record and emits that instruction, falling back to reporting that it is
// waiting or that it has finished.
func (f *FollowUp[K, N, M]) Advance(ctx context.Context, now time.Time, ev PublishEvent) (out PublishState) {
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

	_, isStopEvent := ev.(*EventPublishStop)
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
		return &StatePublishStoreRecord[K, N, M]{
			QueryID: f.queryID,
			NodeID:  n,
			Message: f.msg,
		}
	}

	if len(f.waiting) > 0 {
		// a store record request carries no deadline, so there is no time at which
		// advancing this state machine would make progress on its own
		return &StatePublishWaiting{}
	}

	if isStopEvent || (len(f.todo) == 0 && len(f.closest) != 0) {
		return &StatePublishFinished[K, N]{
			QueryID:    f.queryID,
			Contacted:  f.closest,
			Errors:     f.failed,
			QueryStats: f.queryStats,
		}
	}

	return &StatePublishIdle{}
}

// handleEvent receives a [PublishEvent] and returns the corresponding query
// pool event ([query.PoolEvent]). Some [PublishEvent] events don't map to
// a query pool event, in which case this method handles that event and returns
// nil.
func (f *FollowUp[K, N, M]) handleEvent(ctx context.Context, ev PublishEvent) (out query.PoolEvent) {
	_, span := f.tracer.Start(ctx, "FollowUp.handleEvent", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventPublishStart[K, N]:
		return &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID: f.queryID,
			Target:  ev.Target,
			Seed:    f.seeds,
		}
	case *EventPublishStop:
		if f.isQueryDone() {
			return nil
		}

		return &query.EventPoolStopQuery{
			QueryID: f.queryID,
		}
	case *EventPublishNodeResponse[K, N]:
		return &query.EventPoolNodeResponse[K, N]{
			QueryID:     f.queryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}
	case *EventPublishNodeFailure[K, N]:
		return &query.EventPoolNodeFailure[K, N]{
			QueryID: f.queryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}
	case *EventPublishStoreRecordSuccess[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.success[ev.NodeID.String()] = ev.NodeID
	case *EventPublishStoreRecordFailure[K, N, M]:
		delete(f.waiting, ev.NodeID.String())
		f.failed[ev.NodeID.String()] = struct {
			Node N
			Err  error
		}{Node: ev.NodeID, Err: ev.Error}
	case *EventPublishPoll:
		// ignore, nothing to do
		return &query.EventPoolPoll{}
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil
}

// advancePool advances the query pool with the event returned by [FollowUp.handleEvent] and
// translates the state it reports into a [PublishState]. The boolean reports whether that
// state is one the caller should return; when it is false the returned state is nil.
func (f *FollowUp[K, N, M]) advancePool(ctx context.Context, now time.Time, ev query.PoolEvent) (out PublishState, term bool) {
	ctx, span := f.tracer.Start(ctx, "FollowUp.advancePool", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	state := f.queryPool.Advance(ctx, now, ev)
	switch st := state.(type) {
	case *query.StatePoolFindCloser[K, N]:
		return &StatePublishFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}, true
	case *query.StatePoolWaitingAtCapacity:
		return &StatePublishWaiting{
			QueryID: f.queryID,
			NextDue: st.NextDue,
		}, true
	case *query.StatePoolWaitingWithCapacity:
		return &StatePublishWaiting{
			QueryID: f.queryID,
			NextDue: st.NextDue,
		}, true
	case *query.StatePoolQueryFinished[K, N]:
		f.queryStats = st.Stats

		if len(st.ClosestNodes) == 0 {
			return &StatePublishFinished[K, N]{
				QueryID:   f.queryID,
				Contacted: make([]N, 0),
				Errors: map[string]struct {
					Node N
					Err  error
				}{},
				QueryStats: f.queryStats,
			}, true
		}

		f.closest = st.ClosestNodes

		for _, n := range st.ClosestNodes {
			f.todo[n.String()] = n
		}

	case *query.StatePoolQueryTimeout:
		f.queryStats = st.Stats

		return &StatePublishFinished[K, N]{
			QueryID:   f.queryID,
			Contacted: make([]N, 0),
			Errors: map[string]struct {
				Node N
				Err  error
			}{},
			QueryStats: f.queryStats,
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
