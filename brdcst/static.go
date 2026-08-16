package brdcst

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
)

// The Static state machine broadcasts a record to a fixed set of nodes.
//
// The set is the seed list carried by the [EventBroadcastStart] event, and no query is run
// to discover any further nodes. Every seed becomes an outstanding piece of work, and the
// state machine emits [StateBroadcastStoreRecord] to ask for the record to be stored with
// one of them, a single node per call.
//
// The state machine expects to be notified of the outcome of each store with the
// [EventBroadcastStoreRecordSuccess] or [EventBroadcastStoreRecordFailure] events. A store
// request carries no deadline, so a node that never responds leaves the operation
// outstanding indefinitely. While any node is outstanding and none is left to contact the
// state machine emits [StateBroadcastWaiting].
//
// Once no node is outstanding the state machine emits [StateBroadcastFinished], reporting
// every node contacted and the error for any that failed.
//
// The [EventBroadcastStop] event abandons the operation, recording every node not yet
// contacted or not yet heard from as having failed.
type Static[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// queryID is the unique id of this broadcast operation
	queryID coordt.QueryID

	// cfg is the configuration supplied to the Static
	cfg *StaticConfig

	// msg is the message sent to each node to store the record
	msg M

	// todo holds the nodes that have yet to be asked to store the record, seeded from the
	// node list carried by the start event
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
	// created it, since the per-broadcast configuration carries no telemetry of its own.
	tracer trace.Tracer
}

// NewStatic creates a state machine that broadcasts msg to the nodes it is seeded with when
// it is started, reporting progress under the query id qid.
func NewStatic[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](qid coordt.QueryID, msg M, cfg *StaticConfig, tracer trace.Tracer) *Static[K, N, M] {
	return &Static[K, N, M]{
		queryID: qid,
		cfg:     cfg,
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

// Advance advances the state of the [Static] [Broadcast] state machine.
func (f *Static[K, N, M]) Advance(ctx context.Context, now time.Time, ev BroadcastEvent) (out BroadcastState) {
	_, span := f.tracer.Start(ctx, "Static.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(
			coordt.AttrOutEvent(out),
			attribute.Int("todo", len(f.todo)),
			attribute.Int("waiting", len(f.waiting)),
			attribute.Int("success", len(f.success)),
			attribute.Int("failed", len(f.failed)),
		)
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventBroadcastStart[K, N]:
		span.SetAttributes(attribute.Int("seed", len(ev.Seed)))
		for _, seed := range ev.Seed {
			f.todo[seed.String()] = seed
		}
	case *EventBroadcastStop:
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
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
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

	if len(f.todo) == 0 {
		contacted := make([]N, 0, len(f.success)+len(f.failed))
		for _, n := range f.success {
			contacted = append(contacted, n)
		}
		for _, n := range f.failed {
			contacted = append(contacted, n.Node)
		}

		return &StateBroadcastFinished[K, N]{
			QueryID:   f.queryID,
			Contacted: contacted,
			Errors:    f.failed,
		}
	}

	return &StateBroadcastIdle{}
}
