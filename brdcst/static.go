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
// Every node in the set becomes an outstanding piece of work, and the state machine emits
// [StateBroadcastStoreRecord] to ask for the record to be stored with one of them, a single
// node per call.
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
	cfg *StaticConfig[K, N]

	// msg is the message sent to each node to store the record
	msg M

	// todo holds the nodes that have yet to be asked to store the record, taken from the
	// configured node set when the operation starts
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

// StaticConfig specifies the configuration for the [Static] state machine.
type StaticConfig[K kad.Key[K], N kad.NodeID[K]] struct {
	// Nodes is the set of nodes the record is stored with. It must hold at least one node.
	Nodes []N
}

func (c *StaticConfig[K, N]) broadcastConfig() {}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *StaticConfig[K, N]) Validate() error {
	if c == nil {
		return &coordt.ConfigurationError{
			Component: "StaticConfig",
			Err:       fmt.Errorf("config must not be nil"),
		}
	}

	if len(c.Nodes) == 0 {
		return &coordt.ConfigurationError{
			Component: "StaticConfig",
			Err:       fmt.Errorf("at least one node must be supplied"),
		}
	}

	return nil
}

// DefaultStaticConfig returns the default configuration options for the
// [Static] state machine.
func DefaultStaticConfig[K kad.Key[K], N kad.NodeID[K]]() *StaticConfig[K, N] {
	return &StaticConfig[K, N]{}
}

// NewStatic creates a state machine that broadcasts msg to the nodes in cfg, reporting progress
// under the query id qid.
func NewStatic[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](qid coordt.QueryID, msg M, cfg *StaticConfig[K, N], tracer trace.Tracer) *Static[K, N, M] {
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
		span.SetAttributes(attribute.Int("nodes", len(f.cfg.Nodes)))
		for _, n := range f.cfg.Nodes {
			f.todo[n.String()] = n
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
