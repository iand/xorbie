package coord

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/internal/coord/brdcst"
	"github.com/iand/xorbie/internal/coord/coordt"
)

type BroadcastConfig[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter

	// QueueCapacity is the maximum number of events that may be waiting to be processed by
	// the behaviour. Events arriving when the queue is full are dropped. It must be larger
	// than [NetworkConfig.Capacity], since a node handler queues a response here before
	// releasing the capacity it held, so that many responses can be waiting at once.
	QueueCapacity int

	// VerifyResponse reports whether a node's reply to a stored record shows that it stored
	// the record, returning a nil error when it did. A nil VerifyResponse takes every reply
	// that is not itself an error as a success.
	VerifyResponse func(req, resp M) error
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BroadcastConfig[K, N, M]) Validate() error {
	if cfg.Logger == nil {
		return &coordt.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &coordt.ConfigurationError{
			Component: "BroadcastConfig",
			Err:       fmt.Errorf("queue capacity must be greater than zero"),
		}
	}

	return nil
}

func DefaultBroadcastConfig[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]]() *BroadcastConfig[K, N, M] {
	return &BroadcastConfig[K, N, M]{
		Logger:        slog.Default(),
		Tracer:        coordt.NoopTracer(),
		Meter:         coordt.NoopMeter(),
		QueueCapacity: 1024, // MAGIC
	}
}

type PooledBroadcastBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	logger *slog.Logger
	tracer trace.Tracer

	// verifyResponse reports whether a reply shows that a record was stored.
	verifyResponse func(req, resp M) error

	// performMu is held while Perform is executing to ensure sequential execution of work.
	performMu sync.Mutex

	// pool is the broadcast pool state machine used for managing individual broadcasts.
	// it must only be accessed while performMu is held
	pool coordt.StateMachine[brdcst.PoolEvent, brdcst.PoolState]

	// pendingOutbound is a queue of outbound events.
	// it must only be accessed while performMu is held
	pendingOutbound []BehaviourEvent

	// notifiers is a map that keeps track of event notifications for each running broadcast.
	// it must only be accessed while performMu is held
	notifiers map[coordt.QueryID]*queryNotifier[K, N, M, *EventBroadcastFinished[K, N]]

	// inbound is a bounded queue of inbound events that are awaiting processing
	inbound *inboundQueue

	// counterInboundDropped counts the events dropped because the inbound queue was full.
	counterInboundDropped metric.Int64Counter

	// gaugeInboundDepth tracks the number of events waiting in the inbound queue.
	gaugeInboundDepth metric.Int64ObservableGauge

	// nextDue is the time the broadcast pool last reported it could next make progress
	// without an event arriving, or the zero time if it reported none.
	// it must only be accessed while performMu is held
	nextDue time.Time

	// pollAgain records that the pool reported a broadcast ending rather than a due time,
	// so nextDue is stale until the pool is advanced again.
	// it must only be accessed while performMu is held
	pollAgain bool

	ready chan struct{}

	// readyTimer signals ready when the pool's next due time arrives.
	readyTimer *readyTimer
}

func NewPooledBroadcastBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](brdcstPool *brdcst.Pool[K, N, M], cfg *BroadcastConfig[K, N, M]) (*PooledBroadcastBehaviour[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultBroadcastConfig[K, N, M]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := &PooledBroadcastBehaviour[K, N, M]{
		pool:      brdcstPool,
		notifiers: make(map[coordt.QueryID]*queryNotifier[K, N, M, *EventBroadcastFinished[K, N]]),
		inbound:   newInboundQueue(cfg.QueueCapacity),
		ready:     make(chan struct{}, 1),
		logger:    cfg.Logger.With("behaviour", "pooledBroadcast"),
		tracer:    cfg.Tracer,

		verifyResponse: cfg.VerifyResponse,
	}

	if b.verifyResponse == nil {
		b.verifyResponse = func(req, resp M) error { return nil }
	}

	var err error

	b.counterInboundDropped, err = cfg.Meter.Int64Counter(
		"broadcast_inbound_events_dropped",
		metric.WithDescription("Total number of events dropped because the broadcast behaviour's inbound queue was full"),
	)
	if err != nil {
		return nil, fmt.Errorf("create broadcast_inbound_events_dropped counter: %w", err)
	}

	b.gaugeInboundDepth, err = cfg.Meter.Int64ObservableGauge(
		"broadcast_inbound_queue_depth",
		metric.WithDescription("Number of events waiting in the broadcast behaviour's inbound queue"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(b.inbound.depth.Load())
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create broadcast_inbound_queue_depth gauge: %w", err)
	}

	b.readyTimer = newReadyTimer(b.ready)

	return b, nil
}

func (b *PooledBroadcastBehaviour[K, N, M]) Ready() <-chan struct{} {
	return b.ready
}

func (b *PooledBroadcastBehaviour[K, N, M]) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.Notify")
	defer span.End()

	if !b.inbound.enqueue(CtxEvent[BehaviourEvent]{Ctx: ctx, Event: ev}) {
		b.counterInboundDropped.Add(ctx, 1)
		b.logger.Debug("dropped inbound event", slog.String("event", fmt.Sprintf("%T", ev)))
		b.reportDropped(ctx, ev)
		return
	}

	select {
	case b.ready <- struct{}{}:
	default:
	}
}

// reportDropped tells the caller of a dropped operation that it will not be carried out. An
// event that starts a broadcast leaves a caller waiting on its monitor for a terminal event
// that would otherwise never arrive.
func (b *PooledBroadcastBehaviour[K, N, M]) reportDropped(ctx context.Context, ev BehaviourEvent) {
	sev, ok := ev.(*EventStartBroadcast[K, N, M])
	if !ok || sev.Notify == nil {
		return
	}

	n := &queryNotifier[K, N, M, *EventBroadcastFinished[K, N]]{monitor: sev.Notify}
	n.NotifyFinished(ctx, &EventBroadcastFinished[K, N]{QueryID: sev.QueryID, Err: ErrEventDropped})
}

func (b *PooledBroadcastBehaviour[K, N, M]) Perform(ctx context.Context) (out BehaviourEvent, performed bool) {
	b.performMu.Lock()
	defer b.performMu.Unlock()

	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.Perform")
	defer span.End()

	defer func() { b.updateReadyStatus(performed) }()

	// first send any pending query notifications
	for _, w := range b.notifiers {
		w.DrainPending()
	}

	// drain queued outbound events before starting new work.
	ev, ok := b.nextPendingOutbound()
	if ok {
		return ev, true
	}

	// perform one piece of pending inbound work.
	ev, ok = b.perfomNextInbound(ctx)
	if ok {
		return ev, true
	}

	// poll the broadcast pool to trigger any timeouts and other scheduled work
	ev, ok = b.advancePool(ctx, time.Now(), &brdcst.EventPoolPoll{})
	if ok {
		return ev, true
	}

	// return any queued outbound work that may have been generated
	return b.nextPendingOutbound()
}

func (b *PooledBroadcastBehaviour[K, N, M]) nextPendingOutbound() (BehaviourEvent, bool) {
	if len(b.pendingOutbound) == 0 {
		return nil, false
	}
	var ev BehaviourEvent
	ev, b.pendingOutbound = b.pendingOutbound[0], b.pendingOutbound[1:]
	return ev, true
}

func (b *PooledBroadcastBehaviour[K, N, M]) nextPendingInbound() (CtxEvent[BehaviourEvent], bool) {
	return b.inbound.dequeue()
}

// updateReadyStatus signals whether the behaviour has further work to do. It is
// called at the end of every Perform, passing whether that call produced an
// event.
//
// A Perform that produced an event may be able to produce another one straight
// away: the broadcast pool dispatches at most one message per advance, so a
// broadcast with several seed nodes needs several calls to contact them all.
// The event loop only calls Perform in response to a ready signal, so without
// re-signalling here a broadcast would contact one node and then wait for that
// node's response before contacting the next.
//
// A behaviour with no work to do arms a timer for the broadcast's next due time.
func (b *PooledBroadcastBehaviour[K, N, M]) updateReadyStatus(performed bool) {
	if performed || b.pollAgain || len(b.pendingOutbound) != 0 {
		signalReady(b.ready)
		return
	}

	if !b.inbound.empty() {
		signalReady(b.ready)
		return
	}

	b.readyTimer.Arm(b.nextDue)
}

func (b *PooledBroadcastBehaviour[K, N, M]) perfomNextInbound(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.perfomNextInbound")
	defer span.End()
	pev, ok := b.nextPendingInbound()
	if !ok {
		return nil, false
	}

	var cmd brdcst.PoolEvent
	switch ev := pev.Event.(type) {
	case *EventStartBroadcast[K, N, M]:
		cmd = &brdcst.EventPoolStartBroadcast[K, N, M]{
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Message: ev.Message,
			Seed:    ev.Seed,
			Config:  ev.Config,
		}
		if ev.Notify != nil {
			b.notifiers[ev.QueryID] = &queryNotifier[K, N, M, *EventBroadcastFinished[K, N]]{monitor: ev.Notify}
		}

	case *EventGetCloserNodesSuccess[K, N]:
		for _, info := range ev.CloserNodes {
			b.pendingOutbound = append(b.pendingOutbound, &EventAddNode[K, N]{
				NodeID: info,
			})
		}

		waiter, ok := b.notifiers[ev.QueryID]
		if ok {
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed[K, N, M]{
				NodeID:  ev.To,
				QueryID: ev.QueryID,
			})
		}

		cmd = &brdcst.EventPoolGetCloserNodesSuccess[K, N]{
			NodeID:      ev.To,
			QueryID:     ev.QueryID,
			Target:      ev.Target,
			CloserNodes: ev.CloserNodes,
		}

	case *EventGetCloserNodesFailure[K, N]:
		// queue an event that will notify the routing behaviour of a failed node
		b.pendingOutbound = append(b.pendingOutbound, &EventNotifyNonConnectivity[K, N]{
			ev.To,
		})

		cmd = &brdcst.EventPoolGetCloserNodesFailure[K, N]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Error:   ev.Err,
		}

	case *EventSendMessageSuccess[K, N, M]:
		for _, info := range ev.CloserNodes {
			b.pendingOutbound = append(b.pendingOutbound, &EventAddNode[K, N]{
				NodeID: info,
			})
		}
		waiter, ok := b.notifiers[ev.QueryID]
		if ok {
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed[K, N, M]{
				NodeID:   ev.To,
				QueryID:  ev.QueryID,
				Response: ev.Response,
			})
		}
		if err := b.verifyResponse(ev.Request, ev.Response); err != nil {
			cmd = &brdcst.EventPoolStoreRecordFailure[K, N, M]{
				QueryID: ev.QueryID,
				NodeID:  ev.To,
				Request: ev.Request,
				Error:   err,
			}
			break
		}

		// TODO: How do we know it's a StoreRecord response?
		cmd = &brdcst.EventPoolStoreRecordSuccess[K, N, M]{
			QueryID:  ev.QueryID,
			NodeID:   ev.To,
			Request:  ev.Request,
			Response: ev.Response,
		}

	case *EventSendMessageFailure[K, N, M]:
		// queue an event that will notify the routing behaviour of a failed node
		b.pendingOutbound = append(b.pendingOutbound, &EventNotifyNonConnectivity[K, N]{
			ev.To,
		})

		// TODO: How do we know it's a StoreRecord response?
		cmd = &brdcst.EventPoolStoreRecordFailure[K, N, M]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Request: ev.Request,
			Error:   ev.Err,
		}

	case *EventStopQuery:
		cmd = &brdcst.EventPoolStopBroadcast{
			QueryID: ev.QueryID,
		}
	}

	// attempt to advance the broadcast pool
	return b.advancePool(ctx, time.Now(), cmd)
}

func (b *PooledBroadcastBehaviour[K, N, M]) advancePool(ctx context.Context, now time.Time, ev brdcst.PoolEvent) (out BehaviourEvent, term bool) {
	ctx, span := b.tracer.Start(ctx, "PooledBroadcastBehaviour.advancePool", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	b.pollAgain = false

	pstate := b.pool.Advance(ctx, now, ev)
	switch st := pstate.(type) {
	case *brdcst.StatePoolIdle:
		// nothing to do
		b.nextDue = time.Time{}
	case *brdcst.StatePoolWaiting:
		// nothing to do except wait for message responses or timeouts
		b.nextDue = st.NextDue
	case *brdcst.StatePoolFindCloser[K, N]:
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  b,
		}, true
	case *brdcst.StatePoolStoreRecord[K, N, M]:
		return &EventOutboundSendMessage[K, N, M]{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Message: st.Message,
			Notify:  b,
		}, true
	case *brdcst.StatePoolBroadcastFinished[K, N]:
		// the state carries no due time and the pool has removed the broadcast, so the
		// pool must be advanced again to report when the remaining broadcasts are next due
		b.pollAgain = true
		waiter, ok := b.notifiers[st.QueryID]
		if ok {
			waiter.NotifyFinished(ctx, &EventBroadcastFinished[K, N]{
				QueryID:   st.QueryID,
				Contacted: st.Contacted,
				Errors:    st.Errors,
			})
			delete(b.notifiers, st.QueryID)
		}
	}

	return nil, false
}

// A BroadcastWaiter implements [QueryMonitor] for broadcasts
type BroadcastWaiter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	progressed chan CtxEvent[*EventQueryProgressed[K, N, M]]
	finished   chan CtxEvent[*EventBroadcastFinished[K, N]]
}

func NewBroadcastWaiter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](n int) *BroadcastWaiter[K, N, M] {
	w := &BroadcastWaiter[K, N, M]{
		progressed: make(chan CtxEvent[*EventQueryProgressed[K, N, M]], n),
		finished:   make(chan CtxEvent[*EventBroadcastFinished[K, N]], 1),
	}
	return w
}

func (w *BroadcastWaiter[K, N, M]) Progressed() <-chan CtxEvent[*EventQueryProgressed[K, N, M]] {
	return w.progressed
}

func (w *BroadcastWaiter[K, N, M]) Finished() <-chan CtxEvent[*EventBroadcastFinished[K, N]] {
	return w.finished
}

func (w *BroadcastWaiter[K, N, M]) NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed[K, N, M]] {
	return w.progressed
}

func (w *BroadcastWaiter[K, N, M]) NotifyFinished() chan<- CtxEvent[*EventBroadcastFinished[K, N]] {
	return w.finished
}
