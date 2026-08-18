package xorbie

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/netsize"
	"github.com/iand/xorbie/query"
)

type QueryConfig[K kad.Key[K], N kad.NodeID[K]] struct {
	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter

	// NetworkSize is the estimator that the results of completed queries are reported to.
	// A nil estimator means results are not reported.
	NetworkSize *netsize.Estimator[K, N]

	// QueueCapacity is the maximum number of events that may be waiting to be processed by
	// the behaviour. Events arriving when the queue is full are dropped. It must be larger
	// than [NetworkConfig.Capacity], since a node handler queues a response here before
	// releasing the capacity it held, so that many responses can be waiting at once.
	QueueCapacity int

	// Concurrency is the maximum number of queries that may be waiting for message responses at any one time.
	Concurrency int

	// Timeout the time to wait before terminating a query that is not making progress.
	Timeout time.Duration

	// RequestConcurrency is the maximum number of concurrent requests that each query may have in flight.
	RequestConcurrency int

	// RequestTimeout is the timeout queries should use for contacting a single node
	RequestTimeout time.Duration
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *QueryConfig[K, N]) Validate() error {
	if cfg.Logger == nil {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("queue capacity must be greater than zero"),
		}
	}

	if cfg.Concurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("query concurrency must be greater than zero"),
		}
	}
	if cfg.Timeout < 1 {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("query timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "PooledQueryConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	return nil
}

func DefaultQueryConfig[K kad.Key[K], N kad.NodeID[K]]() *QueryConfig[K, N] {
	return &QueryConfig[K, N]{
		Logger:             slog.Default(),
		Tracer:             coordt.NoopTracer(),
		Meter:              coordt.NoopMeter(),
		QueueCapacity:      1024,            // MAGIC
		Concurrency:        3,               // MAGIC
		Timeout:            5 * time.Minute, // MAGIC
		RequestConcurrency: 3,               // MAGIC
		RequestTimeout:     time.Minute,     // MAGIC

	}
}

// QueryBehaviour holds the behaviour and state for managing a pool of queries.
type QueryBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// cfg is a copy of the optional configuration supplied to the behaviour.
	cfg QueryConfig[K, N]

	// performMu is held while Perform is executing to ensure sequential execution of work.
	performMu sync.Mutex

	// pool is the query pool state machine used for managing individual queries.
	// it must only be accessed while performMu is held
	pool *query.Pool[K, N, M]

	// notifiers is a map that keeps track of event notifications for each running query.
	// it must only be accessed while performMu is held
	notifiers map[coordt.QueryID]*queryNotifier[K, N, M, *EventQueryFinished[K, N]]

	// pendingOutbound is a queue of outbound events.
	// it must only be accessed while performMu is held
	pendingOutbound []BehaviourEvent

	// inbound is a bounded queue of inbound events that are awaiting processing
	inbound *inboundQueue

	// counterInboundDropped counts the events dropped because the inbound queue was full.
	counterInboundDropped metric.Int64Counter

	// gaugeInboundDepth tracks the number of events waiting in the inbound queue.
	gaugeInboundDepth metric.Int64ObservableGauge

	// nextDue is the time the query pool last reported it could next make progress
	// without an event arriving, or the zero time if it reported none.
	// it must only be accessed while performMu is held
	nextDue time.Time

	// pollAgain records that the pool reported a query ending rather than a due time, so
	// nextDue is stale until the pool is advanced again.
	// it must only be accessed while performMu is held
	pollAgain bool

	// ready is a channel signaling that the behaviour has work to perform.
	ready chan struct{}

	// readyTimer signals ready when the pool's next due time arrives.
	readyTimer *readyTimer
}

// NewQueryBehaviour initialises a new [QueryBehaviour], setting up the query
// pool and other internal state.
func NewQueryBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, cfg *QueryConfig[K, N]) (*QueryBehaviour[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultQueryConfig[K, N]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	qpCfg := query.DefaultPoolConfig()
	qpCfg.Concurrency = cfg.Concurrency
	qpCfg.Timeout = cfg.Timeout
	qpCfg.QueryConcurrency = cfg.RequestConcurrency
	qpCfg.RequestTimeout = cfg.RequestTimeout
	qpCfg.Tracer = cfg.Tracer

	pool, err := query.NewPool[K, N, M](self, qpCfg)
	if err != nil {
		return nil, fmt.Errorf("query pool: %w", err)
	}

	h := &QueryBehaviour[K, N, M]{
		cfg:       *cfg,
		pool:      pool,
		notifiers: make(map[coordt.QueryID]*queryNotifier[K, N, M, *EventQueryFinished[K, N]]),
		inbound:   newInboundQueue(cfg.QueueCapacity),
		ready:     make(chan struct{}, 1),
	}

	h.counterInboundDropped, err = cfg.Meter.Int64Counter(
		"query_inbound_events_dropped",
		metric.WithDescription("Total number of events dropped because the query behaviour's inbound queue was full"),
	)
	if err != nil {
		return nil, fmt.Errorf("create query_inbound_events_dropped counter: %w", err)
	}

	h.gaugeInboundDepth, err = cfg.Meter.Int64ObservableGauge(
		"query_inbound_queue_depth",
		metric.WithDescription("Number of events waiting in the query behaviour's inbound queue"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(h.inbound.depth.Load())
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create query_inbound_queue_depth gauge: %w", err)
	}

	h.readyTimer = newReadyTimer(h.ready)

	return h, nil
}

// Notify receives a behaviour event and takes appropriate actions such as starting,
// stopping, or updating queries. It also queues events for later processing and
// triggers the advancement of the query pool if applicable.
func (p *QueryBehaviour[K, N, M]) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := p.cfg.Tracer.Start(ctx, "PooledQueryBehaviour.Notify")
	defer span.End()

	if !p.inbound.enqueue(CtxEvent[BehaviourEvent]{Ctx: ctx, Event: ev}) {
		p.counterInboundDropped.Add(ctx, 1)
		p.cfg.Logger.Debug("dropped inbound event", slog.String("event", fmt.Sprintf("%T", ev)))
		p.reportDropped(ctx, ev)
		return
	}

	select {
	case p.ready <- struct{}{}:
	default:
	}
}

// reportDropped tells the caller of a dropped operation that it will not be carried out. An
// event that starts a query leaves a caller waiting on its monitor for a terminal event that
// would otherwise never arrive.
func (p *QueryBehaviour[K, N, M]) reportDropped(ctx context.Context, ev BehaviourEvent) {
	var queryID coordt.QueryID
	var monitor QueryMonitor[K, N, M, *EventQueryFinished[K, N]]

	switch ev := ev.(type) {
	case *EventStartFindCloserQuery[K, N, M]:
		queryID, monitor = ev.QueryID, ev.Notify
	case *EventStartMessageQuery[K, N, M]:
		queryID, monitor = ev.QueryID, ev.Notify
	default:
		return
	}

	if monitor == nil {
		return
	}

	n := &queryNotifier[K, N, M, *EventQueryFinished[K, N]]{monitor: monitor}
	n.NotifyFinished(ctx, &EventQueryFinished[K, N]{QueryID: queryID, Err: ErrEventDropped})
}

// Ready returns a channel that signals when the pooled query behaviour is ready to
// perform work.
func (p *QueryBehaviour[K, N, M]) Ready() <-chan struct{} {
	return p.ready
}

// Perform executes the next available task from the queue of pending events or advances
// the query pool. Returns an event containing the result of the work performed and a
// true value, or nil and a false value if no event was generated.
func (p *QueryBehaviour[K, N, M]) Perform(ctx context.Context) (out BehaviourEvent, performed bool) {
	p.performMu.Lock()
	defer p.performMu.Unlock()

	ctx, span := p.cfg.Tracer.Start(ctx, "PooledQueryBehaviour.Perform")
	defer span.End()

	defer func() { p.updateReadyStatus(performed) }()

	// first send any pending query notifications
	for _, w := range p.notifiers {
		w.DrainPending()
	}

	// drain queued outbound events before starting new work.
	ev, ok := p.nextPendingOutbound()
	if ok {
		return ev, true
	}

	// perform one piece of pending inbound work.
	ev, ok = p.perfomNextInbound(ctx)
	if ok {
		return ev, true
	}

	// poll the query pool to trigger any timeouts and other scheduled work
	ev, ok = p.advancePool(ctx, time.Now(), &query.EventPoolPoll{})
	if ok {
		return ev, true
	}

	// return any queued outbound work that may have been generated
	return p.nextPendingOutbound()
}

func (p *QueryBehaviour[K, N, M]) nextPendingOutbound() (BehaviourEvent, bool) {
	if len(p.pendingOutbound) == 0 {
		return nil, false
	}
	var ev BehaviourEvent
	ev, p.pendingOutbound = p.pendingOutbound[0], p.pendingOutbound[1:]
	return ev, true
}

func (p *QueryBehaviour[K, N, M]) nextPendingInbound() (CtxEvent[BehaviourEvent], bool) {
	return p.inbound.dequeue()
}

func (p *QueryBehaviour[K, N, M]) perfomNextInbound(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := p.cfg.Tracer.Start(ctx, "PooledQueryBehaviour.perfomNextInbound")
	defer span.End()
	pev, ok := p.nextPendingInbound()
	if !ok {
		return nil, false
	}

	var cmd query.PoolEvent = &query.EventPoolPoll{}

	switch ev := pev.Event.(type) {
	case *EventStartFindCloserQuery[K, N, M]:
		cmd = &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Seed:    ev.KnownClosestNodes,
		}
		if ev.Notify != nil {
			p.notifiers[ev.QueryID] = &queryNotifier[K, N, M, *EventQueryFinished[K, N]]{monitor: ev.Notify}
		}
	case *EventStartMessageQuery[K, N, M]:
		cmd = &query.EventPoolAddQuery[K, N, M]{
			QueryID: ev.QueryID,
			Target:  ev.Target,
			Message: ev.Message,
			Seed:    ev.KnownClosestNodes,
		}
		if ev.Notify != nil {
			p.notifiers[ev.QueryID] = &queryNotifier[K, N, M, *EventQueryFinished[K, N]]{monitor: ev.Notify}
		}
	case *EventStopQuery:
		cmd = &query.EventPoolStopQuery{
			QueryID: ev.QueryID,
		}
	case *EventGetCloserNodesSuccess[K, N]:
		p.queueAddNodeEvents(ev.CloserNodes)
		waiter, ok := p.notifiers[ev.QueryID]
		if ok {
			// The pool has not advanced on this response yet, so its stats do not count it. This
			// notification is a successful response, so the copy on the event counts it.
			stats, _ := p.pool.Stats(ev.QueryID)
			stats.Success++
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed[K, N, M]{
				NodeID:  ev.To,
				QueryID: ev.QueryID,
				// CloserNodes: CloserNodeIDs(ev.CloserNodes),
				Stats: stats,
			})
		}
		cmd = &query.EventPoolNodeResponse[K, N]{
			NodeID:      ev.To,
			QueryID:     ev.QueryID,
			CloserNodes: ev.CloserNodes,
		}
	case *EventGetCloserNodesFailure[K, N]:
		// queue an event that will notify the routing behaviour of a failed node
		p.cfg.Logger.Debug("node has no connectivity", logAttrNodeID(ev.To), "source", "query")
		p.queueNonConnectivityEvent(ev.To)

		cmd = &query.EventPoolNodeFailure[K, N]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Error:   ev.Err,
		}
	case *EventSendMessageSuccess[K, N, M]:
		p.queueAddNodeEvents(ev.CloserNodes)
		waiter, ok := p.notifiers[ev.QueryID]
		if ok {
			// The pool has not advanced on this response yet, so its stats do not count it. This
			// notification is a successful response, so the copy on the event counts it.
			stats, _ := p.pool.Stats(ev.QueryID)
			stats.Success++
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed[K, N, M]{
				NodeID:   ev.To,
				QueryID:  ev.QueryID,
				Response: ev.Response,
				Stats:    stats,
			})
		}
		cmd = &query.EventPoolNodeResponse[K, N]{
			NodeID:      ev.To,
			QueryID:     ev.QueryID,
			CloserNodes: ev.CloserNodes,
		}
	case *EventSendMessageFailure[K, N, M]:
		// queue an event that will notify the routing behaviour of a failed node
		p.cfg.Logger.Debug("node has no connectivity", logAttrNodeID(ev.To), "source", "query")
		p.queueNonConnectivityEvent(ev.To)

		cmd = &query.EventPoolNodeFailure[K, N]{
			NodeID:  ev.To,
			QueryID: ev.QueryID,
			Error:   ev.Err,
		}
	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	// attempt to advance the query pool
	return p.advancePool(pev.Ctx, time.Now(), cmd)
}

// updateReadyStatus signals whether the behaviour has further work to do. It is
// called at the end of every Perform, passing whether that call produced an
// event.
//
// A Perform that produced an event may be able to produce another one straight
// away: the query pool dispatches at most one request per advance, so a query
// with spare request concurrency and unqueried nodes needs several calls to
// reach its configured concurrency. The event loop only calls Perform in
// response to a ready signal, so without re-signalling here the pool would sit
// on those nodes until some external event arrived, holding every query to one
// request in flight regardless of configuration.
//
// A behaviour with no work to do arms a timer for the query's next due time.
func (p *QueryBehaviour[K, N, M]) updateReadyStatus(performed bool) {
	if performed || p.pollAgain || len(p.pendingOutbound) != 0 {
		signalReady(p.ready)
		return
	}

	if !p.inbound.empty() {
		signalReady(p.ready)
		return
	}

	p.readyTimer.Arm(p.nextDue)
}

// advancePool advances the query pool state machine and returns an outbound event if
// there is work to be performed. Also notifies waiters of query completion or
// progress.
func (p *QueryBehaviour[K, N, M]) advancePool(ctx context.Context, now time.Time, ev query.PoolEvent) (out BehaviourEvent, term bool) {
	ctx, span := p.cfg.Tracer.Start(ctx, "PooledQueryBehaviour.advancePool", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	p.pollAgain = false

	pstate := p.pool.Advance(ctx, now, ev)
	switch st := pstate.(type) {
	case *query.StatePoolFindCloser[K, N]:
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  p,
		}, true
	case *query.StatePoolSendMessage[K, N, M]:
		return &EventOutboundSendMessage[K, N, M]{
			QueryID: st.QueryID,
			To:      st.NodeID,
			Message: st.Message,
			Notify:  p,
		}, true
	case *query.StatePoolWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
		p.nextDue = st.NextDue
	case *query.StatePoolWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
		p.nextDue = st.NextDue
	case *query.StatePoolQueryFinished[K, N]:
		if p.cfg.NetworkSize != nil && len(st.ClosestNodes) > 0 {
			if err := p.cfg.NetworkSize.Track(now, st.Target, st.ClosestNodes); err != nil {
				p.cfg.Logger.Warn("track query result", logAttrError(err))
			}
		}

		// the state carries no due time and the pool has removed the query, so the pool
		// must be advanced again to report when the remaining queries are next due
		p.pollAgain = true
		p.notifyQueryFinished(ctx, &EventQueryFinished[K, N]{
			QueryID:      st.QueryID,
			Stats:        st.Stats,
			ClosestNodes: st.ClosestNodes,
		})
	case *query.StatePoolQueryTimeout:
		// the state carries no due time and the pool has removed the query, so the pool
		// must be advanced again to report when the remaining queries are next due
		p.pollAgain = true
		p.notifyQueryFinished(ctx, &EventQueryFinished[K, N]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			Err:     coordt.ErrQueryTimeout,
		})
	case *query.StatePoolIdle:
		// nothing to do
		p.nextDue = st.NextDue
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	return nil, false
}

// notifyQueryFinished tells the query's waiter that the query has ended and releases the
// notifier held for it.
func (p *QueryBehaviour[K, N, M]) notifyQueryFinished(ctx context.Context, ev *EventQueryFinished[K, N]) {
	waiter, ok := p.notifiers[ev.QueryID]
	if !ok {
		return
	}
	waiter.NotifyFinished(ctx, ev)
	delete(p.notifiers, ev.QueryID)
}

func (p *QueryBehaviour[K, N, M]) queueAddNodeEvents(nodes []N) {
	for _, info := range nodes {
		p.pendingOutbound = append(p.pendingOutbound, &EventAddNode[K, N]{
			NodeID: info,
		})
	}
}

func (p *QueryBehaviour[K, N, M]) queueNonConnectivityEvent(nid N) {
	p.pendingOutbound = append(p.pendingOutbound, &EventNotifyNonConnectivity[K, N]{
		NodeID: nid,
	})
}

type queryNotifier[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N], E TerminalQueryEvent] struct {
	monitor  QueryMonitor[K, N, M, E]
	pending  []CtxEvent[*EventQueryProgressed[K, N, M]]
	stopping bool
}

func (w *queryNotifier[K, N, M, E]) TryNotifyProgressed(ctx context.Context, ev *EventQueryProgressed[K, N, M]) bool {
	if w.stopping {
		return false
	}
	ce := CtxEvent[*EventQueryProgressed[K, N, M]]{Ctx: ctx, Event: ev}
	select {
	case w.monitor.NotifyProgressed() <- ce:
		return true
	default:
		w.pending = append(w.pending, ce)
		return false
	}
}

// DrainPending attempts to drain as many pending progress events as possible
func (w *queryNotifier[K, N, M, E]) DrainPending() {
	for i, ce := range w.pending {
		select {
		case w.monitor.NotifyProgressed() <- ce:
		default:
			w.pending = w.pending[i:]
			return
		}
	}
}

func (w *queryNotifier[K, N, M, E]) NotifyFinished(ctx context.Context, ev E) {
	w.stopping = true
	w.DrainPending()
	close(w.monitor.NotifyProgressed())

	select {
	case w.monitor.NotifyFinished() <- CtxEvent[E]{Ctx: ctx, Event: ev}:
	default:
	}
	close(w.monitor.NotifyFinished())
}
