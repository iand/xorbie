package coord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/internal/coord/brdcst"
	"github.com/iand/xorbie/internal/coord/coordt"
	"github.com/iand/xorbie/internal/coord/routing"
)

// A Coordinator coordinates the state machines that comprise a Kademlia DHT
type Coordinator[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// self is the node id of the system the dht is running on
	self N

	// cancel is used to cancel all running goroutines when the coordinator is cleaning up
	cancel context.CancelFunc

	// done will be closed when the coordinator's eventLoop exits. Block-read
	// from this channel to wait until resources of this coordinator were
	// cleaned up
	done chan struct{}

	// cfg is a copy of the optional configuration supplied to the dht
	cfg CoordinatorConfig[K, N, M]

	// rt is the routing table used to look up nodes by distance
	rt kad.RoutingTable[K, N]

	// rtr is the message router used to send messages
	rtr coordt.Router[K, N, M]

	// networkBehaviour is the behaviour responsible for communicating with the network
	networkBehaviour *NetworkBehaviour[K, N, M]

	// routingBehaviour is the behaviour responsible for maintaining the routing table
	routingBehaviour Behaviour[BehaviourEvent, BehaviourEvent]

	// queryBehaviour is the behaviour responsible for running user-submitted queries
	queryBehaviour Behaviour[BehaviourEvent, BehaviourEvent]

	// brdcstBehaviour is the behaviour responsible for running user-submitted queries to store records with nodes
	brdcstBehaviour Behaviour[BehaviourEvent, BehaviourEvent]

	// tele provides tracing and metric reporting capabilities
	tele *Telemetry

	// routingNotifierMu guards access to routingNotifier which may be changed during coordinator operation
	routingNotifierMu sync.RWMutex

	// routingNotifier receives routing notifications
	routingNotifier RoutingNotifier

	// lastQueryID holds the last numeric query id generated
	lastQueryID atomic.Uint64
}

type RoutingNotifier interface {
	Notify(context.Context, RoutingNotification)
}

type CoordinatorConfig[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// MeterProvider is the the meter provider to use when initialising metric instruments.
	MeterProvider metric.MeterProvider

	// TracerProvider is the tracer provider to use when initialising tracing
	TracerProvider trace.TracerProvider

	// Network is the configuration used for the [NetworkBehaviour] which sends requests to other nodes.
	Network NetworkConfig

	// Routing is the configuration used for the [RoutingBehaviour] which maintains the health of the routing table.
	Routing RoutingConfig[K, N]

	// Query is the configuration used for the [PooledQueryBehaviour] which manages the execution of user queries.
	Query QueryConfig

	// Brdcst is the configuration used for the [PooledBroadcastBehaviour] which manages the storing of records with other nodes.
	Brdcst BroadcastConfig[K, N, M]
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *CoordinatorConfig[K, N, M]) Validate() error {
	if cfg.Logger == nil {
		return &coordt.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.MeterProvider == nil {
		return &coordt.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("meter provider must not be nil"),
		}
	}

	if cfg.TracerProvider == nil {
		return &coordt.ConfigurationError{
			Component: "CoordinatorConfig",
			Err:       fmt.Errorf("tracer provider must not be nil"),
		}
	}

	// A node handler queues a response with a behaviour before releasing the network
	// capacity its request held, so a behaviour whose queue is no larger than that capacity
	// drops responses it should have been able to accept.
	for _, c := range []struct {
		component string
		capacity  int
	}{
		{"Query", cfg.Query.QueueCapacity},
		{"Routing", cfg.Routing.QueueCapacity},
		{"Brdcst", cfg.Brdcst.QueueCapacity},
	} {
		if c.capacity <= cfg.Network.Capacity {
			return &coordt.ConfigurationError{
				Component: "CoordinatorConfig",
				Err:       fmt.Errorf("%s queue capacity must be greater than network capacity", c.component),
			}
		}
	}

	return nil
}

func DefaultCoordinatorConfig[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]]() *CoordinatorConfig[K, N, M] {
	cfg := &CoordinatorConfig[K, N, M]{
		Logger:         slog.Default(),
		MeterProvider:  otel.GetMeterProvider(),
		TracerProvider: otel.GetTracerProvider(),
	}

	cfg.Query = *DefaultQueryConfig()
	cfg.Query.Logger = cfg.Logger.With("behaviour", "pooledquery")
	cfg.Query.Tracer = cfg.TracerProvider.Tracer(tracerName)
	cfg.Query.Meter = cfg.MeterProvider.Meter(meterName)

	cfg.Routing = *DefaultRoutingConfig[K, N]()
	cfg.Routing.Logger = cfg.Logger.With("behaviour", "routing")
	cfg.Routing.Tracer = cfg.TracerProvider.Tracer(tracerName)
	cfg.Routing.Meter = cfg.MeterProvider.Meter(meterName)

	cfg.Brdcst = *DefaultBroadcastConfig[K, N, M]()
	cfg.Brdcst.Logger = cfg.Logger
	cfg.Brdcst.Tracer = cfg.TracerProvider.Tracer(tracerName)
	cfg.Brdcst.Meter = cfg.MeterProvider.Meter(meterName)

	cfg.Network = *DefaultNetworkConfig()
	cfg.Network.Logger = cfg.Logger
	cfg.Network.Tracer = cfg.TracerProvider.Tracer(tracerName)
	cfg.Network.Meter = cfg.MeterProvider.Meter(meterName)

	return cfg
}

func NewCoordinator[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](
	self N,
	rtr coordt.Router[K, N, M],
	rt routing.RoutingTableCpl[K, N],
	cplFn routing.NodeIDForCplFunc[K, N],
	cfg *CoordinatorConfig[K, N, M],
) (*Coordinator[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultCoordinatorConfig[K, N, M]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Each behaviour traces and records metrics through the coordinator's providers. The
	// configuration is copied first, leaving the caller's struct unchanged.
	ccfg := *cfg
	cfg = &ccfg

	behaviourTracer := cfg.TracerProvider.Tracer(tracerName)
	behaviourMeter := cfg.MeterProvider.Meter(meterName)

	cfg.Query.Tracer, cfg.Query.Meter = behaviourTracer, behaviourMeter
	cfg.Routing.Tracer, cfg.Routing.Meter = behaviourTracer, behaviourMeter
	cfg.Brdcst.Tracer, cfg.Brdcst.Meter = behaviourTracer, behaviourMeter
	cfg.Network.Tracer, cfg.Network.Meter = behaviourTracer, behaviourMeter

	// initialize a new telemetry struct
	tele, err := NewTelemetry(cfg.MeterProvider, cfg.TracerProvider)
	if err != nil {
		return nil, fmt.Errorf("init telemetry: %w", err)
	}

	queryBehaviour, err := NewQueryBehaviour[K, N, M](self, &cfg.Query)
	if err != nil {
		return nil, fmt.Errorf("query behaviour: %w", err)
	}

	routingBehaviour, err := NewRoutingBehaviour[K, N](self, rt, cplFn, &cfg.Routing)
	if err != nil {
		return nil, fmt.Errorf("routing behaviour: %w", err)
	}

	networkBehaviour, err := NewNetworkBehaviour[K, N, M](rtr, &cfg.Network)
	if err != nil {
		return nil, fmt.Errorf("network behaviour: %w", err)
	}

	bpCfg := brdcst.DefaultConfigPool()
	bpCfg.Tracer = tele.Tracer

	b, err := brdcst.NewPool[K, N, M](self, bpCfg)
	if err != nil {
		return nil, fmt.Errorf("broadcast: %w", err)
	}

	brdcstBehaviour, err := NewPooledBroadcastBehaviour[K, N, M](b, &cfg.Brdcst)
	if err != nil {
		return nil, fmt.Errorf("broadcast behaviour: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	d := &Coordinator[K, N, M]{
		self:   self,
		tele:   tele,
		cfg:    *cfg,
		rtr:    rtr,
		rt:     rt,
		cancel: cancel,
		done:   make(chan struct{}),

		networkBehaviour: networkBehaviour,
		routingBehaviour: routingBehaviour,
		queryBehaviour:   queryBehaviour,
		brdcstBehaviour:  brdcstBehaviour,

		routingNotifier: nullRoutingNotifier{},
	}

	go d.eventLoop(ctx)

	return d, nil
}

// Close cleans up all resources associated with this Coordinator.
func (c *Coordinator[K, N, M]) Close() error {
	c.cancel()
	<-c.done

	// the event loop has exited so no further work can be dispatched to the
	// node handlers, stop them and release their goroutines
	c.networkBehaviour.Close()
	return nil
}

func (c *Coordinator[K, N, M]) ID() N {
	return c.self
}

func (c *Coordinator[K, N, M]) eventLoop(ctx context.Context) {
	defer close(c.done)

	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.eventLoop")
	defer span.End()

	for {
		// The select is the loop's only idle point, so the work of a pass is everything
		// after it. Choosing the behaviour here rather than performing it in the case
		// keeps that boundary in one place.
		var perform func(context.Context) (BehaviourEvent, bool)

		select {
		case <-ctx.Done():
			// coordinator is closing
			return
		case <-c.networkBehaviour.Ready():
			perform = c.networkBehaviour.Perform
		case <-c.routingBehaviour.Ready():
			perform = c.routingBehaviour.Perform
		case <-c.queryBehaviour.Ready():
			perform = c.queryBehaviour.Perform
		case <-c.brdcstBehaviour.Ready():
			perform = c.brdcstBehaviour.Perform
		}

		start := time.Now()

		if ev, ok := perform(ctx); ok {
			c.dispatchEvent(ctx, ev)
		}

		c.tele.RecordEventLoopPass(ctx, time.Since(start))
	}
}

func (c *Coordinator[K, N, M]) dispatchEvent(ctx context.Context, ev BehaviourEvent) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.dispatchEvent", trace.WithAttributes(attribute.String("event_type", fmt.Sprintf("%T", ev))))
	defer span.End()

	switch ev := ev.(type) {
	case NetworkCommand:
		c.networkBehaviour.Notify(ctx, ev)
	case QueryCommand:
		c.queryBehaviour.Notify(ctx, ev)
	case BrdcstCommand:
		c.brdcstBehaviour.Notify(ctx, ev)
	case RoutingCommand:
		c.routingBehaviour.Notify(ctx, ev)
	case RoutingNotification:
		c.routingNotifierMu.RLock()
		rn := c.routingNotifier
		c.routingNotifierMu.RUnlock()
		rn.Notify(ctx, ev)
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}
}

func (c *Coordinator[K, N, M]) SetRoutingNotifier(rn RoutingNotifier) {
	c.routingNotifierMu.Lock()
	c.routingNotifier = rn
	c.routingNotifierMu.Unlock()
}

// IsRoutable reports whether the supplied node is present in the local routing table.
func (c *Coordinator[K, N, M]) IsRoutable(ctx context.Context, id N) bool {
	_, exists := c.rt.GetNode(id.Key())

	return exists
}

// GetClosestNodes requests the n closest nodes to the key from the node's local routing table.
func (c *Coordinator[K, N, M]) GetClosestNodes(ctx context.Context, k K, n int) ([]N, error) {
	return c.rt.NearestNodes(k, n), nil
}

// QueryClosest starts a query that attempts to find the closest nodes to the target key.
// It returns the closest nodes found to the target key and statistics on the actions of the query.
//
// The supplied [QueryFunc] is called after each successful request to a node with the ID of the node,
// the response received from the find nodes request made to the node and the current query stats. The query
// terminates when [QueryFunc] returns an error or when the query has visited the configured minimum number
// of closest nodes (default 20)
//
// numResults specifies the minimum number of nodes to successfully contact before considering iteration complete.
// The query is considered to be exhausted when it has received responses from at least this number of nodes
// and there are no closer nodes remaining to be contacted. A default of 20 is used if this value is less than 1.
func (c *Coordinator[K, N, M]) QueryClosest(ctx context.Context, target K, fn coordt.QueryFunc[K, N, M], numResults int) ([]N, coordt.QueryStats, error) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.Query")
	defer span.End()
	c.cfg.Logger.Debug("starting query for closest nodes", logAttrKey(target))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	seedIDs, err := c.GetClosestNodes(ctx, target, 20)
	if err != nil {
		return nil, coordt.QueryStats{}, err
	}

	waiter := NewQueryWaiter[K, N, M](numResults)
	queryID := c.newOperationID()

	cmd := &EventStartFindCloserQuery[K, N, M]{
		QueryID:           queryID,
		Target:            target,
		KnownClosestNodes: seedIDs,
		Notify:            waiter,
		NumResults:        numResults,
	}

	// queue the start of the query
	c.queryBehaviour.Notify(ctx, cmd)

	return c.waitForQuery(ctx, queryID, waiter, fn)
}

// QueryMessage starts a query that iterates over the closest nodes to the target key in the supplied message.
// The message is sent to each node that is visited.
//
// The supplied [QueryFunc] is called after each successful request to a node with the ID of the node,
// the response received from the find nodes request made to the node and the current query stats. The query
// terminates when [QueryFunc] returns an error or when the query has visited the configured minimum number
// of closest nodes (default 20)
//
// numResults specifies the minimum number of nodes to successfully contact before considering iteration complete.
// The query is considered to be exhausted when it has received responses from at least this number of nodes
// and there are no closer nodes remaining to be contacted. A default of 20 is used if this value is less than 1.
func (c *Coordinator[K, N, M]) QueryMessage(ctx context.Context, msg M, fn coordt.QueryFunc[K, N, M], numResults int) ([]N, coordt.QueryStats, error) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.QueryMessage")
	defer span.End()
	if any(msg) == nil {
		return nil, coordt.QueryStats{}, fmt.Errorf("no message supplied for query")
	}
	c.cfg.Logger.Debug("starting query with message", logAttrKey(msg.Target()))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if numResults < 1 {
		numResults = 20 // TODO: parameterize
	}

	seedIDs, err := c.GetClosestNodes(ctx, msg.Target(), numResults)
	if err != nil {
		return nil, coordt.QueryStats{}, err
	}

	waiter := NewQueryWaiter[K, N, M](numResults)
	queryID := c.newOperationID()

	cmd := &EventStartMessageQuery[K, N, M]{
		QueryID:           queryID,
		Target:            msg.Target(),
		Message:           msg,
		KnownClosestNodes: seedIDs,
		Notify:            waiter,
		NumResults:        numResults,
	}

	// queue the start of the query
	c.queryBehaviour.Notify(ctx, cmd)

	closest, stats, err := c.waitForQuery(ctx, queryID, waiter, fn)
	return closest, stats, err
}

func (c *Coordinator[K, N, M]) BroadcastRecord(ctx context.Context, msg M) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.BroadcastRecord")
	defer span.End()
	if any(msg) == nil {
		return fmt.Errorf("no message supplied for broadcast")
	}
	c.cfg.Logger.Debug("starting broadcast with message", logAttrKey(msg.Target()))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	seeds, err := c.GetClosestNodes(ctx, msg.Target(), 20) // TODO: parameterize
	if err != nil {
		return err
	}
	return c.broadcast(ctx, msg, seeds, brdcst.DefaultConfigFollowUp())
}

func (c *Coordinator[K, N, M]) BroadcastStatic(ctx context.Context, msg M, seeds []N) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.BroadcastStatic")
	defer span.End()
	return c.broadcast(ctx, msg, seeds, brdcst.DefaultConfigStatic())
}

func (c *Coordinator[K, N, M]) broadcast(ctx context.Context, msg M, seeds []N, cfg brdcst.Config) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.broadcast")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	waiter := NewBroadcastWaiter[K, N, M](0) // zero capacity since waitForBroadcast ignores progress events
	queryID := c.newOperationID()

	cmd := &EventStartBroadcast[K, N, M]{
		QueryID: queryID,
		Target:  msg.Target(),
		Message: msg,
		Seed:    seeds,
		Notify:  waiter,
		Config:  cfg,
	}

	// queue the start of the query
	c.brdcstBehaviour.Notify(ctx, cmd)

	contacted, _, err := c.waitForBroadcast(ctx, waiter)
	if err != nil {
		return err
	}

	if len(contacted) == 0 {
		return fmt.Errorf("no nodes contacted")
	}

	// TODO: define threshold below which we consider the provide to have failed

	return nil
}

func (c *Coordinator[K, N, M]) waitForQuery(ctx context.Context, queryID coordt.QueryID, waiter *QueryWaiter[K, N, M], fn coordt.QueryFunc[K, N, M]) ([]N, coordt.QueryStats, error) {
	var lastStats coordt.QueryStats

	// progressed is set to nil once the notifier closes the progress channel, which it
	// does before sending the terminal event. A closed channel is always ready, so
	// leaving it in the select would win the race against the terminal event and
	// discard the outcome of the query.
	progressed := waiter.Progressed()

	for {
		select {
		case <-ctx.Done():
			return nil, lastStats, ctx.Err()

		case wev, more := <-progressed:
			if !more {
				progressed = nil
				continue
			}
			ctx, ev := wev.Ctx, wev.Event
			c.cfg.Logger.Debug("query made progress", "query_id", queryID, logAttrNodeID(ev.NodeID), slog.Duration("elapsed", time.Since(ev.Stats.Start)), slog.Int("requests", ev.Stats.Requests), slog.Int("failures", ev.Stats.Failure))
			lastStats = coordt.QueryStats{
				Start:    ev.Stats.Start,
				Requests: ev.Stats.Requests,
				Success:  ev.Stats.Success,
				Failure:  ev.Stats.Failure,
			}
			err := fn(ctx, ev.NodeID, ev.Response, lastStats)
			if errors.Is(err, coordt.ErrSkipRemaining) {
				// done
				c.cfg.Logger.Debug("query done", "query_id", queryID)
				c.queryBehaviour.Notify(ctx, &EventStopQuery{QueryID: queryID})
				return nil, lastStats, nil
			}
			if err != nil {
				// user defined error that terminates the query
				c.queryBehaviour.Notify(ctx, &EventStopQuery{QueryID: queryID})
				return nil, lastStats, err
			}
		case wev, more := <-waiter.Finished():
			// drain the progress notification channel
			for pev := range waiter.Progressed() {
				ctx, ev := pev.Ctx, pev.Event
				c.cfg.Logger.Debug("query made progress", "query_id", queryID, logAttrNodeID(ev.NodeID), slog.Duration("elapsed", time.Since(ev.Stats.Start)), slog.Int("requests", ev.Stats.Requests), slog.Int("failures", ev.Stats.Failure))
				lastStats = coordt.QueryStats{
					Start:    ev.Stats.Start,
					Requests: ev.Stats.Requests,
					Success:  ev.Stats.Success,
					Failure:  ev.Stats.Failure,
				}
				err := fn(ctx, ev.NodeID, ev.Response, lastStats)
				if errors.Is(err, coordt.ErrSkipRemaining) {
					// the caller has seen all it wants to, so stop offering nodes and
					// report the outcome of the query as usual
					c.cfg.Logger.Debug("query done", "query_id", queryID)
					break
				}
				if err != nil {
					// user defined error that terminates the query
					return nil, lastStats, err
				}
			}
			if !more {
				return nil, lastStats, ctx.Err()
			}

			if wev.Event.Err != nil {
				c.cfg.Logger.Debug("query ended early", "query_id", queryID, slog.String("reason", wev.Event.Err.Error()), slog.Int("requests", wev.Event.Stats.Requests), slog.Int("failures", wev.Event.Stats.Failure))
				return nil, lastStats, wev.Event.Err
			}

			// query is done
			lastStats.Exhausted = true
			c.cfg.Logger.Debug("query ran to exhaustion", "query_id", queryID, slog.Duration("elapsed", wev.Event.Stats.End.Sub(wev.Event.Stats.Start)), slog.Int("requests", wev.Event.Stats.Requests), slog.Int("failures", wev.Event.Stats.Failure))
			return wev.Event.ClosestNodes, lastStats, nil

		}
	}
}

func (c *Coordinator[K, N, M]) waitForBroadcast(ctx context.Context, waiter *BroadcastWaiter[K, N, M]) ([]N, map[string]struct {
	Node N
	Err  error
}, error,
) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case wev, more := <-waiter.Finished():
			if !more {
				return nil, nil, ctx.Err()
			}
			if wev.Event.Err != nil {
				return nil, nil, wev.Event.Err
			}
			return wev.Event.Contacted, wev.Event.Errors, nil
		}
	}
}

// AddNodes suggests new DHT nodes to be added to the routing table.
// If the routing table is updated as a result of this operation an EventRoutingUpdated notification
// is emitted on the routing notification channel.
func (c *Coordinator[K, N, M]) AddNodes(ctx context.Context, ids []N) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.AddNodes")
	defer span.End()
	for _, id := range ids {
		if id.String() == c.self.String() {
			// skip self
			continue
		}

		c.routingBehaviour.Notify(ctx, &EventAddNode[K, N]{
			NodeID: id,
		})

	}

	return nil
}

// Bootstrap instructs the dht to begin bootstrapping the routing table from the nodes
// configured as [RoutingConfig.BootstrapPeers]. A bootstrap also starts automatically
// whenever the routing table holds fewer than
// [RoutingConfig.BootstrapMinimumPopulation] nodes.
func (c *Coordinator[K, N, M]) Bootstrap(ctx context.Context) error {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.Bootstrap")
	defer span.End()

	c.routingBehaviour.Notify(ctx, &EventStartBootstrap[K, N]{})

	return nil
}

// NotifyConnectivity notifies the coordinator that a node has passed a connectivity check
// which means it is connected and supports finding closer nodes
func (c *Coordinator[K, N, M]) NotifyConnectivity(ctx context.Context, id N) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.NotifyConnectivity")
	defer span.End()

	c.cfg.Logger.Debug("node has connectivity", logAttrNodeID(id), "source", "notify")
	c.routingBehaviour.Notify(ctx, &EventNotifyConnectivity[K, N]{
		NodeID: id,
	})
}

// NotifyNonConnectivity notifies the coordinator that a node has failed a connectivity check
// which means it is not connected and/or it doesn't support finding closer nodes
func (c *Coordinator[K, N, M]) NotifyNonConnectivity(ctx context.Context, id N) {
	ctx, span := c.tele.Tracer.Start(ctx, "Coordinator.NotifyNonConnectivity")
	defer span.End()

	c.cfg.Logger.Debug("node has no connectivity", logAttrNodeID(id), "source", "notify")
	c.routingBehaviour.Notify(ctx, &EventNotifyNonConnectivity[K, N]{
		NodeID: id,
	})
}

func (c *Coordinator[K, N, M]) newOperationID() coordt.QueryID {
	next := c.lastQueryID.Add(1)
	return coordt.QueryID(fmt.Sprintf("%016x", next))
}

// A BufferedRoutingNotifier is a [RoutingNotifier] that buffers [RoutingNotification] events and provides methods
// to expect occurrences of specific events. It is designed for use in a test environment.
type BufferedRoutingNotifier[K kad.Key[K], N kad.NodeID[K]] struct {
	mu       sync.Mutex
	buffered []RoutingNotification
	signal   chan struct{}
}

func NewBufferedRoutingNotifier[K kad.Key[K], N kad.NodeID[K]]() *BufferedRoutingNotifier[K, N] {
	return &BufferedRoutingNotifier[K, N]{
		signal: make(chan struct{}, 1),
	}
}

func (w *BufferedRoutingNotifier[K, N]) Notify(ctx context.Context, ev RoutingNotification) {
	w.mu.Lock()
	w.buffered = append(w.buffered, ev)
	select {
	case w.signal <- struct{}{}:
	default:
	}
	w.mu.Unlock()
}

func (w *BufferedRoutingNotifier[K, N]) Expect(ctx context.Context, expected RoutingNotification) (RoutingNotification, error) {
	for {
		// look in buffered events
		w.mu.Lock()
		for i, ev := range w.buffered {
			if reflect.TypeOf(ev) == reflect.TypeOf(expected) {
				// remove first from buffer and return it
				w.buffered = w.buffered[:i+copy(w.buffered[i:], w.buffered[i+1:])]
				w.mu.Unlock()
				return ev, nil
			}
		}
		w.mu.Unlock()

		// wait to be signaled that there is a new event
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for event %T", expected)
		case <-w.signal:
		}
	}
}

// ExpectRoutingUpdated blocks until an [EventRoutingUpdated] event is seen for the specified node id
func (w *BufferedRoutingNotifier[K, N]) ExpectRoutingUpdated(ctx context.Context, id N) (*EventRoutingUpdated[K, N], error) {
	for {
		// look in buffered events
		w.mu.Lock()
		for i, ev := range w.buffered {
			if tev, ok := ev.(*EventRoutingUpdated[K, N]); ok {
				if id.String() == tev.NodeID.String() {
					// remove first from buffer and return it
					w.buffered = w.buffered[:i+copy(w.buffered[i:], w.buffered[i+1:])]
					w.mu.Unlock()
					return tev, nil
				}
			}
		}
		w.mu.Unlock()

		// wait to be signaled that there is a new event
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for routing updated event")
		case <-w.signal:
		}
	}
}

// ExpectRoutingRemoved blocks until an [EventRoutingRemoved] event is seen for the specified node id
func (w *BufferedRoutingNotifier[K, N]) ExpectRoutingRemoved(ctx context.Context, id N) (*EventRoutingRemoved[K, N], error) {
	for {
		// look in buffered events
		w.mu.Lock()
		for i, ev := range w.buffered {
			if tev, ok := ev.(*EventRoutingRemoved[K, N]); ok {
				if id.String() == tev.NodeID.String() {
					// remove first from buffer and return it
					w.buffered = w.buffered[:i+copy(w.buffered[i:], w.buffered[i+1:])]
					w.mu.Unlock()
					return tev, nil
				}
			}
		}
		w.mu.Unlock()

		// wait to be signaled that there is a new event
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("test deadline exceeded while waiting for routing removed event")
		case <-w.signal:
		}
	}
}

type nullRoutingNotifier struct{}

func (nullRoutingNotifier) Notify(context.Context, RoutingNotification) {}

// A QueryWaiter implements [QueryMonitor] for general queries
type QueryWaiter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	progressed chan CtxEvent[*EventQueryProgressed[K, N, M]]
	finished   chan CtxEvent[*EventQueryFinished[K, N]]
}

func NewQueryWaiter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](n int) *QueryWaiter[K, N, M] {
	w := &QueryWaiter[K, N, M]{
		progressed: make(chan CtxEvent[*EventQueryProgressed[K, N, M]], n),
		finished:   make(chan CtxEvent[*EventQueryFinished[K, N]], 1),
	}
	return w
}

func (w *QueryWaiter[K, N, M]) Progressed() <-chan CtxEvent[*EventQueryProgressed[K, N, M]] {
	return w.progressed
}

func (w *QueryWaiter[K, N, M]) Finished() <-chan CtxEvent[*EventQueryFinished[K, N]] {
	return w.finished
}

func (w *QueryWaiter[K, N, M]) NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed[K, N, M]] {
	return w.progressed
}

func (w *QueryWaiter[K, N, M]) NotifyFinished() chan<- CtxEvent[*EventQueryFinished[K, N]] {
	return w.finished
}
