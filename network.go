package xorbie

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/coordt"
)

// ErrRequestDropped is the error reported for a request dropped because no capacity was
// available for it, either for the node it was addressed to or across all nodes.
var ErrRequestDropped = errors.New("request dropped")

type NetworkConfig struct {
	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter

	// Capacity is the maximum number of requests that may be queued or in flight across
	// all nodes.
	Capacity int

	// NodeCapacity is the maximum number of requests that may be queued or in flight for
	// any one node.
	NodeCapacity int

	// IdleTimeout is how long an unused node handler is kept before it is evicted and the
	// goroutine it uses to send messages is released.
	IdleTimeout time.Duration
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *NetworkConfig) Validate() error {
	if cfg.Logger == nil {
		return &coordt.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.Capacity < 1 {
		return &coordt.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("capacity must be greater than zero"),
		}
	}

	if cfg.NodeCapacity < 1 {
		return &coordt.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("node capacity must be greater than zero"),
		}
	}

	if cfg.IdleTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "NetworkConfig",
			Err:       fmt.Errorf("idle timeout must be greater than zero"),
		}
	}

	return nil
}

func DefaultNetworkConfig() *NetworkConfig {
	return &NetworkConfig{
		Logger:       slog.Default(),
		Tracer:       coordt.NoopTracer(),
		Meter:        coordt.NoopMeter(),
		Capacity:     256,         // MAGIC
		NodeCapacity: 3,           // MAGIC
		IdleTimeout:  time.Minute, // MAGIC
	}
}

type NetworkBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// cfg is a copy of the optional configuration supplied to the behaviour
	cfg NetworkConfig

	// rtr is the message router used to send messages
	rtr coordt.Router[K, N, M]

	nodeHandlersMu sync.Mutex
	nodeHandlers   map[string]*NodeHandler[K, N, M]

	// slots limits the number of requests queued or in flight across all node handlers
	slots *slots

	// counterRequestsDropped tracks the number of requests dropped due to no available
	// capacity.
	counterRequestsDropped metric.Int64Counter

	// gaugeRequestsInFlight tracks the number of requests accepted but not yet completed.
	gaugeRequestsInFlight metric.Int64ObservableGauge

	// gaugeNodeHandlers tracks the number of node handlers the behaviour is holding.
	gaugeNodeHandlers metric.Int64ObservableGauge

	pendingMu sync.Mutex
	pending   []BehaviourEvent
	ready     chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

func NewNetworkBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](rtr coordt.Router[K, N, M], cfg *NetworkConfig) (*NetworkBehaviour[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultNetworkConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := &NetworkBehaviour[K, N, M]{
		cfg:          *cfg,
		rtr:          rtr,
		nodeHandlers: make(map[string]*NodeHandler[K, N, M]),
		slots:        newSlots(cfg.Capacity),
		ready:        make(chan struct{}, 1),
		logger:       cfg.Logger.With("behaviour", "network"),
		tracer:       cfg.Tracer,
	}

	var err error
	b.counterRequestsDropped, err = cfg.Meter.Int64Counter(
		"network_requests_dropped",
		metric.WithDescription("Total number of requests dropped due to no available capacity"),
	)
	if err != nil {
		return nil, fmt.Errorf("create network_requests_dropped counter: %w", err)
	}

	b.gaugeRequestsInFlight, err = cfg.Meter.Int64ObservableGauge(
		"network_requests_in_flight",
		metric.WithDescription("Number of requests accepted by a node handler but not yet completed"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(int64(b.slots.held()))
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create network_requests_in_flight gauge: %w", err)
	}

	b.gaugeNodeHandlers, err = cfg.Meter.Int64ObservableGauge(
		"network_node_handlers",
		metric.WithDescription("Number of node handlers the network behaviour is holding"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(int64(b.handlerCount()))
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create network_node_handlers gauge: %w", err)
	}

	return b, nil
}

// slots is a counting semaphore that never blocks a caller.
type slots struct {
	tokens chan struct{}
}

func newSlots(n int) *slots {
	return &slots{tokens: make(chan struct{}, n)}
}

// acquire takes a slot, reporting whether one was free.
func (s *slots) acquire() bool {
	select {
	case s.tokens <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns a slot taken by a previous call to acquire. It does not block when no
// slots are held, so an unbalanced release cannot stop the caller.
func (s *slots) release() {
	select {
	case <-s.tokens:
	default:
	}
}

// held returns the number of slots currently taken.
func (s *slots) held() int {
	return len(s.tokens)
}

// handlerCount returns the number of node handlers the behaviour is holding.
func (b *NetworkBehaviour[K, N, M]) handlerCount() int {
	b.nodeHandlersMu.Lock()
	defer b.nodeHandlersMu.Unlock()

	return len(b.nodeHandlers)
}

// Notify hands a request to the node handler for the node it is addressed to. It does not
// block: a request that finds no available capacity is dropped and reported back to whoever
// asked for it as an ordinary failure, since blocking here would stop the event loop that
// is the only thing able to drain the handler.
func (b *NetworkBehaviour[K, N, M]) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := b.tracer.Start(ctx, "NetworkBehaviour.Notify")
	defer span.End()

	switch ev := ev.(type) {
	case *EventOutboundGetCloserNodes[K, N]:
		if !b.notifyHandler(ctx, ev.To, ev) {
			b.counterRequestsDropped.Add(ctx, 1)
			b.logger.Debug("dropped request to find closer nodes", logAttrNodeID(ev.To))
			if ev.Notify != nil {
				ev.Notify.Notify(ctx, &EventGetCloserNodesFailure[K, N]{
					ActivityID: ev.ActivityID,
					To:         ev.To,
					Target:     ev.Target,
					Err:        ErrRequestDropped,
				})
			}
		}
	case *EventOutboundSendMessage[K, N, M]:
		if !b.notifyHandler(ctx, ev.To, ev) {
			b.counterRequestsDropped.Add(ctx, 1)
			b.logger.Debug("dropped request to send message", logAttrNodeID(ev.To))
			if ev.Notify != nil {
				ev.Notify.Notify(ctx, &EventSendMessageFailure[K, N, M]{
					ActivityID: ev.ActivityID,
					To:         ev.To,
					Request:    ev.Message,
					Err:        ErrRequestDropped,
				})
			}
		}
	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	if len(b.pending) > 0 {
		select {
		case b.ready <- struct{}{}:
		default:
		}
	}
}

// notifyHandler gives a request to the node handler for a node, creating the handler if
// there is not one already, and reports whether the handler accepted it. The handler map
// lock is held across the hand-over so that a handler cannot be evicted between being
// chosen and being given the request.
func (b *NetworkBehaviour[K, N, M]) notifyHandler(ctx context.Context, to N, ev NodeHandlerRequest) bool {
	b.nodeHandlersMu.Lock()
	defer b.nodeHandlersMu.Unlock()

	nh, ok := b.nodeHandlers[to.String()]
	if !ok {
		nh = NewNodeHandler(to, b.rtr, b.slots, &b.cfg, b.evictIdle)
		b.nodeHandlers[to.String()] = nh
	}
	return nh.Notify(ctx, ev)
}

// evictIdle removes and closes a node handler that has been idle for its timeout, reporting
// whether it was removed. It refuses a handler that is no longer the handler for its node,
// and one that has taken on a request since the timeout expired.
func (b *NetworkBehaviour[K, N, M]) evictIdle(h *NodeHandler[K, N, M]) bool {
	b.nodeHandlersMu.Lock()
	defer b.nodeHandlersMu.Unlock()

	if b.nodeHandlers[h.self.String()] != h || !h.idle() {
		return false
	}

	delete(b.nodeHandlers, h.self.String())
	h.Close()

	return true
}

func (b *NetworkBehaviour[K, N, M]) Ready() <-chan struct{} {
	return b.ready
}

func (b *NetworkBehaviour[K, N, M]) Perform(ctx context.Context) (BehaviourEvent, bool) {
	_, span := b.tracer.Start(ctx, "NetworkBehaviour.Perform")
	defer span.End()
	// No inbound work can be done until Perform is complete
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	// drain queued events.
	if len(b.pending) > 0 {
		var ev BehaviourEvent
		ev, b.pending = b.pending[0], b.pending[1:]

		if len(b.pending) > 0 {
			select {
			case b.ready <- struct{}{}:
			default:
			}
		}
		return ev, true
	}

	return nil, false
}

// Close stops all the node handlers managed by the behaviour, releasing the
// goroutines they use to send messages. It is safe to call Close more than once.
func (b *NetworkBehaviour[K, N, M]) Close() {
	b.nodeHandlersMu.Lock()
	defer b.nodeHandlersMu.Unlock()

	for _, nh := range b.nodeHandlers {
		nh.Close()
	}
	clear(b.nodeHandlers)
}

// A NodeHandler sends requests to a single node, one at a time, from a goroutine of its
// own. Requests that arrive with no available capacity are dropped rather than queued.
type NodeHandler[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	self   N
	rtr    coordt.Router[K, N, M]
	tracer trace.Tracer

	// slots is the capacity shared with every other node handler, and nodeSlots the
	// capacity of this handler alone. A request holds one of each from the moment it is
	// accepted until it has been sent or discarded.
	slots     *slots
	nodeSlots *slots

	// pending holds the accepted requests that have not been sent yet
	pending chan CtxEvent[NodeHandlerRequest]

	// idleTimeout is how long the handler waits with nothing outstanding before offering
	// itself to onIdle, which reports whether the handler was removed.
	idleTimeout time.Duration
	onIdle      func(*NodeHandler[K, N, M]) bool

	stop     chan struct{}
	stopOnce sync.Once

	// done is closed when the handler's goroutine has exited.
	done chan struct{}
}

func NewNodeHandler[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, rtr coordt.Router[K, N, M], slots *slots, cfg *NetworkConfig, onIdle func(*NodeHandler[K, N, M]) bool) *NodeHandler[K, N, M] {
	h := &NodeHandler[K, N, M]{
		self:        self,
		rtr:         rtr,
		tracer:      cfg.Tracer,
		slots:       slots,
		nodeSlots:   newSlots(cfg.NodeCapacity),
		pending:     make(chan CtxEvent[NodeHandlerRequest], cfg.NodeCapacity),
		idleTimeout: cfg.IdleTimeout,
		onIdle:      onIdle,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	go h.run()

	return h
}

// run sends the accepted requests in turn until the handler is closed or is evicted for
// being idle.
func (h *NodeHandler[K, N, M]) run() {
	defer close(h.done)

	// Since Go 1.23 a fired timer leaves no value behind, so Reset alone rearms it.
	idle := time.NewTimer(h.idleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-h.stop:
			return
		case ce := <-h.pending:
			if ce.Ctx.Err() == nil {
				h.send(ce.Ctx, ce.Event)
			}
			h.release()
			idle.Reset(h.idleTimeout)
		case <-idle.C:
			if h.onIdle != nil && h.onIdle(h) {
				return
			}
			idle.Reset(h.idleTimeout)
		}
	}
}

// idle reports whether every request the handler accepted has been sent or discarded. A
// request holds one of the handler's own slots until then.
func (h *NodeHandler[K, N, M]) idle() bool {
	return len(h.nodeSlots.tokens) == 0
}

// Notify accepts a request to be sent to the handler's node, reporting whether capacity was
// available for it. It does not block.
func (h *NodeHandler[K, N, M]) Notify(ctx context.Context, ev NodeHandlerRequest) bool {
	ctx, span := h.tracer.Start(ctx, "NodeHandler.Notify")
	defer span.End()

	if !h.slots.acquire() {
		return false
	}
	if !h.nodeSlots.acquire() {
		h.slots.release()
		return false
	}

	// the send cannot block: this handler holds no more slots than its queue has room for
	h.pending <- CtxEvent[NodeHandlerRequest]{Ctx: ctx, Event: ev}
	return true
}

// release returns the capacity held by a request that has been sent or discarded.
func (h *NodeHandler[K, N, M]) release() {
	h.nodeSlots.release()
	h.slots.release()
}

func (h *NodeHandler[K, N, M]) send(ctx context.Context, ev NodeHandlerRequest) {
	switch cmd := ev.(type) {
	case *EventOutboundGetCloserNodes[K, N]:
		if cmd.Notify == nil {
			break
		}
		nodes, err := h.rtr.GetClosestNodes(ctx, h.self, cmd.Target)
		if err != nil {
			cmd.Notify.Notify(ctx, &EventGetCloserNodesFailure[K, N]{
				ActivityID: cmd.ActivityID,
				To:         h.self,
				Target:     cmd.Target,
				Err:        fmt.Errorf("NodeHandler: %w", err),
			})
			return
		}

		cmd.Notify.Notify(ctx, &EventGetCloserNodesSuccess[K, N]{
			ActivityID:  cmd.ActivityID,
			To:          h.self,
			Target:      cmd.Target,
			CloserNodes: nodes,
		})
	case *EventOutboundSendMessage[K, N, M]:
		if cmd.Notify == nil {
			break
		}
		resp, err := h.rtr.SendMessage(ctx, h.self, cmd.Message)
		if err != nil {
			cmd.Notify.Notify(ctx, &EventSendMessageFailure[K, N, M]{
				ActivityID: cmd.ActivityID,
				To:         h.self,
				Request:    cmd.Message,
				Err:        fmt.Errorf("NodeHandler: %w", err),
			})
			return
		}

		cmd.Notify.Notify(ctx, &EventSendMessageSuccess[K, N, M]{
			ActivityID:  cmd.ActivityID,
			To:          h.self,
			Request:     cmd.Message,
			Response:    resp,
			CloserNodes: resp.CloserNodes(),
		})
	default:
		panic(fmt.Sprintf("unexpected command type: %T", cmd))
	}
}

// Close stops the handler from sending any further requests and discards the requests it
// has accepted but not yet sent. It is safe to call Close more than once.
func (h *NodeHandler[K, N, M]) Close() {
	h.stopOnce.Do(func() {
		close(h.stop)

		// release the capacity held by the requests that will now never be sent
		for {
			select {
			case <-h.pending:
				h.release()
			default:
				return
			}
		}
	})
}

func (h *NodeHandler[K, N, M]) ID() N {
	return h.self
}
