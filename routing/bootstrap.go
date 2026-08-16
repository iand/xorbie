package routing

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/query"
)

// BootstrapQueryID is the id for the query operated by the bootstrap process
const BootstrapQueryID = coordt.QueryID("bootstrap")

// A bootstrapTrigger records what started a bootstrap. It is reported as the trigger
// attribute on the bootstrap_started counter.
type bootstrapTrigger string

const (
	// triggerAutomatic marks a bootstrap the state machine started because the routing
	// table was short of nodes.
	triggerAutomatic bootstrapTrigger = "automatic"

	// triggerRequested marks a bootstrap started by an [EventBootstrapStart].
	triggerRequested bootstrapTrigger = "requested"
)

type Bootstrap[K kad.Key[K], N kad.NodeID[K]] struct {
	// self is the node id of the system the bootstrap is running on
	self N

	// rt is the routing table whose population determines when a bootstrap is needed
	rt kad.RoutingTable[K, N]

	// seeds are the nodes a bootstrap starts from when it is not given any
	seeds []N

	// qry is the query used by the bootstrap process
	qry *query.Query[K, N, coordt.NoMessage[K, N]]

	// lastAttempt is the time the most recent bootstrap was started, or the zero time if
	// none has been
	lastAttempt time.Time

	// cfg is a copy of the optional configuration supplied to the Bootstrap
	cfg BootstrapConfig

	// counterFindSent is a counter that tracks the number of requests to find closer nodes sent.
	counterFindSent metric.Int64Counter

	// counterFindSucceeded is a counter that tracks the number of requests to find closer nodes that succeeded.
	counterFindSucceeded metric.Int64Counter

	// counterFindFailed is a counter that tracks the number of requests to find closer nodes that failed.
	counterFindFailed metric.Int64Counter

	// counterStarted is a counter that tracks the number of bootstraps started.
	counterStarted metric.Int64Counter

	// counterFailed is a counter that tracks the number of bootstraps that ended without completing.
	counterFailed metric.Int64Counter

	// gaugeRunning is a gauge that tracks whether the bootstrap is running.
	gaugeRunning metric.Int64ObservableGauge

	// running records whether the bootstrap is running after the last state change so that it can be read asynchronously by gaugeRunning
	running atomic.Bool
}

// BootstrapConfig specifies optional configuration for a Bootstrap
type BootstrapConfig struct {
	Timeout            time.Duration // the time to wait before terminating a query that is not making progress
	RequestConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout     time.Duration // the timeout queries should use for contacting a single node

	// MinimumPopulation is the routing table population below which a bootstrap is started
	// automatically. Zero means a bootstrap is only ever started on request.
	MinimumPopulation int

	// RetryInterval is the minimum time to leave between bootstraps started because the
	// routing table population is below MinimumPopulation.
	RetryInterval time.Duration

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *BootstrapConfig) Validate() error {
	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.Timeout < 1 {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}

	if cfg.RequestConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	if cfg.MinimumPopulation < 0 {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("minimum population must not be negative"),
		}
	}

	if cfg.MinimumPopulation > 0 && cfg.RetryInterval < 1 {
		return &coordt.ConfigurationError{
			Component: "BootstrapConfig",
			Err:       fmt.Errorf("retry interval must be greater than zero"),
		}
	}

	return nil
}

// DefaultBootstrapConfig returns the default configuration options for a Bootstrap.
// Options may be overridden before passing to NewBootstrap
func DefaultBootstrapConfig() *BootstrapConfig {
	return &BootstrapConfig{
		Tracer: coordt.NoopTracer(),
		Meter:  coordt.NoopMeter(),

		Timeout:            5 * time.Minute, // MAGIC
		RequestConcurrency: 3,               // MAGIC
		RequestTimeout:     time.Minute,     // MAGIC

		MinimumPopulation: 10,          // MAGIC
		RetryInterval:     time.Minute, // MAGIC
	}
}

// NewBootstrap creates a bootstrap that seeds its queries from the given nodes and that
// watches rt to decide when a bootstrap is needed.
func NewBootstrap[K kad.Key[K], N kad.NodeID[K]](self N, rt kad.RoutingTable[K, N], seeds []N, cfg *BootstrapConfig) (*Bootstrap[K, N], error) {
	if cfg == nil {
		cfg = DefaultBootstrapConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := &Bootstrap[K, N]{
		self:  self,
		rt:    rt,
		seeds: seeds,
		cfg:   *cfg,
	}

	var err error
	b.counterFindSent, err = cfg.Meter.Int64Counter(
		"bootstrap_find_sent",
		metric.WithDescription("Total number of find closer nodes requests sent by the bootstrap state machine"),
	)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap_find_sent counter: %w", err)
	}

	b.counterFindSucceeded, err = cfg.Meter.Int64Counter(
		"bootstrap_find_succeeded",
		metric.WithDescription("Total number of find closer nodes requests sent by the bootstrap state machine that were successful"),
	)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap_find_succeeded counter: %w", err)
	}

	b.counterFindFailed, err = cfg.Meter.Int64Counter(
		"bootstrap_find_failed",
		metric.WithDescription("Total number of find closer nodes requests sent by the bootstrap state machine that failed"),
	)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap_find_failed counter: %w", err)
	}

	b.counterStarted, err = cfg.Meter.Int64Counter(
		"bootstrap_started",
		metric.WithDescription("Total number of bootstraps started"),
	)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap_started counter: %w", err)
	}

	b.counterFailed, err = cfg.Meter.Int64Counter(
		"bootstrap_failed",
		metric.WithDescription("Total number of bootstraps that ended without completing"),
	)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap_failed counter: %w", err)
	}

	b.gaugeRunning, err = cfg.Meter.Int64ObservableGauge(
		"bootstrap_running",
		metric.WithDescription("Whether or not the bootstrap is running"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			if b.running.Load() {
				o.Observe(1)
			} else {
				o.Observe(0)
			}
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap_running gauge: %w", err)
	}

	return b, nil
}

// Advance advances the state of the bootstrap by attempting to advance its query if running.
func (b *Bootstrap[K, N]) Advance(ctx context.Context, now time.Time, ev BootstrapEvent) (out BootstrapState) {
	ctx, span := b.cfg.Tracer.Start(ctx, "Bootstrap.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		b.running.Store(b.qry != nil) // record whether the bootstrap is still running for metrics
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	switch tev := ev.(type) {
	case *EventBootstrapStart[K, N]:
		if b.qry != nil {
			return b.advanceQuery(ctx, now, &query.EventQueryPoll{})
		}

		seeds := tev.KnownClosestNodes
		if len(seeds) == 0 {
			seeds = b.seeds
		}
		return b.startQuery(ctx, now, seeds, triggerRequested)

	case *EventBootstrapFindCloserResponse[K, N]:
		// ignore late responses
		if b.qry != nil {
			b.counterFindSucceeded.Add(ctx, 1)
			return b.advanceQuery(ctx, now, &query.EventQueryNodeResponse[K, N]{
				NodeID:      tev.NodeID,
				CloserNodes: tev.CloserNodes,
			})
		}
	case *EventBootstrapFindCloserFailure[K, N]:
		// ignore late responses
		if b.qry != nil {
			b.counterFindFailed.Add(ctx, 1)
			span.RecordError(tev.Error)
			return b.advanceQuery(ctx, now, &query.EventQueryNodeFailure[K, N]{
				NodeID: tev.NodeID,
				Error:  tev.Error,
			})
		}
	case *EventBootstrapPoll:
		// ignore, nothing to do
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if b.qry != nil {
		return b.advanceQuery(ctx, now, &query.EventQueryPoll{})
	}

	// nothing is running, so start a bootstrap if the routing table is short of nodes
	if b.cfg.MinimumPopulation > 0 && len(b.seeds) > 0 && b.populationLow() {
		if retryAt := b.lastAttempt.Add(b.cfg.RetryInterval); !b.lastAttempt.IsZero() && now.Before(retryAt) {
			return &StateBootstrapIdle{NextDue: retryAt}
		}
		return b.startQuery(ctx, now, b.seeds, triggerAutomatic)
	}

	// a routing table that is not short of nodes only becomes so when a node is removed
	// from it, which the bootstrap is told about by being advanced
	return &StateBootstrapIdle{}
}

// populationLow reports whether the routing table holds fewer nodes than the bootstrap's
// minimum population.
func (b *Bootstrap[K, N]) populationLow() bool {
	return len(b.rt.NearestNodes(b.self.Key(), b.cfg.MinimumPopulation)) < b.cfg.MinimumPopulation
}

// startQuery begins a bootstrap query for the local node's key, seeded with the given
// nodes.
func (b *Bootstrap[K, N]) startQuery(ctx context.Context, now time.Time, seeds []N, trigger bootstrapTrigger) BootstrapState {
	iter := query.NewClosestNodesIter[K, N](b.self.Key())

	qryCfg := query.DefaultQueryConfig()
	qryCfg.Concurrency = b.cfg.RequestConcurrency
	qryCfg.RequestTimeout = b.cfg.RequestTimeout
	qryCfg.Timeout = b.cfg.Timeout

	qry, err := query.NewFindCloserQuery[K, N, coordt.NoMessage[K, N]](b.self, BootstrapQueryID, b.self.Key(), iter, seeds, qryCfg)
	if err != nil {
		// TODO: don't panic
		panic(err)
	}
	b.qry = qry
	b.lastAttempt = now
	b.counterStarted.Add(ctx, 1, metric.WithAttributes(attribute.String("trigger", string(trigger))))

	return b.advanceQuery(ctx, now, &query.EventQueryPoll{})
}

func (b *Bootstrap[K, N]) advanceQuery(ctx context.Context, now time.Time, qev query.QueryEvent) BootstrapState {
	ctx, span := b.cfg.Tracer.Start(ctx, "Bootstrap.advanceQuery")
	defer span.End()
	state := b.qry.Advance(ctx, now, qev)
	switch st := state.(type) {
	case *query.StateQueryFindCloser[K, N]:
		b.counterFindSent.Add(ctx, 1)
		span.SetAttributes(attribute.String("out_state", "StateQueryFindCloser"))
		return &StateBootstrapFindCloser[K, N]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}
	case *query.StateQueryFinished[K, N]:
		span.SetAttributes(attribute.String("out_state", "StateBootstrapFinished"))
		b.qry = nil
		return &StateBootstrapFinished[K, N]{
			Stats:        st.Stats,
			Target:       st.Target,
			ClosestNodes: st.ClosestNodes,
		}
	case *query.StateQueryWaitingAtCapacity:
		if now.After(st.Deadline) {
			b.counterFindFailed.Add(ctx, 1)
			b.counterFailed.Add(ctx, 1)
			span.SetAttributes(attribute.String("out_state", "StateBootstrapTimeout"))
			b.qry = nil
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		span.SetAttributes(attribute.String("out_state", "StateBootstrapWaiting"))
		return &StateBootstrapWaiting{
			Stats:   st.Stats,
			NextDue: earlier(st.NextDue, st.Deadline),
		}
	case *query.StateQueryWaitingWithCapacity:
		if now.After(st.Deadline) {
			b.counterFindFailed.Add(ctx, 1)
			b.counterFailed.Add(ctx, 1)
			span.SetAttributes(attribute.String("out_state", "StateBootstrapTimeout"))
			b.qry = nil
			return &StateBootstrapTimeout{
				Stats: st.Stats,
			}
		}
		span.SetAttributes(attribute.String("out_state", "StateBootstrapWaiting"))
		return &StateBootstrapWaiting{
			Stats:   st.Stats,
			NextDue: earlier(st.NextDue, st.Deadline),
		}
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}
}

// BootstrapState is the state of a bootstrap.
type BootstrapState interface {
	bootstrapState()
}

// StateBootstrapFindCloser indicates that the bootstrap query wants to send a find closer nodes message to a node.
type StateBootstrapFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   query.QueryStats
}

// StateBootstrapIdle indicates that the bootstrap is not running its query.
type StateBootstrapIdle struct {
	NextDue time.Time // the earliest time a bootstrap could be started, zero if none is due
}

// StateBootstrapFinished indicates that the bootstrap has finished.
type StateBootstrapFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	Stats        query.QueryStats
	Target       K   // the key the query was looking for the closest nodes to
	ClosestNodes []N // the closest nodes to the target key that were found
}

// StateBootstrapTimeout indicates that the bootstrap query has timed out.
type StateBootstrapTimeout struct {
	Stats query.QueryStats
}

// StateBootstrapWaiting indicates that the bootstrap query is waiting for a response.
type StateBootstrapWaiting struct {
	NextDue time.Time // the earliest time advancing the bootstrap could make progress, zero if there is none
	Stats   query.QueryStats
}

// bootstrapState() ensures that only Bootstrap states can be assigned to a BootstrapState.
func (*StateBootstrapFindCloser[K, N]) bootstrapState() {}
func (*StateBootstrapIdle) bootstrapState()             {}
func (*StateBootstrapFinished[K, N]) bootstrapState()   {}
func (*StateBootstrapTimeout) bootstrapState()          {}
func (*StateBootstrapWaiting) bootstrapState()          {}

// BootstrapEvent is an event intended to advance the state of a bootstrap.
type BootstrapEvent interface {
	bootstrapEvent()
}

// EventBootstrapPoll is an event that signals the bootstrap that it can perform housekeeping work such as time out queries.
type EventBootstrapPoll struct{}

// EventBootstrapStart is an event that attempts to start a new bootstrap
type EventBootstrapStart[K kad.Key[K], N kad.NodeID[K]] struct {
	KnownClosestNodes []N
}

// EventBootstrapFindCloserResponse notifies a bootstrap that an attempt to find closer nodes has received a successful response.
type EventBootstrapFindCloserResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventBootstrapFindCloserFailure notifies a bootstrap that an attempt to find closer nodes has failed.
type EventBootstrapFindCloserFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// bootstrapEvent() ensures that only events accepted by a [Bootstrap] can be assigned to the [BootstrapEvent] interface.
func (*EventBootstrapPoll) bootstrapEvent()                     {}
func (*EventBootstrapStart[K, N]) bootstrapEvent()              {}
func (*EventBootstrapFindCloserResponse[K, N]) bootstrapEvent() {}
func (*EventBootstrapFindCloserFailure[K, N]) bootstrapEvent()  {}
