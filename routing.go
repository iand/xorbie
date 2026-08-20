package xorbie

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/netsize"
	"github.com/iand/xorbie/prefix"
	"github.com/iand/xorbie/routing"
)

const (
	// IncludeQueryID is the id for connectivity checks performed by the include state machine.
	// This identifier is used for routing network responses to the state machine.
	IncludeQueryID = coordt.QueryID("include")

	// ProbeQueryID is the id for connectivity checks performed by the probe state machine
	// This identifier is used for routing network responses to the state machine.
	ProbeQueryID = coordt.QueryID("probe")
)

type RoutingConfig[K kad.Key[K], N kad.NodeID[K]] struct {
	// Logger is a structured logger that will be used when logging.
	Logger *slog.Logger

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter

	// NetworkSize is the estimator that the results of completed explores are reported to.
	// A nil estimator means results are not reported.
	NetworkSize *netsize.Estimator[K, N]

	// QueueCapacity is the maximum number of events that may be waiting to be processed by
	// the behaviour. Events arriving when the queue is full are dropped. It must be larger
	// than [NetworkConfig.Capacity], since a node handler queues a response here before
	// releasing the capacity it held, so that many responses can be waiting at once.
	QueueCapacity int

	// BootstrapTimeout is the time the behaviour should wait before terminating a bootstrap if it is not making progress.
	BootstrapTimeout time.Duration

	// BootstrapRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight during bootstrap.
	BootstrapRequestConcurrency int

	// BootstrapRequestTimeout is the timeout the behaviour should use when attempting to contact a node during bootstrap.
	BootstrapRequestTimeout time.Duration

	// BootstrapPeers is the list of nodes used to bootstrap the routing table.
	BootstrapPeers []N

	// BootstrapMinimumPopulation is the routing table population below which the behaviour should
	// start a bootstrap automatically. Zero means a bootstrap is only ever started on request.
	BootstrapMinimumPopulation int

	// BootstrapRetryInterval is the minimum time the behaviour should leave between bootstraps
	// started because the routing table population is below BootstrapMinimumPopulation.
	BootstrapRetryInterval time.Duration

	// ConnectivityCheckTimeout is the timeout the behaviour should use when performing a connectivity check.
	ConnectivityCheckTimeout time.Duration

	// ProbeRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight while performing
	// connectivity checks for nodes in the routing table.
	ProbeRequestConcurrency int

	// ProbeCheckInterval is the time interval the behaviour should use between connectivity checks for the same node in the routing table.
	ProbeCheckInterval time.Duration

	// IncludeQueueCapacity is the maximum number of nodes the behaviour should keep queued as candidates for inclusion in the routing table.
	IncludeQueueCapacity int

	// IncludeRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight while performing
	// connectivity checks for nodes in the inclusion candidate queue.
	IncludeRequestConcurrency int

	// EnableExplore turns on the routing table explore, which increases routing table occupancy by
	// exploring the network. When enabled an explore cpl function must be supplied.
	EnableExplore bool

	// ExploreCplFunc mints a node id that occupies a given routing table bucket, used to synthesise
	// explore targets. It must be supplied when the explore is enabled.
	ExploreCplFunc routing.NodeIDForCplFunc[K, N]

	// ExploreTimeout is the time the behaviour should wait before terminating an exploration of a routing table bucket if it is not making progress.
	ExploreTimeout time.Duration

	// ExploreRequestConcurrency is the maximum number of concurrent requests that the behaviour may have in flight while exploring the
	// network to increase routing table occupancy.
	ExploreRequestConcurrency int

	// ExploreRequestTimeout is the timeout the behaviour should use when attempting to contact a node while exploring the
	// network to increase routing table occupancy.
	ExploreRequestTimeout time.Duration

	// ExploreMaximumCpl is the maximum CPL (common prefix length) the behaviour should explore to increase routing table occupancy.
	// All CPLs from this value to zero will be explored on a repeating schedule.
	ExploreMaximumCpl int

	// ExploreInterval is the base time interval the behaviour should leave between explorations of the same CPL.
	// See the documentation for [routing.DynamicExploreSchedule] for the precise formula used to calculate explore intervals.
	ExploreInterval time.Duration

	// ExploreIntervalMultiplier is a factor that is applied to the base time interval for CPLs lower than the maximum to increase the delay between
	// explorations for lower CPLs.
	// See the documentation for [routing.DynamicExploreSchedule] for the precise formula used to calculate explore intervals.
	ExploreIntervalMultiplier float64

	// ExploreIntervalJitter is a factor that is used to increase the calculated interval for an exploration by a small random amount.
	// It must be between 0 and 0.05. When zero, no jitter is applied.
	// See the documentation for [routing.DynamicExploreSchedule] for the precise formula used to calculate explore intervals.
	ExploreIntervalJitter float64

	// EnableSurvey turns on the region survey, which keeps a region map current by surveying each
	// region on a schedule. When enabled a survey target function must be supplied.
	EnableSurvey bool

	// SurveyTargetFunc mints a key inside a region from its prefix, used to survey the region. It
	// must be supplied when the survey is enabled.
	SurveyTargetFunc routing.PrefixTargetFunc[K]

	// SurveyInterval is the time within which every region in the network is surveyed once.
	SurveyInterval time.Duration

	// SurveyRegionTimeout is the maximum time to allow for surveying a region.
	SurveyRegionTimeout time.Duration

	// SurveyRequestConcurrency is the maximum number of concurrent requests that a region survey may have in flight.
	SurveyRequestConcurrency int

	// SurveyRequestTimeout is the timeout the behaviour should use when attempting to contact a node while surveying a region.
	SurveyRequestTimeout time.Duration

	// SurveyWalkInBound is the number of nodes a region survey contacts without finding a region member
	// before concluding the region is empty.
	SurveyWalkInBound int

	// SurveyInitialPrefixLen is the prefix length the region map is seeded with, giving 2^SurveyInitialPrefixLen regions.
	SurveyInitialPrefixLen int

	// SurveyMinPopulation is the region population at or below which two sibling regions merge.
	SurveyMinPopulation int

	// SurveyMaxPopulation is the region population above which a region splits.
	SurveyMaxPopulation int
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *RoutingConfig[K, N]) Validate() error {
	if cfg.Logger == nil {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("queue capacity must be greater than zero"),
		}
	}

	if cfg.BootstrapTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap timeout must be greater than zero"),
		}
	}

	if cfg.BootstrapRequestConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap request concurrency must be greater than zero"),
		}
	}

	if cfg.BootstrapRequestTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap request timeout must be greater than zero"),
		}
	}

	if cfg.BootstrapMinimumPopulation < 0 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap minimum population must not be negative"),
		}
	}

	if cfg.BootstrapMinimumPopulation > 0 && cfg.BootstrapRetryInterval < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("bootstrap retry interval must be greater than zero"),
		}
	}

	if cfg.ConnectivityCheckTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("connectivity check timeout must be greater than zero"),
		}
	}

	if cfg.ProbeRequestConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("probe request concurrency must be greater than zero"),
		}
	}

	if cfg.ProbeCheckInterval < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("probe check interval must be greater than zero"),
		}
	}

	if cfg.IncludeQueueCapacity < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("include queue capacity must be greater than zero"),
		}
	}

	if cfg.IncludeRequestConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "RoutingConfig",
			Err:       fmt.Errorf("include request concurrency must be greater than zero"),
		}
	}

	if cfg.EnableExplore {
		if cfg.ExploreCplFunc == nil {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore cpl function must not be nil when explore is enabled"),
			}
		}

		if cfg.ExploreTimeout < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore timeout must be greater than zero"),
			}
		}

		if cfg.ExploreRequestConcurrency < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore request concurrency must be greater than zero"),
			}
		}

		if cfg.ExploreRequestTimeout < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore request timeout must be greater than zero"),
			}
		}

		if cfg.ExploreMaximumCpl < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore maximum cpl must be greater than zero"),
			}
		}

		// Exploring a cpl needs a node id that occupies that bucket, and the supplied
		// [routing.NodeIDForCplFunc] is not required to synthesise one beyond a 15 bit prefix.
		if cfg.ExploreMaximumCpl > 15 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore maximum cpl must be 15 or less"),
			}
		}

		if cfg.ExploreInterval < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore interval must be greater than zero"),
			}
		}

		if cfg.ExploreIntervalMultiplier < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore interval multiplier must be one or greater"),
			}
		}

		if cfg.ExploreIntervalJitter < 0 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore interval jitter must be greater than 0"),
			}
		}

		if cfg.ExploreIntervalJitter > 0.05 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("explore interval jitter must be 0.05 or less"),
			}
		}
	}

	if cfg.EnableSurvey {
		if cfg.SurveyTargetFunc == nil {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("survey target function must not be nil when survey is enabled"),
			}
		}

		if cfg.SurveyInterval < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("survey interval must be greater than zero"),
			}
		}

		if cfg.SurveyRegionTimeout < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("survey region timeout must be greater than zero"),
			}
		}

		if cfg.SurveyRequestConcurrency < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("survey request concurrency must be greater than zero"),
			}
		}

		if cfg.SurveyRequestTimeout < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("survey request timeout must be greater than zero"),
			}
		}

		if cfg.SurveyWalkInBound < 1 {
			return &coordt.ConfigurationError{
				Component: "RoutingConfig",
				Err:       fmt.Errorf("survey walk-in bound must be greater than zero"),
			}
		}
	}

	return nil
}

func DefaultRoutingConfig[K kad.Key[K], N kad.NodeID[K]]() *RoutingConfig[K, N] {
	return &RoutingConfig[K, N]{
		Logger: slog.Default(),
		Tracer: coordt.NoopTracer(),
		Meter:  coordt.NoopMeter(),

		QueueCapacity: 1024, // MAGIC

		BootstrapTimeout:            5 * time.Minute, // MAGIC
		BootstrapRequestConcurrency: 3,               // MAGIC
		BootstrapRequestTimeout:     time.Minute,     // MAGIC
		BootstrapMinimumPopulation:  10,              // MAGIC
		BootstrapRetryInterval:      time.Minute,     // MAGIC

		ConnectivityCheckTimeout: time.Minute, // MAGIC

		ProbeRequestConcurrency: 3,             // MAGIC
		ProbeCheckInterval:      6 * time.Hour, // MAGIC

		IncludeRequestConcurrency: 3,   // MAGIC
		IncludeQueueCapacity:      128, // MAGIC

		ExploreTimeout:            5 * time.Minute, // MAGIC
		ExploreRequestConcurrency: 3,               // MAGIC
		ExploreRequestTimeout:     time.Minute,     // MAGIC
		EnableExplore:             false,
		ExploreMaximumCpl:         14,
		ExploreInterval:           10 * time.Minute, // MAGIC
		ExploreIntervalMultiplier: 1,                // MAGIC
		ExploreIntervalJitter:     0,                // MAGIC

		EnableSurvey:             false,
		SurveyInterval:           22 * time.Hour,  // MAGIC
		SurveyRegionTimeout:      5 * time.Minute, // MAGIC
		SurveyRequestConcurrency: 3,               // MAGIC
		SurveyRequestTimeout:     time.Minute,     // MAGIC
		SurveyWalkInBound:        20,              // MAGIC
		SurveyInitialPrefixLen:   0,               // MAGIC
		SurveyMinPopulation:      10,              // MAGIC
		SurveyMaxPopulation:      40,              // MAGIC
	}
}

// A RoutingBehaviour provides the behaviours for bootstrapping and maintaining a DHT's routing table.
type RoutingBehaviour[K kad.Key[K], N kad.NodeID[K]] struct {
	// self is the node id of the system the dht is running on
	self N

	// cfg is a copy of the optional configuration supplied to the behaviour
	cfg RoutingConfig[K, N]

	// performMu is held while Perform is executing to ensure sequential execution of work.
	performMu sync.Mutex

	// bootstrap is the bootstrap state machine, responsible for bootstrapping the routing table
	// it must only be accessed while performMu is held
	bootstrap coordt.StateMachine[routing.BootstrapEvent, routing.BootstrapState]

	// include is the inclusion state machine, responsible for vetting nodes before including them in the routing table
	// it must only be accessed while performMu is held
	include coordt.StateMachine[routing.IncludeEvent, routing.IncludeState]

	// probe is the node probing state machine, responsible for periodically checking connectivity of nodes in the routing table
	// it must only be accessed while performMu is held
	probe coordt.StateMachine[routing.ProbeEvent, routing.ProbeState]

	// explore is the routing table explore state machine, responsible for increasing the occupanct of the routing table
	// it must only be accessed while performMu is held
	explore coordt.StateMachine[routing.ExploreEvent, routing.ExploreState]

	// survey is the region survey state machine, responsible for keeping the region map current by surveying
	// each region on a schedule. It is nil unless [RoutingConfig.EnableSurvey] is set.
	// it must only be accessed while performMu is held
	survey coordt.StateMachine[routing.SurveyEvent, routing.SurveyState]

	// pendingOutbound is a queue of outbound events.
	// it must only be accessed while performMu is held
	pendingOutbound []BehaviourEvent

	// inbound is a bounded queue of inbound events that are awaiting processing
	inbound *inboundQueue

	// counterInboundDropped counts the events dropped because the inbound queue was full.
	counterInboundDropped metric.Int64Counter

	// gaugeInboundDepth tracks the number of events waiting in the inbound queue.
	gaugeInboundDepth metric.Int64ObservableGauge

	// bootstrapDue, includeDue, probeDue, exploreDue and surveyDue hold the time each child
	// state machine last reported it could next make progress without an event arriving, or the
	// zero time if it reported none. Each is written only when its own child is advanced.
	// they must only be accessed while performMu is held
	bootstrapDue time.Time
	includeDue   time.Time
	probeDue     time.Time
	exploreDue   time.Time
	surveyDue    time.Time

	// pollAgain records that a child reported the end of its work rather than a due time,
	// so the recorded due times are stale until the children are polled again.
	// it must only be accessed while performMu is held
	pollAgain bool

	ready chan struct{}

	// readyTimer signals ready when the earliest of the children's due times arrives.
	readyTimer *readyTimer
}

func NewRoutingBehaviour[K kad.Key[K], N kad.NodeID[K]](self N, rt routing.RoutingTableCpl[K, N], cfg *RoutingConfig[K, N]) (*RoutingBehaviour[K, N], error) {
	if cfg == nil {
		cfg = DefaultRoutingConfig[K, N]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	bootstrapCfg := routing.DefaultBootstrapConfig()
	bootstrapCfg.Tracer = cfg.Tracer
	bootstrapCfg.Meter = cfg.Meter
	bootstrapCfg.Timeout = cfg.BootstrapTimeout
	bootstrapCfg.RequestConcurrency = cfg.BootstrapRequestConcurrency
	bootstrapCfg.RequestTimeout = cfg.BootstrapRequestTimeout
	bootstrapCfg.MinimumPopulation = cfg.BootstrapMinimumPopulation
	bootstrapCfg.RetryInterval = cfg.BootstrapRetryInterval

	bootstrap, err := routing.NewBootstrap(self, rt, cfg.BootstrapPeers, bootstrapCfg)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	includeCfg := routing.DefaultIncludeConfig()
	includeCfg.Tracer = cfg.Tracer
	includeCfg.Meter = cfg.Meter
	includeCfg.Timeout = cfg.ConnectivityCheckTimeout
	includeCfg.QueueCapacity = cfg.IncludeQueueCapacity
	includeCfg.Concurrency = cfg.IncludeRequestConcurrency

	include, err := routing.NewInclude(rt, includeCfg)
	if err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}

	probeCfg := routing.DefaultProbeConfig()
	probeCfg.Tracer = cfg.Tracer
	probeCfg.Meter = cfg.Meter
	probeCfg.Timeout = cfg.ConnectivityCheckTimeout
	probeCfg.Concurrency = cfg.ProbeRequestConcurrency
	probeCfg.CheckInterval = cfg.ProbeCheckInterval

	probe, err := routing.NewProbe(rt, probeCfg)
	if err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}

	var explore coordt.StateMachine[routing.ExploreEvent, routing.ExploreState]
	if cfg.EnableExplore {
		exploreCfg := routing.DefaultExploreConfig()
		exploreCfg.Tracer = cfg.Tracer
		exploreCfg.Meter = cfg.Meter
		exploreCfg.Timeout = cfg.ExploreTimeout
		exploreCfg.RequestConcurrency = cfg.ExploreRequestConcurrency
		exploreCfg.RequestTimeout = cfg.ExploreRequestTimeout

		schedule, err := routing.NewDynamicExploreSchedule(cfg.ExploreMaximumCpl, time.Now(), cfg.ExploreInterval, cfg.ExploreIntervalMultiplier, cfg.ExploreIntervalJitter)
		if err != nil {
			return nil, fmt.Errorf("explore schedule: %w", err)
		}

		explore, err = routing.NewExplore(self, rt, cfg.ExploreCplFunc, schedule, exploreCfg)
		if err != nil {
			return nil, fmt.Errorf("explore: %w", err)
		}
	}

	var survey coordt.StateMachine[routing.SurveyEvent, routing.SurveyState]
	if cfg.EnableSurvey {
		table, err := prefix.NewTable[K](&prefix.Config{
			InitialPrefixLen: cfg.SurveyInitialPrefixLen,
			MinPopulation:    cfg.SurveyMinPopulation,
			MaxPopulation:    cfg.SurveyMaxPopulation,
		})
		if err != nil {
			return nil, fmt.Errorf("survey table: %w", err)
		}

		surveyCfg := routing.DefaultSurveyConfig()
		surveyCfg.Tracer = cfg.Tracer
		surveyCfg.Meter = cfg.Meter
		surveyCfg.Interval = cfg.SurveyInterval
		surveyCfg.RegionTimeout = cfg.SurveyRegionTimeout
		surveyCfg.RequestConcurrency = cfg.SurveyRequestConcurrency
		surveyCfg.RequestTimeout = cfg.SurveyRequestTimeout
		surveyCfg.WalkInBound = cfg.SurveyWalkInBound

		survey, err = routing.NewSurvey(self, rt, table, cfg.SurveyTargetFunc, surveyCfg)
		if err != nil {
			return nil, fmt.Errorf("survey: %w", err)
		}
	}

	return ComposeRoutingBehaviour(self, bootstrap, include, probe, explore, survey, cfg)
}

// ComposeRoutingBehaviour creates a [RoutingBehaviour] composed of the supplied state machines.
// The state machines are assumed to pre-configured so any [RoutingConfig] values relating to the state machines will not be applied.
func ComposeRoutingBehaviour[K kad.Key[K], N kad.NodeID[K]](
	self N,
	bootstrap coordt.StateMachine[routing.BootstrapEvent, routing.BootstrapState],
	include coordt.StateMachine[routing.IncludeEvent, routing.IncludeState],
	probe coordt.StateMachine[routing.ProbeEvent, routing.ProbeState],
	explore coordt.StateMachine[routing.ExploreEvent, routing.ExploreState],
	survey coordt.StateMachine[routing.SurveyEvent, routing.SurveyState],
	cfg *RoutingConfig[K, N],
) (*RoutingBehaviour[K, N], error) {
	if cfg == nil {
		cfg = DefaultRoutingConfig[K, N]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	r := &RoutingBehaviour[K, N]{
		self:      self,
		cfg:       *cfg,
		bootstrap: bootstrap,
		include:   include,
		probe:     probe,
		explore:   explore,
		survey:    survey,
		inbound:   newInboundQueue(cfg.QueueCapacity),
		ready:     make(chan struct{}, 1),
	}

	var err error

	r.counterInboundDropped, err = cfg.Meter.Int64Counter(
		"routing_inbound_events_dropped",
		metric.WithDescription("Total number of events dropped because the routing behaviour's inbound queue was full"),
	)
	if err != nil {
		return nil, fmt.Errorf("create routing_inbound_events_dropped counter: %w", err)
	}

	r.gaugeInboundDepth, err = cfg.Meter.Int64ObservableGauge(
		"routing_inbound_queue_depth",
		metric.WithDescription("Number of events waiting in the routing behaviour's inbound queue"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(r.inbound.depth.Load())
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create routing_inbound_queue_depth gauge: %w", err)
	}

	r.readyTimer = newReadyTimer(r.ready)

	// The explore and survey schedules start running as soon as they are created, so signal ready
	// once to get the Perform that arms a timer for them. Otherwise a node that is never notified
	// never explores or surveys.
	signalReady(r.ready)

	return r, nil
}

func (r *RoutingBehaviour[K, N]) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.Notify")
	defer span.End()

	// No routing event has a caller waiting on it, so a drop needs no report beyond the
	// counter: the work it would have done is either retried or not needed.
	if !r.inbound.enqueue(CtxEvent[BehaviourEvent]{Ctx: ctx, Event: ev}) {
		r.counterInboundDropped.Add(ctx, 1)
		r.cfg.Logger.Debug("dropped inbound event", slog.String("event", fmt.Sprintf("%T", ev)))
		return
	}

	select {
	case r.ready <- struct{}{}:
	default:
	}
}

func (r *RoutingBehaviour[K, N]) Ready() <-chan struct{} {
	return r.ready
}

func (r *RoutingBehaviour[K, N]) Perform(ctx context.Context) (out BehaviourEvent, performed bool) {
	r.performMu.Lock()
	defer r.performMu.Unlock()

	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.Perform")
	defer span.End()

	defer func() { r.updateReadyStatus(performed) }()

	// drain queued events first.
	// drain queued outbound events before starting new work.
	ev, ok := r.nextPendingOutbound()
	if ok {
		return ev, true
	}

	// perform one piece of pending inbound work.
	ev, ok = r.perfomNextInbound()
	if ok {
		return ev, true
	}

	// poll the child state machines in priority order to give each an opportunity to perform work
	r.pollChildren(ctx)

	// finally check if any pending events were accumulated in the meantime
	return r.nextPendingOutbound()
}

func (r *RoutingBehaviour[K, N]) nextPendingOutbound() (BehaviourEvent, bool) {
	if len(r.pendingOutbound) == 0 {
		return nil, false
	}
	var ev BehaviourEvent
	ev, r.pendingOutbound = r.pendingOutbound[0], r.pendingOutbound[1:]
	return ev, true
}

// updateReadyStatus signals whether the behaviour has further work to do. It is
// called at the end of every Perform, passing whether that call produced an
// event.
//
// A Perform that produced an event may be able to produce another one straight
// away: each child state machine dispatches at most one request per advance, so
// a bootstrap with spare request concurrency and unqueried seeds needs several
// calls to reach its configured concurrency. The event loop only calls Perform
// in response to a ready signal, so without re-signalling here the children
// would sit on those seeds until some external event arrived, holding them to
// one request in flight regardless of configuration.
//
// A behaviour with no work to do arms a timer for the earliest of its children's next
// due times.
func (r *RoutingBehaviour[K, N]) updateReadyStatus(performed bool) {
	if performed || r.pollAgain || len(r.pendingOutbound) != 0 {
		signalReady(r.ready)
		return
	}

	if !r.inbound.empty() {
		signalReady(r.ready)
		return
	}

	r.readyTimer.Arm(r.nextDue())
}

// nextDue returns the earliest time at which advancing any child state machine could
// make progress without an event arriving, or the zero time if there is no such time.
func (r *RoutingBehaviour[K, N]) nextDue() time.Time {
	due := earlier(r.bootstrapDue, r.includeDue)
	due = earlier(due, r.probeDue)
	due = earlier(due, r.exploreDue)
	return earlier(due, r.surveyDue)
}

func (r *RoutingBehaviour[K, N]) nextPendingInbound() (CtxEvent[BehaviourEvent], bool) {
	return r.inbound.dequeue()
}

func (r *RoutingBehaviour[K, N]) perfomNextInbound() (BehaviourEvent, bool) {
	pev, ok := r.nextPendingInbound()
	if !ok {
		return nil, false
	}

	// every state machine advanced for this event sees the same instant
	now := time.Now()

	ctx, span := r.cfg.Tracer.Start(pev.Ctx, "PooledQueryBehaviour.perfomNextInbound")
	defer span.End()

	switch ev := pev.Event.(type) {
	case *EventStartBootstrap[K, N]:
		span.SetAttributes(attribute.String("event", "EventStartBootstrap"))
		cmd := &routing.EventBootstrapStart[K, N]{
			KnownClosestNodes: ev.SeedNodes,
		}
		// attempt to advance the bootstrap
		return r.advanceBootstrap(ctx, now, cmd)

	case *EventAddNode[K, N]:
		span.SetAttributes(attribute.String("event", "EventAddAddrInfo"))
		// Ignore self
		if r.self.String() == ev.NodeID.String() {
			break
		}
		cmd := &routing.EventIncludeAddCandidate[K, N]{
			NodeID: ev.NodeID,
		}
		// attempt to advance the include
		return r.advanceInclude(ctx, now, cmd)

	case *EventRoutingUpdated[K, N]:
		span.SetAttributes(attribute.String("event", "EventRoutingUpdated"), attribute.String("nodeid", ev.NodeID.String()))
		cmd := &routing.EventProbeAdd[K, N]{
			NodeID: ev.NodeID,
		}
		// attempt to advance the probe state machine
		return r.advanceProbe(ctx, now, cmd)

	case *EventGetCloserNodesSuccess[K, N]:
		span.SetAttributes(attribute.String("event", "EventGetCloserNodesSuccess"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", ev.To.String()))
		switch ev.QueryID {
		case routing.BootstrapQueryID:
			for _, info := range ev.CloserNodes {
				// TODO: do this after advancing bootstrap
				r.pendingOutbound = append(r.pendingOutbound, &EventAddNode[K, N]{
					NodeID: info,
				})
			}
			cmd := &routing.EventBootstrapFindCloserResponse[K, N]{
				NodeID:      ev.To,
				CloserNodes: ev.CloserNodes,
			}
			// attempt to advance the bootstrap
			return r.advanceBootstrap(ctx, now, cmd)

		case IncludeQueryID:
			var cmd routing.IncludeEvent
			// require that the node responded with at least one closer node
			if len(ev.CloserNodes) > 0 {
				cmd = &routing.EventIncludeConnectivityCheckSuccess[K, N]{
					NodeID: ev.To,
				}
			} else {
				cmd = &routing.EventIncludeConnectivityCheckFailure[K, N]{
					NodeID: ev.To,
					Error:  fmt.Errorf("response did not include any closer nodes"),
				}
			}
			// attempt to advance the include
			return r.advanceInclude(ctx, now, cmd)

		case ProbeQueryID:
			var cmd routing.ProbeEvent
			// require that the node responded with at least one closer node
			if len(ev.CloserNodes) > 0 {
				cmd = &routing.EventProbeConnectivityCheckSuccess[K, N]{
					NodeID: ev.To,
				}
			} else {
				cmd = &routing.EventProbeConnectivityCheckFailure[K, N]{
					NodeID: ev.To,
					Error:  fmt.Errorf("response did not include any closer nodes"),
				}
			}
			// attempt to advance the probe state machine
			return r.advanceProbe(ctx, now, cmd)

		case routing.ExploreQueryID:
			for _, info := range ev.CloserNodes {
				r.pendingOutbound = append(r.pendingOutbound, &EventAddNode[K, N]{
					NodeID: info,
				})
			}
			cmd := &routing.EventExploreFindCloserResponse[K, N]{
				NodeID:      ev.To,
				CloserNodes: ev.CloserNodes,
			}
			return r.advanceExplore(ctx, now, cmd)

		case routing.SurveyQueryID:
			cmd := &routing.EventSurveyFindCloserResponse[K, N]{
				NodeID:      ev.To,
				CloserNodes: ev.CloserNodes,
			}
			return r.advanceSurvey(ctx, now, cmd)

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
		}
	case *EventGetCloserNodesFailure[K, N]:
		span.SetAttributes(attribute.String("event", "EventGetCloserNodesFailure"), attribute.String("queryid", string(ev.QueryID)), attribute.String("nodeid", ev.To.String()))
		span.RecordError(ev.Err)
		switch ev.QueryID {
		case routing.BootstrapQueryID:
			cmd := &routing.EventBootstrapFindCloserFailure[K, N]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the bootstrap
			return r.advanceBootstrap(ctx, now, cmd)

		case IncludeQueryID:
			var cmd routing.IncludeEvent = &routing.EventIncludeConnectivityCheckFailure[K, N]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			if errors.Is(ev.Err, ErrRequestDropped) {
				cmd = &routing.EventIncludeConnectivityCheckDropped[K, N]{
					NodeID: ev.To,
				}
			}
			// attempt to advance the include state machine
			return r.advanceInclude(ctx, now, cmd)

		case ProbeQueryID:
			var cmd routing.ProbeEvent = &routing.EventProbeConnectivityCheckFailure[K, N]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			if errors.Is(ev.Err, ErrRequestDropped) {
				cmd = &routing.EventProbeConnectivityCheckDropped[K, N]{
					NodeID: ev.To,
				}
			}
			// attempt to advance the probe state machine
			return r.advanceProbe(ctx, now, cmd)

		case routing.ExploreQueryID:
			cmd := &routing.EventExploreFindCloserFailure[K, N]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the explore
			return r.advanceExplore(ctx, now, cmd)

		case routing.SurveyQueryID:
			cmd := &routing.EventSurveyFindCloserFailure[K, N]{
				NodeID: ev.To,
				Error:  ev.Err,
			}
			// attempt to advance the survey
			return r.advanceSurvey(ctx, now, cmd)

		default:
			panic(fmt.Sprintf("unexpected query id: %s", ev.QueryID))
		}
	case *EventNotifyConnectivity[K, N]:
		span.SetAttributes(attribute.String("event", "EventNotifyConnectivity"), attribute.String("nodeid", ev.NodeID.String()))
		// ignore self
		if r.self.String() == ev.NodeID.String() {
			break
		}
		r.cfg.Logger.Debug("node has connectivity", logAttrNodeID(ev.NodeID))

		// tell the include state machine in case this is a new node that could be added to the routing table
		cmd := &routing.EventIncludeAddCandidate[K, N]{
			NodeID: ev.NodeID,
		}
		next, ok := r.advanceInclude(ctx, now, cmd)
		if ok {
			r.pendingOutbound = append(r.pendingOutbound, next)
		}

		// tell the probe state machine in case there is are connectivity checks that could satisfied
		cmdProbe := &routing.EventProbeNotifyConnectivity[K, N]{
			NodeID: ev.NodeID,
		}
		return r.advanceProbe(ctx, now, cmdProbe)
	case *EventNotifyNonConnectivity[K, N]:
		span.SetAttributes(attribute.String("event", "EventNotifyConnectivity"), attribute.String("nodeid", ev.NodeID.String()))

		// tell the probe state machine to remove the node from the routing table and probe list
		cmdProbe := &routing.EventProbeRemove[K, N]{
			NodeID: ev.NodeID,
		}
		return r.advanceProbe(ctx, now, cmdProbe)
	case *EventRoutingPoll:
		r.pollChildren(ctx)

	default:
		panic(fmt.Sprintf("unexpected dht event: %T", ev))
	}

	return nil, false
}

// pollChildren must only be called while r.pendingMu is locked
func (r *RoutingBehaviour[K, N]) pollChildren(ctx context.Context) {
	// every state machine advanced for this poll sees the same instant
	now := time.Now()

	r.pollAgain = false

	ev, ok := r.advanceBootstrap(ctx, now, &routing.EventBootstrapPoll{})
	if ok {
		r.pendingOutbound = append(r.pendingOutbound, ev)
	}

	ev, ok = r.advanceInclude(ctx, now, &routing.EventIncludePoll{})
	if ok {
		r.pendingOutbound = append(r.pendingOutbound, ev)
	}

	ev, ok = r.advanceProbe(ctx, now, &routing.EventProbePoll{})
	if ok {
		r.pendingOutbound = append(r.pendingOutbound, ev)
	}

	ev, ok = r.advanceExplore(ctx, now, &routing.EventExplorePoll{})
	if ok {
		r.pendingOutbound = append(r.pendingOutbound, ev)
	}

	ev, ok = r.advanceSurvey(ctx, now, &routing.EventSurveyPoll{})
	if ok {
		r.pendingOutbound = append(r.pendingOutbound, ev)
	}
}

func (r *RoutingBehaviour[K, N]) advanceBootstrap(ctx context.Context, now time.Time, ev routing.BootstrapEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceBootstrap")
	defer span.End()
	bstate := r.bootstrap.Advance(ctx, now, ev)
	switch st := bstate.(type) {

	case *routing.StateBootstrapFindCloser[K, N]:
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: routing.BootstrapQueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  r,
		}, true

	case *routing.StateBootstrapWaiting:
		// bootstrap waiting for a message response, nothing to do
		r.bootstrapDue = st.NextDue
	case *routing.StateBootstrapFinished[K, N]:
		r.cfg.Logger.Debug("bootstrap finished", slog.Duration("elapsed", st.Stats.End.Sub(st.Stats.Start)), slog.Int("requests", st.Stats.Requests), slog.Int("failures", st.Stats.Failure))

		if r.cfg.NetworkSize != nil && len(st.ClosestNodes) > 0 {
			if err := r.cfg.NetworkSize.Track(now, st.Target, st.ClosestNodes); err != nil {
				r.cfg.Logger.Warn("track bootstrap result", logAttrError(err))
			}
		}

		r.bootstrapDue = time.Time{}
		return &EventBootstrapFinished{
			Stats: st.Stats,
		}, true
	case *routing.StateBootstrapTimeout:
		r.cfg.Logger.Debug("bootstrap timed out", slog.Int("requests", st.Stats.Requests), slog.Int("failures", st.Stats.Failure))
		r.bootstrapDue = time.Time{}
		return &EventBootstrapFinished{
			Stats: st.Stats,
			Err:   coordt.ErrQueryTimeout,
		}, true
	case *routing.StateBootstrapIdle:
		// bootstrap not running, nothing to do
		r.bootstrapDue = st.NextDue
	default:
		panic(fmt.Sprintf("unexpected bootstrap state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour[K, N]) advanceInclude(ctx context.Context, now time.Time, ev routing.IncludeEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceInclude")
	defer span.End()

	istate := r.include.Advance(ctx, now, ev)
	switch st := istate.(type) {
	case *routing.StateIncludeConnectivityCheck[K, N]:
		span.SetAttributes(attribute.String("out_event", "EventOutboundGetCloserNodes"))
		// include wants to send a find node message to a node
		r.cfg.Logger.Debug("starting connectivity check", logAttrNodeID(st.NodeID), "source", "include")
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: IncludeQueryID,
			To:      st.NodeID,
			Target:  st.NodeID.Key(),
			Notify:  r,
		}, true

	case *routing.StateIncludeRoutingUpdated[K, N]:
		// a node has been included in the routing table

		// notify other routing state machines that there is a new node in the routing table
		r.Notify(ctx, &EventRoutingUpdated[K, N]{
			NodeID: st.NodeID,
		})

		// return the event to notify outwards too
		span.SetAttributes(attribute.String("out_event", "EventRoutingUpdated"))
		r.cfg.Logger.Debug("node added to routing table", logAttrNodeID(st.NodeID))
		return &EventRoutingUpdated[K, N]{
			NodeID: st.NodeID,
		}, true
	case *routing.StateIncludeWaitingAtCapacity:
		// nothing to do except wait for message response or timeout
		r.includeDue = st.NextDue
	case *routing.StateIncludeWaitingWithCapacity:
		// nothing to do except wait for message response or timeout
		r.includeDue = st.NextDue
	case *routing.StateIncludeWaitingFull:
		// nothing to do except wait for message response or timeout
		r.includeDue = st.NextDue
	case *routing.StateIncludeIdle:
		// nothing to do except wait for new nodes to be added to queue
		r.includeDue = time.Time{}
	default:
		panic(fmt.Sprintf("unexpected include state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour[K, N]) advanceProbe(ctx context.Context, now time.Time, ev routing.ProbeEvent) (BehaviourEvent, bool) {
	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceProbe")
	defer span.End()
	st := r.probe.Advance(ctx, now, ev)
	switch st := st.(type) {
	case *routing.StateProbeConnectivityCheck[K, N]:
		// include wants to send a find node message to a node
		r.cfg.Logger.Debug("starting connectivity check", logAttrNodeID(st.NodeID), "source", "probe")
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: ProbeQueryID,
			To:      st.NodeID,
			Target:  st.NodeID.Key(),
			Notify:  r,
		}, true
	case *routing.StateProbeNodeFailure[K, N]:
		// a node has failed a connectivity check and been removed from the routing table and the probe list

		// emit an EventRoutingRemoved event to notify clients that the node has been removed
		r.cfg.Logger.Debug("node removed from routing table", logAttrNodeID(st.NodeID))
		r.pendingOutbound = append(r.pendingOutbound, &EventRoutingRemoved[K, N]{
			NodeID: st.NodeID,
		})

		// add the node to the inclusion list for a second chance
		r.Notify(ctx, &EventAddNode[K, N]{
			NodeID: st.NodeID,
		})

	case *routing.StateProbeWaitingAtCapacity:
		// the probe state machine is waiting for responses for checks and the maximum number of concurrent checks has been reached.
		// nothing to do except wait for message response or timeout
		r.probeDue = st.NextDue
	case *routing.StateProbeWaitingWithCapacity:
		// the probe state machine is waiting for responses for checks but has capacity to perform more
		// nothing to do except wait for message response or timeout
		r.probeDue = st.NextDue
	case *routing.StateProbeIdle:
		// the probe state machine is not running any checks.
		// nothing to do except wait for message response or timeout
		r.probeDue = st.NextDue
	default:
		panic(fmt.Sprintf("unexpected include state: %T", st))
	}

	return nil, false
}

func (r *RoutingBehaviour[K, N]) advanceExplore(ctx context.Context, now time.Time, ev routing.ExploreEvent) (BehaviourEvent, bool) {
	if r.explore == nil {
		return nil, false
	}

	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceExplore")
	defer span.End()
	bstate := r.explore.Advance(ctx, now, ev)
	switch st := bstate.(type) {

	case *routing.StateExploreFindCloser[K, N]:
		r.cfg.Logger.Debug("starting explore", slog.Int("cpl", st.Cpl), logAttrNodeID(st.NodeID))
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: routing.ExploreQueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  r,
		}, true

	case *routing.StateExploreWaiting:
		// explore waiting for a message response, nothing to do
		r.exploreDue = st.NextDue
	case *routing.StateExploreQueryFinished[K, N]:
		// An explore walks to a key synthesised at a cpl, so its results are spread across the
		// keyspace rather than clustered on keys this node has been asked about.
		if r.cfg.NetworkSize != nil && len(st.ClosestNodes) > 0 {
			if err := r.cfg.NetworkSize.Track(now, st.Target, st.ClosestNodes); err != nil {
				r.cfg.Logger.Warn("track explore result", slog.Int("cpl", st.Cpl), logAttrError(err))
			}
		}

		// nothing to do except notify via telemetry. The explore has released its query,
		// so it must be advanced again to report when the next cpl falls due.
		r.pollAgain = true
	case *routing.StateExploreQueryTimeout:
		// nothing to do except notify via telemetry. The explore has released its query,
		// so it must be advanced again to report when the next cpl falls due.
		r.pollAgain = true
	case *routing.StateExploreFailure:
		// the failed cpl has been rescheduled, so the explore must be advanced again to
		// report when the next cpl falls due
		r.cfg.Logger.Warn("explore failure", slog.Int("cpl", st.Cpl), logAttrError(st.Error))
		r.pollAgain = true
	case *routing.StateExploreIdle:
		// bootstrap not running, nothing to do
		r.exploreDue = st.NextDue
	default:
		panic(fmt.Sprintf("unexpected explore state: %T", st))
	}

	return nil, false
}

// advanceSurvey advances the survey state machine. When no survey is configured it is a no-op, so
// callers may advance it unconditionally.
func (r *RoutingBehaviour[K, N]) advanceSurvey(ctx context.Context, now time.Time, ev routing.SurveyEvent) (BehaviourEvent, bool) {
	if r.survey == nil {
		return nil, false
	}

	ctx, span := r.cfg.Tracer.Start(ctx, "RoutingBehaviour.advanceSurvey")
	defer span.End()
	state := r.survey.Advance(ctx, now, ev)
	switch st := state.(type) {

	case *routing.StateSurveyFindCloser[K, N]:
		r.cfg.Logger.Debug("starting survey", logAttrNodeID(st.NodeID))
		return &EventOutboundGetCloserNodes[K, N]{
			QueryID: routing.SurveyQueryID,
			To:      st.NodeID,
			Target:  st.Target,
			Notify:  r,
		}, true

	case *routing.StateSurveyWaiting:
		// survey waiting for a message response, nothing to do
		r.surveyDue = st.NextDue
	case *routing.StateSurveyFinished[K, N]:
		// the region has been surveyed, so report its members outwards. The survey has released
		// its query and rescheduled the region, so it must be advanced again to report when the
		// next region falls due.
		r.surveyDue = time.Time{}
		r.pollAgain = true
		return &EventRegionSurveyed[K, N]{
			Prefix: st.Prefix,
			Nodes:  st.Nodes,
		}, true
	case *routing.StateSurveyTimeout:
		// the region has been rescheduled, so the survey must be advanced again to report when the
		// next region falls due
		r.cfg.Logger.Debug("survey timed out", slog.String("prefix", string(st.Prefix)))
		r.pollAgain = true
	case *routing.StateSurveyFailure:
		// the region has been rescheduled, so the survey must be advanced again to report when the
		// next region falls due
		r.cfg.Logger.Warn("survey failure", slog.String("prefix", string(st.Prefix)), logAttrError(st.Error))
		r.pollAgain = true
	case *routing.StateSurveyIdle:
		// no region is due to be surveyed, nothing to do
		r.surveyDue = st.NextDue
	default:
		panic(fmt.Sprintf("unexpected survey state: %T", st))
	}

	return nil, false
}
