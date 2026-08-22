package xorbie

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key/bitstr"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/keystore"
	"github.com/iand/xorbie/prefix"
	"github.com/iand/xorbie/publish"
)

type PublishConfig[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
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

	// OptimisticIndividualCertainty is how sure an optimistic publish must be that a node it
	// stores with during its lookup is really one of the ReplicationFactor closest to the key.
	OptimisticIndividualCertainty float64

	// OptimisticSetStrictness is the probability that the closest set is in fact further from
	// the key than an optimistic publish's set threshold.
	OptimisticSetStrictness float64

	// Keystore enumerates the keys this node provides by prefix, so a region publish can find
	// every key inside a surveyed region. Region publishing is disabled if it is nil.
	Keystore keystore.Keystore[K]

	// RecordSource builds the message that stores a region key. Region publishing is disabled
	// if it is nil.
	RecordSource func(k K) M

	// RegionReplication is the number of closest nodes a region publish stores each key with.
	RegionReplication int

	// RegionMaxInFlight is the greatest number of per-key publishes a region publish may have in
	// flight at once.
	RegionMaxInFlight int

	// EnableSurvey turns on the region survey, which keeps a region map current by surveying each
	// region on a schedule. When enabled a survey target function must be supplied.
	EnableSurvey bool

	// SurveyTargetFunc mints a key inside a region from its prefix, used to survey the region. It
	// must be supplied when the survey is enabled.
	SurveyTargetFunc publish.PrefixTargetFunc[K]

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
func (cfg *PublishConfig[K, N, M]) Validate() error {
	if cfg.Logger == nil {
		return &coordt.ConfigurationError{
			Component: "PublishConfig",
			Err:       fmt.Errorf("logger must not be nil"),
		}
	}

	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "PublishConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "PublishConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}

	if cfg.QueueCapacity < 1 {
		return &coordt.ConfigurationError{
			Component: "PublishConfig",
			Err:       fmt.Errorf("queue capacity must be greater than zero"),
		}
	}

	// A region publish needs both a keystore to enumerate keys and a way to build their records
	if cfg.Keystore != nil && cfg.RecordSource != nil {
		if cfg.RegionReplication < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("region replication must be greater than zero"),
			}
		}

		if cfg.RegionMaxInFlight < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("region max in flight must be greater than zero"),
			}
		}
	}

	if cfg.EnableSurvey {
		if cfg.SurveyTargetFunc == nil {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("survey target function must not be nil when survey is enabled"),
			}
		}

		if cfg.SurveyInterval < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("survey interval must be greater than zero"),
			}
		}

		if cfg.SurveyRegionTimeout < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("survey region timeout must be greater than zero"),
			}
		}

		if cfg.SurveyRequestConcurrency < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("survey request concurrency must be greater than zero"),
			}
		}

		if cfg.SurveyRequestTimeout < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("survey request timeout must be greater than zero"),
			}
		}

		if cfg.SurveyWalkInBound < 1 {
			return &coordt.ConfigurationError{
				Component: "PublishConfig",
				Err:       fmt.Errorf("survey walk-in bound must be greater than zero"),
			}
		}
	}

	return nil
}

func DefaultPublishConfig[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]]() *PublishConfig[K, N, M] {
	return &PublishConfig[K, N, M]{
		Logger:                        slog.Default(),
		Tracer:                        coordt.NoopTracer(),
		Meter:                         coordt.NoopMeter(),
		QueueCapacity:                 1024, // MAGIC
		OptimisticIndividualCertainty: 0.9,  // MAGIC
		OptimisticSetStrictness:       0.1,  // MAGIC
		RegionMaxInFlight:             16,   // MAGIC

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

type PublishBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	logger *slog.Logger
	tracer trace.Tracer

	// verifyResponse reports whether a reply shows that a record was stored.
	verifyResponse func(req, resp M) error

	// performMu is held while Perform is executing to ensure sequential execution of work.
	performMu sync.Mutex

	// pool is the publish pool state machine used for managing individual publishes.
	// it must only be accessed while performMu is held
	pool coordt.StateMachine[publish.PoolEvent, publish.PoolState]

	// survey is the region survey state machine, responsible for keeping the region map current by
	// surveying each region on a schedule. It is nil unless [PublishConfig.EnableSurvey] is set.
	// it must only be accessed while performMu is held
	survey coordt.StateMachine[publish.SurveyEvent, publish.SurveyState]

	// keystore enumerates the keys this node provides by prefix. It is nil when region
	// publishing is disabled.
	keystore keystore.Keystore[K]

	// recordSource builds the message that stores a region key. It is nil when region
	// publishing is disabled.
	recordSource func(k K) M

	// regionReplication is the number of closest nodes a region publish stores each key with.
	regionReplication int

	// regionMaxInFlight is the greatest number of per-key publishes a region publish may have in
	// flight at once.
	regionMaxInFlight int

	// regions holds every running region publish, keyed by its region id.
	// it must only be accessed while performMu is held
	regions map[coordt.ActivityID]*publish.RegionPublish[K, N]

	// children records which region started each per-key publish, keyed by the
	// publish operation's activity id
	// it must only be accessed while performMu is held
	children map[coordt.ActivityID]coordt.ActivityID

	// pendingOutbound is a queue of outbound events.
	// it must only be accessed while performMu is held
	pendingOutbound []BehaviourEvent

	// notifiers is a map that keeps track of event notifications for each running publish.
	// it must only be accessed while performMu is held
	notifiers map[coordt.ActivityID]*queryNotifier[K, N, M, *EventPublishFinished[K, N]]

	// inbound is a bounded queue of inbound events that are awaiting processing
	inbound *inboundQueue

	// counterInboundDropped counts the events dropped because the inbound queue was full.
	counterInboundDropped metric.Int64Counter

	// gaugeInboundDepth tracks the number of events waiting in the inbound queue.
	gaugeInboundDepth metric.Int64ObservableGauge

	// nextDue is the time the publish pool last reported it could next make progress
	// without an event arriving, or the zero time if it reported none.
	// it must only be accessed while performMu is held
	nextDue time.Time

	// surveyDue is the time the survey last reported it could next make progress without an event
	// arriving, or the zero time if it reported none.
	// it must only be accessed while performMu is held
	surveyDue time.Time

	// pollAgain records that the pool reported a publish ending rather than a due time,
	// so nextDue is stale until the pool is advanced again.
	// it must only be accessed while performMu is held
	pollAgain bool

	ready chan struct{}

	// readyTimer signals ready when the pool's next due time arrives.
	readyTimer *readyTimer
}

func NewPublishBehaviour[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](publishPool *publish.Pool[K, N, M], self N, rt kad.RoutingTable[K, N], cfg *PublishConfig[K, N, M]) (*PublishBehaviour[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultPublishConfig[K, N, M]()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := &PublishBehaviour[K, N, M]{
		pool:      publishPool,
		notifiers: make(map[coordt.ActivityID]*queryNotifier[K, N, M, *EventPublishFinished[K, N]]),
		inbound:   newInboundQueue(cfg.QueueCapacity),
		ready:     make(chan struct{}, 1),
		logger:    cfg.Logger.With("behaviour", "publish"),
		tracer:    cfg.Tracer,

		verifyResponse: cfg.VerifyResponse,

		keystore:          cfg.Keystore,
		recordSource:      cfg.RecordSource,
		regionReplication: cfg.RegionReplication,
		regionMaxInFlight: cfg.RegionMaxInFlight,
		regions:           make(map[coordt.ActivityID]*publish.RegionPublish[K, N]),
		children:          make(map[coordt.ActivityID]coordt.ActivityID),
	}

	if b.verifyResponse == nil {
		b.verifyResponse = func(req, resp M) error { return nil }
	}

	var err error

	b.counterInboundDropped, err = cfg.Meter.Int64Counter(
		"publish_inbound_events_dropped",
		metric.WithDescription("Total number of events dropped because the publish behaviour's inbound queue was full"),
	)
	if err != nil {
		return nil, fmt.Errorf("create publish_inbound_events_dropped counter: %w", err)
	}

	b.gaugeInboundDepth, err = cfg.Meter.Int64ObservableGauge(
		"publish_inbound_queue_depth",
		metric.WithDescription("Number of events waiting in the publish behaviour's inbound queue"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(b.inbound.depth.Load())
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create publish_inbound_queue_depth gauge: %w", err)
	}

	if cfg.EnableSurvey {
		table, err := prefix.NewTable[K](&prefix.Config{
			InitialPrefixLen: cfg.SurveyInitialPrefixLen,
			MinPopulation:    cfg.SurveyMinPopulation,
			MaxPopulation:    cfg.SurveyMaxPopulation,
		})
		if err != nil {
			return nil, fmt.Errorf("survey table: %w", err)
		}

		surveyCfg := publish.DefaultSurveyConfig()
		surveyCfg.Tracer = cfg.Tracer
		surveyCfg.Meter = cfg.Meter
		surveyCfg.Interval = cfg.SurveyInterval
		surveyCfg.RegionTimeout = cfg.SurveyRegionTimeout
		surveyCfg.RequestConcurrency = cfg.SurveyRequestConcurrency
		surveyCfg.RequestTimeout = cfg.SurveyRequestTimeout
		surveyCfg.WalkInBound = cfg.SurveyWalkInBound

		b.survey, err = publish.NewSurvey(self, rt, table, cfg.SurveyTargetFunc, surveyCfg)
		if err != nil {
			return nil, fmt.Errorf("survey: %w", err)
		}
	}

	b.readyTimer = newReadyTimer(b.ready)

	// The survey schedule starts running as soon as it is created, so signal ready once to get the
	// Perform that arms a timer for it. Otherwise a node that is never notified never surveys.
	if b.survey != nil {
		signalReady(b.ready)
	}

	return b, nil
}

func (b *PublishBehaviour[K, N, M]) Ready() <-chan struct{} {
	return b.ready
}

func (b *PublishBehaviour[K, N, M]) Notify(ctx context.Context, ev BehaviourEvent) {
	ctx, span := b.tracer.Start(ctx, "PublishBehaviour.Notify")
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
// event that starts a publish leaves a caller waiting on its monitor for a terminal event
// that would otherwise never arrive.
func (b *PublishBehaviour[K, N, M]) reportDropped(ctx context.Context, ev BehaviourEvent) {
	var activityID coordt.ActivityID
	var monitor QueryMonitor[K, N, M, *EventPublishFinished[K, N]]

	switch ev := ev.(type) {
	case *EventStartFollowUpPublish[K, N, M]:
		activityID, monitor = ev.ActivityID, ev.Notify
	case *EventStartStaticPublish[K, N, M]:
		activityID, monitor = ev.ActivityID, ev.Notify
	case *EventStartOptimisticPublish[K, N, M]:
		activityID, monitor = ev.ActivityID, ev.Notify
	default:
		return
	}

	if monitor == nil {
		return
	}

	n := &queryNotifier[K, N, M, *EventPublishFinished[K, N]]{monitor: monitor}
	n.NotifyFinished(ctx, &EventPublishFinished[K, N]{ActivityID: activityID, Err: ErrEventDropped})
}

func (b *PublishBehaviour[K, N, M]) Perform(ctx context.Context) (out BehaviourEvent, performed bool) {
	b.performMu.Lock()
	defer b.performMu.Unlock()

	ctx, span := b.tracer.Start(ctx, "PublishBehaviour.Perform")
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
	ev, ok = b.performNextInbound(ctx)
	if ok {
		return ev, true
	}

	// advance one region publish, startimg the publish it has ready
	ev, ok = b.advanceRegions(ctx, time.Now())
	if ok {
		return ev, true
	}

	// poll the survey so a due region survey can start
	ev, ok = b.advanceSurvey(ctx, time.Now(), &publish.EventSurveyPoll{})
	if ok {
		return ev, true
	}

	// poll the publish pool to trigger any timeouts and other scheduled work
	ev, ok = b.advancePool(ctx, time.Now(), &publish.EventPoolPoll{})
	if ok {
		return ev, true
	}

	// return any queued outbound work that may have been generated
	return b.nextPendingOutbound()
}

func (b *PublishBehaviour[K, N, M]) nextPendingOutbound() (BehaviourEvent, bool) {
	if len(b.pendingOutbound) == 0 {
		return nil, false
	}
	var ev BehaviourEvent
	ev, b.pendingOutbound = b.pendingOutbound[0], b.pendingOutbound[1:]
	return ev, true
}

func (b *PublishBehaviour[K, N, M]) nextPendingInbound() (CtxEvent[BehaviourEvent], bool) {
	return b.inbound.dequeue()
}

// updateReadyStatus signals whether the behaviour has further work to do. It is
// called at the end of every Perform, passing whether that call produced an
// event.
//
// A Perform that produced an event may be able to produce another one straight
// away: the publish pool dispatches at most one message per advance, so a
// publish with several seed nodes needs several calls to contact them all.
// The event loop only calls Perform in response to a ready signal, so without
// re-signalling here a publish would contact one node and then wait for that
// node's response before contacting the next.
//
// A behaviour with no work to do arms a timer for the publish's next due time.
func (b *PublishBehaviour[K, N, M]) updateReadyStatus(performed bool) {
	if performed || b.pollAgain || len(b.pendingOutbound) != 0 {
		signalReady(b.ready)
		return
	}

	if !b.inbound.empty() {
		signalReady(b.ready)
		return
	}

	b.readyTimer.Arm(earlier(b.nextDue, b.surveyDue))
}

func (b *PublishBehaviour[K, N, M]) performNextInbound(ctx context.Context) (BehaviourEvent, bool) {
	ctx, span := b.tracer.Start(ctx, "PublishBehaviour.performNextInbound")
	defer span.End()
	pev, ok := b.nextPendingInbound()
	if !ok {
		return nil, false
	}

	var cmd publish.PoolEvent
	switch ev := pev.Event.(type) {
	case *EventStartFollowUpPublish[K, N, M]:
		cmd = &publish.EventPoolStartFollowUp[K, N, M]{
			ActivityID: ev.ActivityID,
			Target:     ev.Target,
			Message:    ev.Message,
			Seed:       ev.KnownClosestNodes,
		}
		if ev.Notify != nil {
			b.notifiers[ev.ActivityID] = &queryNotifier[K, N, M, *EventPublishFinished[K, N]]{monitor: ev.Notify}
		}

	case *EventStartStaticPublish[K, N, M]:
		cmd = &publish.EventPoolStartStatic[K, N, M]{
			ActivityID: ev.ActivityID,
			Target:     ev.Target,
			Message:    ev.Message,
			Nodes:      ev.Nodes,
		}
		if ev.Notify != nil {
			b.notifiers[ev.ActivityID] = &queryNotifier[K, N, M, *EventPublishFinished[K, N]]{monitor: ev.Notify}
		}

	case *EventStartOptimisticPublish[K, N, M]:
		cmd = &publish.EventPoolStartOptimistic[K, N, M]{
			ActivityID:  ev.ActivityID,
			Target:      ev.Target,
			Message:     ev.Message,
			Seed:        ev.KnownClosestNodes,
			NetworkSize: ev.NetworkSize,
		}
		if ev.Notify != nil {
			b.notifiers[ev.ActivityID] = &queryNotifier[K, N, M, *EventPublishFinished[K, N]]{monitor: ev.Notify}
		}

	case *EventGetCloserNodesSuccess[K, N]:
		if ev.ActivityID == publish.SurveyActivityID {
			return b.advanceSurvey(ctx, time.Now(), &publish.EventSurveyFindCloserResponse[K, N]{
				NodeID:      ev.To,
				CloserNodes: ev.CloserNodes,
			})
		}

		for _, info := range ev.CloserNodes {
			b.pendingOutbound = append(b.pendingOutbound, &EventAddNode[K, N]{
				NodeID: info,
			})
		}

		waiter, ok := b.notifiers[ev.ActivityID]
		if ok {
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed[K, N, M]{
				NodeID:     ev.To,
				ActivityID: ev.ActivityID,
			})
		}

		cmd = &publish.EventPoolGetCloserNodesSuccess[K, N]{
			NodeID:      ev.To,
			ActivityID:  ev.ActivityID,
			Target:      ev.Target,
			CloserNodes: ev.CloserNodes,
		}

	case *EventGetCloserNodesFailure[K, N]:
		if ev.ActivityID == publish.SurveyActivityID {
			return b.advanceSurvey(ctx, time.Now(), &publish.EventSurveyFindCloserFailure[K, N]{
				NodeID: ev.To,
				Error:  ev.Err,
			})
		}

		// queue an event that will notify the routing behaviour of a failed node
		b.pendingOutbound = append(b.pendingOutbound, &EventNotifyNonConnectivity[K, N]{
			ev.To,
		})

		cmd = &publish.EventPoolGetCloserNodesFailure[K, N]{
			NodeID:     ev.To,
			ActivityID: ev.ActivityID,
			Target:     ev.Target,
			Error:      ev.Err,
		}

	case *EventSendMessageSuccess[K, N, M]:
		for _, info := range ev.CloserNodes {
			b.pendingOutbound = append(b.pendingOutbound, &EventAddNode[K, N]{
				NodeID: info,
			})
		}
		waiter, ok := b.notifiers[ev.ActivityID]
		if ok {
			waiter.TryNotifyProgressed(ctx, &EventQueryProgressed[K, N, M]{
				NodeID:     ev.To,
				ActivityID: ev.ActivityID,
				Response:   ev.Response,
			})
		}
		if err := b.verifyResponse(ev.Request, ev.Response); err != nil {
			cmd = &publish.EventPoolStoreRecordFailure[K, N, M]{
				ActivityID: ev.ActivityID,
				NodeID:     ev.To,
				Request:    ev.Request,
				Error:      err,
			}
			break
		}

		// TODO: How do we know it's a StoreRecord response?
		cmd = &publish.EventPoolStoreRecordSuccess[K, N, M]{
			ActivityID: ev.ActivityID,
			NodeID:     ev.To,
			Request:    ev.Request,
			Response:   ev.Response,
		}

	case *EventSendMessageFailure[K, N, M]:
		// queue an event that will notify the routing behaviour of a failed node
		b.pendingOutbound = append(b.pendingOutbound, &EventNotifyNonConnectivity[K, N]{
			ev.To,
		})

		// TODO: How do we know it's a StoreRecord response?
		cmd = &publish.EventPoolStoreRecordFailure[K, N, M]{
			NodeID:     ev.To,
			ActivityID: ev.ActivityID,
			Request:    ev.Request,
			Error:      ev.Err,
		}

	case *EventStopQuery:
		cmd = &publish.EventPoolStopPublish{
			ActivityID: ev.ActivityID,
		}
	}

	// attempt to advance the publish pool
	return b.advancePool(ctx, time.Now(), cmd)
}

func (b *PublishBehaviour[K, N, M]) advancePool(ctx context.Context, now time.Time, ev publish.PoolEvent) (out BehaviourEvent, term bool) {
	ctx, span := b.tracer.Start(ctx, "PublishBehaviour.advancePool", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	b.pollAgain = false

	pstate := b.pool.Advance(ctx, now, ev)
	switch st := pstate.(type) {
	case *publish.StatePoolIdle:
		// nothing to do
		b.nextDue = time.Time{}
	case *publish.StatePoolWaiting:
		// nothing to do except wait for message responses or timeouts
		b.nextDue = st.NextDue
	case *publish.StatePoolFindCloser[K, N]:
		return &EventOutboundGetCloserNodes[K, N]{
			ActivityID: st.ActivityID,
			To:         st.NodeID,
			Target:     st.Target,
			Notify:     b,
		}, true
	case *publish.StatePoolStoreRecord[K, N, M]:
		return &EventOutboundSendMessage[K, N, M]{
			ActivityID: st.ActivityID,
			To:         st.NodeID,
			Message:    st.Message,
			Notify:     b,
		}, true
	case *publish.StatePoolPublishFinished[K, N]:
		// the state carries no due time and the pool has removed the publish, so the
		// pool must be advanced again to report when the remaining publishes are next due
		b.pollAgain = true

		// a finished publish that belongs to a region frees that region's slot
		if regionID, ok := b.children[st.ActivityID]; ok {
			delete(b.children, st.ActivityID)
			b.advanceRegion(ctx, now, regionID, &publish.EventRegionKeyDone{ChildID: st.ActivityID})
			return nil, false
		}

		waiter, ok := b.notifiers[st.ActivityID]
		if ok {
			waiter.NotifyFinished(ctx, &EventPublishFinished[K, N]{
				ActivityID: st.ActivityID,
				Contacted:  st.Contacted,
				Errors:     st.Errors,
				QueryStats: st.QueryStats,
			})
			delete(b.notifiers, st.ActivityID)
		}
	}

	return nil, false
}

// advanceRegions advances one region publish, returning the outbound event it produced, if any.
// Each region is polled in turn until one starts a per-key publish; regions that have nothing to
// start are skipped, and a region that has finished is dropped.
func (b *PublishBehaviour[K, N, M]) advanceRegions(ctx context.Context, now time.Time) (BehaviourEvent, bool) {
	for regionID := range b.regions {
		ev, ok := b.advanceRegion(ctx, now, regionID, &publish.EventRegionPoll{})
		if ok {
			return ev, true
		}
	}
	return nil, false
}

// advanceRegion advances the region publish with the given id and acts on the state it reports. A
// [publish.StateRegionStartKey] starts a static publish for the key in the shared pool; a
// [publish.StateRegionFinished] drops the region.
func (b *PublishBehaviour[K, N, M]) advanceRegion(ctx context.Context, now time.Time, regionID coordt.ActivityID, rev publish.RegionEvent) (BehaviourEvent, bool) {
	rp, ok := b.regions[regionID]
	if !ok {
		return nil, false
	}

	switch st := rp.Advance(ctx, now, rev).(type) {
	case *publish.StateRegionStartKey[K, N]:
		b.children[st.ChildID] = regionID
		return b.advancePool(ctx, now, &publish.EventPoolStartStatic[K, N, M]{
			ActivityID: st.ChildID,
			Target:     st.Target,
			Message:    b.recordSource(st.Target),
			Nodes:      st.Nodes,
		})
	case *publish.StateRegionFinished:
		// TODO: record the region's last-provided time before dropping the region
		delete(b.regions, regionID)
	case *publish.StateRegionWaiting:
		// nothing to start now; a per-key publish finishing frees a slot
	}

	return nil, false
}

// advanceSurvey advances the survey state machine. When no survey is configured it is a no-op, so
// callers may advance it unconditionally. A finished survey starts a region publish directly.
func (b *PublishBehaviour[K, N, M]) advanceSurvey(ctx context.Context, now time.Time, ev publish.SurveyEvent) (BehaviourEvent, bool) {
	if b.survey == nil {
		return nil, false
	}

	ctx, span := b.tracer.Start(ctx, "PublishBehaviour.advanceSurvey")
	defer span.End()

	state := b.survey.Advance(ctx, now, ev)
	switch st := state.(type) {
	case *publish.StateSurveyFindCloser[K, N]:
		b.logger.Debug("starting survey", logAttrNodeID(st.NodeID))
		return &EventOutboundGetCloserNodes[K, N]{
			ActivityID: publish.SurveyActivityID,
			To:         st.NodeID,
			Target:     st.Target,
			Notify:     b,
		}, true
	case *publish.StateSurveyWaiting:
		// survey waiting for a message response, nothing to do
		b.surveyDue = st.NextDue
	case *publish.StateSurveyFinished[K, N]:
		// the region has been surveyed, so publish every key inside it. The survey has released its
		// query and rescheduled the region, so it must be advanced again to report when the next
		// region falls due.
		b.surveyDue = time.Time{}
		b.pollAgain = true
		b.startRegionPublish(st.Prefix, st.Nodes)
	case *publish.StateSurveyTimeout:
		// the region has been rescheduled, so the survey must be advanced again to report when the
		// next region falls due
		b.logger.Debug("survey timed out", slog.String("prefix", string(st.Prefix)))
		b.pollAgain = true
	case *publish.StateSurveyFailure:
		// the region has been rescheduled, so the survey must be advanced again to report when the
		// next region falls due
		b.logger.Warn("survey failure", slog.String("prefix", string(st.Prefix)), logAttrError(st.Error))
		b.pollAgain = true
	case *publish.StateSurveyIdle:
		// no region is due to be surveyed, nothing to do
		b.surveyDue = st.NextDue
	default:
		panic(fmt.Sprintf("unexpected survey state: %T", st))
	}

	return nil, false
}

// startRegionPublish begins a region publish for every provided key inside the surveyed region
// named by prefix, storing each with the nodes found there. It does nothing when region publishing
// is disabled, the region holds no nodes, or the region contains no provided key.
func (b *PublishBehaviour[K, N, M]) startRegionPublish(region bitstr.Key, nodes []N) {
	// region publishing needs a keystore to enumerate keys and a way to build their records
	if b.keystore == nil || b.recordSource == nil {
		return
	}
	// a region with no nodes can store nothing
	if len(nodes) == 0 {
		return
	}
	keys := slices.Collect(b.keystore.KeysUnder(region))
	if len(keys) == 0 {
		return
	}
	// the region id is derived from the prefix so it is stable and unique across regions
	regionID := coordt.ActivityID("region-" + string(region))
	b.regions[regionID] = publish.NewRegion(regionID, keys, nodes, b.regionReplication, b.regionMaxInFlight, b.tracer)
	// the region step in Perform starts the first key
}

// A PublishWaiter implements [QueryMonitor] for publishes
type PublishWaiter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	progressed chan CtxEvent[*EventQueryProgressed[K, N, M]]
	finished   chan CtxEvent[*EventPublishFinished[K, N]]
}

func NewPublishWaiter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](n int) *PublishWaiter[K, N, M] {
	w := &PublishWaiter[K, N, M]{
		progressed: make(chan CtxEvent[*EventQueryProgressed[K, N, M]], n),
		finished:   make(chan CtxEvent[*EventPublishFinished[K, N]], 1),
	}
	return w
}

func (w *PublishWaiter[K, N, M]) Progressed() <-chan CtxEvent[*EventQueryProgressed[K, N, M]] {
	return w.progressed
}

func (w *PublishWaiter[K, N, M]) Finished() <-chan CtxEvent[*EventPublishFinished[K, N]] {
	return w.finished
}

func (w *PublishWaiter[K, N, M]) NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed[K, N, M]] {
	return w.progressed
}

func (w *PublishWaiter[K, N, M]) NotifyFinished() chan<- CtxEvent[*EventPublishFinished[K, N]] {
	return w.finished
}
