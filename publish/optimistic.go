package publish

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/trace"
	"gonum.org/v1/gonum/mathext"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/netsize"
	"github.com/iand/xorbie/query"
)

// OptimisticConfig specifies the configuration for the [Optimistic] state machine.
type OptimisticConfig struct {
	// ReplicationFactor is the number of nodes the record is meant to be stored with.
	ReplicationFactor int

	// IndividualCertainty is how sure the strategy must be that a node it stores with during
	// the walk is really one of the ReplicationFactor closest to the target. Raising it
	// tightens the individual threshold, so fewer nodes qualify before the walk ends.
	IndividualCertainty float64

	// SetStrictness is the probability that the closest set is in fact further from the
	// target than the set threshold, which is the chance of declining to stop a walk that
	// could have been stopped. Raising it tightens the set threshold, so walks carry on for
	// longer.
	SetStrictness float64
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *OptimisticConfig) Validate() error {
	if c.ReplicationFactor < 1 {
		return &coordt.ConfigurationError{
			Component: "OptimisticConfig",
			Err:       fmt.Errorf("replication factor must be greater than zero"),
		}
	}

	if c.IndividualCertainty <= 0 || c.IndividualCertainty >= 1 {
		return &coordt.ConfigurationError{
			Component: "OptimisticConfig",
			Err:       fmt.Errorf("individual certainty must be greater than zero and less than one"),
		}
	}

	if c.SetStrictness <= 0 || c.SetStrictness >= 1 {
		return &coordt.ConfigurationError{
			Component: "OptimisticConfig",
			Err:       fmt.Errorf("set strictness must be greater than zero and less than one"),
		}
	}

	return nil
}

// thresholds returns the individual and set distance thresholds for a network of networkSize
// nodes, each as a fraction of the width of the keyspace.
//
// The distance from a key to the i-th closest of n nodes spread uniformly over the keyspace is
// Beta(i, n-i+1) distributed, which for a large network approaches Gamma(i) scaled by 1/n. The
// quantile of that Gamma at the chosen probability is therefore the distance within which a node
// may be assumed to hold the rank being asked about. The individual threshold asks about rank
// ReplicationFactor. The set threshold asks about rank ReplicationFactor/2+1, which sits just
// beyond the mean rank of the closest set and is the rank the algorithm was published with.
func (c *OptimisticConfig) thresholds(networkSize int) (individual float64, set float64, err error) {
	k := float64(c.ReplicationFactor)
	n := float64(networkSize)

	individual = mathext.GammaIncRegInv(k, 1-c.IndividualCertainty) / n
	set = mathext.GammaIncRegInv(k/2+1, 1-c.SetStrictness) / n

	if math.IsNaN(individual) || math.IsNaN(set) {
		return 0, 0, fmt.Errorf("distance thresholds do not converge for replication factor %d", c.ReplicationFactor)
	}

	return individual, set, nil
}

// DefaultOptimisticConfig returns the default configuration options for the
// [Optimistic] state machine.
func DefaultOptimisticConfig() *OptimisticConfig {
	return &OptimisticConfig{
		ReplicationFactor:   20,  // MAGIC
		IndividualCertainty: 0.9, // MAGIC
		SetStrictness:       0.1, // MAGIC
	}
}

// The Optimistic state machine publishes a record while it is still walking towards the target
// key, rather than waiting for the walk to settle on the closest nodes first.
//
// It rests on knowing roughly how large the network is. Given an estimate, the distance within
// which a node is probably one of the closest to the target can be calculated, so a node
// discovered inside that distance can be asked to store the record immediately, and the walk can
// be abandoned once the nodes already found are close enough that finding better ones is
// unlikely. Both thresholds come from the configuration alone and are computed once, when the
// state machine is created.
//
// The state machine emits [StatePublishFindCloser] to advance the walk, expecting an
// [EventPublishNodeResponse] or [EventPublishNodeFailure] in reply, and
// [StatePublishStoreRecord] to ask for the record to be stored with one node. Only one
// instruction is emitted per advance and the walk takes precedence, so stores interleave with the
// walk, filling the advances where the walk is waiting, rather than pre-empting it.
//
// Once the walk has ended, by exhaustion or by the set threshold, every node among the closest
// that has not already been asked is asked in turn. When no store is outstanding the machine
// emits [StatePublishFinished], reporting every node asked to store the record and the error
// for any that failed.
//
// The [EventPublishStop] event abandons the operation, cancelling the walk if it is still
// running and recording every node not yet contacted or not yet heard from as having failed.
//
// This is the algorithm described in Trautwein et al. 2023, "IPFS in the Fast Lane:
// Accelerating Record Storage with Optimistic Provide".
type Optimistic[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// queryID is the unique id of this publish operation
	queryID coordt.QueryID

	// cfg is the configuration supplied to the Optimistic
	cfg *OptimisticConfig

	// queryPool is the pool in which the walk is run. It belongs to the publish pool that
	// created this state machine and is shared with its siblings, so advancing it may produce
	// work for another publish.
	queryPool *query.Pool[K, N, M]

	// msg is the message sent to each node to store the record
	msg M

	// seeds holds the nodes the walk starts from
	seeds []N

	// target is the key the record is stored under, set when the operation starts
	target K

	// individualThreshold is the distance within which a discovered node is assumed to be one
	// of the closest to the target, as a fraction of the width of the keyspace
	individualThreshold float64

	// setThreshold is the mean distance of the closest known nodes at which the walk is
	// abandoned, as a fraction of the width of the keyspace
	setThreshold float64

	// found holds every node discovered so far with its distance from the target, ordered by
	// that distance
	found []foundNode[N]

	// seen records which nodes are already in found, keyed by the string form of the node
	seen map[string]struct{}

	// nextDue is the time the walk could next make progress, as reported by the query pool
	// during the current call to Advance. It is recalculated on each call.
	nextDue time.Time

	// queryStats holds the stats reported by the walk when it ended
	queryStats query.QueryStats

	// walking records whether the walk is still running
	walking bool

	// started records whether the operation has begun
	started bool

	// stopped records whether the operation was abandoned by an [EventPublishStop]
	stopped bool

	// todo holds the nodes that have yet to be asked to store the record
	todo map[string]N

	// waiting holds the nodes that have been asked to store the record but have yet to reply
	waiting map[string]N

	// contacted holds every node asked to store the record, in the order they were asked
	contacted []N

	// failed holds the nodes that did not store the record, with the error each returned
	failed map[string]struct {
		Node N
		Err  error
	}

	// tracer traces the execution of this state machine. It is supplied by the pool that
	// created it, since the per-publish configuration carries no telemetry of its own.
	tracer trace.Tracer
}

// A foundNode is a node the walk has discovered, held with its distance from the target so that
// the distance is measured once rather than on every comparison.
type foundNode[N any] struct {
	node     N
	distance float64
}

// NewOptimistic creates a state machine that publishes msg to nodes close to the target it is
// started with, walking from seeds in pool and reporting progress under the query id qid.
//
// networkSize is the estimated number of nodes in the network, from which both distance
// thresholds are derived, and must be greater than zero. A nil cfg uses
// [DefaultOptimisticConfig], and a non-nil one is validated.
func NewOptimistic[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](qid coordt.QueryID, pool *query.Pool[K, N, M], msg M, seeds []N, networkSize int, cfg *OptimisticConfig, tracer trace.Tracer) (*Optimistic[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultOptimisticConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if networkSize < 1 {
		return nil, &coordt.ConfigurationError{
			Component: "Optimistic",
			Err:       fmt.Errorf("network size must be greater than zero"),
		}
	}

	individual, set, err := cfg.thresholds(networkSize)
	if err != nil {
		return nil, err
	}

	return &Optimistic[K, N, M]{
		queryID:             qid,
		cfg:                 cfg,
		queryPool:           pool,
		tracer:              tracer,
		msg:                 msg,
		seeds:               seeds,
		individualThreshold: individual,
		setThreshold:        set,
		seen:                map[string]struct{}{},
		todo:                map[string]N{},
		waiting:             map[string]N{},
		failed: map[string]struct {
			Node N
			Err  error
		}{},
	}, nil
}

// Advance advances the state of the [Optimistic] [Publish] state machine.
//
// An event belonging to the walk is forwarded to the query pool. When the pool asks for a node to
// be contacted, that instruction is returned rather than held back in favour of a store: the pool
// counts a node as contacted when it emits the instruction rather than when the request is sent,
// so an instruction the caller never receives holds a request slot open until it times out. Only
// when the walk has nothing to ask for does the state machine look for a node still to be asked to
// store the record, falling back to reporting that it is waiting or that it has finished.
func (o *Optimistic[K, N, M]) Advance(ctx context.Context, now time.Time, ev PublishEvent) (out PublishState) {
	ctx, span := o.tracer.Start(ctx, "Optimistic.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	// the walk's due time is taken from the query pool as this call advances it
	o.nextDue = time.Time{}

	pev := o.handleEvent(ctx, ev)

	// What the event brought may have put the closest known nodes near enough that continuing
	// the walk is not worth the requests. The walk is abandoned in favour of storing with them,
	// which replaces whatever the event would otherwise have asked the query pool to do. Judging
	// this before the pool is advanced is what keeps a request that has become pointless from
	// being sent.
	if o.walking && !o.stopped && o.setThresholdMet() {
		pev = &query.EventPoolStopQuery{QueryID: o.queryID}
		o.endWalk()
	}

	if pev != nil {
		if state, terminal := o.advancePool(ctx, now, pev); terminal {
			return state
		}
	}

	// A stopped operation asks for nothing more, so every node it had not finished with counts
	// as a failure and the operation is over. Reporting it here rather than falling through to
	// the checks below is what stops a walk that is still marked as running from holding the
	// operation open indefinitely.
	if o.stopped {
		for k, n := range o.todo {
			delete(o.todo, k)
			o.fail(n, fmt.Errorf("cancelled"))
		}

		for k, n := range o.waiting {
			delete(o.waiting, k)
			o.fail(n, fmt.Errorf("cancelled"))
		}

		return &StatePublishFinished[K, N]{
			QueryID:    o.queryID,
			Contacted:  o.contacted,
			Errors:     o.failed,
			QueryStats: o.queryStats,
		}
	}

	for k, n := range o.todo {
		delete(o.todo, k)
		o.waiting[k] = n
		o.contacted = append(o.contacted, n)
		return &StatePublishStoreRecord[K, N, M]{
			QueryID: o.queryID,
			NodeID:  n,
			Message: o.msg,
		}
	}

	if len(o.waiting) > 0 || o.walking {
		// a store record request carries no deadline, so the only time at which this state
		// machine could make progress on its own is when a walk request times out
		return &StatePublishWaiting{QueryID: o.queryID, NextDue: o.nextDue}
	}

	if o.started {
		return &StatePublishFinished[K, N]{
			QueryID:    o.queryID,
			Contacted:  o.contacted,
			Errors:     o.failed,
			QueryStats: o.queryStats,
		}
	}

	return &StatePublishIdle{}
}

// handleEvent receives a [PublishEvent] and returns the corresponding query pool event
// ([query.PoolEvent]). Some [PublishEvent] events don't map to a query pool event, in which
// case this method handles that event and returns nil.
func (o *Optimistic[K, N, M]) handleEvent(ctx context.Context, ev PublishEvent) (out query.PoolEvent) {
	_, span := o.tracer.Start(ctx, "Optimistic.handleEvent", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventPublishStart[K, N]:
		o.started = true
		o.walking = true
		o.target = ev.Target
		o.discover(o.seeds)

		return &query.EventPoolAddFindCloserQuery[K, N]{
			QueryID: o.queryID,
			Target:  ev.Target,
			Seed:    o.seeds,
		}
	case *EventPublishStop:
		o.stopped = true
		if !o.walking {
			return nil
		}

		return &query.EventPoolStopQuery{QueryID: o.queryID}
	case *EventPublishNodeResponse[K, N]:
		o.discover(ev.CloserNodes)

		return &query.EventPoolNodeResponse[K, N]{
			QueryID:     o.queryID,
			NodeID:      ev.NodeID,
			CloserNodes: ev.CloserNodes,
		}
	case *EventPublishNodeFailure[K, N]:
		return &query.EventPoolNodeFailure[K, N]{
			QueryID: o.queryID,
			NodeID:  ev.NodeID,
			Error:   ev.Error,
		}
	case *EventPublishStoreRecordSuccess[K, N, M]:
		delete(o.waiting, ev.NodeID.String())
	case *EventPublishStoreRecordFailure[K, N, M]:
		delete(o.waiting, ev.NodeID.String())
		o.fail(ev.NodeID, ev.Error)
	case *EventPublishPoll:
		// ignore, nothing to do
		return &query.EventPoolPoll{}
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	return nil
}

// advancePool advances the query pool with the event returned by [Optimistic.handleEvent] and
// translates the state it reports into a [PublishState]. The boolean reports whether that
// state is one the caller should return; when it is false the returned state is nil.
//
// A pool that reports it is waiting is not terminal here, since a node may be waiting to be asked
// to store the record while the walk is still in progress.
func (o *Optimistic[K, N, M]) advancePool(ctx context.Context, now time.Time, ev query.PoolEvent) (out PublishState, term bool) {
	ctx, span := o.tracer.Start(ctx, "Optimistic.advancePool", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	state := o.queryPool.Advance(ctx, now, ev)
	switch st := state.(type) {
	case *query.StatePoolFindCloser[K, N]:
		return &StatePublishFindCloser[K, N]{
			QueryID: st.QueryID,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}, true
	case *query.StatePoolWaitingAtCapacity:
		// the walk cannot be advanced, but a store may still be outstanding, so the time it
		// could next be advanced is kept for the caller rather than returned here
		o.nextDue = st.NextDue
	case *query.StatePoolWaitingWithCapacity:
		o.nextDue = st.NextDue
	case *query.StatePoolQueryFinished[K, N]:
		o.queryStats = st.Stats
		o.discover(st.ClosestNodes)
		o.endWalk()
	case *query.StatePoolQueryTimeout:
		o.queryStats = st.Stats
		o.endWalk()
	case *query.StatePoolIdle:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected pool state: %T", st))
	}

	return nil, false
}

// discover records the nodes as having been found by the walk, queueing a store for any that
// falls within the individual threshold. Nodes already seen are ignored.
func (o *Optimistic[K, N, M]) discover(nodes []N) {
	for _, n := range nodes {
		key := n.String()
		if _, ok := o.seen[key]; ok {
			continue
		}
		o.seen[key] = struct{}{}

		distance := netsize.NormedDistance(o.target, n.Key())
		o.found = append(o.found, foundNode[N]{node: n, distance: distance})

		if distance < o.individualThreshold {
			o.todo[key] = n
		}
	}

	slices.SortFunc(o.found, func(a, b foundNode[N]) int {
		if a.distance < b.distance {
			return -1
		} else if a.distance > b.distance {
			return 1
		}
		return 0
	})
}

// setThresholdMet reports whether the mean distance of the closest known nodes has fallen below
// the set threshold, which is the point at which the walk is abandoned.
//
// It reports false until the replication factor's worth of nodes have been found, since the mean
// of a smaller set is not the quantity the threshold was derived for. A network holding fewer
// nodes than that therefore always walks to exhaustion.
func (o *Optimistic[K, N, M]) setThresholdMet() bool {
	if len(o.found) < o.cfg.ReplicationFactor {
		return false
	}

	var sum float64
	for _, f := range o.found[:o.cfg.ReplicationFactor] {
		sum += f.distance
	}

	return sum/float64(o.cfg.ReplicationFactor) < o.setThreshold
}

// endWalk marks the walk as over and queues a store for every one of the closest known nodes
// that has not already been asked. An abandoned operation queues nothing, so that a node it
// never contacted is not reported as one it failed to store with.
func (o *Optimistic[K, N, M]) endWalk() {
	o.walking = false

	if o.stopped {
		return
	}

	limit := min(o.cfg.ReplicationFactor, len(o.found))
	for _, f := range o.found[:limit] {
		key := f.node.String()
		if _, ok := o.waiting[key]; ok {
			continue
		}
		if _, ok := o.failed[key]; ok {
			continue
		}
		if slices.ContainsFunc(o.contacted, func(n N) bool { return n.String() == key }) {
			continue
		}
		o.todo[key] = f.node
	}
}

// fail records that a node did not store the record.
func (o *Optimistic[K, N, M]) fail(n N, err error) {
	o.failed[n.String()] = struct {
		Node N
		Err  error
	}{Node: n, Err: err}
}
