package query

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
)

// QueryStats holds the counts and timings a [Query] accumulates as it runs. A query reports
// them with every state it emits.
//
// The counters track requests rather than nodes, and each request that has completed counts
// once, so Success and Failure together never exceed Requests.
type QueryStats struct {
	Start    time.Time // the time the first request was dispatched, zero until then
	End      time.Time // the time the query finished, zero until then
	Requests int       // the number of requests dispatched
	Success  int       // the number of requests answered within their deadline
	Failure  int       // the number of requests that errored or passed their deadline
}

// QueryConfig specifies optional configuration for a Query
type QueryConfig struct {
	Concurrency    int           // the maximum number of concurrent requests that may be in flight
	NumResults     int           // for a closest-nodes query, the number of nodes to contact successfully before completing and the most it returns; for a coverage query, the number of nodes to contact without finding a region member before concluding the region is empty
	RequestTimeout time.Duration // the timeout for contacting a single node
	Timeout        time.Duration // the time to wait before the query is considered to have stopped making progress

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *QueryConfig) Validate() error {
	if cfg.Concurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}
	if cfg.NumResults < 1 {
		return &coordt.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("num results must be greater than zero"),
		}
	}
	if cfg.RequestTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}
	if cfg.Timeout < 1 {
		return &coordt.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}
	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "QueryConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}
	return nil
}

// DefaultQueryConfig returns the default configuration options for a Query.
// Options may be overridden before passing to NewQuery
func DefaultQueryConfig() *QueryConfig {
	return &QueryConfig{
		Concurrency:    3,
		NumResults:     20,
		RequestTimeout: time.Minute,
		Timeout:        5 * time.Minute,
		Tracer:         coordt.NoopTracer(),
	}
}

// A Query is one iterative Kademlia lookup.
//
// It contacts nodes in ascending order of distance from a target key. Each response supplies
// further nodes, so the query walks towards the target until the closest nodes it knows of
// stop improving. The ordering is decided by the [NodeIter] the query is given rather than by
// the query itself.
//
// A query makes two independent choices. It either asks each node for the nodes it holds
// closest to the target, emitting [StateQueryFindCloser], or sends each node a message and
// takes the closer nodes from the reply, emitting [StateQuerySendMessage]. Separately, it
// either stops at the [QueryConfig.NumResults] closest nodes or, for a find-closer query,
// enumerates every node inside the region named by a prefix of the target. [NewFindCloserQuery]
// and [NewQuery] create the two request kinds, both stopping at the closest nodes;
// [NewCoverageQuery] creates a find-closer query that enumerates a region instead.
//
// Every advance walks the whole iterator and returns at the first thing it can do, so one
// advance produces at most one request. Along the way it marks any node whose request
// deadline has passed as unresponsive, which frees a concurrency slot. The first node that
// has not been contacted is then sent a request if a slot is free, and that instruction is
// what the advance returns. The walk stops early if every slot is already in use.
//
// A closest-nodes query is finished when, walking outward, it reaches a node that responded
// successfully having already counted [QueryConfig.NumResults] successes with no request
// outstanding nearer the target. A coverage query is instead finished when the walk reaches a
// node outside the region with nothing nearer still in flight, having either found a region
// member or contacted [QueryConfig.NumResults] nodes without one. Either kind also finishes
// when the walk reaches the end with nothing in flight and nothing left to contact, or when it
// is cancelled by [EventQueryCancel]. Finishing is sticky, since the finished flag is tested
// before the event, so every later advance returns the same [StateQueryFinished] carrying the
// same nodes.
//
// Two deadlines apply and the query treats them differently. [QueryConfig.RequestTimeout]
// bounds a single request and the query enforces it itself by marking the node unresponsive.
// [QueryConfig.Timeout] bounds the whole query, is set when the first node is contacted, and
// is only reported: the query keeps running past it and the caller decides what running out
// of time means.
//
// The node the query is running on is excluded throughout, both from the seed set and from
// the closer nodes carried by any response.
type Query[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// self is the node id of the system the query is running on. It is excluded from the seed
	// set and from the closer nodes carried by any response.
	self N

	// id is the query id the query reports its progress under
	id coordt.ActivityID

	// cfg is a copy of the optional configuration supplied to the query
	cfg QueryConfig

	// iter holds every node the query knows of along with the state of the request made to
	// it, and decides the order in which the query walks them
	iter NodeIter[K, N]

	// target is the key the query walks towards. For a closest-nodes query it is the
	// key whose closest nodes are wanted; for a coverage query it is a key inside the
	// region, whose first prefixLen bits name that region.
	target K

	// msg is the message sent to each node contacted, and is unset when findCloser is
	// true, which covers both closest-nodes and coverage queries.
	msg M

	// findCloser reports whether the query asks each node for the nodes it holds closest to
	// the target rather than sending it msg
	findCloser bool

	// coverage reports whether the query enumerates a region rather than finding the
	// closest NumResults nodes. A coverage query keeps walking while it finds nodes
	// inside the region and finishes once the region is exhausted.
	coverage bool

	// prefixLen is the bit length of the region prefix when coverage is set. A node is
	// inside the region when its key shares at least prefixLen leading bits with target.
	prefixLen int

	// stats holds the counts and timings accumulated so far
	stats QueryStats

	// finished indicates that the query has completed its work or has been stopped. It is
	// tested before the incoming event, so a finished query reports the same result on every
	// later advance.
	finished bool

	// deadline is the time by which the query must have completed. It is set when the
	// query contacts its first node. The query reports it in its waiting states but does
	// not act on it, since the caller decides what a query that has run out of time means.
	deadline time.Time

	// nextDue is the earliest time at which advancing the query again could make
	// progress without a response arriving, or the zero time if there is no such
	// time. It is recomputed whenever the query reports that it is waiting, and is
	// never later than the truth: a request that completes only removes a deadline
	// from the set, so a stale value wakes the caller early rather than late.
	nextDue time.Time

	// targetNodes is the set of responsive nodes the query settled on, populated once
	// it has been marked finished. A closest-nodes query holds up to
	// [QueryConfig.NumResults] nodes closest to the target; a coverage query holds
	// every responsive node it found inside the region, which may be more.
	targetNodes []N

	// inFlight is the number of requests awaiting a response. It never exceeds
	// [QueryConfig.Concurrency], which is what bounds how many requests the query has out at
	// once.
	inFlight int
}

// NewFindCloserQuery creates a query that asks each node it contacts for the nodes that node
// holds closest to target, and takes those as the nodes to contact next. It sends no message
// of its own, so the message type parameter goes unused, and it reports the node it wants
// contacted by emitting [StateQueryFindCloser].
//
// The query is seeded with knownClosestNodes, from which self is excluded, and orders every
// node it learns of using iter. It reports its progress under the query id id. A nil cfg
// uses [DefaultQueryConfig], and a non-nil one is validated.
func NewFindCloserQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, id coordt.ActivityID, target K, iter NodeIter[K, N], knownClosestNodes []N, cfg *QueryConfig) (*Query[K, N, M], error) {
	var empty M
	q, err := NewQuery(self, id, target, empty, iter, knownClosestNodes, cfg)
	if err != nil {
		return nil, err
	}
	q.findCloser = true
	return q, nil
}

// NewCoverageQuery creates a query that finds every node inside the region named by
// the first prefixLen bits of target. Like a find-closer query it asks each node it
// contacts for the nodes closest to target, but instead of stopping at the closest
// NumResults nodes it walks in to the region and continues until every node it has
// heard of inside the region has been contacted, then reports them all.
//
// target is any key inside the region. QueryConfig.NumResults is the walk-in bound
// rather than a result cap: if the query contacts that many nodes without entering
// the region it concludes the region is empty. The result is never truncated to it.
func NewCoverageQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, id coordt.ActivityID, target K, prefixLen int, iter NodeIter[K, N], knownClosestNodes []N, cfg *QueryConfig) (*Query[K, N, M], error) {
	if prefixLen < 0 || prefixLen > target.BitLen() {
		return nil, &coordt.ConfigurationError{
			Component: "Query",
			Err:       fmt.Errorf("prefix length must be between 0 and %d", target.BitLen()),
		}
	}
	q, err := NewFindCloserQuery[K, N, M](self, id, target, iter, knownClosestNodes, cfg)
	if err != nil {
		return nil, err
	}
	q.coverage = true
	q.prefixLen = prefixLen
	return q, nil
}

// inPrefix reports whether k is inside the region a coverage query enumerates.
func (q *Query[K, N, M]) inPrefix(k K) bool {
	return k.CommonPrefixLength(q.target) >= q.prefixLen
}

// NewQuery creates a query that sends msg to each node it contacts and takes the nodes
// closer to target from each reply, so the reply serves both as the answer to the message
// and as the source of the next nodes to walk to. It reports the node it wants contacted by
// emitting [StateQuerySendMessage].
//
// The query is seeded with knownClosestNodes, from which self is excluded, and orders every
// node it learns of using iter. It reports its progress under the query id id. A nil cfg
// uses [DefaultQueryConfig], and a non-nil one is validated.
func NewQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, id coordt.ActivityID, target K, msg M, iter NodeIter[K, N], knownClosestNodes []N, cfg *QueryConfig) (*Query[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultQueryConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	for _, node := range knownClosestNodes {
		// exclude self from closest nodes
		if key.Equal(node.Key(), self.Key()) {
			continue
		}
		iter.Add(&NodeStatus[K, N]{
			NodeID: node,
			State:  &StateNodeNotContacted{},
		})
	}

	return &Query[K, N, M]{
		self:   self,
		id:     id,
		cfg:    *cfg,
		msg:    msg,
		iter:   iter,
		target: target,
	}, nil
}

func (q *Query[K, N, M]) Advance(ctx context.Context, now time.Time, ev QueryEvent) (out QueryState) {
	ctx, span := q.cfg.Tracer.Start(ctx, "Query.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	if q.finished {
		return &StateQueryFinished[K, N]{
			ActivityID:   q.id,
			Stats:        q.stats,
			Target:       q.target,
			ClosestNodes: q.targetNodes,
		}
	}

	switch tev := ev.(type) {
	case *EventQueryCancel:
		q.markFinished(ctx, now)
		return &StateQueryFinished[K, N]{
			ActivityID:   q.id,
			Stats:        q.stats,
			Target:       q.target,
			ClosestNodes: q.targetNodes,
		}
	case *EventQueryNodeResponse[K, N]:
		q.onNodeResponse(ctx, tev.NodeID, tev.CloserNodes)
	case *EventQueryNodeFailure[K, N]:
		span.RecordError(tev.Error)
		q.onNodeFailure(ctx, tev.NodeID)
	case *EventQueryPoll:
		// no event to process

	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	// count number of successes in the order of the iteration
	successes := 0

	// count successes inside the region, used only by a coverage query
	inPrefixSuccesses := 0

	// progressing is set to true if any node is still awaiting contact
	progressing := false

	// TODO: if stalled then we should contact all remaining nodes that have not already been queried
	atCapacity := func() bool {
		return q.inFlight >= q.cfg.Concurrency
	}

	// get all the nodes in order of distance from the target
	// TODO: turn this into a walk or iterator on trie.Trie

	var returnState QueryState

	// atCapacityWaiting records that the walk stopped because every request slot is
	// in use. The resulting state is built after the walk so that the scan for the
	// next deadline is not run inside iter.Each.
	atCapacityWaiting := false

	q.iter.Each(ctx, func(ctx context.Context, ni *NodeStatus[K, N]) bool {
		switch st := ni.State.(type) {
		case *StateNodeWaiting:
			if now.After(st.Deadline) {
				// mark node as unresponsive
				ni.State = &StateNodeUnresponsive{}
				q.inFlight--
				q.stats.Failure++
			} else if atCapacity() {
				atCapacityWaiting = true
				return true
			} else {
				// The iterator is still waiting for a result from a node so can't be considered done
				progressing = true
			}
		case *StateNodeSucceeded:
			successes++
			// A coverage query never stops at a succeeded node: it stops when the walk
			// reaches the region boundary, handled in the not-contacted case.
			if q.coverage {
				if q.inPrefix(ni.NodeID.Key()) {
					inPrefixSuccesses++
				}
				break
			}
			// The iterator has attempted to contact all nodes closer than this one.
			// If the iterator is not progressing then it doesn't expect any more nodes to be added to the list.
			// If it has contacted at least NumResults nodes successfully then the iteration is done.
			if !progressing && successes >= q.cfg.NumResults {
				q.markFinished(ctx, now)
				returnState = &StateQueryFinished[K, N]{
					ActivityID:   q.id,
					Stats:        q.stats,
					Target:       q.target,
					ClosestNodes: q.targetNodes,
				}
				return true
			}

		case *StateNodeNotContacted:
			if q.coverage && !q.inPrefix(ni.NodeID.Key()) && !progressing &&
				(inPrefixSuccesses > 0 || successes >= q.cfg.NumResults) {
				// The walk has reached a node outside the region with everything closer
				// resolved, so the region is exhausted, or empty if none was found within
				// the walk-in bound. Report the region's members.
				q.markFinished(ctx, now)
				returnState = &StateQueryFinished[K, N]{
					ActivityID:   q.id,
					Stats:        q.stats,
					Target:       q.target,
					ClosestNodes: q.targetNodes,
				}
				return true
			}
			if !atCapacity() {
				deadline := now.Add(q.cfg.RequestTimeout)
				ni.State = &StateNodeWaiting{Deadline: deadline}
				q.inFlight++
				q.stats.Requests++
				if q.stats.Start.IsZero() {
					q.stats.Start = now
					q.deadline = q.stats.Start.Add(q.cfg.Timeout)
				}

				if q.findCloser {
					returnState = &StateQueryFindCloser[K, N]{
						NodeID:     ni.NodeID,
						ActivityID: q.id,
						Stats:      q.stats,
						Target:     q.target,
					}
				} else {
					returnState = &StateQuerySendMessage[K, N, M]{
						NodeID:     ni.NodeID,
						ActivityID: q.id,
						Stats:      q.stats,
						Message:    q.msg,
					}
				}
				return true
			}

			atCapacityWaiting = true

			return true
		case *StateNodeUnresponsive:
			// ignore
		case *StateNodeFailed:
			// ignore
		default:
			panic(fmt.Sprintf("unexpected state: %T", ni.State))
		}

		return false
	})

	if returnState != nil {
		return returnState
	}

	if atCapacityWaiting {
		q.nextDue = q.earliestNodeDeadline(ctx)
		return &StateQueryWaitingAtCapacity{
			ActivityID: q.id,
			Stats:      q.stats,
			Deadline:   q.deadline,
			NextDue:    q.nextDue,
		}
	}

	if q.inFlight > 0 {
		// The iterator is still waiting for results and not at capacity
		q.nextDue = q.earliestNodeDeadline(ctx)
		return &StateQueryWaitingWithCapacity{
			ActivityID: q.id,
			Stats:      q.stats,
			Deadline:   q.deadline,
			NextDue:    q.nextDue,
		}
	}

	// The iterator is finished because all available nodes have been contacted
	// and the iterator is not waiting for any more results.
	q.markFinished(ctx, now)
	return &StateQueryFinished[K, N]{
		ActivityID:   q.id,
		Stats:        q.stats,
		Target:       q.target,
		ClosestNodes: q.targetNodes,
	}
}

// earliestNodeDeadline returns the earliest request deadline among the nodes the
// query is waiting on, or the zero time if it is waiting on none. That instant is
// when advancing the query would next mark a node unresponsive, which is the only
// way it can make progress without a response arriving.
func (q *Query[K, N, M]) earliestNodeDeadline(ctx context.Context) time.Time {
	var earliest time.Time
	q.iter.Each(ctx, func(ctx context.Context, ni *NodeStatus[K, N]) bool {
		st, ok := ni.State.(*StateNodeWaiting)
		if ok && (earliest.IsZero() || st.Deadline.Before(earliest)) {
			earliest = st.Deadline
		}
		return false
	})
	return earliest
}

func (q *Query[K, N, M]) markFinished(ctx context.Context, now time.Time) {
	q.finished = true
	q.nextDue = time.Time{}
	if q.stats.End.IsZero() {
		q.stats.End = now
	}

	q.targetNodes = make([]N, 0, q.cfg.NumResults)

	q.iter.Each(ctx, func(ctx context.Context, ni *NodeStatus[K, N]) bool {
		switch ni.State.(type) {
		case *StateNodeSucceeded:
			// A coverage query reports every succeeded node inside the region and is
			// not capped at NumResults, so a region larger than NumResults is seen.
			if q.coverage {
				if q.inPrefix(ni.NodeID.Key()) {
					q.targetNodes = append(q.targetNodes, ni.NodeID)
				}
				return false
			}
			q.targetNodes = append(q.targetNodes, ni.NodeID)
			if len(q.targetNodes) >= q.cfg.NumResults {
				return true
			}
		}
		return false
	})
}

// onNodeResponse processes the result of a successful response received from a node.
func (q *Query[K, N, M]) onNodeResponse(ctx context.Context, node N, closer []N) {
	ni, found := q.iter.Find(node.Key())
	if !found {
		// got a rogue message
		return
	}
	switch st := ni.State.(type) {
	case *StateNodeWaiting:
		q.inFlight--
		q.stats.Success++
	case *StateNodeUnresponsive:
		// The request passed its deadline and is counted as a failure. The node is still
		// marked as having succeeded and the closer nodes it carries are still used, since a
		// node that answers late is slow rather than absent. Excluding it would shrink the
		// result below the replication factor on a slow network.
		//
		// TODO: revisit. Being marked as succeeded also lets a late answer count towards
		// [QueryConfig.NumResults] and so finish the query, and puts the node in the result
		// the caller acts on, which for a provide is the set that receives the record.

	case *StateNodeNotContacted:
		// ignore duplicate or late response
		return
	case *StateNodeFailed:
		// ignore duplicate or late response
		return
	case *StateNodeSucceeded:
		// ignore duplicate or late response
		return
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}

	// add closer nodes to list
	for _, id := range closer {
		// exclude self from closest nodes
		if key.Equal(id.Key(), q.self.Key()) {
			continue
		}
		q.iter.Add(&NodeStatus[K, N]{
			NodeID: id,
			State:  &StateNodeNotContacted{},
		})
	}
	ni.State = &StateNodeSucceeded{}
}

// onNodeFailure processes the result of a failed attempt to contact a node.
func (q *Query[K, N, M]) onNodeFailure(ctx context.Context, node N) {
	ni, found := q.iter.Find(node.Key())
	if !found {
		// got a rogue message
		return
	}
	switch st := ni.State.(type) {
	case *StateNodeWaiting:
		q.inFlight--
		q.stats.Failure++
	case *StateNodeUnresponsive:
		// update node state to failed
		break
	case *StateNodeNotContacted:
		// update node state to failed
		break
	case *StateNodeFailed:
		// ignore duplicate or late response
		return
	case *StateNodeSucceeded:
		// ignore duplicate or late response
		return
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}

	ni.State = &StateNodeFailed{}
}

type QueryState interface {
	queryState()
}

// StateQueryFinished indicates that the [Query] has finished.
type StateQueryFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	ActivityID   coordt.ActivityID
	Stats        QueryStats
	Target       K   // the key the query was looking for the closest nodes to
	ClosestNodes []N // the nodes the query settled on: the closest to the target, or a coverage query's region members
}

// StateQueryFindCloser indicates that the [Query] wants to send a find closer nodes message to a node.
type StateQueryFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	ActivityID coordt.ActivityID
	Target     K // the key that the query wants to find closer nodes for
	NodeID     N // the node to send the message to
	Stats      QueryStats
}

// StateQuerySendMessage indicates that the [Query] wants to send a message to a node.
type StateQuerySendMessage[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	ActivityID coordt.ActivityID
	NodeID     N // the node to send the message to
	Message    M
	Stats      QueryStats
}

// StateQueryWaitingAtCapacity indicates that the [Query] is waiting for results and is at capacity.
type StateQueryWaitingAtCapacity struct {
	ActivityID coordt.ActivityID
	Stats      QueryStats
	Deadline   time.Time // the time by which the query must have completed
	NextDue    time.Time // the earliest time advancing the query could make progress, zero if there is none
}

// StateQueryWaitingWithCapacity indicates that the [Query] is waiting for results but has no further nodes to contact.
type StateQueryWaitingWithCapacity struct {
	ActivityID coordt.ActivityID
	Stats      QueryStats
	Deadline   time.Time // the time by which the query must have completed
	NextDue    time.Time // the earliest time advancing the query could make progress, zero if there is none
}

// queryState() ensures that only [Query] states can be assigned to a QueryState.
func (*StateQueryFinished[K, N]) queryState()       {}
func (*StateQueryFindCloser[K, N]) queryState()     {}
func (*StateQuerySendMessage[K, N, M]) queryState() {}
func (*StateQueryWaitingAtCapacity) queryState()    {}
func (*StateQueryWaitingWithCapacity) queryState()  {}

type QueryEvent interface {
	queryEvent()
}

// EventQueryMessageResponse notifies a query to stop all work and enter the finished state.
type EventQueryCancel struct{}

// EventQueryNodeResponse notifies a [Query] that an attempt to contact a node has received a successful response.
type EventQueryNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventQueryNodeFailure notifies a [Query] that an attempt to to contact a node has failed.
type EventQueryNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// EventQueryPoll is an event that signals a [Query] that it can perform housekeeping work.
type EventQueryPoll struct{}

// queryEvent() ensures that only events accepted by [Query] can be assigned to a [QueryEvent].
func (*EventQueryCancel) queryEvent()             {}
func (*EventQueryNodeResponse[K, N]) queryEvent() {}
func (*EventQueryNodeFailure[K, N]) queryEvent()  {}
func (*EventQueryPoll) queryEvent()               {}
