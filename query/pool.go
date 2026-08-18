package query

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
)

type Pool[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	// self is the node id of the system the pool is running on
	self       N
	queries    []*Query[K, N, M]
	queryIndex map[coordt.QueryID]*Query[K, N, M]

	// cfg is a copy of the optional configuration supplied to the pool
	cfg PoolConfig

	// queriesInFlight is number of queries that are waiting for message responses
	queriesInFlight int
}

// PoolConfig specifies optional configuration for a Pool
type PoolConfig struct {
	Concurrency      int           // the maximum number of queries that may be waiting for message responses at any one time
	Timeout          time.Duration // the time to wait before terminating a query that is not making progress
	Replication      int           // the 'k' parameter defined by Kademlia
	QueryConcurrency int           // the maximum number of concurrent requests that each query may have in flight
	RequestTimeout   time.Duration // the timeout queries should use for contacting a single node

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *PoolConfig) Validate() error {
	if cfg.Concurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("concurrency must be greater than zero"),
		}
	}
	if cfg.Timeout < 1 {
		return &coordt.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("timeout must be greater than zero"),
		}
	}
	if cfg.Replication < 1 {
		return &coordt.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("replication must be greater than zero"),
		}
	}

	if cfg.QueryConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("query concurrency must be greater than zero"),
		}
	}

	if cfg.RequestTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}

	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "PoolConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}

	return nil
}

// DefaultPoolConfig returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		Concurrency:      3,
		Timeout:          5 * time.Minute,
		Replication:      20,
		QueryConcurrency: 3,
		RequestTimeout:   time.Minute,
		Tracer:           coordt.NoopTracer(),
	}
}

func NewPool[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, cfg *PoolConfig) (*Pool[K, N, M], error) {
	if cfg == nil {
		cfg = DefaultPoolConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Pool[K, N, M]{
		self:       self,
		cfg:        *cfg,
		queries:    make([]*Query[K, N, M], 0),
		queryIndex: make(map[coordt.QueryID]*Query[K, N, M]),
	}, nil
}

// Advance advances the state of the pool by attempting to advance one of its queries
func (p *Pool[K, N, M]) Advance(ctx context.Context, now time.Time, ev PoolEvent) PoolState {
	ctx, span := p.cfg.Tracer.Start(ctx, "Pool.Advance")
	defer span.End()

	// reset the in flight counter so it can be calculated as the queries are advanced
	p.queriesInFlight = 0

	// eventQueryID keeps track of a query that was advanced via a specific event, to avoid it
	// being advanced twice
	eventQueryID := coordt.InvalidQueryID

	switch tev := ev.(type) {
	case *EventPoolAddFindCloserQuery[K, N]:
		p.addFindCloserQuery(ctx, tev.QueryID, tev.Target, tev.Seed, tev.NumResults)
	case *EventPoolAddQuery[K, N, M]:
		p.addQuery(ctx, tev.QueryID, tev.Target, tev.Message, tev.Seed, tev.NumResults)
		// TODO: return error as state
	case *EventPoolStopQuery:
		if qry, ok := p.queryIndex[tev.QueryID]; ok {
			state, terminal := p.advanceQuery(ctx, now, qry, &EventQueryCancel{})
			if terminal {
				return state
			}
			eventQueryID = qry.id
		}
	case *EventPoolNodeResponse[K, N]:
		if qry, ok := p.queryIndex[tev.QueryID]; ok {
			state, terminal := p.advanceQuery(ctx, now, qry, &EventQueryNodeResponse[K, N]{
				NodeID:      tev.NodeID,
				CloserNodes: tev.CloserNodes,
			})
			if terminal {
				return state
			}
			eventQueryID = qry.id
		}
	case *EventPoolNodeFailure[K, N]:
		if qry, ok := p.queryIndex[tev.QueryID]; ok {
			state, terminal := p.advanceQuery(ctx, now, qry, &EventQueryNodeFailure[K, N]{
				NodeID: tev.NodeID,
				Error:  tev.Error,
			})
			if terminal {
				return state
			}
			eventQueryID = qry.id
		}
	case *EventPoolPoll:
		// no event to process
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	if len(p.queries) == 0 {
		return &StatePoolIdle{}
	}

	// Attempt to advance another query
	for _, qry := range p.queries {
		if eventQueryID == qry.id {
			// avoid advancing query twice
			continue
		}

		state, terminal := p.advanceQuery(ctx, now, qry, &EventQueryPoll{})
		if terminal {
			return state
		}

		// check if we have the maximum number of queries in flight
		if p.queriesInFlight >= p.cfg.Concurrency {
			return &StatePoolWaitingAtCapacity{NextDue: p.nextDue()}
		}
	}

	if p.queriesInFlight > 0 {
		return &StatePoolWaitingWithCapacity{NextDue: p.nextDue()}
	}

	return &StatePoolIdle{NextDue: p.nextDue()}
}

// nextDue returns the earliest time at which advancing the pool could make progress
// without a response arriving, or the zero time if there is no such time. It is the
// earliest of every query's own next due time and every query's deadline, since a
// query that passes its deadline is timed out.
//
// It reads each query's last reported due time rather than advancing it. A query
// that has not been advanced this round cannot have gained an earlier deadline,
// because a deadline is only set when a request is dispatched.
func (p *Pool[K, N, M]) nextDue() time.Time {
	var earliest time.Time
	for _, qry := range p.queries {
		for _, t := range [...]time.Time{qry.nextDue, qry.deadline} {
			if t.IsZero() {
				continue
			}
			if earliest.IsZero() || t.Before(earliest) {
				earliest = t
			}
		}
	}
	return earliest
}

func (p *Pool[K, N, M]) advanceQuery(ctx context.Context, now time.Time, qry *Query[K, N, M], qev QueryEvent) (PoolState, bool) {
	state := qry.Advance(ctx, now, qev)
	switch st := state.(type) {
	case *StateQueryFindCloser[K, N]:
		p.queriesInFlight++
		return &StatePoolFindCloser[K, N]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Target:  st.Target,
		}, true
	case *StateQuerySendMessage[K, N, M]:
		p.queriesInFlight++
		return &StatePoolSendMessage[K, N, M]{
			QueryID: st.QueryID,
			Stats:   st.Stats,
			NodeID:  st.NodeID,
			Message: st.Message,
		}, true
	case *StateQueryFinished[K, N]:
		p.removeQuery(qry.id)
		return &StatePoolQueryFinished[K, N]{
			QueryID:      st.QueryID,
			Stats:        st.Stats,
			Target:       st.Target,
			ClosestNodes: st.ClosestNodes,
		}, true
	case *StateQueryWaitingAtCapacity:
		if now.After(st.Deadline) {
			p.removeQuery(qry.id)
			return &StatePoolQueryTimeout{
				QueryID: st.QueryID,
				Stats:   st.Stats,
			}, true
		}
		p.queriesInFlight++
	case *StateQueryWaitingWithCapacity:
		if now.After(st.Deadline) {
			p.removeQuery(qry.id)
			return &StatePoolQueryTimeout{
				QueryID: st.QueryID,
				Stats:   st.Stats,
			}, true
		}
		p.queriesInFlight++
	}
	return nil, false
}

func (p *Pool[K, N, M]) removeQuery(queryID coordt.QueryID) {
	for i := range p.queries {
		if p.queries[i].id != queryID {
			continue
		}
		// remove from slice
		copy(p.queries[i:], p.queries[i+1:])
		p.queries[len(p.queries)-1] = nil
		p.queries = p.queries[:len(p.queries)-1]
		break
	}
	delete(p.queryIndex, queryID)
}

// Stats returns the stats accumulated so far by the query with the given id. The bool result is
// false if no query with that id is running in the pool.
func (p *Pool[K, N, M]) Stats(queryID coordt.QueryID) (QueryStats, bool) {
	qry, ok := p.queryIndex[queryID]
	if !ok {
		return QueryStats{}, false
	}
	return qry.stats, true
}

// addQuery adds a query to the pool, returning the new query id
// TODO: remove target argument and use msg.Target
func (p *Pool[K, N, M]) addQuery(ctx context.Context, queryID coordt.QueryID, target K, msg M, knownClosestNodes []N, numResults int) error {
	if _, exists := p.queryIndex[queryID]; exists {
		return fmt.Errorf("query id already in use")
	}
	iter := NewClosestNodesIter[K, N](target)

	qryCfg := DefaultQueryConfig()
	qryCfg.Concurrency = p.cfg.QueryConcurrency
	qryCfg.RequestTimeout = p.cfg.RequestTimeout
	qryCfg.Timeout = p.cfg.Timeout
	qryCfg.Tracer = p.cfg.Tracer

	if numResults > 0 {
		qryCfg.NumResults = numResults
	}

	qry, err := NewQuery(p.self, queryID, target, msg, iter, knownClosestNodes, qryCfg)
	if err != nil {
		return fmt.Errorf("new query: %w", err)
	}

	p.queries = append(p.queries, qry)
	p.queryIndex[queryID] = qry

	return nil
}

// addQuery adds a find closer query to the pool, returning the new query id
func (p *Pool[K, N, M]) addFindCloserQuery(ctx context.Context, queryID coordt.QueryID, target K, knownClosestNodes []N, numResults int) error {
	if _, exists := p.queryIndex[queryID]; exists {
		return fmt.Errorf("query id already in use")
	}
	iter := NewClosestNodesIter[K, N](target)

	qryCfg := DefaultQueryConfig()
	qryCfg.Concurrency = p.cfg.QueryConcurrency
	qryCfg.RequestTimeout = p.cfg.RequestTimeout
	qryCfg.Timeout = p.cfg.Timeout
	qryCfg.Tracer = p.cfg.Tracer

	if numResults > 0 {
		qryCfg.NumResults = numResults
	}

	qry, err := NewFindCloserQuery[K, N, M](p.self, queryID, target, iter, knownClosestNodes, qryCfg)
	if err != nil {
		return fmt.Errorf("new query: %w", err)
	}

	p.queries = append(p.queries, qry)
	p.queryIndex[queryID] = qry

	return nil
}

// States

type PoolState interface {
	poolState()
}

// StatePoolIdle indicates that the pool is idle, i.e. there are no queries to process.
type StatePoolIdle struct {
	NextDue time.Time // the earliest time advancing the pool could make progress, zero if there is none
}

// StatePoolFindCloser indicates that a pool query wants to send a find closer nodes message to a node.
type StatePoolFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	Target  K // the key that the query wants to find closer nodes for
	NodeID  N // the node to send the message to
	Stats   QueryStats
}

// StatePoolSendMessage indicates that a pool query wants to send a message to a node.
type StatePoolSendMessage[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID
	NodeID  N // the node to send the message to
	Message M
	Stats   QueryStats
}

// StatePoolWaitingAtCapacity indicates that at least one query is waiting for results and the pool has reached
// its maximum number of concurrent queries.
type StatePoolWaitingAtCapacity struct {
	NextDue time.Time // the earliest time advancing the pool could make progress, zero if there is none
}

// StatePoolWaitingWithCapacity indicates that at least one query is waiting for results but capacity to
// start more is available.
type StatePoolWaitingWithCapacity struct {
	NextDue time.Time // the earliest time advancing the pool could make progress, zero if there is none
}

// StatePoolQueryFinished indicates that a query has finished.
type StatePoolQueryFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID      coordt.QueryID
	Stats        QueryStats
	Target       K // the key the query was looking for the closest nodes to
	ClosestNodes []N
}

// StatePoolQueryTimeout indicates that a query has timed out.
type StatePoolQueryTimeout struct {
	QueryID coordt.QueryID
	Stats   QueryStats
}

// poolState() ensures that only Pool states can be assigned to the PoolState interface.
func (*StatePoolIdle) poolState()                 {}
func (*StatePoolFindCloser[K, N]) poolState()     {}
func (*StatePoolSendMessage[K, N, M]) poolState() {}
func (*StatePoolWaitingAtCapacity) poolState()    {}
func (*StatePoolWaitingWithCapacity) poolState()  {}
func (*StatePoolQueryFinished[K, N]) poolState()  {}
func (*StatePoolQueryTimeout) poolState()         {}

// PoolEvent is an event intended to advance the state of a pool.
type PoolEvent interface {
	poolEvent()
}

// EventPoolAddQuery is an event that attempts to add a new query that finds closer nodes to a target key.
type EventPoolAddFindCloserQuery[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID    coordt.QueryID // the id to use for the new query
	Target     K              // the target key for the query
	Seed       []N            // an initial set of close nodes the query should use
	NumResults int            // the minimum number of nodes to successfully contact before considering iteration complete
}

// EventPoolAddQuery is an event that attempts to add a new query that sends a message.
type EventPoolAddQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID    coordt.QueryID // the id to use for the new query
	Target     K              // the target key for the query
	Message    M              // message to be sent to each node
	Seed       []N            // an initial set of close nodes the query should use
	NumResults int            // the minimum number of nodes to successfully contact before considering iteration complete
}

// EventPoolStopQuery notifies a [Pool] to stop a query.
type EventPoolStopQuery struct {
	QueryID coordt.QueryID // the id of the query that should be stopped
}

// EventPoolNodeResponse notifies a [Pool] that an attempt to contact a node has received a successful response.
type EventPoolNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     coordt.QueryID // the id of the query that sent the message
	NodeID      N              // the node the message was sent to
	CloserNodes []N            // the closer nodes sent by the node
}

// EventPoolNodeFailure notifies a [Pool] that an attempt to contact a node has failed.
type EventPoolNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the query that sent the message
	NodeID  N              // the node the message was sent to
	Error   error          // the error that caused the failure, if any
}

// EventPoolPoll is an event that signals the pool that it can perform housekeeping work such as time out queries.
type EventPoolPoll struct{}

// poolEvent() ensures that only events accepted by a [Pool] can be assigned to the [PoolEvent] interface.
func (*EventPoolAddQuery[K, N, M]) poolEvent()        {}
func (*EventPoolAddFindCloserQuery[K, N]) poolEvent() {}
func (*EventPoolStopQuery) poolEvent()                {}
func (*EventPoolNodeResponse[K, N]) poolEvent()       {}
func (*EventPoolNodeFailure[K, N]) poolEvent()        {}
func (*EventPoolPoll) poolEvent()                     {}
