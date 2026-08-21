package coordt

import (
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-libdht/kad"
)

// ActivityID identifies a strand of work a state machine is carrying out, such as a query, a
// publish, or a routing probe. It is set on the requests that work sends out and echoed back on
// their responses so the initiator can match a response to the activity it belongs to.
type ActivityID string

const InvalidActivityID ActivityID = ""

// StateMachine is a pure state machine: Advance applies an event and returns the resulting
// state. now is the time the event is being applied at, supplied by the caller so the machine
// has no clock of its own.
type StateMachine[E any, S any] interface {
	Advance(ctx context.Context, now time.Time, ev E) S
}

var (
	ErrNodeNotFound     = errors.New("node not found")
	ErrValueNotFound    = errors.New("value not found")
	ErrValueNotAccepted = errors.New("value not accepted")

	// ErrQueryTimeout is returned when a query ran out of time before it could visit
	// every node it had to.
	ErrQueryTimeout = errors.New("query timed out")
)

// QueryFunc is the type of the function called by Query to visit each node.
//
// The error result returned by the function controls how Query proceeds. If the function returns the special value
// SkipNode, Query skips fetching closer nodes from the current node. If the function returns the special value
// SkipRemaining, Query skips all visiting all remaining nodes. Otherwise, if the function returns a non-nil error,
// Query stops entirely and returns that error.
//
// The stats argument contains statistics on the progress of the query so far.
//
// K is the key type, N the node type and M the message type the query operates on.
type QueryFunc[K kad.Key[K], N kad.NodeID[K], M Message[K, N]] func(ctx context.Context, id N, resp M, stats QueryStats) error

type QueryStats struct {
	Start     time.Time // Start is the time the query began executing.
	End       time.Time // End is the time the query stopped executing.
	Requests  int       // Requests is a count of the number of requests made by the query.
	Success   int       // Success is a count of the number of nodes the query succesfully contacted.
	Failure   int       // Failure is a count of the number of nodes the query received an error response from.
	Exhausted bool      // Exhausted is true if the query ended after visiting every node it could.
}

// PublishStats holds the counts and timings of one publish operation. The Query counts cover
// the lookup that finds the nodes to store the record with, and are zero for a publish that runs
// no lookup. The Store counts cover the requests that carry the record.
type PublishStats struct {
	Start time.Time // Start is the time the publish began executing.
	End   time.Time // End is the time the publish stopped executing.

	QueryRequests int // QueryRequests is a count of the number of requests made by the lookup.
	QuerySuccess  int // QuerySuccess is a count of the number of nodes the lookup successfully contacted.
	QueryFailure  int // QueryFailure is a count of the number of nodes the lookup received an error response from.

	StoreRequests int // StoreRequests is a count of the number of nodes asked to store the record.
	StoreSuccess  int // StoreSuccess is a count of the number of nodes that stored the record.
	StoreFailure  int // StoreFailure is a count of the number of nodes that did not store the record.
}

var (
	// ErrSkipNode is used as a return value from a QueryFunc to indicate that the node is to be skipped.
	ErrSkipNode = errors.New("skip node")

	// ErrSkipRemaining is used as a return value a QueryFunc to indicate that all remaining nodes are to be skipped.
	ErrSkipRemaining = errors.New("skip remaining nodes")
)

// Message is the contract a protocol message must satisfy for the core to route a query
// with it.
type Message[K kad.Key[K], N kad.NodeID[K]] interface {
	// Target returns the key the message is directed at.
	Target() K

	// CloserNodes returns the nodes that the sender considers closest to the target.
	CloserNodes() []N
}

// NoMessage satisfies [Message] for queries that only ask for closer nodes and never send
// one.
type NoMessage[K kad.Key[K], N kad.NodeID[K]] struct{}

func (NoMessage[K, N]) Target() K {
	var zero K
	return zero
}

func (NoMessage[K, N]) CloserNodes() []N { return nil }

type Router[K kad.Key[K], N kad.NodeID[K], M Message[K, N]] interface {
	// SendMessage attempts to send a request to another node. This method blocks until a response is received or an error is encountered.
	SendMessage(ctx context.Context, to N, req M) (M, error)

	// GetClosestNodes attempts to send a request to another node asking it for nodes that it considers to be
	// closest to the target key.
	GetClosestNodes(ctx context.Context, to N, target K) ([]N, error)
}
