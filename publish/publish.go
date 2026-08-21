package publish

import (
	"time"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/query"
)

// PublishState must be implemented by all states that a [Publish] state machine can
// reach. A state is the output of advancing a machine, and carries an instruction that the
// caller is expected to act on.
type PublishState interface {
	publishState()
}

// StatePublishFindCloser indicates to the publish [Pool] or any other upper
// layer that a [Publish] state machine wants to query the given node (NodeID)
// for closer nodes to the target key (Target).
type StatePublishFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	ActivityID coordt.ActivityID // the id of the publish operation that wants to send the message
	NodeID     N                 // the node to send the message to
	Target     K                 // the key that the query wants to find closer nodes for
}

// StatePublishStoreRecord indicates to the publish [Pool] or any other
// upper layer that a [Publish] state machine wants to store a record using
// the given Message with the given NodeID.
type StatePublishStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	ActivityID coordt.ActivityID // the id of the publish operation that wants to send the message
	NodeID     N                 // the node to send the message to
	Message    M                 // the message the publish behaviour wants to send
}

// StatePublishWaiting indicates that a [Publish] state machine is waiting
// for network I/O to finish. It means the state machine isn't idle, but that
// there are operations in-flight that it is waiting on to finish.
type StatePublishWaiting struct {
	NextDue    time.Time         // the earliest time advancing the publish could make progress, zero if there is none
	ActivityID coordt.ActivityID // the id of the publish operation that is waiting
}

// StatePublishFinished indicates that a [Publish] state machine has finished its
// operation. Contacted holds every node asked to store the record, and excludes the nodes
// queried to find the closest ones. Errors is keyed by the string representation of a node
// in Contacted and holds the error that node returned, so an operation in which every store
// succeeded carries an empty map.
type StatePublishFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	ActivityID coordt.ActivityID   // the id of the publish operation that has finished
	Contacted  []N                 // all nodes asked to store the record, successful or not
	Errors     map[string]struct { // the error returned by any contacted node that failed
		Node N     // a node from the Contacted slice
		Err  error // the error that happened when contacting that Node
	}
	QueryStats query.QueryStats // the stats of the lookup that found the nodes, zero if none was run
}

// StatePublishIdle indicates that a [Publish] state machine has nothing to do. It is
// emitted by a machine polled before it has been started. A machine that has finished does
// not become idle: it reports [StatePublishFinished] on every later advance. A [Pool] never
// observes this state, since it starts a publish as soon as it creates one.
type StatePublishIdle struct{}

func (*StatePublishFindCloser[K, N]) publishState()     {}
func (*StatePublishStoreRecord[K, N, M]) publishState() {}
func (*StatePublishWaiting) publishState()              {}
func (*StatePublishFinished[K, N]) publishState()       {}
func (*StatePublishIdle) publishState()                 {}

// PublishEvent is an event intended to advance the state of a [Publish] state machine.
// An event flows into a machine and a state flows out of it.
type PublishEvent interface {
	publishEvent()
}

// EventPublishPoll is an event that signals a [Publish] state machine that
// it can perform housekeeping work such as time out queries.
type EventPublishPoll struct{}

// EventPublishStart is an event that instructs a publish state machine to
// start the operation.
type EventPublishStart[K kad.Key[K], N kad.NodeID[K]] struct {
	Target K // the key the record is stored under
}

// EventPublishStop notifies a [Publish] state machine to stop the
// operation. This comprises all in-flight queries.
type EventPublishStop struct{}

// EventPublishNodeResponse notifies a [Publish] state machine that a remote
// node (NodeID) has successfully responded with closer nodes (CloserNodes) to
// the Target key that's stored on the [Publish] state machine
type EventPublishNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to and that replied
	CloserNodes []N // the closer nodes sent by the node
}

// EventPublishNodeFailure notifies a [Publish] state machine that a remote
// node (NodeID) has failed responding with closer nodes to the target key.
type EventPublishNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to and that has replied
	Error  error // the error that caused the failure, if any
}

// EventPublishStoreRecordSuccess notifies a [Publish] state machine that storing a
// record with a remote node succeeded. Request holds the message that was sent. A response
// is optional, since a protocol need not confirm a store, so Response may be nil.
type EventPublishStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	NodeID   N // the node the message was sent to
	Request  M // the message that was sent to the remote node
	Response M // the reply from the remote node, nil if the protocol sends none
}

// EventPublishStoreRecordFailure notifies a publish [Publish] state
// machine that storing a record with a remote node (NodeID) has failed. The
// message that was sent is held in Request, and the error will be in Error.
type EventPublishStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	NodeID  N     // the node the message was sent to
	Request M     // the message that was sent to the remote node
	Error   error // the error that caused the failure, if any
}

// publishEvent() ensures that only events accepted by a [Publish] state
// machine can be assigned to the [PublishEvent] interface.
func (*EventPublishStop) publishEvent()                        {}
func (*EventPublishPoll) publishEvent()                        {}
func (*EventPublishStart[K, N]) publishEvent()                 {}
func (*EventPublishNodeResponse[K, N]) publishEvent()          {}
func (*EventPublishNodeFailure[K, N]) publishEvent()           {}
func (*EventPublishStoreRecordSuccess[K, N, M]) publishEvent() {}
func (*EventPublishStoreRecordFailure[K, N, M]) publishEvent() {}
