package brdcst

import (
	"time"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/coordt"
)

// BroadcastState must be implemented by all states that a [Broadcast] state machine can
// reach. A state is the output of advancing a machine, and carries an instruction that the
// caller is expected to act on.
type BroadcastState interface {
	broadcastState()
}

// StateBroadcastFindCloser indicates to the broadcast [Pool] or any other upper
// layer that a [Broadcast] state machine wants to query the given node (NodeID)
// for closer nodes to the target key (Target).
type StateBroadcastFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	NodeID  N              // the node to send the message to
	Target  K              // the key that the query wants to find closer nodes for
}

// StateBroadcastStoreRecord indicates to the broadcast [Pool] or any other
// upper layer that a [Broadcast] state machine wants to store a record using
// the given Message with the given NodeID.
type StateBroadcastStoreRecord[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID // the id of the broadcast operation that wants to send the message
	NodeID  N              // the node to send the message to
	Message M              // the message the broadcast behaviour wants to send
}

// StateBroadcastWaiting indicates that a [Broadcast] state machine is waiting
// for network I/O to finish. It means the state machine isn't idle, but that
// there are operations in-flight that it is waiting on to finish.
type StateBroadcastWaiting struct {
	NextDue time.Time      // the earliest time advancing the broadcast could make progress, zero if there is none
	QueryID coordt.QueryID // the id of the broadcast operation that is waiting
}

// StateBroadcastFinished indicates that a [Broadcast] state machine has finished its
// operation. Contacted holds every node asked to store the record, and excludes the nodes
// queried to find the closest ones. Errors is keyed by the string representation of a node
// in Contacted and holds the error that node returned, so an operation in which every store
// succeeded carries an empty map.
type StateBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID      // the id of the broadcast operation that has finished
	Contacted []N                 // all nodes asked to store the record, successful or not
	Errors    map[string]struct { // the error returned by any contacted node that failed
		Node N     // a node from the Contacted slice
		Err  error // the error that happened when contacting that Node
	}
}

// StateBroadcastIdle indicates that a [Broadcast] state machine has nothing left to do. It
// is emitted when a machine is polled after its operation has already finished. A [Pool]
// never observes it, because a machine that reports [StateBroadcastFinished] is dropped from
// the pool in the same advance.
type StateBroadcastIdle struct{}

func (*StateBroadcastFindCloser[K, N]) broadcastState()     {}
func (*StateBroadcastStoreRecord[K, N, M]) broadcastState() {}
func (*StateBroadcastWaiting) broadcastState()              {}
func (*StateBroadcastFinished[K, N]) broadcastState()       {}
func (*StateBroadcastIdle) broadcastState()                 {}

// BroadcastEvent is an event intended to advance the state of a [Broadcast] state machine.
// An event flows into a machine and a state flows out of it.
type BroadcastEvent interface {
	broadcastEvent()
}

// EventBroadcastPoll is an event that signals a [Broadcast] state machine that
// it can perform housekeeping work such as time out queries.
type EventBroadcastPoll struct{}

// EventBroadcastStart is an event that instructs a broadcast state machine to
// start the operation.
type EventBroadcastStart[K kad.Key[K], N kad.NodeID[K]] struct {
	Target K   // the key the record is stored under
	Seed   []N // the closest nodes known so far, from where the operation starts
}

// EventBroadcastStop notifies a [Broadcast] state machine to stop the
// operation. This comprises all in-flight queries.
type EventBroadcastStop struct{}

// EventBroadcastNodeResponse notifies a [Broadcast] state machine that a remote
// node (NodeID) has successfully responded with closer nodes (CloserNodes) to
// the Target key that's stored on the [Broadcast] state machine
type EventBroadcastNodeResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to and that replied
	CloserNodes []N // the closer nodes sent by the node
}

// EventBroadcastNodeFailure notifies a [Broadcast] state machine that a remote
// node (NodeID) has failed responding with closer nodes to the target key.
type EventBroadcastNodeFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to and that has replied
	Error  error // the error that caused the failure, if any
}

// EventBroadcastStoreRecordSuccess notifies a [Broadcast] state machine that storing a
// record with a remote node succeeded. Request holds the message that was sent. A response
// is optional, since a protocol need not confirm a store, so Response may be nil.
type EventBroadcastStoreRecordSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	NodeID   N // the node the message was sent to
	Request  M // the message that was sent to the remote node
	Response M // the reply from the remote node, nil if the protocol sends none
}

// EventBroadcastStoreRecordFailure notifies a broadcast [Broadcast] state
// machine that storing a record with a remote node (NodeID) has failed. The
// message that was sent is held in Request, and the error will be in Error.
type EventBroadcastStoreRecordFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	NodeID  N     // the node the message was sent to
	Request M     // the message that was sent to the remote node
	Error   error // the error that caused the failure, if any
}

// broadcastEvent() ensures that only events accepted by a [Broadcast] state
// machine can be assigned to the [BroadcastEvent] interface.
func (*EventBroadcastStop) broadcastEvent()                        {}
func (*EventBroadcastPoll) broadcastEvent()                        {}
func (*EventBroadcastStart[K, N]) broadcastEvent()                 {}
func (*EventBroadcastNodeResponse[K, N]) broadcastEvent()          {}
func (*EventBroadcastNodeFailure[K, N]) broadcastEvent()           {}
func (*EventBroadcastStoreRecordSuccess[K, N, M]) broadcastEvent() {}
func (*EventBroadcastStoreRecordFailure[K, N, M]) broadcastEvent() {}
