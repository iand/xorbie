package coord

import (
	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/internal/coord/coordt"
	"github.com/iand/xorbie/internal/coord/query"
)

type BehaviourEvent interface {
	behaviourEvent()
}

// RoutingCommand is a type of [BehaviourEvent] that instructs a [RoutingBehaviour] to perform an action.
type RoutingCommand interface {
	BehaviourEvent
	routingCommand()
}

// NetworkCommand is a type of [BehaviourEvent] that instructs a [NetworkBehaviour] to perform an action.
type NetworkCommand interface {
	BehaviourEvent
	networkCommand()
}

// QueryCommand is a type of [BehaviourEvent] that instructs a [QueryBehaviour] to perform an action.
type QueryCommand interface {
	BehaviourEvent
	queryCommand()
}

// BrdcstCommand is a type of [BehaviourEvent] that instructs a [BrdcstBehaviour] to perform an action.
type BrdcstCommand interface {
	BehaviourEvent
	brdcstCommand()
}

type NodeHandlerRequest interface {
	BehaviourEvent
	nodeHandlerRequest()
}

type NodeHandlerResponse interface {
	BehaviourEvent
	nodeHandlerResponse()
}

type RoutingNotification interface {
	BehaviourEvent
	routingNotification()
}

// TerminalQueryEvent is a type of [BehaviourEvent] that indicates a query has completed.
type TerminalQueryEvent interface {
	BehaviourEvent
	terminalQueryEvent()
}

type EventStartBootstrap[K kad.Key[K], N kad.NodeID[K]] struct {
	// SeedNodes are the nodes the bootstrap should start from. When empty the nodes
	// configured as [RoutingConfig.BootstrapPeers] are used.
	SeedNodes []N
}

func (*EventStartBootstrap[K, N]) behaviourEvent() {}
func (*EventStartBootstrap[K, N]) routingCommand() {}

type EventOutboundGetCloserNodes[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	To      N
	Target  K
	Notify  Notify[BehaviourEvent]
}

func (*EventOutboundGetCloserNodes[K, N]) behaviourEvent()     {}
func (*EventOutboundGetCloserNodes[K, N]) nodeHandlerRequest() {}
func (*EventOutboundGetCloserNodes[K, N]) networkCommand()     {}

type EventOutboundSendMessage[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID
	To      N
	Message M
	Notify  Notify[BehaviourEvent]
}

func (*EventOutboundSendMessage[K, N, M]) behaviourEvent()     {}
func (*EventOutboundSendMessage[K, N, M]) nodeHandlerRequest() {}
func (*EventOutboundSendMessage[K, N, M]) networkCommand()     {}

type EventStartMessageQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID           coordt.QueryID
	Target            K
	Message           M
	KnownClosestNodes []N
	Notify            QueryMonitor[K, N, M, *EventQueryFinished[K, N]]
	NumResults        int // the minimum number of nodes to successfully contact before considering iteration complete
}

func (*EventStartMessageQuery[K, N, M]) behaviourEvent() {}
func (*EventStartMessageQuery[K, N, M]) queryCommand()   {}

type EventStartFindCloserQuery[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID           coordt.QueryID
	Target            K
	KnownClosestNodes []N
	Notify            QueryMonitor[K, N, M, *EventQueryFinished[K, N]]
	NumResults        int // the minimum number of nodes to successfully contact before considering iteration complete
}

func (*EventStartFindCloserQuery[K, N, M]) behaviourEvent() {}
func (*EventStartFindCloserQuery[K, N, M]) queryCommand()   {}

type EventStopQuery struct {
	QueryID coordt.QueryID
}

func (*EventStopQuery) behaviourEvent() {}
func (*EventStopQuery) queryCommand()   {}

// EventAddNode notifies the routing behaviour of a potential new node.
type EventAddNode[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N
}

func (*EventAddNode[K, N]) behaviourEvent() {}
func (*EventAddNode[K, N]) routingCommand() {}

// EventGetCloserNodesSuccess notifies a behaviour that a GetCloserNodes request, initiated by an
// [EventOutboundGetCloserNodes] event has produced a successful response.
type EventGetCloserNodesSuccess[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID     coordt.QueryID
	To          N // To is the node that the GetCloserNodes request was sent to.
	Target      K
	CloserNodes []N
}

func (*EventGetCloserNodesSuccess[K, N]) behaviourEvent()      {}
func (*EventGetCloserNodesSuccess[K, N]) nodeHandlerResponse() {}

// EventGetCloserNodesFailure notifies a behaviour that a GetCloserNodes request, initiated by an
// [EventOutboundGetCloserNodes] event has failed to produce a valid response.
type EventGetCloserNodesFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID
	To      N // To is the node that the GetCloserNodes request was sent to.
	Target  K
	Err     error
}

func (*EventGetCloserNodesFailure[K, N]) behaviourEvent()      {}
func (*EventGetCloserNodesFailure[K, N]) nodeHandlerResponse() {}

// EventSendMessageSuccess notifies a behaviour that a SendMessage request, initiated by an
// [EventOutboundSendMessage] event has produced a successful response.
type EventSendMessageSuccess[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID     coordt.QueryID
	Request     M
	To          N // To is the node that the SendMessage request was sent to.
	Response    M
	CloserNodes []N
}

func (*EventSendMessageSuccess[K, N, M]) behaviourEvent()      {}
func (*EventSendMessageSuccess[K, N, M]) nodeHandlerResponse() {}

// EventSendMessageFailure notifies a behaviour that a SendMessage request, initiated by an
// [EventOutboundSendMessage] event has failed to produce a valid response.
type EventSendMessageFailure[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID
	Request M
	To      N // To is the node that the SendMessage request was sent to.
	Target  K
	Err     error
}

func (*EventSendMessageFailure[K, N, M]) behaviourEvent()      {}
func (*EventSendMessageFailure[K, N, M]) nodeHandlerResponse() {}

// EventQueryProgressed is emitted by the coordinator when a query has received a
// response from a node.
type EventQueryProgressed[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID  coordt.QueryID
	NodeID   N
	Response M
	Stats    query.QueryStats
}

func (*EventQueryProgressed[K, N, M]) behaviourEvent() {}

// EventQueryFinished is emitted by the coordinator when a query has finished, either through
// running to completion or by being canceled.
type EventQueryFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID      coordt.QueryID
	Stats        query.QueryStats
	ClosestNodes []N

	// Err records why the query ended when it ended for a reason other than visiting
	// every node it could, and is nil otherwise. ClosestNodes is not populated when
	// Err is set.
	Err error
}

func (*EventQueryFinished[K, N]) behaviourEvent()     {}
func (*EventQueryFinished[K, N]) terminalQueryEvent() {}

// EventRoutingUpdated is emitted by the coordinator when a new node has been verified and added to the routing table.
type EventRoutingUpdated[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N
}

func (*EventRoutingUpdated[K, N]) behaviourEvent()      {}
func (*EventRoutingUpdated[K, N]) routingNotification() {}

// EventRoutingRemoved is emitted by the coordinator when new node has been removed from the routing table.
type EventRoutingRemoved[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N
}

func (*EventRoutingRemoved[K, N]) behaviourEvent()      {}
func (*EventRoutingRemoved[K, N]) routingNotification() {}

// EventBootstrapFinished is emitted by the coordinator when a bootstrap has finished, either through
// running to completion or by being canceled.
type EventBootstrapFinished struct {
	Stats query.QueryStats

	// Err records why the bootstrap ended when it ended for a reason other than visiting
	// every node it could, and is nil otherwise.
	Err error
}

func (*EventBootstrapFinished) behaviourEvent()      {}
func (*EventBootstrapFinished) routingNotification() {}

// EventNotifyConnectivity notifies a behaviour that a node's connectivity and support for finding closer nodes
// has been confirmed such as from a successful query response or an inbound query. This should not be used for
// general connections to the host but only when it is confirmed that the node responds to requests for closer
// nodes.
type EventNotifyConnectivity[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N
}

func (*EventNotifyConnectivity[K, N]) behaviourEvent() {}
func (*EventNotifyConnectivity[K, N]) routingCommand() {}

// EventNotifyNonConnectivity notifies a behaviour that a node does not have connectivity and/or does not support
// finding closer nodes is known.
type EventNotifyNonConnectivity[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N
}

func (*EventNotifyNonConnectivity[K, N]) behaviourEvent() {}
func (*EventNotifyNonConnectivity[K, N]) routingCommand() {}

// EventRoutingPoll notifies a routing behaviour that it may proceed with any pending work.
type EventRoutingPoll struct{}

func (*EventRoutingPoll) behaviourEvent() {}
func (*EventRoutingPoll) routingCommand() {}
