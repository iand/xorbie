package xorbie

import (
	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key/bitstr"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/query"
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

// PublishCommand is a type of [BehaviourEvent] that instructs a [PublishBehaviour] to perform an action.
type PublishCommand interface {
	BehaviourEvent
	publishCommand()
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

// EventStartFollowUpPublish starts a publish that finds the nodes closest to the target key
// before storing the record with any of them.
type EventStartFollowUpPublish[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID           coordt.QueryID
	Target            K
	Message           M
	KnownClosestNodes []N
	Notify            QueryMonitor[K, N, M, *EventPublishFinished[K, N]]
}

// EventStartStaticPublish starts a publish that stores the record with a fixed set of nodes.
type EventStartStaticPublish[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID
	Target  K
	Message M
	Nodes   []N
	Notify  QueryMonitor[K, N, M, *EventPublishFinished[K, N]]
}

// EventStartOptimisticPublish starts a publish that stores the record with nodes as the walk
// towards the target key finds them.
type EventStartOptimisticPublish[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID           coordt.QueryID
	Target            K
	Message           M
	KnownClosestNodes []N
	NetworkSize       int
	Notify            QueryMonitor[K, N, M, *EventPublishFinished[K, N]]
}

// EventStartRegionPublish instructs the publish behaviour to publish every provided key in a
// surveyed region.
type EventStartRegionPublish[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID coordt.QueryID // the id of the region publish, from which its per-key publish ids derive
	Prefix  bitstr.Key     // the prefix of the surveyed region
	Nodes   []N            // the nodes found inside the region
}

func (*EventStartFollowUpPublish[K, N, M]) behaviourEvent()   {}
func (*EventStartStaticPublish[K, N, M]) behaviourEvent()     {}
func (*EventStartOptimisticPublish[K, N, M]) behaviourEvent() {}
func (*EventStartRegionPublish[K, N]) behaviourEvent()        {}

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

// EventPublishFinished is emitted by the coordinator when a publishing
// a record to the network has finished, either through running to completion or
// by being canceled.
type EventPublishFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID
	Contacted []N
	Errors    map[string]struct {
		Node N
		Err  error
	}

	// QueryStats holds the stats of the lookup that found the nodes the record was stored
	// with, and is zero for a publish that ran no lookup.
	QueryStats query.QueryStats

	// Err records why the publish ended when it ended without being attempted, and is
	// nil otherwise. A publish that ran records per node outcomes in Errors instead.
	Err error
}

func (*EventPublishFinished[K, N]) behaviourEvent()     {}
func (*EventPublishFinished[K, N]) terminalQueryEvent() {}

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

// EventRegionSurveyed is emitted by the coordinator when a survey of a region has finished. It
// carries the region's prefix and the nodes found inside it.
type EventRegionSurveyed[K kad.Key[K], N kad.NodeID[K]] struct {
	Prefix bitstr.Key // the prefix of the region that was surveyed
	Nodes  []N        // the nodes found inside the region
}

func (*EventRegionSurveyed[K, N]) behaviourEvent()      {}
func (*EventRegionSurveyed[K, N]) routingNotification() {}

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
