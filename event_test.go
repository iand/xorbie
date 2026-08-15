package xorbie

import "github.com/iand/xorbie/internal/tiny"

var _ NetworkCommand = (*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])(nil)

var (
	_ RoutingCommand = (*EventAddNode[tiny.Key, tiny.Node])(nil)
	_ RoutingCommand = (*EventStartBootstrap[tiny.Key, tiny.Node])(nil)
)

var (
	_ QueryCommand = (*EventStartMessageQuery[tiny.Key, tiny.Node, tiny.Message])(nil)
	_ QueryCommand = (*EventStartFindCloserQuery[tiny.Key, tiny.Node, tiny.Message])(nil)
	_ QueryCommand = (*EventStopQuery)(nil)
)

var (
	_ RoutingNotification = (*EventRoutingUpdated[tiny.Key, tiny.Node])(nil)
	_ RoutingNotification = (*EventBootstrapFinished)(nil)
)

var _ NodeHandlerRequest = (*EventOutboundGetCloserNodes[tiny.Key, tiny.Node])(nil)

var (
	_ NodeHandlerResponse = (*EventGetCloserNodesSuccess[tiny.Key, tiny.Node])(nil)
	_ NodeHandlerResponse = (*EventGetCloserNodesFailure[tiny.Key, tiny.Node])(nil)
)
