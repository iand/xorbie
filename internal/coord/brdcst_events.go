package coord

import (
	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/internal/coord/brdcst"
	"github.com/iand/xorbie/internal/coord/coordt"
)

// EventStartBroadcast starts a new
type EventStartBroadcast[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	QueryID coordt.QueryID
	Target  K
	Message M
	Seed    []N
	Config  brdcst.Config
	Notify  QueryMonitor[K, N, M, *EventBroadcastFinished[K, N]]
}

func (*EventStartBroadcast[K, N, M]) behaviourEvent() {}

// EventBroadcastFinished is emitted by the coordinator when a broadcasting
// a record to the network has finished, either through running to completion or
// by being canceled.
type EventBroadcastFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	QueryID   coordt.QueryID
	Contacted []N
	Errors    map[string]struct {
		Node N
		Err  error
	}

	// Err records why the broadcast ended when it ended without being attempted, and is
	// nil otherwise. A broadcast that ran records per node outcomes in Errors instead.
	Err error
}

func (*EventBroadcastFinished[K, N]) behaviourEvent()     {}
func (*EventBroadcastFinished[K, N]) terminalQueryEvent() {}
