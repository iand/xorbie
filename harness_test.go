package xorbie

import (
	"github.com/iand/xorbie/internal/nettest"
	"github.com/iand/xorbie/internal/tiny"
)

// The coordinator tests run on the tiny key, node and message types.
type (
	testTopology = nettest.Topology[tiny.Key, tiny.Node, tiny.Message]
	testPeer     = nettest.Peer[tiny.Key, tiny.Node, tiny.Message]
)

var _ nettest.Protocol[tiny.Key, tiny.Node, tiny.Message] = (*tiny.Protocol)(nil)

// linearTopology returns n nodes wired into a linear chain, along with the topology holding
// them. See [nettest.LinearTopology] for the routing tables each node is given.
func linearTopology(n int) (*testTopology, []*testPeer, error) {
	return nettest.LinearTopology(n, tiny.NewProtocol())
}
