package nettest

import (
	"context"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/triert"

	"github.com/iand/xorbie/coordt"
)

// LinearTopology creates a network topology consisting of n nodes peered in a linear chain.
// The nodes are configured with routing tables that contain immediate neighbours.
// It returns the topology and the nodes ordered such that nodes[x] has nodes[x-1] and nodes[x+1] in its routing table
// The topology is not a ring: nodes[0] only has nodes[1] in its table and nodes[n-1] only has nodes[n-2] in its table.
// nodes[1] has nodes[0] and nodes[2] in its routing table.
// If n > 2 then the first and last nodes will not have one another in their routing tables.
func LinearTopology[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](n int, proto Protocol[K, N, M]) (*Topology[K, N, M], []*Peer[K, N, M], error) {
	nodes := make([]*Peer[K, N, M], n)

	top := NewTopology(proto)
	for i := range nodes {

		id, err := proto.NewNodeID()
		if err != nil {
			return nil, nil, err
		}

		rt, err := triert.New(id, nil)
		if err != nil {
			return nil, nil, err
		}

		nodes[i] = &Peer[K, N, M]{
			NodeID:       id,
			Router:       NewRouter(id, top),
			RoutingTable: rt,
		}
	}

	// Define the network topology, with default network links between every node
	for i := range nodes {
		for j := i + 1; j < len(nodes); j++ {
			top.ConnectPeers(nodes[i], nodes[j])
		}
	}

	// Connect nodes in a chain
	for i := range nodes {
		if i > 0 {
			nodes[i].Router.AddToPeerStore(context.Background(), nodes[i-1].NodeID)
			nodes[i].RoutingTable.AddNode(nodes[i-1].NodeID)
		}
		if i < len(nodes)-1 {
			nodes[i].Router.AddToPeerStore(context.Background(), nodes[i+1].NodeID)
			nodes[i].RoutingTable.AddNode(nodes[i+1].NodeID)
		}
	}

	return top, nodes, nil
}
