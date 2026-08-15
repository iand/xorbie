package nettest

import (
	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/internal/coord/coordt"
)

// A Protocol supplies the node identities and the messages a [Topology] needs. It is all the
// harness knows about the protocol under test.
type Protocol[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] interface {
	// NewNodeID returns a node id that differs from every id it has already returned.
	NewNodeID() (N, error)

	// FindRequest returns a message asking for the nodes closest to target.
	FindRequest(target K) M

	// Reply generates a response to req. The response reports nodes as the closest the
	// sender knows to the target of req.
	Reply(req M, nodes []N) M
}
