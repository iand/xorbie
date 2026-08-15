package nettest

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/internal/coord/coordt"
	"github.com/iand/xorbie/internal/coord/routing"
)

// A Peer is a participant in a [Topology], holding everything a coordinator needs to run on
// top of the simulated network.
type Peer[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	NodeID       N
	Router       *Router[K, N, M]
	RoutingTable routing.RoutingTableCpl[K, N]
}

type Topology[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	proto     Protocol[K, N, M]
	links     map[string]Link
	nodes     []*Peer[K, N, M]
	nodeIndex map[string]*Peer[K, N, M]
	routers   map[string]*Router[K, N, M]
}

func NewTopology[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](proto Protocol[K, N, M]) *Topology[K, N, M] {
	return &Topology[K, N, M]{
		proto:     proto,
		links:     make(map[string]Link),
		nodeIndex: make(map[string]*Peer[K, N, M]),
		routers:   make(map[string]*Router[K, N, M]),
	}
}

func (t *Topology[K, N, M]) Peers() []*Peer[K, N, M] {
	return t.nodes
}

func (t *Topology[K, N, M]) ConnectPeers(a *Peer[K, N, M], b *Peer[K, N, M]) {
	t.ConnectPeersWithRoute(a, b, &DefaultLink{})
}

func (t *Topology[K, N, M]) ConnectPeersWithRoute(a *Peer[K, N, M], b *Peer[K, N, M], l Link) {
	akey := a.NodeID.String()
	if _, exists := t.nodeIndex[akey]; !exists {
		t.nodeIndex[akey] = a
		t.nodes = append(t.nodes, a)
		t.routers[akey] = a.Router
	}

	bkey := b.NodeID.String()
	if _, exists := t.nodeIndex[bkey]; !exists {
		t.nodeIndex[bkey] = b
		t.nodes = append(t.nodes, b)
		t.routers[bkey] = b.Router
	}

	atob := fmt.Sprintf("%s->%s", akey, bkey)
	t.links[atob] = l

	// symmetrical routing assumed
	btoa := fmt.Sprintf("%s->%s", bkey, akey)
	t.links[btoa] = l
}

func (t *Topology[K, N, M]) findRoute(from N, to N) (Link, error) {
	key := fmt.Sprintf("%s->%s", from.String(), to.String())

	route, ok := t.links[key]
	if !ok {
		return nil, fmt.Errorf("no route to node")
	}

	return route, nil
}

func (t *Topology[K, N, M]) Dial(ctx context.Context, from N, to N) error {
	if from.String() == to.String() {
		_, ok := t.nodeIndex[to.String()]
		if !ok {
			return fmt.Errorf("unknown node")
		}

		return nil
	}

	route, err := t.findRoute(from, to)
	if err != nil {
		return fmt.Errorf("find route: %w", err)
	}

	latency := route.DialLatency()
	if latency > 0 {
		time.Sleep(latency)
	}

	if err := route.DialErr(); err != nil {
		return err
	}

	_, ok := t.nodeIndex[to.String()]
	if !ok {
		return fmt.Errorf("unknown node")
	}

	return nil
}

func (t *Topology[K, N, M]) RouteMessage(ctx context.Context, from N, to N, req M) (M, error) {
	var zero M

	if from.String() == to.String() {
		node, ok := t.nodeIndex[to.String()]
		if !ok {
			return zero, fmt.Errorf("unknown node")
		}

		return node.Router.handleMessage(ctx, from, req)
	}

	route, err := t.findRoute(from, to)
	if err != nil {
		return zero, fmt.Errorf("find route: %w", err)
	}

	latency := route.ConnLatency()
	if latency > 0 {
		time.Sleep(latency)
	}

	node, ok := t.nodeIndex[to.String()]
	if !ok {
		return zero, fmt.Errorf("no route to node")
	}

	return node.Router.handleMessage(ctx, from, req)
}
