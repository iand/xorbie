package nettest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key"

	"github.com/iand/xorbie/internal/coord/coordt"
)

// Link represents the route between two nodes. It allows latency and transport failures to be simulated.
type Link interface {
	ConnLatency() time.Duration // the simulated time taken to return an error or successful outcome
	DialLatency() time.Duration // the simulated time taken to connect to a node
	DialErr() error             // an error that should be returned on dial, nil if the dial is successful
}

// DefaultLink is the default link used if none is specified.
// It has zero latency and always succeeds.
type DefaultLink struct{}

func (l *DefaultLink) DialErr() error             { return nil }
func (l *DefaultLink) ConnLatency() time.Duration { return 0 }
func (l *DefaultLink) DialLatency() time.Duration { return 0 }

type Router[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]] struct {
	self  N
	top   *Topology[K, N, M]
	mu    sync.Mutex // guards nodes
	nodes map[string]*nodeStatus[N]
}

type nodeStatus[N any] struct {
	NodeID    N
	Connected bool
}

func NewRouter[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N]](self N, top *Topology[K, N, M]) *Router[K, N, M] {
	return &Router[K, N, M]{
		self:  self,
		top:   top,
		nodes: make(map[string]*nodeStatus[N]),
	}
}

func (r *Router[K, N, M]) NodeID() kad.NodeID[K] {
	return r.self
}

func (r *Router[K, N, M]) handleMessage(ctx context.Context, from N, req M) (M, error) {
	closer := make([]N, 0)

	r.mu.Lock()
	for _, n := range r.nodes {
		// only include self if it was the target of the request
		if n.NodeID.String() == r.self.String() && !key.Equal(n.NodeID.Key(), req.Target()) {
			continue
		}
		closer = append(closer, n.NodeID)
	}
	r.mu.Unlock()

	return r.top.proto.Reply(req, closer), nil
}

func (r *Router[K, N, M]) dial(ctx context.Context, to N) error {
	r.mu.Lock()
	status, ok := r.nodes[to.String()]
	r.mu.Unlock()

	if !ok {
		status = &nodeStatus[N]{
			NodeID:    to,
			Connected: false,
		}
	}

	if status.Connected {
		return nil
	}
	if err := r.top.Dial(ctx, r.self, to); err != nil {
		return err
	}

	status.Connected = true
	r.mu.Lock()
	r.nodes[to.String()] = status
	r.mu.Unlock()
	return nil
}

// AddToPeerStore records a node as one the router knows how to reach.
func (r *Router[K, N, M]) AddToPeerStore(ctx context.Context, id N) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.nodes[id.String()]; !ok {
		r.nodes[id.String()] = &nodeStatus[N]{
			NodeID:    id,
			Connected: false,
		}
	}
	return nil
}

func (r *Router[K, N, M]) SendMessage(ctx context.Context, to N, req M) (M, error) {
	if err := r.dial(ctx, to); err != nil {
		var zero M
		return zero, fmt.Errorf("dial: %w", err)
	}

	return r.top.RouteMessage(ctx, r.self, to, req)
}

func (r *Router[K, N, M]) GetClosestNodes(ctx context.Context, to N, target K) ([]N, error) {
	resp, err := r.SendMessage(ctx, to, r.top.proto.FindRequest(target))
	if err != nil {
		return nil, err
	}

	// possibly learned about some new nodes
	for _, id := range resp.CloserNodes() {
		r.AddToPeerStore(ctx, id)
	}

	return resp.CloserNodes(), nil
}
