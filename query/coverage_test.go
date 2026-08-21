package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/kadtest"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

// runCoverage drives a coverage query to completion. Whenever the query asks to
// contact a node it answers from graph, or fails the node if it is in failing,
// so a test states the network as a map from a node to the closer nodes it
// returns. It returns the finished state.
func runCoverage[K kad.Key[K], N kad.NodeID[K]](t *testing.T, qry *Query[K, N, coordt.NoMessage[K, N]], graph map[string][]N, failing map[string]bool) *StateQueryFinished[K, N] {
	t.Helper()
	ctx := context.Background()
	var ev QueryEvent = &EventQueryPoll{}
	for range 1000 {
		st := qry.Advance(ctx, epoch, ev)
		switch s := st.(type) {
		case *StateQueryFindCloser[K, N]:
			id := s.NodeID.String()
			if failing[id] {
				ev = &EventQueryNodeFailure[K, N]{NodeID: s.NodeID, Error: fmt.Errorf("boom")}
			} else {
				ev = &EventQueryNodeResponse[K, N]{NodeID: s.NodeID, CloserNodes: graph[id]}
			}
		case *StateQueryFinished[K, N]:
			return s
		default:
			ev = &EventQueryPoll{}
		}
	}
	t.Fatal("coverage query did not finish")
	return nil
}

// nodeKeys returns the string ids of the nodes, for order-independent comparison.
func nodeKeys[K kad.Key[K], N kad.NodeID[K]](nodes []N) map[string]bool {
	m := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		m[n.String()] = true
	}
	return m
}

// tinyCoverageConfig returns a query config for the tiny-key coverage tests. numResults
// is the walk-in bound for coverage rather than a result cap.
func tinyCoverageConfig(numResults int) *QueryConfig {
	cfg := DefaultQueryConfig()
	cfg.NumResults = numResults
	return cfg
}

// When using tiny keys (8-bits) a region prefix of length 4 with target 0 holds the
// keys 0..15: a key is in the region when it shares at least 4 leading bits with the
// zero target, which for the zero target means at least 4 leading zeros.
const tinyCoverageRegionLen = 4

func tinyCoverageQuery(t *testing.T, target tiny.Key, prefixLen int, seeds []tiny.Node, cfg *QueryConfig) *Query[tiny.Key, tiny.Node, coordt.NoMessage[tiny.Key, tiny.Node]] {
	t.Helper()
	iter := NewClosestNodesIter[tiny.Key, tiny.Node](target)
	self := tiny.NewNode(0)
	qry, err := NewCoverageQuery[tiny.Key, tiny.Node, coordt.NoMessage[tiny.Key, tiny.Node]](self, coordt.ActivityID("cover"), target, prefixLen, iter, seeds, cfg)
	require.NoError(t, err)
	return qry
}

// TestCoverageEnumeratesRegion checks that a coverage query returns every node
// inside the region and stops at the region boundary rather than walking on.
func TestCoverageEnumeratesRegion(t *testing.T) {
	n1 := tiny.NewNode(0b00000001)
	n2 := tiny.NewNode(0b00000010)
	n3 := tiny.NewNode(0b00000011)
	out := tiny.NewNode(0b00010000) // 16, outside the length-4 region

	// The in-region nodes reveal one another, as mutually close nodes do.
	graph := map[string][]tiny.Node{
		n1.String():  {n2, n3},
		n2.String():  {n1, n3},
		n3.String():  {n1, n2},
		out.String(): {},
	}

	qry := tinyCoverageQuery(t, tiny.Key(0), tinyCoverageRegionLen, []tiny.Node{n1, n2, n3, out}, tinyCoverageConfig(20))
	st := runCoverage(t, qry, graph, nil)

	got := nodeKeys(st.ClosestNodes)
	require.Equal(t, map[string]bool{n1.String(): true, n2.String(): true, n3.String(): true}, got)
}

// TestCoverageDoesNotCapAtNumResults checks that a region holding more nodes than
// NumResults is fully enumerated: coverage does not truncate its result the way a
// closest-k lookup does, which is what lets a dense region be detected.
func TestCoverageDoesNotCapAtNumResults(t *testing.T) {
	// Six in-region nodes, NumResults of 4: a closest-k lookup would return 4.
	in := []tiny.Node{
		tiny.NewNode(0b00000001),
		tiny.NewNode(0b00000010),
		tiny.NewNode(0b00000011),
		tiny.NewNode(0b00000100),
		tiny.NewNode(0b00000101),
		tiny.NewNode(0b00000110),
	}
	out := tiny.NewNode(0b00100000) // 32, outside the region

	graph := map[string][]tiny.Node{out.String(): {}}
	for _, n := range in {
		graph[n.String()] = in // every in-region node knows the whole region
	}

	seeds := append(append([]tiny.Node{}, in...), out)
	qry := tinyCoverageQuery(t, tiny.Key(0), tinyCoverageRegionLen, seeds, tinyCoverageConfig(4))
	st := runCoverage(t, qry, graph, nil)

	require.Len(t, st.ClosestNodes, len(in))
	require.Equal(t, nodeKeys(in), nodeKeys(st.ClosestNodes))
}

// TestCoverageSparseRegion checks that a region holding fewer than NumResults
// nodes returns just those nodes.
func TestCoverageSparseRegion(t *testing.T) {
	n5 := tiny.NewNode(0b00000101)
	out1 := tiny.NewNode(0b00010000) // 16
	out2 := tiny.NewNode(0b10000000) // 128

	graph := map[string][]tiny.Node{
		n5.String():   {},
		out1.String(): {},
		out2.String(): {},
	}

	qry := tinyCoverageQuery(t, tiny.Key(0), tinyCoverageRegionLen, []tiny.Node{n5, out1, out2}, tinyCoverageConfig(20))
	st := runCoverage(t, qry, graph, nil)

	require.Equal(t, map[string]bool{n5.String(): true}, nodeKeys(st.ClosestNodes))
}

// TestCoverageEmptyRegionStopsAtWalkInBound checks that a region with no members
// finishes with an empty result and stops after the walk-in bound rather than
// contacting the whole reachable network.
func TestCoverageEmptyRegionStopsAtWalkInBound(t *testing.T) {
	// A chain of out-of-region nodes, each revealing the next, none in-region.
	c16 := tiny.NewNode(0b00010000) // 16
	c32 := tiny.NewNode(0b00100000) // 32
	c64 := tiny.NewNode(0b01000000) // 64
	c96 := tiny.NewNode(0b01100000) // 96

	graph := map[string][]tiny.Node{
		c16.String(): {c32},
		c32.String(): {c64},
		c64.String(): {c96},
		c96.String(): {},
	}

	qry := tinyCoverageQuery(t, tiny.Key(0), tinyCoverageRegionLen, []tiny.Node{c16}, tinyCoverageConfig(2))
	st := runCoverage(t, qry, graph, nil)

	require.Empty(t, st.ClosestNodes)
	// The walk-in bound is NumResults, so only the two nearest are contacted before
	// coverage concludes the region is empty.
	require.Equal(t, 2, st.Stats.Requests)
}

// TestCoverageWalksInFromDistantSeeds checks that a coverage query seeded only
// with out-of-region nodes walks in to discover the region, and does not finish
// prematurely before it has entered the region.
func TestCoverageWalksInFromDistantSeeds(t *testing.T) {
	n1 := tiny.NewNode(0b00000001)
	n2 := tiny.NewNode(0b00000010)
	n3 := tiny.NewNode(0b00000011)
	mid := tiny.NewNode(0b01000000) // 64, out-of-region stepping stone
	far := tiny.NewNode(0b10000000) // 128, the only seed

	graph := map[string][]tiny.Node{
		far.String(): {mid, n1},
		n1.String():  {n2, n3},
		n2.String():  {n1, n3},
		n3.String():  {n1, n2},
		mid.String(): {},
	}

	qry := tinyCoverageQuery(t, tiny.Key(0), tinyCoverageRegionLen, []tiny.Node{far}, tinyCoverageConfig(20))
	st := runCoverage(t, qry, graph, nil)

	require.Equal(t, map[string]bool{n1.String(): true, n2.String(): true, n3.String(): true}, nodeKeys(st.ClosestNodes))
}

// TestCoverageMemberFailure checks that coverage completes when a member fails to
// answer, excluding the failed node from the result.
func TestCoverageMemberFailure(t *testing.T) {
	n1 := tiny.NewNode(0b00000001)
	n2 := tiny.NewNode(0b00000010)
	n3 := tiny.NewNode(0b00000011)
	out := tiny.NewNode(0b00010000) // 16

	graph := map[string][]tiny.Node{
		n1.String():  {n2, n3},
		n3.String():  {n2},
		out.String(): {},
	}

	qry := tinyCoverageQuery(t, tiny.Key(0), tinyCoverageRegionLen, []tiny.Node{n1, n2, n3, out}, tinyCoverageConfig(20))
	st := runCoverage(t, qry, graph, map[string]bool{n2.String(): true})

	require.Equal(t, map[string]bool{n1.String(): true, n3.String(): true}, nodeKeys(st.ClosestNodes))
	require.Equal(t, 1, st.Stats.Failure)
}

// TestCoverageLargerKey exercises coverage on 32-bit keys, a wider space than the
// 8-bit tiny keys, with a region holding more nodes than NumResults so the result
// is not capped.
func TestCoverageLargerKey(t *testing.T) {
	type node = *kadtest.ID[kadtest.Key32]

	newNode := func(prefix string) node {
		return kadtest.NewID(kadtest.RandomKeyWithPrefix(prefix))
	}

	// The region is the length-4 prefix 0000. Its members share those bits; the
	// out-of-region nodes differ in the first bit and so are farther from any
	// target inside the region.
	const prefix = "0000"
	const prefixLen = 4

	in := make([]node, 6)
	for i := range in {
		in[i] = newNode(prefix)
	}
	out1 := newNode("1000")
	out2 := newNode("1100")

	graph := map[string][]node{
		out1.String(): {},
		out2.String(): {},
	}
	for _, n := range in {
		graph[n.String()] = in
	}

	target := kadtest.RandomKeyWithPrefix(prefix)
	seeds := append(append([]node{}, in...), out1, out2)
	iter := NewClosestNodesIter[kadtest.Key32, node](target)
	self := kadtest.NewID(kadtest.RandomKeyWithPrefix("1111"))

	cfg := DefaultQueryConfig()
	cfg.NumResults = 4

	qry, err := NewCoverageQuery[kadtest.Key32, node, coordt.NoMessage[kadtest.Key32, node]](self, coordt.ActivityID("cover"), target, prefixLen, iter, seeds, cfg)
	require.NoError(t, err)

	st := runCoverage(t, qry, graph, nil)

	require.Len(t, st.ClosestNodes, len(in))
	require.Equal(t, nodeKeys(in), nodeKeys(st.ClosestNodes))
	// Every returned node is inside the region.
	for _, n := range st.ClosestNodes {
		require.GreaterOrEqual(t, n.Key().CommonPrefixLength(target), prefixLen)
	}
}
