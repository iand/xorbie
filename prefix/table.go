// Package prefix maintains a map of a Kademlia keyspace divided into regions.
//
// A [Table] is a binary trie whose leaves are regions that tile the keyspace: every key falls in
// exactly one region and the regions cover the whole space. Each region records how many nodes its
// last survey found and how fresh that count is. The table splits a region that has grown too
// large and merges neighbours that have grown too small, so the map tracks the network's density
// over time.
//
// The table holds region information, not nodes, and names each region only by its prefix. It
// carries no schedule: when a region is surveyed is decided by whatever maintains it.
//
// Like a routing table it is maintained by one component and read by several, so it is safe for
// concurrent use. Reads take an immutable snapshot of the trie without locking; a mutation holds a
// mutex, rebuilds the changed path leaving the rest shared, and swaps the root in atomically.
package prefix

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key/bitstr"

	"github.com/iand/xorbie/coordt"
)

// Config specifies the configuration for a [Table].
type Config struct {
	// InitialPrefixLen is the prefix length the table is seeded with, giving 2^InitialPrefixLen
	// regions. It is chosen from an estimate of the network size so that a region holds roughly
	// the replication factor of nodes.
	InitialPrefixLen int

	// MinPopulation is the population at or below which two sibling regions merge.
	MinPopulation int

	// MaxPopulation is the population above which a region splits.
	MaxPopulation int
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *Config) Validate() error {
	if cfg.InitialPrefixLen < 0 {
		return &coordt.ConfigurationError{
			Component: "prefix.Config",
			Err:       fmt.Errorf("initial prefix length must not be negative"),
		}
	}

	if cfg.MinPopulation < 0 {
		return &coordt.ConfigurationError{
			Component: "prefix.Config",
			Err:       fmt.Errorf("minimum population must not be negative"),
		}
	}

	if cfg.MaxPopulation <= cfg.MinPopulation {
		return &coordt.ConfigurationError{
			Component: "prefix.Config",
			Err:       fmt.Errorf("maximum population must be greater than minimum population"),
		}
	}

	return nil
}

// DefaultConfig returns the default configuration options for a [Table].
// Options may be overridden before passing to [NewTable].
func DefaultConfig() *Config {
	return &Config{
		InitialPrefixLen: 0,
		MinPopulation:    10, // MAGIC
		MaxPopulation:    40, // MAGIC
	}
}

// A Region is a snapshot of one region of the keyspace.
type Region struct {
	Prefix       bitstr.Key // the region's prefix; its length is the prefix length
	Population   int        // the number of nodes the last survey found, zero if never surveyed
	LastSurveyed time.Time  // when the region was last surveyed, zero if never
}

// A Table maps a Kademlia keyspace into regions. It is generic over the key type only, since it
// holds no nodes.
type Table[K kad.Key[K]] struct {
	cfg  Config
	mu   sync.Mutex           // held to serialise mutations
	root atomic.Pointer[node] // the immutable trie, replaced whole on a mutation
}

// node is an immutable trie node. A leaf carries a region and has no children; an internal node
// has children but no region.
type node struct {
	children [2]*node
	region   *regionData
}

// regionData is the immutable state the table keeps for a leaf.
type regionData struct {
	population   int
	lastSurveyed time.Time
}

// NewTable creates a table seeded with 2^InitialPrefixLen regions.
func NewTable[K kad.Key[K]](cfg *Config) (*Table[K], error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var zero K
	if cfg.InitialPrefixLen > zero.BitLen() {
		return nil, &coordt.ConfigurationError{
			Component: "prefix.Config",
			Err:       fmt.Errorf("initial prefix length exceeds key length"),
		}
	}

	t := &Table[K]{cfg: *cfg}
	t.root.Store(buildTrie(cfg.InitialPrefixLen))
	return t, nil
}

// buildTrie returns a full binary trie of the given depth, every leaf an unsurveyed region.
func buildTrie(depth int) *node {
	if depth == 0 {
		return &node{region: &regionData{}}
	}
	child := buildTrie(depth - 1)
	// The two subtrees are independent trees so that a later mutation of one does not touch the
	// other; buildTrie is cheap, so build the second rather than share.
	return &node{children: [2]*node{child, buildTrie(depth - 1)}}
}

// Locate returns the region the key falls in.
func (t *Table[K]) Locate(key K) Region {
	n := t.root.Load()
	var prefix strings.Builder
	for depth := 0; n.region == nil; depth++ {
		b := key.Bit(depth)
		n = n.children[b]
		prefix.WriteByte(bitByte(b))
	}
	return regionOf(n, prefix.String())
}

// Regions returns a snapshot of every region.
func (t *Table[K]) Regions() []Region {
	var out []Region
	var walk func(n *node, prefix string)
	walk = func(n *node, prefix string) {
		if n.region != nil {
			out = append(out, regionOf(n, prefix))
			return
		}
		walk(n.children[0], prefix+"0")
		walk(n.children[1], prefix+"1")
	}
	walk(t.root.Load(), "")
	return out
}

// Observe records the result of surveying the region named by prefix. It sets the region's
// population from the members found and its freshness to now, then splits the region if it holds
// more than MaxPopulation members, or merges it with its sibling if both are surveyed and hold no
// more than MinPopulation. It returns the prefixes of the regions a split or merge removed and
// added, so the caller can keep a schedule in step with the table.
func (t *Table[K]) Observe(prefix bitstr.Key, members []K, now time.Time) (removed, added []bitstr.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()

	old := t.root.Load()
	next := t.observeNode(old, prefix, 0, members, now)
	if next == old {
		return nil, nil
	}
	t.root.Store(next)
	return diffPrefixes(leafPrefixes(old), leafPrefixes(next))
}

// observeNode returns a new version of the subtree rooted at n with the observation applied at
// prefix, or n unchanged if the prefix no longer names a leaf.
func (t *Table[K]) observeNode(n *node, prefix bitstr.Key, depth int, members []K, now time.Time) *node {
	if n.region != nil {
		if depth != len(prefix) {
			// The prefix is finer than this leaf, so the region was merged away; ignore it.
			return n
		}
		return t.buildLeaf(members, depth, now)
	}

	if depth == len(prefix) {
		// The prefix names an internal node, so the region was split; ignore it.
		return n
	}

	b := prefix.Bit(depth)
	newChild := t.observeNode(n.children[b], prefix, depth+1, members, now)
	if newChild == n.children[b] {
		return n
	}

	other := n.children[1-b]
	if l1 := t.sparseSurveyedLeaf(newChild); l1 != nil {
		if l2 := t.sparseSurveyedLeaf(other); l2 != nil {
			return &node{region: &regionData{
				population:   l1.population + l2.population,
				lastSurveyed: earlierTime(l1.lastSurveyed, l2.lastSurveyed),
			}}
		}
	}

	var children [2]*node
	children[b] = newChild
	children[1-b] = other
	return &node{children: children}
}

// buildLeaf returns the subtree for a surveyed region: a single leaf, or, when the region holds
// more than MaxPopulation members, an internal node whose members are partitioned by successive
// bits until every leaf is within the threshold.
func (t *Table[K]) buildLeaf(members []K, depth int, now time.Time) *node {
	var zero K
	if len(members) <= t.cfg.MaxPopulation || depth >= zero.BitLen() {
		return &node{region: &regionData{population: len(members), lastSurveyed: now}}
	}

	var groups [2][]K
	for _, m := range members {
		b := m.Bit(depth)
		groups[b] = append(groups[b], m)
	}
	return &node{children: [2]*node{
		t.buildLeaf(groups[0], depth+1, now),
		t.buildLeaf(groups[1], depth+1, now),
	}}
}

// sparseSurveyedLeaf returns the region of n if n is a leaf that has been surveyed and holds no
// more than MinPopulation members, and nil otherwise.
func (t *Table[K]) sparseSurveyedLeaf(n *node) *regionData {
	if n.region == nil {
		return nil
	}
	if n.region.lastSurveyed.IsZero() {
		return nil
	}
	if n.region.population > t.cfg.MinPopulation {
		return nil
	}
	return n.region
}

// leafPrefixes returns the prefix of every leaf in the trie rooted at n.
func leafPrefixes(n *node) []bitstr.Key {
	var out []bitstr.Key
	var walk func(n *node, prefix string)
	walk = func(n *node, prefix string) {
		if n.region != nil {
			out = append(out, bitstr.Key(prefix))
			return
		}
		walk(n.children[0], prefix+"0")
		walk(n.children[1], prefix+"1")
	}
	walk(n, "")
	return out
}

// diffPrefixes returns the prefixes in old but not new, and in new but not old.
func diffPrefixes(old, next []bitstr.Key) (removed, added []bitstr.Key) {
	oldSet := make(map[bitstr.Key]bool, len(old))
	for _, p := range old {
		oldSet[p] = true
	}
	nextSet := make(map[bitstr.Key]bool, len(next))
	for _, p := range next {
		nextSet[p] = true
	}
	for _, p := range old {
		if !nextSet[p] {
			removed = append(removed, p)
		}
	}
	for _, p := range next {
		if !oldSet[p] {
			added = append(added, p)
		}
	}
	return removed, added
}

func regionOf(n *node, prefix string) Region {
	return Region{
		Prefix:       bitstr.Key(prefix),
		Population:   n.region.population,
		LastSurveyed: n.region.lastSurveyed,
	}
}

func bitByte(b uint) byte {
	if b == 1 {
		return '1'
	}
	return '0'
}

func earlierTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
