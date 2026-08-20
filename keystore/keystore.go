// Package keystore holds the keys a node provides, enumerable by prefix so a region publish can
// find every key that falls inside a surveyed region. It provides the [Keystore] contract and an
// in-memory [Trie] implementation of it.
package keystore

import (
	"iter"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/ipfs/go-libdht/kad/trie"
)

// Keystore enumerates the keys a node provides by prefix, so a region publish can find every key
// inside a surveyed region. An implementation must be safe for concurrent use: KeysUnder is called
// on the event loop while the provide path records keys. The in-memory [Trie] is the default; a
// driver may instead read from its own persistent datastore.
type Keystore[K kad.Key[K]] interface {
	// KeysUnder yields every provided key whose leading bits equal prefix. The empty prefix yields
	// every key.
	KeysUnder(prefix bitstr.Key) iter.Seq[K]
}

// Trie is an in-memory [Keystore] backed by a binary trie. It is safe for concurrent use: a read
// takes a lock-free snapshot of the trie while a write rebuilds the changed path and swaps the root
// in atomically.
type Trie[K kad.Key[K]] struct {
	writeMu sync.Mutex
	root    atomic.Pointer[trie.Trie[K, struct{}]]
}

// New returns an empty [Trie].
func New[K kad.Key[K]]() *Trie[K] {
	t := &Trie[K]{}
	t.root.Store(trie.New[K, struct{}]())
	return t
}

// Add records a key this node provides. Adding a key already present has no effect.
func (t *Trie[K]) Add(k K) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	// trie.Add returns a new root sharing the unchanged structure; it errors only on a key length
	// that cannot vary for a fixed K.
	next, err := trie.Add(t.root.Load(), k, struct{}{})
	if err != nil {
		return
	}
	t.root.Store(next)
}

// Remove drops a key. Removing a key that is absent has no effect.
func (t *Trie[K]) Remove(k K) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	next, err := trie.Remove(t.root.Load(), k)
	if err != nil {
		return
	}
	t.root.Store(next)
}

// KeysUnder yields every stored key whose leading bits equal prefix. The empty prefix yields every
// key. The sequence walks a snapshot taken when KeysUnder is called, so a concurrent Add or Remove
// does not affect it.
func (t *Trie[K]) KeysUnder(prefix bitstr.Key) iter.Seq[K] {
	root := t.root.Load()
	return func(yield func(K) bool) {
		node := root
		for i, n := 0, prefix.BitLen(); i < n; i++ {
			if node.IsLeaf() {
				// A leaf reached before the prefix is consumed holds the only key on this path; it
				// belongs to the region only if it carries the whole prefix.
				if node.IsNonEmptyLeaf() && hasPrefix(*node.Key(), prefix) {
					yield(*node.Key())
				}
				return
			}
			node = node.Branch(int(prefix.Bit(i)))
		}
		collect(node, yield)
	}
}

// collect yields every key in the subtree rooted at node, stopping early if yield returns false.
func collect[K kad.Key[K]](node *trie.Trie[K, struct{}], yield func(K) bool) bool {
	if node.IsLeaf() {
		if node.IsNonEmptyLeaf() {
			return yield(*node.Key())
		}
		return true
	}
	return collect(node.Branch(0), yield) && collect(node.Branch(1), yield)
}

// hasPrefix reports whether the leading bits of k equal prefix.
func hasPrefix[K kad.Key[K]](k K, prefix bitstr.Key) bool {
	n := prefix.BitLen()
	if k.BitLen() < n {
		return false
	}
	for i := range n {
		if k.Bit(i) != prefix.Bit(i) {
			return false
		}
	}
	return true
}
