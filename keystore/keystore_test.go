package keystore_test

import (
	"iter"
	"slices"
	"testing"

	"github.com/ipfs/go-libdht/kad/key/bitstr"

	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/keystore"
)

// Trie satisfies the Keystore contract.
var _ keystore.Keystore[tiny.Key] = (*keystore.Trie[tiny.Key])(nil)

func collectKeys(seq iter.Seq[tiny.Key]) []int {
	var got []int
	for k := range seq {
		got = append(got, int(k))
	}
	slices.Sort(got)
	return got
}

func newTrie(keys ...tiny.Key) *keystore.Trie[tiny.Key] {
	t := keystore.New[tiny.Key]()
	for _, k := range keys {
		t.Add(k)
	}
	return t
}

func TestKeysUnder(t *testing.T) {
	// keys: 0, 1, 64, 128, 192
	store := newTrie(0b00000000, 0b00000001, 0b01000000, 0b10000000, 0b11000000)

	testCases := []struct {
		name   string
		prefix bitstr.Key
		want   []int
	}{
		{"empty", "", []int{0, 1, 64, 128, 192}},
		{"0", "0", []int{0, 1, 64}},
		{"00", "00", []int{0, 1}},
		{"01", "01", []int{64}},
		{"010", "010", []int{64}},
		{"011", "011", nil}, // early leaf 64 diverges at bit 2
		{"1", "1", []int{128, 192}},
		{"11", "11", []int{192}},
		{"111", "111", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := collectKeys(store.KeysUnder(tc.prefix))
			if !slices.Equal(got, tc.want) {
				t.Errorf("KeysUnder(%q) = %v, want %v", string(tc.prefix), got, tc.want)
			}
		})
	}
}

func TestAddIdempotent(t *testing.T) {
	store := newTrie(0b10101010, 0b10101010)
	if got := collectKeys(store.KeysUnder("")); !slices.Equal(got, []int{0b10101010}) {
		t.Errorf("adding the same key twice gave %v", got)
	}
}

func TestRemove(t *testing.T) {
	store := newTrie(1, 2, 3)
	store.Remove(2)
	store.Remove(200) // absent, no-op
	if got := collectKeys(store.KeysUnder("")); !slices.Equal(got, []int{1, 3}) {
		t.Errorf("after remove got %v", got)
	}
}

func TestConcurrentAddDuringWalk(t *testing.T) {
	store := keystore.New[tiny.Key]()
	for i := range 128 {
		store.Add(tiny.Key(i))
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 128; i < 256; i++ {
			store.Add(tiny.Key(i))
		}
	}()
	for range store.KeysUnder("") { // snapshot walk must not race the writes
	}
	<-done
	if got := len(collectKeys(store.KeysUnder(""))); got != 256 {
		t.Errorf("final key count = %d, want 256", got)
	}
}
