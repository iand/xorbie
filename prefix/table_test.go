package prefix

import (
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/internal/tiny"
)

var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func newTinyTable(t *testing.T, cfg *Config) *Table[tiny.Key] {
	t.Helper()
	tbl, err := NewTable[tiny.Key](cfg)
	require.NoError(t, err)
	return tbl
}

// regionByPrefix indexes regions by their prefix string.
func regionByPrefix(regions []Region) map[string]Region {
	m := make(map[string]Region, len(regions))
	for _, r := range regions {
		m[string(r.Prefix)] = r
	}
	return m
}

func TestConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		require.NoError(t, DefaultConfig().Validate())
	})

	t.Run("initial prefix length not negative", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.InitialPrefixLen = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("min population not negative", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MinPopulation = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("max greater than min", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MinPopulation = 10
		cfg.MaxPopulation = 10
		require.Error(t, cfg.Validate())
	})

	t.Run("initial prefix length within key length", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.InitialPrefixLen = 9 // tiny keys are 8 bits
		_, err := NewTable[tiny.Key](cfg)
		require.Error(t, err)
	})
}

// TestNewTableSeedsRegions checks that a table seeded with an initial prefix length holds one
// unsurveyed region per prefix of that length.
func TestNewTableSeedsRegions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 2
	tbl := newTinyTable(t, cfg)

	regions := tbl.Regions()
	require.Len(t, regions, 4)

	byPrefix := regionByPrefix(regions)
	for _, want := range []string{"00", "01", "10", "11"} {
		r, ok := byPrefix[want]
		require.True(t, ok, "missing region %s", want)
		require.Equal(t, 0, r.Population)
		require.True(t, r.LastSurveyed.IsZero())
	}
}

// TestLocate checks that a key maps to the region whose prefix it carries.
func TestLocate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 2
	tbl := newTinyTable(t, cfg)

	testCases := []struct {
		key  tiny.Key
		want bitstr.Key
	}{
		{tiny.Key(0b00000000), "00"},
		{tiny.Key(0b00111111), "00"},
		{tiny.Key(0b01000000), "01"},
		{tiny.Key(0b10000000), "10"},
		{tiny.Key(0b11111111), "11"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.want, tbl.Locate(tc.key).Prefix, "key %08b", uint8(tc.key))
	}
}

// TestObserveNoOp checks that a survey result within the population thresholds updates the region
// without changing the trie shape or reporting a delta.
func TestObserveNoOp(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 0
	cfg.MinPopulation = 1
	cfg.MaxPopulation = 3
	tbl := newTinyTable(t, cfg)

	removed, added := tbl.Observe(bitstr.Key(""), []tiny.Key{tiny.Key(0b00000000), tiny.Key(0b10000000)}, epoch)
	require.Empty(t, removed)
	require.Empty(t, added)

	regions := tbl.Regions()
	require.Len(t, regions, 1)
	require.Equal(t, bitstr.Key(""), regions[0].Prefix)
	require.Equal(t, 2, regions[0].Population)
	require.Equal(t, epoch, regions[0].LastSurveyed)
}

// TestObserveSplits checks that a region holding more than MaxPopulation members splits into two,
// partitioning the members by their next bit, and reports the change.
func TestObserveSplits(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 0
	cfg.MinPopulation = 1
	cfg.MaxPopulation = 3
	tbl := newTinyTable(t, cfg)

	members := []tiny.Key{
		tiny.Key(0b00000000),
		tiny.Key(0b01000000),
		tiny.Key(0b10000000),
		tiny.Key(0b11000000),
	}
	removed, added := tbl.Observe(bitstr.Key(""), members, epoch)
	require.ElementsMatch(t, []bitstr.Key{""}, removed)
	require.ElementsMatch(t, []bitstr.Key{"0", "1"}, added)

	byPrefix := regionByPrefix(tbl.Regions())
	require.Len(t, byPrefix, 2)
	require.Equal(t, 2, byPrefix["0"].Population)
	require.Equal(t, 2, byPrefix["1"].Population)
}

// TestObserveSplitsRecursively checks that a split whose halves still exceed MaxPopulation keeps
// splitting until every region is within the threshold.
func TestObserveSplitsRecursively(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 0
	cfg.MinPopulation = 0
	cfg.MaxPopulation = 1
	tbl := newTinyTable(t, cfg)

	members := []tiny.Key{
		tiny.Key(0b00000000),
		tiny.Key(0b01000000),
		tiny.Key(0b10000000),
		tiny.Key(0b11000000),
	}
	removed, added := tbl.Observe(bitstr.Key(""), members, epoch)
	require.ElementsMatch(t, []bitstr.Key{""}, removed)
	require.ElementsMatch(t, []bitstr.Key{"00", "01", "10", "11"}, added)

	byPrefix := regionByPrefix(tbl.Regions())
	require.Len(t, byPrefix, 4)
	for _, p := range []string{"00", "01", "10", "11"} {
		require.Equal(t, 1, byPrefix[p].Population, "region %s", p)
	}
}

// TestObserveMergesSurveyedSiblings checks that two sibling regions, once both surveyed and sparse,
// merge into their parent, and that an unsurveyed sibling does not trigger a merge.
func TestObserveMergesSurveyedSiblings(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 2
	cfg.MinPopulation = 1
	cfg.MaxPopulation = 3
	tbl := newTinyTable(t, cfg)

	// survey "00" sparse; its sibling "01" is still unsurveyed, so no merge and no delta
	removed, added := tbl.Observe(bitstr.Key("00"), nil, epoch)
	require.Empty(t, removed)
	require.Empty(t, added)
	require.Len(t, tbl.Regions(), 4)

	// survey "01" sparse; both siblings are now surveyed and sparse, so they merge
	removed, added = tbl.Observe(bitstr.Key("01"), nil, epoch.Add(time.Minute))
	require.ElementsMatch(t, []bitstr.Key{"00", "01"}, removed)
	require.ElementsMatch(t, []bitstr.Key{"0"}, added)

	byPrefix := regionByPrefix(tbl.Regions())
	require.Len(t, byPrefix, 3)
	merged, ok := byPrefix["0"]
	require.True(t, ok)
	require.Equal(t, 0, merged.Population)
	// the merged region keeps the earlier (staler) freshness
	require.Equal(t, epoch, merged.LastSurveyed)
}

// TestRegionsTileTheKeyspace checks that after splits every key still maps to exactly one region
// and the regions cover the whole space.
func TestRegionsTileTheKeyspace(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 1
	cfg.MinPopulation = 0
	cfg.MaxPopulation = 1
	tbl := newTinyTable(t, cfg)

	tbl.Observe(bitstr.Key("0"), []tiny.Key{
		tiny.Key(0b00000000),
		tiny.Key(0b00100000),
		tiny.Key(0b01000000),
		tiny.Key(0b01100000),
	}, epoch)

	regions := tbl.Regions()

	// the prefixes form a complete cover: the fractions of the space they cover sum to one
	total := 0
	for _, r := range regions {
		total += 1 << (8 - len(r.Prefix))
	}
	require.Equal(t, 1<<8, total)

	// every key lands in a region whose prefix it carries
	for k := range 256 {
		key := tiny.Key(k)
		r := tbl.Locate(key)
		require.LessOrEqual(t, len(r.Prefix), 8)
		for i := range len(r.Prefix) {
			require.Equal(t, r.Prefix.Bit(i), key.Bit(i), "key %08b region %s", k, r.Prefix)
		}
	}
}

// TestConcurrentReadsDuringObserve checks that reads run safely while the table is being mutated.
// Run with -race to catch data races between the lock-free reads and the copy-on-write mutations.
func TestConcurrentReadsDuringObserve(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialPrefixLen = 4 // 16 regions
	cfg.MinPopulation = 0
	cfg.MaxPopulation = 8
	tbl := newTinyTable(t, cfg)

	const rounds = 500

	var wg sync.WaitGroup
	wg.Go(func() {
		for range rounds {
			for k := range 256 {
				tbl.Locate(tiny.Key(k))
			}
			tbl.Regions()
		}
	})

	// each region i has prefix equal to the 4-bit value of i, so a member with i in its top
	// nibble falls in it; observing keeps population at one so the shape does not collapse.
	for range rounds {
		for i := range 16 {
			tbl.Observe(regionPrefix(i), []tiny.Key{tiny.Key(i << 4)}, epoch)
		}
	}

	wg.Wait()
}

// regionPrefix returns the length-4 prefix naming region i.
func regionPrefix(i int) bitstr.Key {
	b := []byte{'0', '0', '0', '0'}
	for j := range 4 {
		if i&(1<<(3-j)) != 0 {
			b[j] = '1'
		}
	}
	return bitstr.Key(b)
}
