package netsize

import (
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/kadtest"
	"github.com/ipfs/go-libdht/kad/key/bit256"
)

// node is the node type the estimator is exercised with. A 256 bit key is used rather than
// one of the small test keys because the estimator measures where nodes fall in the keyspace
// and a narrow key doesn't give enough of a range.
type node = *kadtest.ID[bit256.Key]

// newNetwork returns n nodes whose keys are drawn from a generator seeded with seed.
func newNetwork(seed int64, n int) []node {
	rng := rand.New(rand.NewSource(seed))
	nodes := make([]node, n)
	for i := range nodes {
		nodes[i] = kadtest.NewID(randomKey(rng))
	}
	return nodes
}

// randomKey returns a 256 bit key drawn from rng.
func randomKey(rng *rand.Rand) bit256.Key {
	buf := make([]byte, 32)
	rng.Read(buf)
	return bit256.NewKey(buf)
}

// newClusteredNetwork returns n nodes whose keys all begin with the same prefixBytes bytes, so
// that the network occupies a narrow band of the keyspace instead of being spread across it.
func newClusteredNetwork(seed int64, n, prefixBytes int) []node {
	rng := rand.New(rand.NewSource(seed))

	prefix := make([]byte, prefixBytes)
	rng.Read(prefix)

	nodes := make([]node, n)
	for i := range nodes {
		buf := make([]byte, 32)
		rng.Read(buf)
		copy(buf, prefix)
		nodes[i] = kadtest.NewID(bit256.NewKey(buf))
	}
	return nodes
}

// closest returns the num nodes of network nearest to target, nearest first. It is the result
// a lookup for target would converge on if every node in the network were reachable.
func closest(network []node, target bit256.Key, num int) []node {
	sorted := slices.Clone(network)
	slices.SortFunc(sorted, func(a, b node) int {
		return target.Xor(a.Key()).Compare(target.Xor(b.Key()))
	})
	return sorted[:min(num, len(sorted))]
}

// sample returns num nodes of network chosen at random rather than by distance to target. It
// can be used for a sample that is not the converged result of a lookup, such as the closer nodes
// carried by a single peer's reply.
func sample(rng *rand.Rand, network []node, num int) []node {
	shuffled := slices.Clone(network)
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:min(num, len(shuffled))]
}

// trackLookups tracks the result of lookups random targets against the given network.
func trackLookups(t *testing.T, e *Estimator[bit256.Key, node], now time.Time, seed int64, network []node, lookups, num int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	for range lookups {
		target := randomKey(rng)
		if err := e.Track(now, target, closest(network, target, num)); err != nil {
			t.Fatalf("Track: %v", err)
		}
	}
}

func newEstimator(t *testing.T, cfg *Config) *Estimator[bit256.Key, node] {
	t.Helper()
	e, err := New[bit256.Key, node](cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

// TestEstimateRecoversNetworkSize checks that the estimate lands near the true size of a
// network whose keys are uniformly distributed, which is the property the whole estimator
// exists to provide.
func TestEstimateRecoversNetworkSize(t *testing.T) {
	testCases := []struct {
		name    string
		size    int
		num     int
		lookups int
		tol     float64 // the fraction of size the estimate may differ from it by
		long    bool    // the case is skipped in short mode
	}{
		{name: "small", size: 200, num: 20, lookups: 200, tol: 0.1},
		{name: "medium", size: 2000, num: 20, lookups: 200, tol: 0.1},
		{name: "large", size: 10000, num: 20, lookups: 200, tol: 0.1, long: true},
	}

	now := time.Now()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.long && testing.Short() {
				t.Skip("skipping a large network in short mode; the smaller cases cover the same property")
			}

			e := newEstimator(t, nil)
			trackLookups(t, e, now, 1, newNetwork(2, tc.size), tc.lookups, tc.num)

			est, err := e.Estimate(now)
			if err != nil {
				t.Fatalf("Estimate: %v", err)
			}

			diff := math.Abs(float64(est.Size-tc.size)) / float64(tc.size)
			if diff > tc.tol {
				t.Errorf("got size %d, want within %.0f%% of %d (off by %.1f%%)", est.Size, tc.tol*100, tc.size, diff*100)
			}
		})
	}
}

// TestEstimateSmallNetwork checks that a network holding fewer nodes than a lookup asks for
// still produces an estimate.
func TestEstimateSmallNetwork(t *testing.T) {
	testCases := []struct {
		name string
		size int
		tol  float64
	}{
		{name: "five", size: 5, tol: 0.1},
		{name: "twelve", size: 12, tol: 0.1},
	}

	now := time.Now()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEstimator(t, nil)
			// A lookup asks for 20 nodes and the network cannot supply that many.
			trackLookups(t, e, now, 1, newNetwork(2, tc.size), 200, 20)

			est, err := e.Estimate(now)
			if err != nil {
				t.Fatalf("Estimate: %v", err)
			}

			diff := math.Abs(float64(est.Size-tc.size)) / float64(tc.size)
			if diff > tc.tol {
				t.Errorf("got size %d, want within %.0f%% of %d (off by %.1f%%)", est.Size, tc.tol*100, tc.size, diff*100)
			}
		})
	}
}

// TestEstimateRaggedSamples checks that samples of differing lengths can be mixed. A lookup
// returns up to [query.QueryConfig.NumResults] nodes and queries are not all configured
// alike, so the number of ranks a sample fills varies between them.
func TestEstimateRaggedSamples(t *testing.T) {
	const size = 2000
	now := time.Now()
	network := newNetwork(2, size)

	e := newEstimator(t, nil)
	trackLookups(t, e, now, 1, network, 100, 20)
	trackLookups(t, e, now, 3, network, 100, 8)
	trackLookups(t, e, now, 4, network, 100, 14)

	est, err := e.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	diff := math.Abs(float64(est.Size-size)) / float64(size)
	if diff > 0.2 {
		t.Errorf("got size %d, want within 20%% of %d (off by %.1f%%)", est.Size, size, diff*100)
	}
}

// TestEstimateIgnoresSampleOrder checks that the estimate does not depend on the order the
// nodes of a sample arrive in. The estimator reads a node's rank from its distance to the
// target, so a caller that supplies nodes in another order must not silently shift every
// measurement to the wrong rank.
func TestEstimateIgnoresSampleOrder(t *testing.T) {
	const size = 2000
	now := time.Now()
	network := newNetwork(2, size)
	rng := rand.New(rand.NewSource(5))

	targets := make([]bit256.Key, 200)
	for i := range targets {
		targets[i] = randomKey(rng)
	}

	ordered := newEstimator(t, nil)
	reversed := newEstimator(t, nil)
	shuffled := newEstimator(t, nil)

	for _, target := range targets {
		nodes := closest(network, target, 20)

		if err := ordered.Track(now, target, nodes); err != nil {
			t.Fatalf("Track: %v", err)
		}

		rev := slices.Clone(nodes)
		slices.Reverse(rev)
		if err := reversed.Track(now, target, rev); err != nil {
			t.Fatalf("Track: %v", err)
		}

		shuf := slices.Clone(nodes)
		rng.Shuffle(len(shuf), func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })
		if err := shuffled.Track(now, target, shuf); err != nil {
			t.Fatalf("Track: %v", err)
		}
	}

	want, err := ordered.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	for _, tc := range []struct {
		name string
		e    *Estimator[bit256.Key, node]
	}{
		{name: "reversed", e: reversed},
		{name: "shuffled", e: shuffled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.e.Estimate(now)
			if err != nil {
				t.Fatalf("Estimate: %v", err)
			}
			if got.Size != want.Size {
				t.Errorf("got size %d, want %d", got.Size, want.Size)
			}
		})
	}
}

// TestEstimateNotEnoughData checks that the estimator reports that it cannot answer rather
// than returning a number drawn from too few observations.
func TestEstimateNotEnoughData(t *testing.T) {
	now := time.Now()
	network := newNetwork(2, 2000)

	testCases := []struct {
		name    string
		lookups int
	}{
		{name: "none", lookups: 0},
		{name: "one", lookups: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEstimator(t, nil)
			trackLookups(t, e, now, 1, network, tc.lookups, 20)

			if _, err := e.Estimate(now); err == nil {
				t.Fatal("got nil error, want ErrNotEnoughData")
			}
		})
	}
}

// TestEstimateExpiresMeasurements checks that observations fall out of the estimate once they
// are older than the measurement window, so that a estimator which has been idle reports that it
// does not know rather than reporting a stale size.
func TestEstimateExpiresMeasurements(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now()

	e := newEstimator(t, cfg)
	trackLookups(t, e, now, 1, newNetwork(2, 2000), 200, 20)

	if _, err := e.Estimate(now.Add(cfg.MaxMeasurementAge / 2)); err != nil {
		t.Fatalf("Estimate within the window: %v", err)
	}

	if _, err := e.Estimate(now.Add(cfg.MaxMeasurementAge + time.Minute)); err == nil {
		t.Fatal("got nil error past the measurement window, want ErrNotEnoughData")
	}
}

// TestEstimateStandardErrorFallsWithSamples checks that the reported uncertainty reflects how
// much the estimate rests on.
func TestEstimateStandardErrorFallsWithSamples(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode; no other test covers the standard error")
	}

	const size = 2000
	now := time.Now()
	network := newNetwork(2, size)

	few := newEstimator(t, nil)
	trackLookups(t, few, now, 1, network, 20, 20)

	many := newEstimator(t, nil)
	trackLookups(t, many, now, 1, network, 500, 20)

	fewEst, err := few.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate from few samples: %v", err)
	}

	manyEst, err := many.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate from many samples: %v", err)
	}

	if !(manyEst.StdErr < fewEst.StdErr) {
		t.Errorf("got standard error %.2f from %d samples and %.2f from %d, want the larger sample to be more certain",
			manyEst.StdErr, manyEst.Samples, fewEst.StdErr, fewEst.Samples)
	}
}

// TestEstimateUnconvergedSampleUnderestimates pins down what happens when a sample is not the
// converged result of a lookup but merely some of the network's nodes, which is what the reply
// of any single peer amounts to. Such a sample reports the size of that peer's view rather
// than of the network.
func TestEstimateUnconvergedSampleUnderestimates(t *testing.T) {
	const size = 2000
	now := time.Now()
	network := newNetwork(2, size)
	rng := rand.New(rand.NewSource(7))

	unconverged := newEstimator(t, nil)
	for range 200 {
		target := randomKey(rng)
		if err := unconverged.Track(now, target, sample(rng, network, 20)); err != nil {
			t.Fatalf("Track: %v", err)
		}
	}

	est, err := unconverged.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	if est.Size > size/2 {
		t.Errorf("got size %d from unconverged samples, want it to fall well short of %d", est.Size, size)
	}
}

// TestEstimateFitDetectsClusteredNetwork checks that the goodness of fit reports when nodes are
// not spread across the keyspace, which is the assumption the estimate rests on. .
func TestEstimateFitDetectsClusteredNetwork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode; no other test covers the fit")
	}

	const size = 2000
	now := time.Now()

	uniform := newEstimator(t, nil)
	trackLookups(t, uniform, now, 1, newNetwork(2, size), 200, 20)

	clustered := newEstimator(t, nil)
	trackLookups(t, clustered, now, 1, newClusteredNetwork(2, size, 2), 200, 20)

	uniformEst, err := uniform.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate for a uniform network: %v", err)
	}

	clusteredEst, err := clustered.Estimate(now)
	if err != nil {
		t.Fatalf("Estimate for a clustered network: %v", err)
	}

	if !(uniformEst.Fit < clusteredEst.Fit) {
		t.Errorf("got fit %.2f for a uniform network and %.2f for a clustered one, want the uniform network to fit better",
			uniformEst.Fit, clusteredEst.Fit)
	}
}
