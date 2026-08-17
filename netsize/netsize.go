// Package netsize estimates how many nodes are in the network from the results of lookups
// returned by queries and exploration.
//
// The estimate rests on where nodes fall in the keyspace. If N nodes are spread uniformly
// then the i-th closest of them to a randomly chosen key sits, on average, at a normed
// distance of i/(N+1) from it. Mean distance plotted against rank is therefore a line
// through the origin whose slope is 1/(N+1), so fitting that slope over many lookups
// recovers N without any node having to be counted.
package netsize

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/coordt"
)

// ErrNotEnoughData reports that too few observations survive in the measurement window for an
// estimate to be made.
var ErrNotEnoughData = errors.New("not enough data")

// ErrEmptySample reports that a sample carried no nodes to add to estimate.
var ErrEmptySample = errors.New("empty sample")

// distanceBits is the number of leading bits of a distance that are measured. This is sized
// to fit in the mantissa of a float64 for effiency.
const distanceBits = 53

// Config specifies optional configuration for an [Estimator].
type Config struct {
	// MaxMeasurementAge is how long an observation counts towards the estimate.
	MaxMeasurementAge time.Duration

	// MinObservations is the number of observations a rank needs before it takes part in the
	// fit. A rank needs at least two for its spread to be measurable at all.
	MinObservations int

	// MinRanks is the number of ranks that must take part before an estimate is reported.
	MinRanks int

	// MaxObservations is the number of observations held for each rank, the oldest being
	// dropped first.
	MaxObservations int
}

// DefaultConfig returns the default configuration options for an [Estimator].
func DefaultConfig() *Config {
	return &Config{
		MaxMeasurementAge: 2 * time.Hour,
		MinObservations:   2,
		MinRanks:          2,
		MaxObservations:   150,
	}
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *Config) Validate() error {
	if cfg.MaxMeasurementAge < 1 {
		return &coordt.ConfigurationError{
			Component: "netsize.Config",
			Err:       fmt.Errorf("max measurement age must be greater than zero"),
		}
	}
	if cfg.MinObservations < 2 {
		return &coordt.ConfigurationError{
			Component: "netsize.Config",
			Err:       fmt.Errorf("min observations must be greater than one"),
		}
	}
	if cfg.MinRanks < 2 {
		return &coordt.ConfigurationError{
			Component: "netsize.Config",
			Err:       fmt.Errorf("min ranks must be greater than one"),
		}
	}
	if cfg.MaxObservations < cfg.MinObservations {
		return &coordt.ConfigurationError{
			Component: "netsize.Config",
			Err:       fmt.Errorf("max observations must not be less than min observations"),
		}
	}
	return nil
}

// An Estimate is the size of the network as measured at one moment, reported with enough
// information for a caller to judge how far to trust it.
type Estimate struct {
	// Size is the estimated number of nodes in the network.
	Size int

	// StdErr is the standard error of Size, in nodes.
	StdErr float64

	// Fit reports how well the measurements are described by a network of uniformly
	// distributed nodes, as a reduced chi-square. Smaller is better. A large value means the
	// model does not hold, which is a different failure from an imprecise estimate and one
	// StdErr will not report.
	Fit float64

	// Samples is the number of observations the estimate was drawn from.
	Samples int
}

// An observation records how far one node sat from the target of one lookup.
type observation struct {
	distance float64
	at       time.Time
}

// An Estimator accumulates the results of completed lookups and reports how many nodes it
// thinks the network holds.
//
// Every lookup passed to [Estimator.Track] contributes one observation per node it discovered,
// recorded under that node's rank among the nodes returned. [Estimator.Estimate] fits a line
// through the origin over the mean distance at each rank and reads the network size from its
// slope.
//
// An Estimator is safe for concurrent use.
type Estimator[K kad.Key[K], N kad.NodeID[K]] struct {
	// cfg is a copy of the optional configuration supplied to the Estimator
	cfg Config

	// mu guards obs
	mu sync.Mutex

	// obs holds the observations made at each rank, indexed by rank and ordered by the time
	// they were made. It grows to the length of the longest sample seen.
	obs [][]observation
}

// New creates an estimator. A nil cfg uses [DefaultConfig].
func New[K kad.Key[K], N kad.NodeID[K]](cfg *Config) (*Estimator[K, N], error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Estimator[K, N]{cfg: *cfg}, nil
}

// Track records the nodes a completed lookup for target found to be closest to it, as observed
// at the time now.
func (e *Estimator[K, N]) Track(now time.Time, target K, closest []N) error {
	if len(closest) == 0 {
		return ErrEmptySample
	}

	// Rank is read from distance rather than from the caller's ordering, since a sample
	// arriving in another order would otherwise file every one of its nodes under the wrong
	// rank.
	sorted := slices.Clone(closest)
	slices.SortFunc(sorted, func(a, b N) int {
		return target.Xor(a.Key()).Compare(target.Xor(b.Key()))
	})

	e.mu.Lock()

	for len(e.obs) < len(sorted) {
		e.obs = append(e.obs, nil)
	}

	for i, node := range sorted {
		obs := append(e.obs[i], observation{
			distance: NormedDistance(target, node.Key()),
			at:       now,
		})
		if len(obs) > e.cfg.MaxObservations {
			obs = slices.Clone(obs[len(obs)-e.cfg.MaxObservations:])
		}
		e.obs[i] = obs
	}

	e.mu.Unlock()

	return nil
}

// Estimate reports the size of the network as measured at the time now, drawing on the
// observations made within [Config.MaxMeasurementAge]. It returns [ErrNotEnoughData] if
// too few observations survive for a line to be fitted.
func (e *Estimator[K, N]) Estimate(now time.Time) (Estimate, error) {
	cutoff := now.Add(-e.cfg.MaxMeasurementAge)

	// Each rank contributes one point to the fit: the mean distance observed at that rank,
	// weighted by the reciprocal of the variance of that mean.
	type point struct {
		rank   float64
		mean   float64
		weight float64
	}

	var (
		points  []point
		sumwxx  float64
		sumwxy  float64
		samples int
	)

	e.mu.Lock()
	for i := range e.obs {
		obs := unexpired(e.obs[i], cutoff)
		if len(obs) < e.cfg.MinObservations {
			continue
		}

		mean, variance := meanVariance(obs)
		if variance <= 0 {
			// Every node observed at this rank sat at the same measured distance, so the mean
			// carries no uncertainty and cannot be weighted against the other ranks.
			continue
		}

		rank := float64(i + 1)
		weight := float64(len(obs)) / variance

		points = append(points, point{rank: rank, mean: mean, weight: weight})
		sumwxx += weight * rank * rank
		sumwxy += weight * rank * mean
		samples += len(obs)
	}
	e.mu.Unlock()

	if len(points) < e.cfg.MinRanks {
		return Estimate{}, ErrNotEnoughData
	}

	// The line passes through the origin, so the fit has one free parameter.
	slope := sumwxy / sumwxx
	if slope <= 0 || slope >= 1 {
		// A slope outside this range implies fewer than one node, so the measurements do not
		// describe a network.
		return Estimate{}, ErrNotEnoughData
	}

	var chiSquare float64
	for _, p := range points {
		residual := p.mean - slope*p.rank
		chiSquare += p.weight * residual * residual
	}

	// Size is 1/slope - 1, so an error in the slope carries into the size scaled by the
	// derivative of that expression, which is 1/slope squared.
	slopeErr := 1 / math.Sqrt(sumwxx)

	return Estimate{
		Size:    int(math.Round(1/slope - 1)),
		StdErr:  slopeErr / (slope * slope),
		Fit:     chiSquare / float64(len(points)-1),
		Samples: samples,
	}, nil
}

// unexpired returns the observations made after cutoff.
func unexpired(obs []observation, cutoff time.Time) []observation {
	i := sort.Search(len(obs), func(j int) bool {
		return obs[j].at.After(cutoff)
	})
	return obs[i:]
}

// meanVariance returns the mean of the observed distances and their variance about it.
func meanVariance(obs []observation) (float64, float64) {
	var sum float64
	for _, o := range obs {
		sum += o.distance
	}
	mean := sum / float64(len(obs))

	var sumSquares float64
	for _, o := range obs {
		diff := o.distance - mean
		sumSquares += diff * diff
	}

	return mean, sumSquares / float64(len(obs)-1)
}

// NormedDistance returns the distance between two keys as a fraction of the widest distance the
// keyspace holds. A kad.Key carries no numeric value, so the leading bits are read out
// individually; scaling them to the unit interval leaves the result independent of the key
// width.
func NormedDistance[K kad.Key[K]](a, b K) float64 {
	distance := a.Xor(b)

	bits := min(distance.BitLen(), distanceBits)

	var leading uint64
	for i := range bits {
		leading = leading<<1 | uint64(distance.Bit(i))
	}

	return float64(leading) / float64(uint64(1)<<bits)
}
