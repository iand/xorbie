package routing

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/internal/tiny"
)

func TestExploreConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("tracer is not nil", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("meter is not nil", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.Meter = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("timeout positive", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.Timeout = 0
		require.Error(t, cfg.Validate())
		cfg.Timeout = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request concurrency positive", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.RequestConcurrency = 0
		require.Error(t, cfg.Validate())
		cfg.RequestConcurrency = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("request timeout positive", func(t *testing.T) {
		cfg := DefaultExploreConfig()
		cfg.RequestTimeout = 0
		require.Error(t, cfg.Validate())
		cfg.RequestTimeout = -1
		require.Error(t, cfg.Validate())
	})
}

// maxCpl is 7 since we are using tiny 8-bit keys
const maxCplTinyKeys = 7

func DefaultDynamicSchedule(t *testing.T, now time.Time) *DynamicExploreSchedule {
	t.Helper()
	s, err := NewDynamicExploreSchedule(maxCplTinyKeys, now, time.Hour, 1, 0)
	require.NoError(t, err)
	return s
}

func TestDynamicExploreSchedule(t *testing.T) {
	testCases := []struct {
		interval   time.Duration
		multiplier float64
	}{
		{
			interval:   time.Hour,
			multiplier: 1,
		},
		{
			interval:   time.Hour,
			multiplier: 1.1,
		},
		{
			interval:   time.Hour,
			multiplier: 2,
		},
	}

	// test invariants
	for _, tc := range testCases {
		now := epoch
		maxCpl := 20

		s, err := NewDynamicExploreSchedule(maxCpl, now, tc.interval, tc.multiplier, 0)
		require.NoError(t, err)

		intervals := make([]time.Duration, 0, maxCpl+1)
		cpl := maxCpl
		for cpl >= 0 {
			intervals = append(intervals, s.cplInterval(cpl))
			cpl--
		}

		// higher cpls must have a shorter interval than lower cpls
		assert.IsIncreasing(t, intervals)

		// intervals increase by at least one base interval for each cpl
		// interval for cpl[x-1] is at twice the interval of than cpl[x]
		// and cpl[x-2] is at least three times larger than cpl[x]
		for i := 1; i < len(intervals); i++ {
			assert.GreaterOrEqual(t, intervals[i], intervals[0]*time.Duration(i+1))
		}

	}
}

func TestExploreStartsIdle(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)
}

func TestExploreFirstQueriesForMaximumCpl(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	now = now.Add(schedule.cplInterval(schedule.maxCpl))

	// explore should now start the explore query
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// the query should have the correct ID
	require.Equal(t, ExploreQueryID, st.QueryID)

	// with the correct cpl
	require.Equal(t, schedule.maxCpl, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.Equal(t, schedule.maxCpl, self.Key().CommonPrefixLength(st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)
}

func TestExploreFindCloserResponse(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := now

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	now = start.Add(interval1)

	// explore should now start the explore query
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was contacted successfully, but no closer nodes
	state = ex.Advance(ctx, now, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})
	require.IsType(t, &StateExploreQueryFinished[tiny.Key, tiny.Node]{}, state)
}

func TestExploreFindCloserFailure(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := now

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	now = start.Add(interval1)

	// explore should now start the explore query
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was not
	state = ex.Advance(ctx, now, &EventExploreFindCloserFailure[tiny.Key, tiny.Node]{
		NodeID: a,
	})
	require.IsType(t, &StateExploreQueryFinished[tiny.Key, tiny.Node]{}, state)
}

func TestExploreProgress(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	a := tiny.NewNode(4)  // 4
	b := tiny.NewNode(8)  // 8
	c := tiny.NewNode(16) // 16

	// ensure the order of the known nodes
	require.True(t, self.Key().Xor(a.Key()).Compare(self.Key().Xor(b.Key())) == -1)
	require.True(t, self.Key().Xor(b.Key()).Compare(self.Key().Xor(c.Key())) == -1)

	// populate the routing table with at least one node
	rt.AddNode(a)

	start := now

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	now = start.Add(interval1)

	// explore should now start the explore query
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, a, st.NodeID)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was contacted successfully, with a closer node
	state = ex.Advance(ctx, now, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      a,
		CloserNodes: []tiny.Node{b},
	})

	// explore tries to contact nearer node
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// notify explore that node was contacted successfully, with a closer node
	state = ex.Advance(ctx, now, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      b,
		CloserNodes: []tiny.Node{c},
	})

	// explore tries to contact nearer node
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the query should attempt to contact the node it was given
	st = state.(*StateExploreFindCloser[tiny.Key, tiny.Node])
	require.Equal(t, c, st.NodeID)

	// notify explore that node was contacted successfully, but no closer nodes
	state = ex.Advance(ctx, now, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: c,
	})
	require.IsType(t, &StateExploreQueryFinished[tiny.Key, tiny.Node]{}, state)
}

func TestExploreQueriesNextHighestCpl(t *testing.T) {
	ctx := context.Background()
	now := epoch

	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	// populate the routing table with at least one node
	a := tiny.NewNode(4)
	rt.AddNode(a)

	start := now

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)

	// advance the clock to the due time of the first explore that should be started
	interval1 := schedule.cplInterval(schedule.maxCpl)
	now = start.Add(interval1)

	// explore should now start the explore query
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)
	st := state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// the query should have the correct ID
	require.Equal(t, ExploreQueryID, st.QueryID)

	// with the correct cpl
	require.Equal(t, schedule.maxCpl, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.Equal(t, schedule.maxCpl, self.Key().CommonPrefixLength(st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)

	// now the explore reports that it is waiting
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// notify explore that node was contacted successfully, but no closer nodes
	state = ex.Advance(ctx, now, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID: a,
	})
	require.IsType(t, &StateExploreQueryFinished[tiny.Key, tiny.Node]{}, state)

	// advance the clock to the due time of the second cpl explore that should be started
	interval2 := schedule.cplInterval(schedule.maxCpl - 1)
	now = start.Add(interval2)

	// explore should now start another explore query
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)
	st = state.(*StateExploreFindCloser[tiny.Key, tiny.Node])

	// with the correct cpl
	require.Equal(t, schedule.maxCpl-1, st.Cpl)

	// the query should attempt to look for nodes near a key with the maximum cpl
	require.Equal(t, schedule.maxCpl-1, self.Key().CommonPrefixLength(st.Target))

	// the query should be contacting the nearest known node
	require.Equal(t, a, st.NodeID)
}

func TestExploreQueryTimeout(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultExploreConfig()
	cfg.Timeout = 3 * time.Minute

	// the request must outlive the explore so its query is still waiting for a
	// response when the explore deadline passes
	cfg.RequestTimeout = time.Hour

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	a := tiny.NewNode(4)
	rt.AddNode(a)

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	// advance the clock to the due time of the first explore so a query starts
	now = schedule.NextDue()
	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the node never responds, but the explore has not run out of time yet
	now = now.Add(cfg.Timeout - time.Second)
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)

	// once the deadline passes the explore gives up on its query
	now = now.Add(2 * time.Second)
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreQueryTimeout{}, state)
}

// TestExploreTimeoutIgnoresLaterResponses checks that a response from a request that was
// still outstanding when an explore query timed out does not advance the abandoned query.
func TestExploreTimeoutIgnoresLaterResponses(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultExploreConfig()
	cfg.Timeout = 3 * time.Minute

	// the request must outlive the explore so its query is still waiting for a
	// response when the explore deadline passes
	cfg.RequestTimeout = time.Hour

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	a := tiny.NewNode(4)
	rt.AddNode(a)

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	// advance the clock to the due time of the first explore so a query starts
	now = schedule.NextDue()
	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the explore gives up on its query while the request to a is still outstanding
	now = now.Add(cfg.Timeout + time.Second)
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreQueryTimeout{}, state)

	// notify explore that the node was contacted successfully after the timeout
	state = ex.Advance(ctx, now, &EventExploreFindCloserResponse[tiny.Key, tiny.Node]{
		NodeID:      a,
		CloserNodes: []tiny.Node{tiny.NewNode(8)},
	})

	// explore should ignore the late message and now be idle until the next cpl falls due
	require.IsType(t, &StateExploreIdle{}, state)
	require.Equal(t, schedule.NextDue(), state.(*StateExploreIdle).NextDue)
}

// TestExploreTimeoutIgnoresLaterFailures checks that a failure from a request that was
// still outstanding when an explore query timed out does not advance the abandoned query.
func TestExploreTimeoutIgnoresLaterFailures(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultExploreConfig()
	cfg.Timeout = 3 * time.Minute

	// the request must outlive the explore so its query is still waiting for a
	// response when the explore deadline passes
	cfg.RequestTimeout = time.Hour

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)

	a := tiny.NewNode(4)
	rt.AddNode(a)

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	// advance the clock to the due time of the first explore so a query starts
	now = schedule.NextDue()
	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	// the explore gives up on its query while the request to a is still outstanding
	now = now.Add(cfg.Timeout + time.Second)
	state = ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreQueryTimeout{}, state)

	// notify explore that the node failed to be contacted after the timeout
	state = ex.Advance(ctx, now, &EventExploreFindCloserFailure[tiny.Key, tiny.Node]{
		NodeID: a,
	})

	// explore should ignore the late message and now be idle until the next cpl falls due
	require.IsType(t, &StateExploreIdle{}, state)
	require.Equal(t, schedule.NextDue(), state.(*StateExploreIdle).NextDue)
}

func TestExploreReportsNextDue(t *testing.T) {
	ctx := context.Background()
	now := epoch
	cfg := DefaultExploreConfig()

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)
	rt.AddNode(tiny.NewNode(4))

	schedule := DefaultDynamicSchedule(t, now)
	ex, err := NewExplore(self, rt, tiny.NodeWithCpl, schedule, cfg)
	require.NoError(t, err)

	// while idle the reported time is when the next cpl falls due
	state := ex.Advance(ctx, now, &EventExplorePoll{})
	require.IsType(t, &StateExploreIdle{}, state)
	require.Equal(t, schedule.NextDue(), state.(*StateExploreIdle).NextDue)

	// once a query is running the reported time follows its request deadline
	due := schedule.NextDue()
	state = ex.Advance(ctx, due, &EventExplorePoll{})
	require.IsType(t, &StateExploreFindCloser[tiny.Key, tiny.Node]{}, state)

	state = ex.Advance(ctx, due, &EventExplorePoll{})
	require.IsType(t, &StateExploreWaiting{}, state)
	require.Equal(t, due.Add(cfg.RequestTimeout), state.(*StateExploreWaiting).NextDue)
}

// TestDynamicExploreScheduleFirstRound checks that a new schedule explores every cpl within one
// base interval.
func TestDynamicExploreScheduleFirstRound(t *testing.T) {
	const (
		maxCpl   = 14
		interval = 10 * time.Minute
	)

	now := epoch

	s, err := NewDynamicExploreSchedule(maxCpl, now, interval, 1, 0)
	if err != nil {
		t.Fatalf("new schedule: %v", err)
	}

	// Every cpl is explored once, in order of decreasing cpl, before the base interval is out.
	seen := make(map[int]time.Duration, maxCpl+1)
	want := maxCpl
	for ts := now; !ts.After(now.Add(interval)); ts = ts.Add(time.Second) {
		cpl, ok := s.NextCpl(ts)
		if !ok {
			continue
		}
		if cpl != want {
			t.Fatalf("got cpl %d at %s, want %d", cpl, ts.Sub(now), want)
		}
		seen[cpl] = ts.Sub(now)
		want--
	}

	if len(seen) != maxCpl+1 {
		t.Fatalf("got %d cpls explored within %s, want %d", len(seen), interval, maxCpl+1)
	}

	// From then on each cpl falls back to its own scaled interval, so over the next half hour
	// the highest cpl comes round again and the lowest does not.
	repeats := make(map[int]int)
	for ts := now.Add(interval); !ts.After(now.Add(interval + 30*time.Minute)); ts = ts.Add(time.Second) {
		if cpl, ok := s.NextCpl(ts); ok {
			repeats[cpl]++
		}
	}

	if repeats[maxCpl] == 0 {
		t.Errorf("got no further explores of cpl %d, want it explored every %s", maxCpl, interval)
	}

	if repeats[0] != 0 {
		t.Errorf("got %d further explores of cpl 0, want none within %s of its first", repeats[0], s.cplInterval(0))
	}
}
