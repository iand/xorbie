package routing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"github.com/ipfs/go-libdht/kad/triert"
	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/prefix"
)

var _ coordt.StateMachine[SurveyEvent, SurveyState] = (*Survey[tiny.Key, tiny.Node])(nil)

// tinyTarget mints a target for a region by placing the prefix bits in the high bits of a tiny key,
// the test analogue of a driver's prefix-to-target function.
func tinyTarget(region bitstr.Key) (tiny.Key, error) {
	var v uint8
	for i := range len(region) {
		v <<= 1
		if region.Bit(i) == 1 {
			v |= 1
		}
	}
	v <<= (8 - len(region))
	return tiny.Key(v), nil
}

func tinySurveyConfig() *SurveyConfig {
	cfg := DefaultSurveyConfig()
	cfg.Interval = 4 * time.Hour
	return cfg
}

func newTinySurvey(t *testing.T, cfg *SurveyConfig, tblCfg *prefix.Config, members []tiny.Node) (*Survey[tiny.Key, tiny.Node], *prefix.Table[tiny.Key]) {
	t.Helper()
	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)
	for _, m := range members {
		rt.AddNode(m)
	}
	tbl, err := prefix.NewTable[tiny.Key](tblCfg)
	require.NoError(t, err)
	sv, err := NewSurvey[tiny.Key, tiny.Node](self, rt, tbl, tinyTarget, cfg)
	require.NoError(t, err)
	return sv, tbl
}

// driveSurvey runs the survey until its first terminal state, answering each find-closer request
// from graph or failing the node if it is in failing.
func driveSurvey(t *testing.T, sv *Survey[tiny.Key, tiny.Node], now time.Time, graph map[string][]tiny.Node, failing map[string]bool) SurveyState {
	t.Helper()
	ctx := context.Background()
	var ev SurveyEvent = &EventSurveyPoll{}
	for range 1000 {
		st := sv.Advance(ctx, now, ev)
		switch s := st.(type) {
		case *StateSurveyFindCloser[tiny.Key, tiny.Node]:
			id := s.NodeID.String()
			if failing[id] {
				ev = &EventSurveyFindCloserFailure[tiny.Key, tiny.Node]{NodeID: s.NodeID, Error: fmt.Errorf("boom")}
			} else {
				ev = &EventSurveyFindCloserResponse[tiny.Key, tiny.Node]{NodeID: s.NodeID, CloserNodes: graph[id]}
			}
		case *StateSurveyFinished[tiny.Key, tiny.Node]:
			return s
		case *StateSurveyTimeout:
			return s
		case *StateSurveyFailure:
			return s
		default:
			ev = &EventSurveyPoll{}
		}
	}
	t.Fatal("survey did not reach a terminal state within the advance budget")
	return nil
}

func nodeSet(nodes []tiny.Node) map[string]bool {
	m := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		m[n.String()] = true
	}
	return m
}

func TestSurveyConfigValidate(t *testing.T) {
	fields := []struct {
		name  string
		spoil func(*SurveyConfig)
	}{
		{"tracer", func(c *SurveyConfig) { c.Tracer = nil }},
		{"meter", func(c *SurveyConfig) { c.Meter = nil }},
		{"interval", func(c *SurveyConfig) { c.Interval = 0 }},
		{"timeout", func(c *SurveyConfig) { c.RegionTimeout = 0 }},
		{"concurrency", func(c *SurveyConfig) { c.RequestConcurrency = 0 }},
		{"request timeout", func(c *SurveyConfig) { c.RequestTimeout = 0 }},
		{"walk-in bound", func(c *SurveyConfig) { c.WalkInBound = 0 }},
	}

	require.NoError(t, DefaultSurveyConfig().Validate())
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			cfg := DefaultSurveyConfig()
			f.spoil(cfg)
			require.Error(t, cfg.Validate())
		})
	}
}

// TestSurveyAutoStartsDueRegion checks that the survey starts a coverage query for the earliest due
// region without being told to, targeting a key inside that region.
func TestSurveyAutoStartsDueRegion(t *testing.T) {
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2 // regions 00, 01, 10, 11

	n := tiny.NewNode(0b00000001) // under region 00
	sv, _ := newTinySurvey(t, tinySurveyConfig(), tblCfg, []tiny.Node{n})

	state := sv.Advance(context.Background(), epoch, &EventSurveyPoll{})

	fc, ok := state.(*StateSurveyFindCloser[tiny.Key, tiny.Node])
	require.True(t, ok)
	require.Equal(t, SurveyActivityID, fc.ActivityID)
	require.Equal(t, tiny.Key(0), fc.Target) // target for region "00"
	require.Equal(t, n, fc.NodeID)
}

// TestSurveyEnumeratesRegionAndObserves checks that a finished survey reports the region's members
// and records them into the table.
func TestSurveyEnumeratesRegionAndObserves(t *testing.T) {
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2

	n1 := tiny.NewNode(0b00000001)
	n2 := tiny.NewNode(0b00000010)
	out := tiny.NewNode(0b01000000) // region 01, outside 00

	sv, tbl := newTinySurvey(t, tinySurveyConfig(), tblCfg, []tiny.Node{n1, n2, out})

	graph := map[string][]tiny.Node{
		n1.String():  {n2},
		n2.String():  {n1},
		out.String(): {},
	}

	st := driveSurvey(t, sv, epoch, graph, nil)

	fin, ok := st.(*StateSurveyFinished[tiny.Key, tiny.Node])
	require.True(t, ok)
	require.Equal(t, bitstr.Key("00"), fin.Prefix)
	require.Equal(t, nodeSet([]tiny.Node{n1, n2}), nodeSet(fin.Nodes))

	// the table now holds the survey result for region 00
	region := tbl.Locate(tiny.Key(0))
	require.Equal(t, bitstr.Key("00"), region.Prefix)
	require.Equal(t, 2, region.Population)
	require.Equal(t, epoch, region.LastSurveyed)
}

// TestSurveyReschedulesAndGoesIdle checks that after surveying the due region the survey reports
// idle until the next region falls due, and that the surveyed region is pushed one interval on.
func TestSurveyReschedulesAndGoesIdle(t *testing.T) {
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2

	n := tiny.NewNode(0b00000001)
	sv, _ := newTinySurvey(t, tinySurveyConfig(), tblCfg, []tiny.Node{n})

	graph := map[string][]tiny.Node{n.String(): {}}
	st := driveSurvey(t, sv, epoch, graph, nil)
	require.IsType(t, &StateSurveyFinished[tiny.Key, tiny.Node]{}, st)

	// region 00 has been rescheduled one interval on, so nothing is due at the epoch; the next
	// region, 01, is due one interval-quarter later
	state := sv.Advance(context.Background(), epoch, &EventSurveyPoll{})
	idle, ok := state.(*StateSurveyIdle)
	require.True(t, ok)
	require.Equal(t, epoch.Add(time.Hour), idle.NextDue)
}

// TestSurveyReconcilesScheduleOnSplit checks that when a survey splits a region the schedule drops
// the region and picks up its children, each inheriting the region's due time.
func TestSurveyReconcilesScheduleOnSplit(t *testing.T) {
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2
	tblCfg.MinPopulation = 0
	tblCfg.MaxPopulation = 1

	// four members inside region 00, differing in bits 2 and 3 so the region splits fully
	m0 := tiny.NewNode(0b00000000)
	m1 := tiny.NewNode(0b00010000)
	m2 := tiny.NewNode(0b00100000)
	m3 := tiny.NewNode(0b00110000)
	out := tiny.NewNode(0b01000000)

	sv, _ := newTinySurvey(t, tinySurveyConfig(), tblCfg, []tiny.Node{m0, m1, m2, m3, out})

	all := []tiny.Node{m0, m1, m2, m3}
	graph := map[string][]tiny.Node{out.String(): {}}
	for _, m := range all {
		graph[m.String()] = all
	}

	st := driveSurvey(t, sv, epoch, graph, nil)
	require.IsType(t, &StateSurveyFinished[tiny.Key, tiny.Node]{}, st)

	// region 00 was surveyed at the epoch and rescheduled to one interval on; its four children
	// inherit that due time, and 00 itself is gone from the schedule
	_, ok := sv.schedule.dueOf(bitstr.Key("00"))
	require.False(t, ok)
	for _, child := range []bitstr.Key{"0000", "0001", "0010", "0011"} {
		due, ok := sv.schedule.dueOf(child)
		require.True(t, ok, "missing child %s", child)
		require.Equal(t, epoch.Add(4*time.Hour), due)
	}
}

// TestSurveyMemberFailure checks that a survey completes when a member fails to answer.
func TestSurveyMemberFailure(t *testing.T) {
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2

	n1 := tiny.NewNode(0b00000001)
	n2 := tiny.NewNode(0b00000010)
	out := tiny.NewNode(0b01000000)

	sv, _ := newTinySurvey(t, tinySurveyConfig(), tblCfg, []tiny.Node{n1, n2, out})

	graph := map[string][]tiny.Node{
		n1.String():  {n2},
		out.String(): {},
	}

	st := driveSurvey(t, sv, epoch, graph, map[string]bool{n2.String(): true})

	fin, ok := st.(*StateSurveyFinished[tiny.Key, tiny.Node])
	require.True(t, ok)
	require.Equal(t, nodeSet([]tiny.Node{n1}), nodeSet(fin.Nodes))
}

// TestSurveyTimeout checks that a survey whose query does not complete within the timeout reports a
// timeout carrying the region it was surveying.
func TestSurveyTimeout(t *testing.T) {
	ctx := context.Background()
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2

	cfg := tinySurveyConfig()
	cfg.RequestTimeout = 2 * cfg.RegionTimeout // the seed stays in flight so the survey times out first

	n := tiny.NewNode(0b00000001)
	sv, _ := newTinySurvey(t, cfg, tblCfg, []tiny.Node{n})

	now := epoch
	state := sv.Advance(ctx, now, &EventSurveyPoll{})
	require.IsType(t, &StateSurveyFindCloser[tiny.Key, tiny.Node]{}, state)

	state = sv.Advance(ctx, now, &EventSurveyPoll{})
	require.IsType(t, &StateSurveyWaiting{}, state)

	now = now.Add(cfg.RegionTimeout + time.Second)
	state = sv.Advance(ctx, now, &EventSurveyPoll{})

	to, ok := state.(*StateSurveyTimeout)
	require.True(t, ok)
	require.Equal(t, bitstr.Key("00"), to.Prefix)
}

// TestSurveyFailureOnBadTarget checks that a survey reports a failure when a target cannot be minted
// for a region.
func TestSurveyFailureOnBadTarget(t *testing.T) {
	tblCfg := prefix.DefaultConfig()
	tblCfg.InitialPrefixLen = 2

	self := tiny.NewNode(128)
	rt, err := triert.New(self, nil)
	require.NoError(t, err)
	tbl, err := prefix.NewTable[tiny.Key](tblCfg)
	require.NoError(t, err)

	badTarget := func(bitstr.Key) (tiny.Key, error) { return 0, fmt.Errorf("no preimage") }
	sv, err := NewSurvey[tiny.Key, tiny.Node](self, rt, tbl, badTarget, tinySurveyConfig())
	require.NoError(t, err)

	state := sv.Advance(context.Background(), epoch, &EventSurveyPoll{})
	fail, ok := state.(*StateSurveyFailure)
	require.True(t, ok)
	require.Equal(t, bitstr.Key("00"), fail.Prefix)
}

// TestRegionSchedule exercises the schedule heap and index directly.
func TestRegionSchedule(t *testing.T) {
	s := newRegionSchedule()
	s.add(bitstr.Key("0"), epoch.Add(2*time.Hour))
	s.add(bitstr.Key("1"), epoch.Add(time.Hour))
	s.add(bitstr.Key("00"), epoch.Add(3*time.Hour))

	// the earliest due is at the head
	e, ok := s.peek()
	require.True(t, ok)
	require.Equal(t, bitstr.Key("1"), e.prefix)

	// rescheduling changes the order
	s.reschedule(bitstr.Key("1"), epoch.Add(4*time.Hour))
	e, _ = s.peek()
	require.Equal(t, bitstr.Key("0"), e.prefix)

	// dueOf reports the stored time
	due, ok := s.dueOf(bitstr.Key("00"))
	require.True(t, ok)
	require.Equal(t, epoch.Add(3*time.Hour), due)

	// removal drops it
	s.remove(bitstr.Key("0"))
	_, ok = s.dueOf(bitstr.Key("0"))
	require.False(t, ok)
	e, _ = s.peek()
	require.Equal(t, bitstr.Key("00"), e.prefix)
}
