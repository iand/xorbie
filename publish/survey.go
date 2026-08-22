package publish

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"github.com/ipfs/go-libdht/kad/key/bitstr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/prefix"
	"github.com/iand/xorbie/query"
)

// SurveyActivityID is the id for the activity operated by the survey process.
const SurveyActivityID = coordt.ActivityID("survey")

// A PrefixTargetFunc returns a key inside the region described by prefix, whose leading
// bits equal prefix. Minting a key with a given prefix is network specific and this
// behaviour must be injected externally.
type PrefixTargetFunc[K kad.Key[K]] func(prefix bitstr.Key) (K, error)

// The Survey state machine keeps a [prefix.Table] current by surveying each of its regions in turn.
//
// It maintains the survey schedule, spreading one survey of every region
// across [SurveyConfig.Interval], and starts each survey when the region falls due. To
// survey a region it mints a target inside it with a [PrefixTargetFunc] and runs a
// coverage query (see [query.NewCoverageQuery]), walking in to the region and
// enumerating its members, which it reports with [StateSurveyFinished].
//
// When a survey finishes the machine records the result into the table, which may split
// or merge regions, and reconciles its schedule with the change.
type Survey[K kad.Key[K], N kad.NodeID[K]] struct {
	// self is the node id of the system the survey is running on
	self N

	// rt is the local routing table, used to seed each coverage query
	rt kad.RoutingTable[K, N]

	// table is the shared region map the survey maintains
	table *prefix.Table[K]

	// targetFn mints a target key inside a region from its prefix
	targetFn PrefixTargetFunc[K]

	// schedule holds the time each region is next due to be surveyed
	schedule *regionSchedule

	// seeded records whether the schedule has been populated from the table
	seeded bool

	// qry is the coverage query used by the current survey, or nil when idle
	qry *query.Query[K, N, coordt.NoMessage[K, N]]

	// prefix is the region currently being surveyed
	prefix bitstr.Key

	// target is a key inside the region currently being surveyed
	target K

	// cfg is a copy of the optional configuration supplied to the Survey
	cfg SurveyConfig

	// counterFindSent is a counter that tracks the number of requests to find closer nodes that have been sent.
	counterFindSent metric.Int64Counter

	// counterFindSucceeded is a counter that tracks the number of requests to find closer nodes that succeeded.
	counterFindSucceeded metric.Int64Counter

	// counterFindFailed is a counter that tracks the number of requests to find closer nodes that failed.
	counterFindFailed metric.Int64Counter

	// gaugeRunning is a gauge that tracks whether a survey is running.
	gaugeRunning metric.Int64ObservableGauge

	// running records whether a survey is running after the last state change so that it can be read asynchronously by gaugeRunning
	running atomic.Bool

	// counterCompleted is a counter that tracks the number of region surveys that ran to completion.
	counterCompleted metric.Int64Counter

	// counterTimeout is a counter that tracks the number of region surveys that timed out.
	counterTimeout metric.Int64Counter

	// counterFailed is a counter that tracks the number of region surveys that could not be started.
	counterFailed metric.Int64Counter

	// histogramPopulation records the number of nodes each completed survey found in its region.
	histogramPopulation metric.Int64Histogram

	// gaugeRegions is a gauge that tracks the number of regions in the table.
	gaugeRegions metric.Int64ObservableGauge

	// gaugeOldestAge is a gauge that tracks the age in seconds of the least recently surveyed region.
	gaugeOldestAge metric.Int64ObservableGauge
}

// SurveyConfig specifies optional configuration for a [Survey].
type SurveyConfig struct {
	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer

	// Meter is the meter that should be used to record metrics.
	Meter metric.Meter

	// Interval is the time within which every region in the network is surveyed once.
	Interval time.Duration

	// RegionTimeout is the maximum time to allow for surveying a region.
	RegionTimeout time.Duration

	// RequestConcurrency is the maximum number of concurrent requests that the coverage query may have in flight.
	RequestConcurrency int

	// RequestTimeout is the timeout the coverage query should use when attempting to contact a single node.
	RequestTimeout time.Duration

	// WalkInBound is the number of nodes the coverage query contacts without finding a region
	// member before concluding the region is empty.
	WalkInBound int
}

// Validate checks the configuration options and returns an error if any have invalid values.
func (cfg *SurveyConfig) Validate() error {
	if cfg.Tracer == nil {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("tracer must not be nil"),
		}
	}
	if cfg.Meter == nil {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("meter must not be nil"),
		}
	}
	if cfg.Interval < 1 {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("interval must be greater than zero"),
		}
	}
	if cfg.RegionTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("region timeout must be greater than zero"),
		}
	}
	if cfg.RequestConcurrency < 1 {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("request concurrency must be greater than zero"),
		}
	}
	if cfg.RequestTimeout < 1 {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("request timeout must be greater than zero"),
		}
	}
	if cfg.WalkInBound < 1 {
		return &coordt.ConfigurationError{
			Component: "SurveyConfig",
			Err:       fmt.Errorf("walk-in bound must be greater than zero"),
		}
	}
	return nil
}

// DefaultSurveyConfig returns the default configuration options for a [Survey].
// Options may be overridden before passing to [NewSurvey].
func DefaultSurveyConfig() *SurveyConfig {
	return &SurveyConfig{
		Tracer: coordt.NoopTracer(),
		Meter:  coordt.NoopMeter(),

		Interval:           22 * time.Hour,  // MAGIC
		RegionTimeout:      5 * time.Minute, // MAGIC
		RequestConcurrency: 3,               // MAGIC
		RequestTimeout:     time.Minute,     // MAGIC
		WalkInBound:        20,              // MAGIC
	}
}

func NewSurvey[K kad.Key[K], N kad.NodeID[K]](self N, rt kad.RoutingTable[K, N], table *prefix.Table[K], targetFn PrefixTargetFunc[K], cfg *SurveyConfig) (*Survey[K, N], error) {
	if cfg == nil {
		cfg = DefaultSurveyConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if table == nil {
		return nil, &coordt.ConfigurationError{Component: "Survey", Err: fmt.Errorf("table must not be nil")}
	}
	if targetFn == nil {
		return nil, &coordt.ConfigurationError{Component: "Survey", Err: fmt.Errorf("target function must not be nil")}
	}

	s := &Survey[K, N]{
		self:     self,
		rt:       rt,
		table:    table,
		targetFn: targetFn,
		schedule: newRegionSchedule(),
		cfg:      *cfg,
	}

	var err error
	s.counterFindSent, err = cfg.Meter.Int64Counter(
		"survey_find_sent",
		metric.WithDescription("Total number of find closer nodes requests sent by the survey state machine"),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_find_sent counter: %w", err)
	}

	s.counterFindSucceeded, err = cfg.Meter.Int64Counter(
		"survey_find_succeeded",
		metric.WithDescription("Total number of find closer nodes requests sent by the survey state machine that were successful"),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_find_succeeded counter: %w", err)
	}

	s.counterFindFailed, err = cfg.Meter.Int64Counter(
		"survey_find_failed",
		metric.WithDescription("Total number of find closer nodes requests sent by the survey state machine that failed"),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_find_failed counter: %w", err)
	}

	s.gaugeRunning, err = cfg.Meter.Int64ObservableGauge(
		"survey_running",
		metric.WithDescription("Whether or not a survey is running"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			if s.running.Load() {
				o.Observe(1)
			} else {
				o.Observe(0)
			}
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_running gauge: %w", err)
	}

	s.counterCompleted, err = cfg.Meter.Int64Counter(
		"surveys_completed",
		metric.WithDescription("Total number of region surveys that ran to completion"),
	)
	if err != nil {
		return nil, fmt.Errorf("create surveys_completed counter: %w", err)
	}

	s.counterTimeout, err = cfg.Meter.Int64Counter(
		"surveys_timeout",
		metric.WithDescription("Total number of region surveys that timed out"),
	)
	if err != nil {
		return nil, fmt.Errorf("create surveys_timeout counter: %w", err)
	}

	s.counterFailed, err = cfg.Meter.Int64Counter(
		"surveys_failed",
		metric.WithDescription("Total number of region surveys that could not be started"),
	)
	if err != nil {
		return nil, fmt.Errorf("create surveys_failed counter: %w", err)
	}

	s.histogramPopulation, err = cfg.Meter.Int64Histogram(
		"survey_region_population",
		metric.WithDescription("Number of nodes a completed survey found in its region"),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_region_population histogram: %w", err)
	}

	s.gaugeRegions, err = cfg.Meter.Int64ObservableGauge(
		"survey_regions",
		metric.WithDescription("Number of regions in the table"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(s.table.Regions())))
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_regions gauge: %w", err)
	}

	s.gaugeOldestAge, err = cfg.Meter.Int64ObservableGauge(
		"survey_oldest_region_age_seconds",
		metric.WithDescription("Age in seconds of the least recently surveyed region"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(oldestRegionAge(s.table.Regions(), time.Now()))
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create survey_oldest_region_age_seconds gauge: %w", err)
	}

	return s, nil
}

// oldestRegionAge returns the age in seconds of the least recently surveyed region, ignoring regions
// that have never been surveyed. It is zero when no region has been surveyed yet.
func oldestRegionAge(regions []prefix.Region, now time.Time) int64 {
	var oldest time.Time
	for _, r := range regions {
		if r.LastSurveyed.IsZero() {
			continue
		}
		if oldest.IsZero() || r.LastSurveyed.Before(oldest) {
			oldest = r.LastSurveyed
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return int64(now.Sub(oldest).Seconds())
}

// Advance advances the state of the survey, starting a survey for the next due region when none is
// running and otherwise advancing the running coverage query.
func (s *Survey[K, N]) Advance(ctx context.Context, now time.Time, ev SurveyEvent) (out SurveyState) {
	ctx, span := s.cfg.Tracer.Start(ctx, "Survey.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		s.running.Store(s.qry != nil)
		span.SetAttributes(coordt.AttrOutEvent(out))
		span.End()
	}()

	switch tev := ev.(type) {
	case *EventSurveyPoll:
		// ignore, nothing to do
	case *EventSurveyFindCloserResponse[K, N]:
		// ignore late responses
		if s.qry != nil {
			s.counterFindSucceeded.Add(ctx, 1)
			return s.advanceQuery(ctx, now, &query.EventQueryNodeResponse[K, N]{
				NodeID:      tev.NodeID,
				CloserNodes: tev.CloserNodes,
			})
		}
	case *EventSurveyFindCloserFailure[K, N]:
		// ignore late responses
		if s.qry != nil {
			s.counterFindFailed.Add(ctx, 1)
			span.RecordError(tev.Error)
			return s.advanceQuery(ctx, now, &query.EventQueryNodeFailure[K, N]{
				NodeID: tev.NodeID,
				Error:  tev.Error,
			})
		}
	default:
		panic(fmt.Sprintf("unexpected event: %T", tev))
	}

	// if a query is running, give it a chance to advance
	if s.qry != nil {
		return s.advanceQuery(ctx, now, &query.EventQueryPoll{})
	}

	s.seedSchedule(now)

	entry, ok := s.schedule.peek()
	if !ok || entry.due.After(now) {
		var nextDue time.Time
		if ok {
			nextDue = entry.due
		}
		return &StateSurveyIdle{NextDue: nextDue}
	}

	// take the region and push it one interval on so it is not surveyed again until then
	region := entry.prefix
	s.schedule.reschedule(region, now.Add(s.cfg.Interval))
	return s.startSurvey(ctx, now, region)
}

// seedSchedule fills the schedule from the table's regions the first time it is needed, spreading
// their due times across one interval.
func (s *Survey[K, N]) seedSchedule(now time.Time) {
	if s.seeded {
		return
	}
	s.seeded = true

	regions := s.table.Regions()
	if len(regions) == 0 {
		return
	}
	step := s.cfg.Interval / time.Duration(len(regions))
	for i, r := range regions {
		s.schedule.add(r.Prefix, now.Add(step*time.Duration(i)))
	}
}

// startSurvey begins a coverage query for the region named by prefix.
func (s *Survey[K, N]) startSurvey(ctx context.Context, now time.Time, region bitstr.Key) SurveyState {
	target, err := s.targetFn(region)
	if err != nil {
		s.counterFailed.Add(ctx, 1)
		return &StateSurveyFailure{Prefix: region, Error: fmt.Errorf("target for prefix %s: %w", region, err)}
	}

	seeds := s.rt.NearestNodes(target, s.cfg.WalkInBound)

	iter := query.NewClosestNodesIter[K, N](target)

	qryCfg := query.DefaultQueryConfig()
	qryCfg.Concurrency = s.cfg.RequestConcurrency
	qryCfg.RequestTimeout = s.cfg.RequestTimeout
	qryCfg.Timeout = s.cfg.RegionTimeout
	qryCfg.NumResults = s.cfg.WalkInBound

	qry, err := query.NewCoverageQuery[K, N, coordt.NoMessage[K, N]](s.self, SurveyActivityID, target, len(region), iter, seeds, qryCfg)
	if err != nil {
		s.counterFailed.Add(ctx, 1)
		return &StateSurveyFailure{Prefix: region, Error: fmt.Errorf("start coverage query: %w", err)}
	}

	s.qry = qry
	s.prefix = region
	s.target = target

	return s.advanceQuery(ctx, now, &query.EventQueryPoll{})
}

func (s *Survey[K, N]) advanceQuery(ctx context.Context, now time.Time, qev query.QueryEvent) SurveyState {
	ctx, span := s.cfg.Tracer.Start(ctx, "Survey.advanceQuery")
	defer span.End()

	region := s.prefix

	state := s.qry.Advance(ctx, now, qev)
	switch st := state.(type) {
	case *query.StateQueryFindCloser[K, N]:
		s.counterFindSent.Add(ctx, 1)
		return &StateSurveyFindCloser[K, N]{
			ActivityID: st.ActivityID,
			Target:     st.Target,
			NodeID:     st.NodeID,
			Stats:      st.Stats,
		}
	case *query.StateQueryFinished[K, N]:
		span.SetAttributes(attribute.String("out_state", "StateSurveyFinished"))
		s.counterCompleted.Add(ctx, 1)
		s.histogramPopulation.Record(ctx, int64(len(st.ClosestNodes)))
		s.clearQuery()
		removed, added := s.table.Observe(region, memberKeys[K, N](st.ClosestNodes), now)
		s.reconcile(removed, added)
		return &StateSurveyFinished[K, N]{
			Prefix: region,
			Nodes:  st.ClosestNodes,
			Stats:  st.Stats,
		}
	case *query.StateQueryWaitingAtCapacity:
		if now.After(st.Deadline) {
			span.SetAttributes(attribute.String("out_state", "StateSurveyTimeout"))
			s.counterTimeout.Add(ctx, 1)
			s.clearQuery()
			return &StateSurveyTimeout{Prefix: region, Stats: st.Stats}
		}
		return &StateSurveyWaiting{Stats: st.Stats, NextDue: earlier(st.NextDue, st.Deadline)}
	case *query.StateQueryWaitingWithCapacity:
		if now.After(st.Deadline) {
			span.SetAttributes(attribute.String("out_state", "StateSurveyTimeout"))
			s.counterTimeout.Add(ctx, 1)
			s.clearQuery()
			return &StateSurveyTimeout{Prefix: region, Stats: st.Stats}
		}
		return &StateSurveyWaiting{Stats: st.Stats, NextDue: earlier(st.NextDue, st.Deadline)}
	default:
		panic(fmt.Sprintf("unexpected state: %T", st))
	}
}

// reconcile brings the schedule into step with a split or merge the table reported. A split's new
// regions inherit their parent's due time; a merge's region takes the earliest due of the regions
// it replaced.
func (s *Survey[K, N]) reconcile(removed, added []bitstr.Key) {
	if len(removed) == 0 && len(added) == 0 {
		return
	}

	dues := make(map[bitstr.Key]time.Time, len(added))
	for _, a := range added {
		var best time.Time
		for _, r := range removed {
			if !prefixRelated(a, r) {
				continue
			}
			if d, ok := s.schedule.dueOf(r); ok && (best.IsZero() || d.Before(best)) {
				best = d
			}
		}
		dues[a] = best
	}

	for _, r := range removed {
		s.schedule.remove(r)
	}
	for _, a := range added {
		s.schedule.add(a, dues[a])
	}
}

func (s *Survey[K, N]) clearQuery() {
	var zero K
	s.qry = nil
	s.prefix = ""
	s.target = zero
}

// memberKeys returns the keys of the nodes.
func memberKeys[K kad.Key[K], N kad.NodeID[K]](nodes []N) []K {
	keys := make([]K, len(nodes))
	for i, n := range nodes {
		keys[i] = n.Key()
	}
	return keys
}

// prefixRelated reports whether one of the prefixes is a prefix of the other, which for a split or
// merge holds between a parent region and each of its children.
func prefixRelated(a, b bitstr.Key) bool {
	return strings.HasPrefix(string(a), string(b)) || strings.HasPrefix(string(b), string(a))
}

// SurveyState is the state of a [Survey].
type SurveyState interface {
	surveyState()
}

// StateSurveyIdle indicates that no region is due to be surveyed.
type StateSurveyIdle struct {
	NextDue time.Time // the time the next region falls due, zero if none is scheduled
}

// StateSurveyFindCloser indicates that the survey's coverage query wants to send a find closer nodes message to a node.
type StateSurveyFindCloser[K kad.Key[K], N kad.NodeID[K]] struct {
	ActivityID coordt.ActivityID
	Target     K // the key the query wants to find closer nodes for
	NodeID     N // the node to send the message to
	Stats      query.QueryStats
}

// StateSurveyWaiting indicates that the survey's coverage query is waiting for a response.
type StateSurveyWaiting struct {
	NextDue time.Time // the earliest time advancing the survey could make progress, zero if there is none
	Stats   query.QueryStats
}

// StateSurveyFinished indicates that a survey of a region has finished and reports the region's members.
type StateSurveyFinished[K kad.Key[K], N kad.NodeID[K]] struct {
	Prefix bitstr.Key // the prefix of the region that was surveyed
	Nodes  []N        // the nodes found inside the region
	Stats  query.QueryStats
}

// StateSurveyTimeout indicates that a survey's coverage query timed out before covering the region.
type StateSurveyTimeout struct {
	Prefix bitstr.Key // the prefix of the region that was being surveyed
	Stats  query.QueryStats
}

// StateSurveyFailure indicates that the survey could not start a coverage query for a region.
type StateSurveyFailure struct {
	Prefix bitstr.Key // the prefix of the region that was to be surveyed
	Error  error
}

// surveyState() ensures that only [Survey] states can be assigned to a [SurveyState].
func (*StateSurveyIdle) surveyState()             {}
func (*StateSurveyFindCloser[K, N]) surveyState() {}
func (*StateSurveyWaiting) surveyState()          {}
func (*StateSurveyFinished[K, N]) surveyState()   {}
func (*StateSurveyTimeout) surveyState()          {}
func (*StateSurveyFailure) surveyState()          {}

// SurveyEvent is an event intended to advance the state of a [Survey].
type SurveyEvent interface {
	surveyEvent()
}

// EventSurveyPoll is an event that signals the survey that it can perform housekeeping work such as starting a due survey or timing out its query.
type EventSurveyPoll struct{}

// EventSurveyFindCloserResponse notifies a survey that an attempt to find closer nodes has received a successful response.
type EventSurveyFindCloserResponse[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID      N   // the node the message was sent to
	CloserNodes []N // the closer nodes sent by the node
}

// EventSurveyFindCloserFailure notifies a survey that an attempt to find closer nodes has failed.
type EventSurveyFindCloserFailure[K kad.Key[K], N kad.NodeID[K]] struct {
	NodeID N     // the node the message was sent to
	Error  error // the error that caused the failure, if any
}

// surveyEvent() ensures that only [Survey] events can be assigned to a [SurveyEvent].
func (*EventSurveyPoll) surveyEvent()                     {}
func (*EventSurveyFindCloserResponse[K, N]) surveyEvent() {}
func (*EventSurveyFindCloserFailure[K, N]) surveyEvent()  {}

// regionSchedule holds the time each region is next due to be surveyed, ordered so the earliest is
// available in constant time and any region can be found by its prefix.
type regionSchedule struct {
	heap  scheduleHeap
	index map[bitstr.Key]*scheduleEntry
}

type scheduleEntry struct {
	prefix  bitstr.Key
	due     time.Time
	heapIdx int
}

func newRegionSchedule() *regionSchedule {
	return &regionSchedule{index: make(map[bitstr.Key]*scheduleEntry)}
}

// add inserts a region with the given due time, or updates it if already scheduled.
func (s *regionSchedule) add(region bitstr.Key, due time.Time) {
	if e, ok := s.index[region]; ok {
		e.due = due
		heap.Fix(&s.heap, e.heapIdx)
		return
	}
	e := &scheduleEntry{prefix: region, due: due}
	heap.Push(&s.heap, e)
	s.index[region] = e
}

// reschedule sets a region's due time. It is add under another name for readability at the call site.
func (s *regionSchedule) reschedule(region bitstr.Key, due time.Time) {
	s.add(region, due)
}

// remove drops a region from the schedule.
func (s *regionSchedule) remove(region bitstr.Key) {
	e, ok := s.index[region]
	if !ok {
		return
	}
	heap.Remove(&s.heap, e.heapIdx)
	delete(s.index, region)
}

// dueOf returns a region's due time.
func (s *regionSchedule) dueOf(region bitstr.Key) (time.Time, bool) {
	e, ok := s.index[region]
	if !ok {
		return time.Time{}, false
	}
	return e.due, true
}

// peek returns the entry with the earliest due time without removing it.
func (s *regionSchedule) peek() (*scheduleEntry, bool) {
	if len(s.heap) == 0 {
		return nil, false
	}
	return s.heap[0], true
}

// scheduleHeap is a min-heap of schedule entries ordered by due time.
type scheduleHeap []*scheduleEntry

func (h scheduleHeap) Len() int { return len(h) }

func (h scheduleHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].prefix < h[j].prefix
	}
	return h[i].due.Before(h[j].due)
}

func (h scheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *scheduleHeap) Push(x any) {
	e := x.(*scheduleEntry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}

func (h *scheduleHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}
