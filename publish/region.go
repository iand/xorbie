package publish

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ipfs/go-libdht/kad"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
)

// The RegionPublish state machine schedules the publishing of every key in a surveyed region.
//
// It doesn't do any network work of its own. For each key it emits [StateRegionStartKey], asking
// the caller to start an ordinary [Static] publish that stores the key with its closest nodes in
// the region. RegionPublish doesn't hold a [Static] machine; those live in the [Pool] the caller
// drives. RegionPublish is only a scheduler over the region's keys.
//
// The keys and the region's node set are fixed when the machine is created. A poll while a key
// remains and fewer than maxInFlight per-key publishes are in flight emits [StateRegionStartKey]
// for the next key, one key per poll. Each key is offered the r nodes closest to it, so an
// over-large region never stores a key with more than r nodes.
//
// The caller reports a per-key publish finishing with [EventRegionKeyDone], which frees a slot but
// doesn't start a key; the next poll starts the key the freed slot allows. While every key has
// started but some publish is still outstanding, or the in-flight limit holds keys back, the
// machine emits [StateRegionWaiting]. Once every key has started and nothing is left in flight it
// emits [StateRegionFinished].
type RegionPublish[K kad.Key[K], N kad.NodeID[K]] struct {
	// regionID is the id of this region publish, used to derive the child publish ids
	regionID coordt.QueryID

	// keys is the snapshot of region keys to publish
	keys []K

	// nodes is the region's node set, from which each key's closest nodes are chosen
	nodes []N

	// r is the number of closest nodes each key is stored with
	r int

	// maxInFlight is the greatest number of per-key publishes that may be in flight at once
	maxInFlight int

	// cursor is the index of the next key to start
	cursor int

	// inflight maps the id of each started per-key publish to the index of its key
	inflight map[coordt.QueryID]int

	// tracer traces the execution of this state machine
	tracer trace.Tracer
}

// NewRegion creates a state machine that publishes each key in keys with its r closest nodes
// drawn from nodes, running at most maxInFlight per-key publishes at once and reporting under the
// region id qid. Both r and maxInFlight must be at least one.
func NewRegion[K kad.Key[K], N kad.NodeID[K]](qid coordt.QueryID, keys []K, nodes []N, r, maxInFlight int, tracer trace.Tracer) *RegionPublish[K, N] {
	return &RegionPublish[K, N]{
		regionID:    qid,
		keys:        keys,
		nodes:       nodes,
		r:           r,
		maxInFlight: maxInFlight,
		inflight:    map[coordt.QueryID]int{},
		tracer:      tracer,
	}
}

// Advance advances the state of the [RegionPublish] state machine.
func (rp *RegionPublish[K, N]) Advance(ctx context.Context, now time.Time, ev RegionEvent) (out RegionState) {
	_, span := rp.tracer.Start(ctx, "RegionPublish.Advance", trace.WithAttributes(coordt.AttrInEvent(ev)))
	defer func() {
		span.SetAttributes(
			coordt.AttrOutEvent(out),
			attribute.Int("cursor", rp.cursor),
			attribute.Int("keys", len(rp.keys)),
			attribute.Int("inflight", len(rp.inflight)),
		)
		span.End()
	}()

	switch ev := ev.(type) {
	case *EventRegionKeyDone:
		// freeing a slot is bookkeeping only; a key is started by a poll, not here
		delete(rp.inflight, ev.ChildID)
	case *EventRegionPoll:
		if rp.cursor < len(rp.keys) && len(rp.inflight) < rp.maxInFlight {
			i := rp.cursor
			rp.cursor++
			k := rp.keys[i]
			child := coordt.QueryID(fmt.Sprintf("%s-%d", rp.regionID, i))
			rp.inflight[child] = i
			return &StateRegionStartKey[K, N]{
				ChildID: child,
				Target:  k,
				Nodes:   closest(rp.nodes, k, rp.r),
			}
		}
	default:
		panic(fmt.Sprintf("unexpected event: %T", ev))
	}

	if rp.cursor < len(rp.keys) || len(rp.inflight) > 0 {
		// either the cap is holding keys back or a per-key publish is still outstanding; a
		// per-key publish doesn't have a deadline of its own, so nothing is scheduled
		return &StateRegionWaiting{}
	}

	return &StateRegionFinished{}
}

// closest returns the r nodes closest to target, or every node when fewer than r are given. It
// does not modify nodes.
func closest[K kad.Key[K], N kad.NodeID[K]](nodes []N, target K, r int) []N {
	sorted := slices.Clone(nodes)
	slices.SortFunc(sorted, func(a, b N) int {
		return a.Key().Xor(target).Compare(b.Key().Xor(target))
	})
	if r < len(sorted) {
		sorted = sorted[:r]
	}
	return sorted
}

// RegionState must be implemented by all states that a [RegionPublish] state machine can reach. A
// state is the output of advancing the machine, and carries an instruction the caller acts on.
type RegionState interface {
	regionState()
}

// StateRegionStartKey indicates that a [RegionPublish] wants a per-key publish started for the
// key Target, storing it with Nodes. ChildID is the id the caller reports the outcome under with
// [EventRegionKeyDone].
type StateRegionStartKey[K kad.Key[K], N kad.NodeID[K]] struct {
	ChildID coordt.QueryID // the id of the per-key publish to start
	Target  K              // the key to publish
	Nodes   []N            // the nodes the key is stored with, its closest in the region
}

// StateRegionWaiting indicates that a [RegionPublish] can't start a key, either because the cap
// on in-flight per-key publishes is reached or because every key has started and some publish is
// still outstanding.
type StateRegionWaiting struct {
	NextDue time.Time // the earliest time advancing the region could make progress, zero if there is none
}

// StateRegionFinished indicates that a [RegionPublish] has started every key and every per-key
// publish has reported done.
type StateRegionFinished struct{}

func (*StateRegionStartKey[K, N]) regionState() {}
func (*StateRegionWaiting) regionState()        {}
func (*StateRegionFinished) regionState()       {}

// RegionEvent is an event intended to advance the state of a [RegionPublish] state machine. An
// event flows into a machine and a state flows out of it.
type RegionEvent interface {
	regionEvent()
}

// EventRegionPoll signals a [RegionPublish] that it can start the next key or do housekeeping.
type EventRegionPoll struct{}

// EventRegionKeyDone notifies a [RegionPublish] that the per-key publish with the id ChildID has
// finished, freeing a slot.
type EventRegionKeyDone struct {
	ChildID coordt.QueryID // the id of the per-key publish that finished
}

func (*EventRegionPoll) regionEvent()    {}
func (*EventRegionKeyDone) regionEvent() {}
