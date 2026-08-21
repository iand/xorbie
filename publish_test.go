package xorbie

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
	"github.com/iand/xorbie/keystore"
	"github.com/iand/xorbie/publish"
)

// TestPublishBehaviourContactsAllSeeds asserts that a static publish sends
// its record to every seed node without waiting for a response from each one in
// turn.
//
// The behaviour is driven exactly as [Coordinator.eventLoop] drives it, which
// is the point: the static strategy holds no concurrency limit and is willing
// to contact every seed, but it only gets the chance if the behaviour keeps
// signalling that it is ready.
func TestPublishBehaviourContactsAllSeeds(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(6)
	require.NoError(t, err)

	self := nodes[0].NodeID

	pool, err := publish.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	b, err := NewPublishBehaviour(pool, nil)
	require.NoError(t, err)

	seeds := []tiny.Node{
		nodes[1].NodeID,
		nodes[2].NodeID,
		nodes[3].NodeID,
		nodes[4].NodeID,
		nodes[5].NodeID,
	}

	msg := tiny.Message{Content: "store"}

	b.Notify(ctx, &EventStartStaticPublish[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: "test",
		Target:  msg.Target(),
		Message: msg,
		Nodes:   seeds,
		Notify:  NewPublishWaiter[tiny.Key, tiny.Node, tiny.Message](0),
	})

	evs := PerformWhileReady(t, ctx, b)

	var contacted []tiny.Node
	for _, ev := range evs {
		if oev, ok := ev.(*EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message]); ok {
			contacted = append(contacted, oev.To)
		}
	}

	require.ElementsMatch(t, seeds, contacted, "expected every seed to be sent the record")
}

// TestPublishBehaviourReportsDroppedPublishStart checks that a request to start a
// publish that finds no room in the behaviour's inbound queue is reported to its caller as
// a finished publish carrying ErrEventDropped. The caller waits on the monitor for a
// terminal event, so dropping the request silently would leave it waiting until its context
// expired.
func TestPublishBehaviourReportsDroppedPublishStart(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(2)
	require.NoError(t, err)

	pool, err := publish.NewPool[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, nil)
	require.NoError(t, err)

	cfg := DefaultPublishConfig[tiny.Key, tiny.Node, tiny.Message]()
	cfg.QueueCapacity = 1

	b, err := NewPublishBehaviour(pool, cfg)
	require.NoError(t, err)

	// take the queue's only place
	b.Notify(ctx, &EventStopQuery{QueryID: "filler"})

	waiter := NewPublishWaiter[tiny.Key, tiny.Node, tiny.Message](1)
	b.Notify(ctx, &EventStartFollowUpPublish[tiny.Key, tiny.Node, tiny.Message]{
		QueryID: "dropped",
		Target:  nodes[1].NodeID.Key(),
		Notify:  waiter,
	})

	select {
	case wev := <-waiter.Finished():
		require.ErrorIs(t, wev.Event.Err, ErrEventDropped)
		require.Equal(t, coordt.QueryID("dropped"), wev.Event.QueryID)
	default:
		t.Fatal("caller was not told the publish had been dropped")
	}
}

// newRegionPublishBehaviour returns a publish behaviour configured to publish the keys held in ks
// with r closest nodes per key and at most maxInFlight per-key publishes at once.
func newRegionPublishBehaviour(t *testing.T, self tiny.Node, ks keystore.Keystore[tiny.Key], r, maxInFlight int) *PublishBehaviour[tiny.Key, tiny.Node, tiny.Message] {
	t.Helper()

	pool, err := publish.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	cfg := DefaultPublishConfig[tiny.Key, tiny.Node, tiny.Message]()
	cfg.Keystore = ks
	cfg.RecordSource = func(k tiny.Key) tiny.Message {
		return tiny.Message{Content: "region", TargetKey: k}
	}
	cfg.RegionReplication = r
	cfg.RegionMaxInFlight = maxInFlight

	b, err := NewPublishBehaviour(pool, cfg)
	require.NoError(t, err)
	return b
}

// sendMessages picks the outbound store messages out of a slice of emitted behaviour events.
func sendMessages(evs []BehaviourEvent) []*EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message] {
	var sends []*EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message]
	for _, ev := range evs {
		if oev, ok := ev.(*EventOutboundSendMessage[tiny.Key, tiny.Node, tiny.Message]); ok {
			sends = append(sends, oev)
		}
	}
	return sends
}

// TestPublishBehaviourStartsRegionKeys checks that a region publish enumerates the keys in the
// keystore and starts a per-key publish for each, storing every key with its closest nodes in the
// region and building the record from the configured source.
func TestPublishBehaviourStartsRegionKeys(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	nodes := testPeers(t, 5)
	self := nodes[0].NodeID
	region := []tiny.Node{nodes[1].NodeID, nodes[2].NodeID, nodes[3].NodeID, nodes[4].NodeID}

	ks := keystore.New[tiny.Key]()
	keys := []tiny.Key{0b0000_0001, 0b0100_0000, 0b1000_0001}
	for _, k := range keys {
		ks.Add(k)
	}

	b := newRegionPublishBehaviour(t, self, ks, 2, 8)

	// the empty prefix names the whole region, so every stored key falls inside it
	b.Notify(ctx, &EventStartRegionPublish[tiny.Key, tiny.Node]{
		QueryID: "region-1",
		Prefix:  "",
		Nodes:   region,
	})

	evs := PerformWhileReady(t, ctx, b)

	perKey := map[tiny.Key][]tiny.Node{}
	for _, oev := range sendMessages(evs) {
		require.Equal(t, "region", oev.Message.Content)
		perKey[oev.Message.TargetKey] = append(perKey[oev.Message.TargetKey], oev.To)
	}

	require.Len(t, perKey, len(keys), "every key should be published")
	for _, k := range keys {
		got := perKey[k]
		require.Len(t, got, 2, "each key is stored with r nodes")

		seen := map[string]bool{}
		for _, n := range got {
			require.Contains(t, region, n, "a stored node must belong to the region")
			require.False(t, seen[n.String()], "a key must not be stored with the same node twice")
			seen[n.String()] = true
		}
	}
}

// TestPublishBehaviourRegionCapAndCompletion checks that a region publish holds keys back at the
// in-flight cap, starts a held key once a per-key publish finishes, and drops the region once every
// key has been published.
func TestPublishBehaviourRegionCapAndCompletion(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	nodes := testPeers(t, 2)
	self := nodes[0].NodeID
	region := []tiny.Node{nodes[1].NodeID}

	ks := keystore.New[tiny.Key]()
	keys := []tiny.Key{0b0000_0001, 0b1000_0000}
	for _, k := range keys {
		ks.Add(k)
	}

	b := newRegionPublishBehaviour(t, self, ks, 1, 1)

	b.Notify(ctx, &EventStartRegionPublish[tiny.Key, tiny.Node]{
		QueryID: "region-1",
		Prefix:  "",
		Nodes:   region,
	})

	// with a cap of one only the first key's store goes out
	first := sendMessages(PerformWhileReady(t, ctx, b))
	require.Len(t, first, 1, "the cap of one allows a single key in flight")
	firstKey := first[0].Message.TargetKey

	// reporting the store succeeded finishes that key's publish and frees the slot
	b.Notify(ctx, &EventSendMessageSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:  first[0].QueryID,
		To:       first[0].To,
		Request:  first[0].Message,
		Response: first[0].Message,
	})

	second := sendMessages(PerformWhileReady(t, ctx, b))
	require.Len(t, second, 1, "freeing the slot lets the next key start")
	require.NotEqual(t, firstKey, second[0].Message.TargetKey, "the second key is a different one")

	// completing the second key finishes the region
	b.Notify(ctx, &EventSendMessageSuccess[tiny.Key, tiny.Node, tiny.Message]{
		QueryID:  second[0].QueryID,
		To:       second[0].To,
		Request:  second[0].Message,
		Response: second[0].Message,
	})
	PerformWhileReady(t, ctx, b)

	require.Empty(t, b.regions, "the region is dropped once every key is published")
	require.Empty(t, b.children, "no per-key publishes remain mapped to a region")
}

// TestPublishBehaviourRegionDisabledWithoutKeystore checks that a request to start a region publish
// does nothing when the behaviour has no keystore, since region publishing is then disabled.
func TestPublishBehaviourRegionDisabledWithoutKeystore(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	nodes := testPeers(t, 2)
	self := nodes[0].NodeID

	pool, err := publish.NewPool[tiny.Key, tiny.Node, tiny.Message](self, nil)
	require.NoError(t, err)

	b, err := NewPublishBehaviour(pool, nil)
	require.NoError(t, err)

	b.Notify(ctx, &EventStartRegionPublish[tiny.Key, tiny.Node]{
		QueryID: "region-1",
		Prefix:  "",
		Nodes:   []tiny.Node{nodes[1].NodeID},
	})

	evs := PerformWhileReady(t, ctx, b)
	require.Empty(t, sendMessages(evs), "no stores should be sent when region publishing is disabled")
	require.Empty(t, b.regions, "no region should be created")
}
