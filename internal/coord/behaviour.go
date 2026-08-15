package coord

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-libdht/kad"

	"github.com/iand/xorbie/internal/coord/coordt"
)

// ErrEventDropped is the error reported to the caller of an operation whose event was
// dropped because the behaviour that would have carried it out had no queue space.
var ErrEventDropped = errors.New("event dropped")

// inboundQueue is a bounded queue of events awaiting processing by a behaviour. Events
// arrive from goroutines other than the one that drains them, so every method is safe to
// call concurrently.
type inboundQueue struct {
	mu       sync.Mutex
	events   []CtxEvent[BehaviourEvent]
	capacity int

	// depth holds the number of queued events so that a metric callback can read it
	// without taking the lock.
	depth atomic.Int64
}

func newInboundQueue(capacity int) *inboundQueue {
	return &inboundQueue{
		events:   make([]CtxEvent[BehaviourEvent], 0, capacity),
		capacity: capacity,
	}
}

// enqueue adds an event to the back of the queue, reporting whether there was room for it.
func (q *inboundQueue) enqueue(ce CtxEvent[BehaviourEvent]) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.events) >= q.capacity {
		return false
	}

	q.events = append(q.events, ce)
	q.depth.Store(int64(len(q.events)))

	return true
}

// dequeue removes the event at the front of the queue, reporting whether there was one.
func (q *inboundQueue) dequeue() (CtxEvent[BehaviourEvent], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.events) == 0 {
		return CtxEvent[BehaviourEvent]{}, false
	}

	var ce CtxEvent[BehaviourEvent]
	ce, q.events = q.events[0], q.events[1:]
	q.depth.Store(int64(len(q.events)))

	return ce, true
}

// empty reports whether the queue holds no events.
func (q *inboundQueue) empty() bool {
	return q.depth.Load() == 0
}

// Notify is the interface that a components to implement to be notified of
// [BehaviourEvent]'s.
type Notify[E BehaviourEvent] interface {
	Notify(ctx context.Context, ev E)
}

type NotifyCloser[E BehaviourEvent] interface {
	Notify[E]
	Close()
}

type NotifyFunc[E BehaviourEvent] func(ctx context.Context, ev E)

func (f NotifyFunc[E]) Notify(ctx context.Context, ev E) {
	f(ctx, ev)
}

type Behaviour[I BehaviourEvent, O BehaviourEvent] interface {
	// Ready returns a channel that signals when the behaviour is ready to perform work.
	// A behaviour must signal whenever it has work available, including work that has
	// become available through the passage of time rather than through an event. It may
	// signal when it has none.
	Ready() <-chan struct{}

	// Notify informs the behaviour of an event. The behaviour may perform the event
	// immediately and queue the result, causing the behaviour to become ready.
	// It is safe to call Notify from the Perform method.
	Notify(ctx context.Context, ev I)

	// Perform gives the behaviour the opportunity to perform work or to return a queued
	// result as an event.
	Perform(ctx context.Context) (O, bool)
}

// signalReady tells a behaviour's client that the behaviour has work to perform. The
// send is non-blocking, so a signal already waiting is left in place.
func signalReady(ready chan<- struct{}) {
	select {
	case ready <- struct{}{}:
	default:
	}
}

// earlier returns the earlier of two due times, treating the zero time as "no time
// scheduled" rather than as the earliest possible instant. It returns the zero time
// only when both arguments are zero.
func earlier(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero():
		return a
	case b.Before(a):
		return b
	default:
		return a
	}
}

// A readyTimer signals a behaviour's ready channel when a deadline held inside the
// behaviour's state machines falls due. It must only be armed from inside the lock that
// guards the behaviour's state.
type readyTimer struct {
	ready chan struct{}
	timer *time.Timer
}

func newReadyTimer(ready chan struct{}) *readyTimer {
	return &readyTimer{
		ready: ready,
	}
}

// Arm schedules a ready signal for the given time, replacing any signal already
// scheduled. The zero time disarms the timer.
func (t *readyTimer) Arm(due time.Time) {
	if due.IsZero() {
		t.Disarm()
		return
	}

	// A state machine treats a deadline as expired only once the clock is strictly past
	// it, so the signal is scheduled at least a nanosecond out.
	d := max(time.Until(due), time.Nanosecond)

	if t.timer == nil {
		t.timer = time.AfterFunc(d, func() { signalReady(t.ready) })
		return
	}
	t.timer.Reset(d)
}

// Disarm cancels any scheduled ready signal. A signal already sent is not withdrawn.
func (t *readyTimer) Disarm() {
	if t.timer != nil {
		t.timer.Stop()
	}
}

// CtxEvent holds and event with an associated context which may carry deadlines or
// tracing information pertinent to the event.
type CtxEvent[E any] struct {
	Ctx   context.Context
	Event E
}

// A Waiter is a Notifiee whose Notify method forwards the
// notified event to a channel which a client can wait on.
type Waiter[E BehaviourEvent] struct {
	pending chan WaiterEvent[E]
	done    atomic.Bool
}

var _ Notify[BehaviourEvent] = (*Waiter[BehaviourEvent])(nil)

func NewWaiter[E BehaviourEvent]() *Waiter[E] {
	w := &Waiter[E]{
		pending: make(chan WaiterEvent[E], 1),
	}
	return w
}

type WaiterEvent[E BehaviourEvent] struct {
	Ctx   context.Context
	Event E
}

func (w *Waiter[E]) Notify(ctx context.Context, ev E) {
	if w.done.Load() {
		return
	}
	select {
	case <-ctx.Done(): // this is the context for the work item
		return
	case w.pending <- WaiterEvent[E]{
		Ctx:   ctx,
		Event: ev,
	}:
		return

	}
}

// Close signals that the waiter should not forward and further calls to Notify.
// It closes the waiter channel so a client selecting on it will receive the close
// operation.
func (w *Waiter[E]) Close() {
	w.done.Store(true)
	close(w.pending)
}

func (w *Waiter[E]) Chan() <-chan WaiterEvent[E] {
	return w.pending
}

// A QueryMonitor receives event notifications on the progress of a query
type QueryMonitor[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N], E TerminalQueryEvent] interface {
	// NotifyProgressed returns a channel that can be used to send notification that a
	// query has made progress. If the notification cannot be sent then it will be
	// queued and retried at a later time. If the query completes before the progress
	// notification can be sent the notification will be discarded.
	NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed[K, N, M]]

	// NotifyFinished returns a channel that can be used to send the notification that a
	// query has completed. It is up to the implemention to ensure that the channel has enough
	// capacity to receive the single notification.
	// The sender must close all other QueryNotifier channels before sending on the NotifyFinished channel.
	// The sender may attempt to drain any pending notifications before closing the other channels.
	// The NotifyFinished channel will be closed once the sender has attempted to send the Finished notification.
	NotifyFinished() chan<- CtxEvent[E]
}

// QueryMonitorHook wraps a [QueryMonitor] interface and provides hooks
// that are invoked before calls to the QueryMonitor methods are forwarded.
type QueryMonitorHook[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N], E TerminalQueryEvent] struct {
	qm               QueryMonitor[K, N, M, E]
	BeforeProgressed func()
	BeforeFinished   func()
}

func NewQueryMonitorHook[K kad.Key[K], N kad.NodeID[K], M coordt.Message[K, N], E TerminalQueryEvent](qm QueryMonitor[K, N, M, E]) *QueryMonitorHook[K, N, M, E] {
	return &QueryMonitorHook[K, N, M, E]{
		qm:               qm,
		BeforeProgressed: func() {},
		BeforeFinished:   func() {},
	}
}

func (n *QueryMonitorHook[K, N, M, E]) NotifyProgressed() chan<- CtxEvent[*EventQueryProgressed[K, N, M]] {
	n.BeforeProgressed()
	return n.qm.NotifyProgressed()
}

func (n *QueryMonitorHook[K, N, M, E]) NotifyFinished() chan<- CtxEvent[E] {
	n.BeforeFinished()
	return n.qm.NotifyFinished()
}
