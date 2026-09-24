// Package lifecycle implements the shared admission gate that drives the
// two-phase drain of the API process.
//
// The gate moves through three states: accepting -> draining -> stopped.
// While accepting, every business request must take a lease before it enters
// its handler; the lease is held until the response is written, so a request
// that won its lease before the barrier runs to completion even if its body
// is still arriving. The first shutdown signal flips the gate to draining:
// no new leases are granted (post-barrier requests are rejected by the
// caller with a stable 503) and the gate waits for the outstanding leases to
// be released. Once the in-flight count reaches zero the HTTP server is shut
// down and the gate is marked stopped.
//
// The state check and the lease grant are decided under a single mutex, so
// "still accepting" and "lease acquired" are one linearization point: a
// lease is either entirely before the barrier (and always honoured) or
// entirely after it (and never granted).
package lifecycle

import (
	"context"
	"sync"
)

// State is the lifecycle phase of the process.
type State string

const (
	// Accepting means business requests are granted leases normally.
	Accepting State = "accepting"
	// Draining means the barrier has fallen: no new leases, waiting for the
	// outstanding ones to be released.
	Draining State = "draining"
	// Stopped means the in-flight count reached zero and the HTTP server
	// has been shut down.
	Stopped State = "stopped"
)

// Gate is the shared admission controller. The zero value is not usable;
// construct with NewGate.
type Gate struct {
	mu       sync.Mutex
	state    State
	inflight int
	drained  chan struct{} // closed once draining with zero leases in flight
}

// NewGate returns a gate in the accepting state.
func NewGate() *Gate {
	return &Gate{state: Accepting, drained: make(chan struct{})}
}

// Acquire takes a lease on behalf of one business request. If the gate is
// still accepting the in-flight count is incremented and a release function
// is returned; the caller must invoke it exactly once when the request has
// finished. If the gate is draining or stopped, Acquire reports failure and
// the request must not enter its handler. The state check and the increment
// happen under the same lock, so the decision is linearizable with
// BeginDrain.
func (g *Gate) Acquire() (release func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != Accepting {
		return nil, false
	}
	g.inflight++
	return g.release, true
}

// release returns one lease; when the last lease disappears while draining
// the drained channel is closed exactly once.
func (g *Gate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inflight--
	if g.inflight == 0 && g.state == Draining {
		close(g.drained)
	}
}

// BeginDrain flips the gate from accepting to draining. It is idempotent:
// repeated signals neither re-open the gate nor panic. If no leases are
// outstanding the drained channel is closed immediately.
func (g *Gate) BeginDrain() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != Accepting {
		return
	}
	g.state = Draining
	if g.inflight == 0 {
		close(g.drained)
	}
}

// MarkStopped records that the HTTP server has been shut down. No further
// leases are granted (they were already refused while draining).
func (g *Gate) MarkStopped() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = Stopped
}

// State reports the current phase, used by the readiness probe.
func (g *Gate) State() State {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state
}

// Drained returns a channel that is closed once the gate is draining and the
// last outstanding lease has been released.
func (g *Gate) Drained() <-chan struct{} {
	return g.drained
}

// WaitDrained blocks until the gate has drained or ctx expires. An already
// drained gate wins the tie against an already expired context, so a zero
// timeout still lets an idle process shut down cleanly.
func (g *Gate) WaitDrained(ctx context.Context) error {
	select {
	case <-g.drained:
		return nil
	default:
	}
	select {
	case <-g.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
