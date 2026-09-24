package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLeaseGrantedWhileAccepting(t *testing.T) {
	g := NewGate()
	release, ok := g.Acquire()
	if !ok {
		t.Fatal("Acquire on an accepting gate must succeed")
	}
	select {
	case <-g.Drained():
		t.Fatal("gate must not be drained while a lease is outstanding")
	default:
	}
	release()
}

func TestDrainRejectsNewLeases(t *testing.T) {
	g := NewGate()
	g.BeginDrain()
	if _, ok := g.Acquire(); ok {
		t.Fatal("Acquire after BeginDrain must fail")
	}
	if got := g.State(); got != Draining {
		t.Fatalf("State = %q, want %q", got, Draining)
	}
}

func TestDrainedOnlyAfterLastLeaseReleased(t *testing.T) {
	g := NewGate()
	rel1, _ := g.Acquire()
	rel2, _ := g.Acquire()
	g.BeginDrain()
	select {
	case <-g.Drained():
		t.Fatal("drained with two leases outstanding")
	default:
	}
	rel1()
	select {
	case <-g.Drained():
		t.Fatal("drained with one lease outstanding")
	default:
	}
	rel2()
	select {
	case <-g.Drained():
	case <-time.After(time.Second):
		t.Fatal("gate never drained after the last release")
	}
}

func TestIdleGateDrainsImmediately(t *testing.T) {
	g := NewGate()
	g.BeginDrain()
	select {
	case <-g.Drained():
	default:
		t.Fatal("an idle gate must be drained as soon as the drain begins")
	}
}

func TestBeginDrainIdempotent(t *testing.T) {
	g := NewGate()
	for i := 0; i < 5; i++ {
		g.BeginDrain() // must not panic or re-close the channel
	}
	g.MarkStopped()
	g.BeginDrain() // still a no-op after stopped
	if got := g.State(); got != Stopped {
		t.Fatalf("State = %q, want %q", got, Stopped)
	}
}

func TestWaitDrainedTimeout(t *testing.T) {
	g := NewGate()
	release, _ := g.Acquire()
	defer release()
	g.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.WaitDrained(ctx); err == nil {
		t.Fatal("WaitDrained with an outstanding lease must time out")
	}
}

func TestWaitDrainedZeroTimeoutWhenIdle(t *testing.T) {
	g := NewGate()
	g.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	if err := g.WaitDrained(ctx); err != nil {
		t.Fatalf("idle gate must drain even with a zero timeout: %v", err)
	}
}

// TestConcurrentBarrier races lease acquisition/release against the drain
// barrier: every lease granted must be released before Drained closes, and
// after the barrier no lease is ever granted again.
func TestConcurrentBarrier(t *testing.T) {
	g := NewGate()
	const workers = 32
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				release, ok := g.Acquire()
				if !ok {
					return
				}
				release()
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	g.BeginDrain()
	close(stop)
	wg.Wait()
	select {
	case <-g.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("gate never drained after all workers stopped")
	}
	if _, ok := g.Acquire(); ok {
		t.Fatal("Acquire after drain must keep failing")
	}
}
