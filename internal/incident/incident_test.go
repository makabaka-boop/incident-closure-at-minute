package incident

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
)

func mustIncident(t *testing.T, spec Spec) *Incident {
	t.Helper()
	in, err := NewIncident(spec)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	return in
}

func arrivals(s Snapshot) map[int]int64 { return s.EarliestArrivals }

func TestEarliestArrivalsChainAndMultiSource(t *testing.T) {
	// 0 -3-> 1 -4-> 2 -5-> 3 ; second source at node 2 releasing at t=6.
	// Node 3 reached via chain at 3+4+5=12; seed at t=6 reaches it at 11.
	spec := Spec{
		N:        4,
		Pipes:    []Pipe{{0, 1, 3}, {1, 2, 4}, {2, 3, 5}},
		Releases: []Release{{0, 0}, {2, 6}},
		Intakes:  []int{3},
		Deadline: 20,
	}
	in := mustIncident(t, spec)
	if got := in.dist[3]; got != 11 {
		t.Fatalf("dist[3]=%d, want 11 (multi-source shortest seed)", got)
	}
	got, snap, err := in.Advance(20)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if snap.Status != Breached {
		t.Fatalf("status=%s, want breached", snap.Status)
	}
	want := map[int]int64{0: 0, 1: 3, 2: 6, 3: 11}
	for v, d := range want {
		if arrivals(snap)[v] != d {
			t.Fatalf("arrival[%d]=%d, want %d (all arrivals=%v)", v, arrivals(snap)[v], d, arrivals(snap))
		}
	}
	if len(got) != 4 {
		t.Fatalf("first advance new_arrivals=%v, want all 4 nodes", got)
	}
}

func TestParallelPipesTakeEarlier(t *testing.T) {
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 9}, {0, 1, 2}, {0, 1, 5}},
		Releases: []Release{{0, 4}},
		Intakes:  []int{1},
		Deadline: 20,
	}
	in := mustIncident(t, spec)
	if got := in.dist[1]; got != 6 {
		t.Fatalf("dist[1]=%d, want 6 (4+2 via the shorter parallel pipe)", got)
	}
}

func TestSelfLoopsAndReverseEdgesDoNotPropagate(t *testing.T) {
	// Forward 0 -> 1; self loops at 0 and 1; reverse pipe 1 -> 0 must not
	// leak contamination back upstream, and the self loops must not delay or
	// accelerate anything.
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 0, 1}, {1, 1, 1}, {1, 0, 2}, {0, 1, 7}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	if got := in.dist[1]; got != 7 {
		t.Fatalf("dist[1]=%d, want 7 (self-loops and reverse edge ignored)", got)
	}
	if got := in.dist[0]; got != 0 {
		t.Fatalf("dist[0]=%d, want 0 (reverse edge cannot improve source)", got)
	}
}

func TestStatusLifecycleScheduledToBreached(t *testing.T) {
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 5}, {1, 2, 3}},
		Releases: []Release{{0, 4}},
		Intakes:  []int{2},
		Deadline: 20,
	}
	in := mustIncident(t, spec)
	if snap := in.Snapshot(); snap.Status != Scheduled || snap.CurrentMinute != 0 || len(arrivals(snap)) != 0 {
		t.Fatalf("initial snapshot=%+v, want scheduled/0/{}", snap)
	}

	ar, snap, err := in.Advance(3) // before first release: still scheduled
	if err != nil {
		t.Fatalf("advance(3): %v", err)
	}
	if snap.Status != Scheduled || len(ar) != 0 {
		t.Fatalf("at t=3 status=%s arrivals=%v, want scheduled/{}", snap.Status, ar)
	}

	ar, snap, err = in.Advance(9) // release at 4; node1 at 9; node2 at 12
	if err != nil {
		t.Fatalf("advance(9): %v", err)
	}
	if snap.Status != Propagating {
		t.Fatalf("status=%s, want propagating", snap.Status)
	}
	if len(ar) != 2 || ar[0] != (Arrival{Node: 0, AtMinute: 4}) || ar[1] != (Arrival{Node: 1, AtMinute: 9}) {
		t.Fatalf("new arrivals=%v, want [{0 4},{1 9}]", ar)
	}

	// Idempotent retry at the same committed minute: empty increment, same
	// snapshot.
	ar2, snap2, err := in.Advance(9)
	if err != nil {
		t.Fatalf("retry advance(9): %v", err)
	}
	if len(ar2) != 0 {
		t.Fatalf("retry arrivals=%v, want empty", ar2)
	}
	if snap2.CurrentMinute != 9 || snap2.Status != Propagating || len(arrivals(snap2)) != 2 {
		t.Fatalf("retry snapshot=%+v, want identical", snap2)
	}

	ar, snap, err = in.Advance(12) // intake node2 reached exactly at 12
	if err != nil {
		t.Fatalf("advance(12): %v", err)
	}
	if snap.Status != Breached {
		t.Fatalf("status=%s, want breached on exact arrival at intake", snap.Status)
	}
	if len(ar) != 1 || ar[0] != (Arrival{Node: 2, AtMinute: 12}) {
		t.Fatalf("arrivals=%v, want [{2 12}]", ar)
	}
}

func TestContainedAtDeadline(t *testing.T) {
	// Intake is unreachable from the release; clock runs to the deadline.
	spec := Spec{
		N:        4,
		Pipes:    []Pipe{{0, 1, 2}, {3, 2, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	in := mustIncident(t, spec)

	if _, _, err := in.Advance(9); err != nil {
		t.Fatalf("advance(9): %v", err)
	}
	ar, snap, err := in.Advance(10)
	if err != nil {
		t.Fatalf("advance(10): %v", err)
	}
	if snap.Status != Contained {
		t.Fatalf("status=%s, want contained at deadline", snap.Status)
	}
	if len(ar) != 0 {
		t.Fatalf("arrivals at deadline=%v, want empty (intake unreachable)", ar)
	}
}

func TestBreachExactlyAtDeadline(t *testing.T) {
	// Arrival at the intake at minute == deadline: breached, never contained.
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 5}},
		Releases: []Release{{0, 5}},
		Intakes:  []int{1},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	_, snap, err := in.Advance(10)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if snap.Status != Breached {
		t.Fatalf("status=%s, want breached when arrival equals deadline", snap.Status)
	}
}

func TestAdvanceConflicts(t *testing.T) {
	// Incident A: intake node 2 is unreachable, so advancing stays
	// non-terminal until the deadline itself.
	specA := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	a := mustIncident(t, specA)
	if _, _, err := a.Advance(8); err != nil {
		t.Fatalf("advance(8): %v", err)
	}
	if _, _, err := a.Advance(7); !errors.Is(err, ErrClockRegression) {
		t.Fatalf("backwards advance: err=%v, want clock regression", err)
	}
	if _, _, err := a.Advance(11); !errors.Is(err, ErrPastDeadline) {
		t.Fatalf("past deadline while propagating: err=%v, want past_deadline", err)
	}
	// Rejected advances leave the snapshot untouched.
	if snap := a.Snapshot(); snap.CurrentMinute != 8 || snap.Status != Propagating {
		t.Fatalf("snapshot changed after 409s: %+v", snap)
	}

	// Incident B: the intake is reached at minute 1, so the event is
	// terminal long before the deadline.
	specB := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 10,
	}
	b := mustIncident(t, specB)
	if _, snap, err := b.Advance(10); err != nil || snap.Status != Breached {
		t.Fatalf("advance(10) err=%v snap=%+v, want breached", err, snap)
	}
	// Same-minute retry on a terminal event is still the idempotent empty
	// increment, not a 409.
	if ar, snap, err := b.Advance(10); err != nil || len(ar) != 0 || snap.Status != Breached {
		t.Fatalf("terminal same-minute retry: err=%v ar=%v snap=%+v", err, ar, snap)
	}
	// Any move on a terminal event is a stable conflict.
	if _, _, err := b.Advance(9); !errors.Is(err, ErrClockRegression) {
		t.Fatalf("terminal backwards: err=%v, want regression", err)
	}
	if _, _, err := b.Advance(11); !errors.Is(err, ErrTerminal) {
		t.Fatalf("terminal forward: err=%v, want terminal", err)
	}
}

func TestFailuresDoNotMutate(t *testing.T) {
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 5,
	}
	in := mustIncident(t, spec)
	_, snapOK, err := in.Advance(3)
	if err != nil {
		t.Fatalf("advance(3): %v", err)
	}
	for _, bad := range []int64{2, 6, -1} {
		if _, _, err := in.Advance(bad); err == nil {
			t.Fatalf("advance(%d) unexpectedly succeeded", bad)
		}
	}
	snap := in.Snapshot()
	if snap.CurrentMinute != snapOK.CurrentMinute || snap.Status != snapOK.Status {
		t.Fatalf("snapshot changed after rejected advances: %+v vs %+v", snap, snapOK)
	}
	if len(arrivals(snap)) != len(arrivals(snapOK)) {
		t.Fatalf("arrivals changed after rejected advances: %v", arrivals(snap))
	}
}

func TestValidationErrors(t *testing.T) {
	base := func() Spec {
		return Spec{
			N:        3,
			Pipes:    []Pipe{{0, 1, 2}},
			Releases: []Release{{0, 0}},
			Intakes:  []int{2},
			Deadline: 5,
		}
	}
	cases := []struct {
		name string
		edit func(*Spec)
	}{
		{"n too small", func(s *Spec) { s.N = 1 }},
		{"n too large", func(s *Spec) { s.N = MaxNodes + 1 }},
		{"negative deadline", func(s *Spec) { s.Deadline = -1 }},
		{"deadline too large", func(s *Spec) { s.Deadline = MaxMinutes + 1 }},
		{"pipe from out of range", func(s *Spec) { s.Pipes[0].From = 3 }},
		{"pipe to out of range", func(s *Spec) { s.Pipes[0].To = -1 }},
		{"pipe minutes zero", func(s *Spec) { s.Pipes[0].Minutes = 0 }},
		{"pipe minutes huge", func(s *Spec) { s.Pipes[0].Minutes = MaxMinutes + 1 }},
		{"no releases", func(s *Spec) { s.Releases = nil }},
		{"release node out of range", func(s *Spec) { s.Releases[0].Node = 9 }},
		{"release after deadline", func(s *Spec) { s.Releases[0].At = 6 }},
		{"release negative minute", func(s *Spec) { s.Releases[0].At = -1 }},
		{"no intakes", func(s *Spec) { s.Intakes = nil }},
		{"intake out of range", func(s *Spec) { s.Intakes[0] = 4 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := base()
			tc.edit(&spec)
			if _, err := NewIncident(spec); err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			} else {
				var ve *ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("error %v is not *ValidationError", err)
				}
			}
		})
	}
	// Too many pipes is checked independently.
	spec := base()
	spec.Pipes = make([]Pipe, MaxPipes+1)
	for i := range spec.Pipes {
		spec.Pipes[i] = Pipe{0, 1, 1}
	}
	if _, err := NewIncident(spec); err == nil {
		t.Fatalf("expected error for %d pipes", MaxPipes+1)
	}
}

func TestStoreUnknownAndCreate(t *testing.T) {
	store := NewStore()
	if _, ok := store.Get("missing"); ok {
		t.Fatal("unknown id unexpectedly found")
	}
	spec := Spec{
		N: 2, Pipes: []Pipe{{0, 1, 1}}, Releases: []Release{{0, 0}}, Intakes: []int{1}, Deadline: 3,
	}
	in, err := store.Create(spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(in.ID()) == 0 {
		t.Fatal("empty incident id")
	}
	got, ok := store.Get(in.ID())
	if !ok || got != in {
		t.Fatal("created incident not retrievable by id")
	}
	// Random identifiers never collide across creations.
	other, _ := store.Create(spec)
	if other.ID() == in.ID() {
		t.Fatal("incident ids collided")
	}
}

// TestConcurrentAdvanceSerialises fires many advances at distinct target
// minutes concurrently. Every accepted commit must land exactly on its
// target minute; collected across the race, accepted ticks must be strictly
// increasing (concurrent requests only take effect monotonically in commit
// order), and the final snapshot must be exactly what a single sequential
// advance to the final minute would produce.
func TestConcurrentAdvanceSerialises(t *testing.T) {
	spec := Spec{
		N:        6,
		Pipes:    []Pipe{{0, 1, 2}, {1, 2, 2}, {2, 3, 2}, {3, 4, 2}, {4, 5, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{5},
		Deadline: 20,
	}
	targets := []int64{2, 4, 6, 8, 10, 12, 14, 16, 18, 20}

	run := func() {
		in := mustIncident(t, spec)
		var wg sync.WaitGroup
		var mu sync.Mutex
		committed := make([]int64, 0, len(targets))
		for _, target := range targets {
			wg.Add(1)
			go func(tg int64) {
				defer wg.Done()
				if _, snap, err := in.Advance(tg); err == nil {
					if snap.CurrentMinute != tg {
						t.Errorf("commit reported minute %d for target %d", snap.CurrentMinute, tg)
					}
					mu.Lock()
					committed = append(committed, snap.CurrentMinute)
					mu.Unlock()
				}
			}(target)
		}
		wg.Wait()

		// Accepted ticks, sorted, must be strictly increasing: no two
		// distinct concurrent requests can commit the same clock minute.
		sort.Slice(committed, func(i, j int) bool { return committed[i] < committed[j] })
		if committed[len(committed)-1] != in.current {
			t.Fatalf("last accepted tick %d != stored clock %d", committed[len(committed)-1], in.current)
		}
		for i := 1; i < len(committed); i++ {
			if committed[i] <= committed[i-1] {
				t.Fatalf("accepted ticks not strictly increasing: %v", committed)
			}
		}
		// The intake is first reachable at minute 10: whoever commits the
		// first target >= 10 produces the terminal snapshot, and no later
		// tick can change it.
		final := in.Snapshot()
		if final.CurrentMinute < 10 || final.CurrentMinute > 20 || final.Status != Breached {
			t.Fatalf("final snapshot=%+v, want minute in [10,20] breached", final)
		}
		if len(arrivals(final)) != 6 {
			t.Fatalf("final arrivals=%v, want all 6 nodes", arrivals(final))
		}
		// Equivalence with a sequential run stopped at the same minute.
		seq := mustIncident(t, spec)
		_, seqSnap, _ := seq.Advance(final.CurrentMinute)
		if seqSnap.Status != final.Status || len(arrivals(seqSnap)) != len(arrivals(final)) {
			t.Fatalf("concurrent final %+v != sequential %+v at minute %d",
				final, seqSnap, final.CurrentMinute)
		}
	}
	// Repeat to shake out scheduler-dependent interleavings.
	for i := 0; i < 20; i++ {
		run()
	}
}

// TestConcurrentSameMinuteRetries races identical target minutes. Exactly
// one request is the initial commit (and reports the arrivals), every other
// request is an idempotent retry returning an empty increment and the
// identical snapshot. None of them is a 409.
func TestConcurrentSameMinuteRetries(t *testing.T) {
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 1}, {1, 2, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	var wg sync.WaitGroup
	var mu sync.Mutex
	nonEmpty := 0
	errors_ := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ar, snap, err := in.Advance(5)
			if err != nil {
				mu.Lock()
				errors_++
				mu.Unlock()
				t.Errorf("same-minute concurrent request got error: %v", err)
				return
			}
			if snap.CurrentMinute != 5 || snap.Status != Breached {
				t.Errorf("snapshot=%+v, want minute 5 breached", snap)
			}
			if len(ar) != 0 {
				mu.Lock()
				nonEmpty++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if errors_ != 0 {
		t.Fatalf("got %d errors during same-minute race", errors_)
	}
	if nonEmpty != 1 {
		t.Fatalf("got %d non-empty increments, want exactly 1 (the single first commit)", nonEmpty)
	}
}

func TestShutdownPreviewInFlightAndExactMinute(t *testing.T) {
	// 0@0 -2-> 1 -2-> 2 -2-> 3(intake). Closing pipe 1 (1->2) at t=2 blocks
	// the traversal that starts exactly at the shutdown minute. Closing at t=3
	// is too late: it entered at t=2, so node 2 still arrives at 4. A future
	// release at node 1 still cannot use the closed pipe.
	spec := Spec{
		N:        4,
		Pipes:    []Pipe{{0, 1, 2}, {1, 2, 2}, {2, 3, 2}},
		Releases: []Release{{0, 0}, {1, 8}},
		Intakes:  []int{3},
		Deadline: 12,
	}

	in := mustIncident(t, spec)
	if _, _, err := in.Advance(2); err != nil {
		t.Fatalf("advance(2): %v", err)
	}
	preview, err := in.PreviewShutdown([]int{1})
	if err != nil {
		t.Fatalf("preview at 2: %v", err)
	}
	if preview.EventMinute != 2 || preview.Snapshot.CurrentMinute != 2 {
		t.Fatalf("preview based on %+v, want minute 2", preview.Snapshot)
	}
	if _, reached := preview.Shutdown.EarliestArrivals[2]; reached {
		t.Fatalf("closed departure at t=2 was not blocked: %v", preview.Shutdown.EarliestArrivals)
	}
	if _, reached := preview.Shutdown.EarliestArrivals[3]; reached {
		t.Fatalf("intake reached through a pipe closed before departure: %v", preview.Shutdown.EarliestArrivals)
	}
	if preview.Shutdown.Status != Contained || !preview.AllIntakesProtected {
		t.Fatalf("preview=%+v, want contained and protected", preview)
	}
	if c := preview.IntakeConclusions[0]; !c.ShutdownProtected || c.ShutdownArrival != nil ||
		c.OriginalArrival == nil || *c.OriginalArrival != 6 {
		t.Fatalf("intake conclusion=%+v, want original arrival 6 and shutdown protection", c)
	}
	change := preview.ArrivalChanges[0]
	if change.Node != 2 || change.OriginalArrival == nil || *change.OriginalArrival != 4 || change.ShutdownArrival != nil {
		t.Fatalf("node 2 change=%+v, want 4 -> null", change)
	}

	in2 := mustIncident(t, spec)
	if _, _, err := in2.Advance(3); err != nil {
		t.Fatalf("advance(3): %v", err)
	}
	preview, err = in2.PreviewShutdown([]int{1})
	if err != nil {
		t.Fatalf("preview at 3: %v", err)
	}
	if got := preview.Shutdown.EarliestArrivals[3]; got != 6 {
		t.Fatalf("in-flight closed pipe arrival=%d, want 6 (all=%v)", got, preview.Shutdown.EarliestArrivals)
	}
	if preview.Shutdown.Status != Breached || preview.AllIntakesProtected {
		t.Fatalf("pipe entered before shutdown minute must still breach: %+v", preview)
	}
}

func TestShutdownPreviewParallelPipes(t *testing.T) {
	// Three parallel pipes 0->1 with lengths 2, 4 and 5. Their indices are
	// independent; closing one leaves the others open.
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 2}, {0, 1, 4}, {0, 1, 5}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 10,
	}

	in := mustIncident(t, spec)
	// Preview at the initial minute 0: the release and shortest-pipe departure
	// both happen exactly at the shutdown minute, so that pipe is blocked.
	preview, err := in.PreviewShutdown([]int{0})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if got := preview.Shutdown.EarliestArrivals[1]; got != 4 {
		t.Fatalf("closing index 0 arrival=%d, want 4 via index 1", got)
	}
	if change := preview.ArrivalChanges[0]; change.Node != 1 || *change.OriginalArrival != 2 || *change.ShutdownArrival != 4 {
		t.Fatalf("parallel change=%+v, want 2 -> 4", change)
	}
	if preview.AllIntakesProtected {
		t.Fatal("other parallel pipe remains open, so intake is not protected")
	}

	preview, err = in.PreviewShutdown([]int{1, 0})
	if err != nil {
		t.Fatalf("preview two parallel pipes: %v", err)
	}
	if got := preview.Shutdown.EarliestArrivals[1]; got != 5 {
		t.Fatalf("closing indices 0 and 1 arrival=%d, want 5 via index 2", got)
	}
	preview, err = in.PreviewShutdown([]int{2, 1, 0})
	if err != nil {
		t.Fatalf("preview all parallels: %v", err)
	}
	if !preview.AllIntakesProtected || preview.Shutdown.Status != Contained {
		t.Fatalf("all parallel pipes closed: %+v", preview)
	}
}

func TestShutdownPreviewSelfLoopAndDeadlineBoundary(t *testing.T) {
	// Indices 0 and 1 are self-loops and may be selected, but they do not
	// participate in propagation. The real arrival is exactly at the deadline.
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 0, 1}, {1, 1, 1}, {0, 1, 5}},
		Releases: []Release{{0, 5}},
		Intakes:  []int{1},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	if _, _, err := in.Advance(6); err != nil {
		t.Fatalf("advance: %v", err)
	}
	preview, err := in.PreviewShutdown([]int{0, 1})
	if err != nil {
		t.Fatalf("self-loop preview: %v", err)
	}
	if got := preview.Shutdown.EarliestArrivals[1]; got != 10 {
		t.Fatalf("self-loops altered propagation: arrival=%d want 10", got)
	}
	if preview.Original.Status != Breached || preview.Shutdown.Status != Breached {
		t.Fatalf("arrival exactly at deadline must breach both plans: %+v", preview)
	}
	if len(preview.ArrivalChanges) != 0 {
		t.Fatalf("self-loop closures caused changes: %+v", preview.ArrivalChanges)
	}
}

func TestShutdownPreviewRejectsInvalidAndTerminal(t *testing.T) {
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 3,
	}
	in := mustIncident(t, spec)
	for _, idx := range []int{1, -1} {
		var ve *ValidationError
		if _, err := in.PreviewShutdown([]int{idx}); !errors.As(err, &ve) {
			t.Fatalf("index %d err=%v, want validation error", idx, err)
		}
	}
	var ve *ValidationError
	if _, err := in.PreviewShutdown([]int{0, 0}); !errors.As(err, &ve) {
		t.Fatalf("duplicate index err=%v, want validation error", err)
	}
	if _, _, err := in.Advance(3); err != nil {
		t.Fatalf("advance to terminal: %v", err)
	}
	if _, err := in.PreviewShutdown([]int{0}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("terminal preview err=%v, want incident_terminal", err)
	}
	if snap := in.Snapshot(); snap.CurrentMinute != 3 || snap.Status != Breached {
		t.Fatalf("preview mutated terminal snapshot: %+v", snap)
	}
}

// TestShutdownPreviewOracle compares the implementation against a direct
// minute-by-minute simulator on many small random graphs. The simulator treats
// a closure time as the linearization minute: departures before that minute
// remain in flight, while departures at or after that minute cannot enter the
// selected pipes.
func TestShutdownPreviewOracle(t *testing.T) {
	state := uint64(20260924)
	next := func(mod uint64) uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return (state >> 11) % mod
	}
	for iter := 0; iter < 300; iter++ {
		n := int(2 + next(5))
		pipeCount := int(next(8))
		pipes := make([]Pipe, 0, pipeCount)
		for len(pipes) < pipeCount {
			from := int(next(uint64(n)))
			to := int(next(uint64(n)))
			// Keep most arcs non-self-loop; self-loops are covered separately.
			if from == to && next(3) != 0 {
				continue
			}
			pipes = append(pipes, Pipe{From: from, To: to, Minutes: int64(1 + next(4))})
		}
		deadline := int64(8 + next(5))
		releaseNode := int(next(uint64(n)))
		releaseAt := int64(next(uint64(deadline + 1)))
		intake := int(next(uint64(n)))
		spec := Spec{
			N: n, Pipes: pipes,
			Releases: []Release{{Node: releaseNode, At: releaseAt}},
			Intakes:  []int{intake},
			Deadline: deadline,
		}
		in := mustIncident(t, spec)
		oracleOriginal := simulatePropagation(spec, nil, -1)
		for v := 0; v < n; v++ {
			want := int64(-1)
			if d, ok := oracleOriginal[v]; ok && d <= deadline {
				want = d
			}
			got := int64(-1)
			if in.dist[v] <= deadline {
				got = in.dist[v]
			}
			if got != want {
				t.Fatalf("iter %d original dist[%d]=%d, oracle=%d spec=%+v", iter, v, got, want, spec)
			}
		}

		for minute := int64(0); minute < deadline; minute++ {
			inAtMinute := mustIncident(t, spec)
			if _, _, err := inAtMinute.Advance(minute); err != nil {
				t.Fatalf("iter %d advance %d: %v", iter, minute, err)
			}
			if inAtMinute.Snapshot().Status.Terminal() {
				break
			}
			for pi := range pipes {
				closed := []int{pi}
				wantDist := simulatePropagation(spec, map[int]bool{pi: true}, minute)
				preview, err := inAtMinute.PreviewShutdown(closed)
				if err != nil {
					t.Fatalf("iter %d preview minute=%d pipe=%d: %v", iter, minute, pi, err)
				}
				if preview.EventMinute != minute {
					t.Fatalf("preview mixed event minute %d with request minute %d", preview.EventMinute, minute)
				}
				for v := 0; v < n; v++ {
					want, hasWant := wantDist[v]
					if hasWant && want > deadline {
						hasWant = false
					}
					got, hasGot := preview.Shutdown.EarliestArrivals[v]
					if hasWant != hasGot || (hasWant && got != want) {
						t.Fatalf("iter %d minute=%d close=%d node=%d got=(%d,%v) want=(%d,%v)\nspec=%+v\nchanges=%+v",
							iter, minute, pi, v, got, hasGot, want, hasWant, spec, preview.ArrivalChanges)
					}
				}
			}
		}
	}
}

func simulatePropagation(spec Spec, closed map[int]bool, shutdownAt int64) map[int]int64 {
	arrival := map[int]int64{}
	type packet struct{ node, at int64 }
	heap2 := make([]packet, 0)
	less := func(i, j int) bool { return heap2[i].at < heap2[j].at }
	push := func(p packet) {
		heap2 = append(heap2, p)
		for i := len(heap2) - 1; i > 0; {
			parent := (i - 1) / 2
			if less(parent, i) {
				break
			}
			heap2[parent], heap2[i] = heap2[i], heap2[parent]
			i = parent
		}
	}
	pop := func() packet {
		out := heap2[0]
		last := len(heap2) - 1
		heap2[0] = heap2[last]
		heap2 = heap2[:last]
		for i := 0; ; {
			left, right, best := 2*i+1, 2*i+2, i
			if left < len(heap2) && less(left, best) {
				best = left
			}
			if right < len(heap2) && less(right, best) {
				best = right
			}
			if best == i {
				break
			}
			heap2[i], heap2[best] = heap2[best], heap2[i]
			i = best
		}
		return out
	}
	for _, r := range spec.Releases {
		push(packet{int64(r.Node), r.At})
	}
	for len(heap2) > 0 {
		cur := pop()
		if cur.at > spec.Deadline {
			continue
		}
		if old, ok := arrival[int(cur.node)]; ok && old <= cur.at {
			continue
		}
		arrival[int(cur.node)] = cur.at
		for i, p := range spec.Pipes {
			if p.From != int(cur.node) || p.From == p.To {
				continue
			}
			if closed != nil && closed[i] && cur.at >= shutdownAt {
				continue
			}
			push(packet{int64(p.To), cur.at + p.Minutes})
		}
	}
	return arrival
}

func TestMaxInt64Unreachable(t *testing.T) {
	spec := Spec{N: 2, Pipes: []Pipe{{1, 0, 1}}, Releases: []Release{{0, 0}}, Intakes: []int{1}, Deadline: 3}
	in := mustIncident(t, spec)
	if in.dist[1] != math.MaxInt64 {
		t.Fatalf("unreachable dist=%d, want MaxInt64", in.dist[1])
	}
	_, snap, _ := in.Advance(3)
	if snap.Status != Contained {
		t.Fatalf("status=%s, want contained", snap.Status)
	}
	// Unreachable node never appears in snapshots.
	if _, ok := arrivals(snap)[1]; ok {
		t.Fatal("unreachable intake appeared in snapshot")
	}
}

// TestSegmentedIncrements checks that a multi-tick advance partitions the
// arrival set exactly with no omissions or duplicates.
func TestSegmentedIncrements(t *testing.T) {
	spec := Spec{
		N:        4,
		Pipes:    []Pipe{{0, 1, 1}, {1, 2, 1}, {2, 3, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{3},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	seen := map[int]int64{}
	for _, m := range []int64{0, 1, 2, 3} {
		ar, _, err := in.Advance(m)
		if err != nil {
			t.Fatalf("advance(%d): %v", m, err)
		}
		for _, a := range ar {
			if _, dup := seen[a.Node]; dup {
				t.Fatalf("node %d reported twice", a.Node)
			}
			seen[a.Node] = a.AtMinute
		}
	}
	if len(seen) != 4 {
		t.Fatalf("seen=%v, want all 4 nodes across segments", seen)
	}
	for v, want := range map[int]int64{0: 0, 1: 1, 2: 2, 3: 3} {
		if seen[v] != want {
			t.Fatalf("arrival of %d = %d, want %d", v, seen[v], want)
		}
	}
}

func BenchmarkDijkstraLarge(b *testing.B) {
	const n = MaxNodes
	const m = MaxPipes
	pipes := make([]Pipe, 0, m)
	state := uint64(20260917)
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state >> 11
	}
	for len(pipes) < m {
		u := int(next() % n)
		v := int(next() % n)
		pipes = append(pipes, Pipe{u, v, int64(1 + next()%1000)})
	}
	spec := Spec{N: n, Pipes: pipes, Releases: []Release{{0, 0}}, Intakes: []int{n - 1}, Deadline: MaxMinutes}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := NewIncident(spec); err != nil {
			b.Fatal(err)
		}
	}
}

func ExampleIncident() {
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 3}},
		Releases: []Release{{0, 2}},
		Intakes:  []int{1},
		Deadline: 10,
	}
	in, _ := NewIncident(spec)
	_, snap, _ := in.Advance(10)
	fmt.Println(snap.Status)
	// Output: breached
}
