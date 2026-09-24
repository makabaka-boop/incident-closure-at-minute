package incident

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Explicit shutdown-rehearsal scenarios
// ---------------------------------------------------------------------------

func mustAdvance(t *testing.T, in *Incident, minute int64) Snapshot {
	t.Helper()
	_, snap, err := in.Advance(minute)
	if err != nil {
		t.Fatalf("advance(%d): %v", minute, err)
	}
	return snap
}

func mustSimulate(t *testing.T, in *Incident, closed ...int64) Simulation {
	t.Helper()
	sim, err := in.SimulateShutdown(closed)
	if err != nil {
		t.Fatalf("simulate(%v): %v", closed, err)
	}
	return sim
}

func changeFor(sim Simulation, node int) *ArrivalChange {
	for i := range sim.Changes {
		if sim.Changes[i].Node == node {
			return &sim.Changes[i]
		}
	}
	return nil
}

func TestSimulateInTransitKeepsOriginalTraversal(t *testing.T) {
	// Chain 0 -5-> 1 -5-> 2, release 0@0, intake 2, deadline 20.
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 5}, {1, 2, 5}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 20,
	}

	// At minute 3 the pollution entered pipe 0 (0->1) at minute 0 and is
	// still in flight. Closing pipe 0 now cannot recall it: node 1 still
	// arrives at 5 and node 2 at 10, exactly like the baseline.
	in := mustIncident(t, spec)
	mustAdvance(t, in, 3)
	sim := mustSimulate(t, in, 0)
	if len(sim.Changes) != 0 {
		t.Fatalf("in-transit arrival was rewritten: changes=%+v", sim.Changes)
	}
	if sim.ShutdownStatus != Breached || sim.Protected {
		t.Fatalf("in-transit shutdown: status=%s protected=%v, want breached/false",
			sim.ShutdownStatus, sim.Protected)
	}
	want := map[int]int64{0: 0, 1: 5, 2: 10}
	if !reflect.DeepEqual(sim.ShutdownArrivals, want) {
		t.Fatalf("shutdown arrivals=%v, want %v", sim.ShutdownArrivals, want)
	}
	if sim.Snapshot.CurrentMinute != 3 || sim.Snapshot.Status != Propagating {
		t.Fatalf("rehearsal snapshot=%+v, want minute 3 propagating", sim.Snapshot)
	}

	// Closing pipe 0 before the clock starts blocks the departure at
	// minute 0 entirely: nothing downstream is ever reached.
	fresh := mustIncident(t, spec)
	sim2 := mustSimulate(t, fresh, 0)
	if sim2.ShutdownStatus != Contained || !sim2.Protected {
		t.Fatalf("pre-departure shutdown: status=%s protected=%v, want contained/true",
			sim2.ShutdownStatus, sim2.Protected)
	}
	if !reflect.DeepEqual(sim2.ShutdownArrivals, map[int]int64{0: 0}) {
		t.Fatalf("pre-departure shutdown arrivals=%v, want {0:0}", sim2.ShutdownArrivals)
	}
}

func TestSimulateDepartureExactlyAtCloseMinuteIsBlocked(t *testing.T) {
	// Chain 0 -2-> 1 -2-> 2, release 0@0, intake 2, deadline 10.
	// At minute 2 node 1 has just arrived; a departure along pipe 1 would
	// start exactly at the close minute and is therefore blocked.
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 2}, {1, 2, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	mustAdvance(t, in, 2)
	sim := mustSimulate(t, in, 1)

	if sim.BaselineStatus != Breached || sim.ShutdownStatus != Contained || !sim.Protected {
		t.Fatalf("statuses baseline=%s shutdown=%s protected=%v, want breached/contained/true",
			sim.BaselineStatus, sim.ShutdownStatus, sim.Protected)
	}
	if !reflect.DeepEqual(sim.ShutdownArrivals, map[int]int64{0: 0, 1: 2}) {
		t.Fatalf("shutdown arrivals=%v, want {0:0, 1:2}", sim.ShutdownArrivals)
	}
	c := changeFor(sim, 2)
	if c == nil || c.Baseline == nil || *c.Baseline != 4 || c.Shutdown != nil {
		t.Fatalf("change for node 2 = %+v, want baseline 4 -> null", c)
	}
	if len(sim.Changes) != 1 {
		t.Fatalf("changes=%+v, want exactly node 2", sim.Changes)
	}
	if len(sim.Intakes) != 1 || sim.Intakes[0].Node != 2 ||
		sim.Intakes[0].BaselineAt == nil || *sim.Intakes[0].BaselineAt != 4 ||
		sim.Intakes[0].ShutdownAt != nil ||
		!sim.Intakes[0].BaselineBreached || sim.Intakes[0].ShutdownBreached {
		t.Fatalf("intake conclusion=%+v, want baseline 4 breached / shutdown safe", sim.Intakes)
	}
}

func TestSimulateParallelPipesAreIndependent(t *testing.T) {
	// Two parallel pipes 0->1 (idx 0: 2 min, idx 1: 5 min) and 1->2 (idx 2).
	// Release 0@0, intake 2, deadline 6.
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 2}, {0, 1, 5}, {1, 2, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 6,
	}
	in := mustIncident(t, spec)
	// Rehearse at minute 0: departures from node 0 start at minute 0, so a
	// closure now still blocks them (at minute 1 the fast pipe's payload
	// would already be in flight and could not be recalled).
	mustAdvance(t, in, 0)

	// Closing only the fast parallel pipe reroutes onto the slow one:
	// 1@5, 2@7 — past the deadline, so the intake is protected.
	sim := mustSimulate(t, in, 0)
	if !reflect.DeepEqual(sim.ShutdownArrivals, map[int]int64{0: 0, 1: 5}) {
		t.Fatalf("shutdown arrivals=%v, want {0:0, 1:5} (2@7 is past the deadline)", sim.ShutdownArrivals)
	}
	c1, c2 := changeFor(sim, 1), changeFor(sim, 2)
	if c1 == nil || *c1.Baseline != 2 || *c1.Shutdown != 5 {
		t.Fatalf("change node1=%+v, want 2 -> 5", c1)
	}
	if c2 == nil || *c2.Baseline != 4 || c2.Shutdown != nil {
		t.Fatalf("change node2=%+v, want 4 -> null", c2)
	}
	if sim.ShutdownStatus != Contained || !sim.Protected {
		t.Fatalf("shutdown status=%s protected=%v, want contained/true",
			sim.ShutdownStatus, sim.Protected)
	}

	// Closing only the slow pipe changes nothing: the fast pipe still
	// carries the baseline schedule.
	sim2 := mustSimulate(t, in, 1)
	if len(sim2.Changes) != 0 {
		t.Fatalf("closing the unused parallel pipe changed arrivals: %+v", sim2.Changes)
	}

	// Closing both parallel pipes isolates node 1 entirely.
	sim3 := mustSimulate(t, in, 0, 1)
	if !reflect.DeepEqual(sim3.ShutdownArrivals, map[int]int64{0: 0}) {
		t.Fatalf("shutdown arrivals=%v, want {0:0}", sim3.ShutdownArrivals)
	}
}

func TestSimulateSelfLoopClosureIsHarmless(t *testing.T) {
	// Self-loops never propagate; closing one must not change anything.
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 0, 1}, {0, 1, 2}, {1, 1, 3}, {1, 2, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	mustAdvance(t, in, 1)
	sim := mustSimulate(t, in, 0, 2)
	if len(sim.Changes) != 0 {
		t.Fatalf("closing self-loops changed arrivals: %+v", sim.Changes)
	}
	if sim.ShutdownStatus != Breached {
		t.Fatalf("status=%s, want breached (self-loop closure is a no-op)", sim.ShutdownStatus)
	}
}

func TestSimulateFutureReleaseStillSeeds(t *testing.T) {
	// Pipe 0: 0->1 in 5; pipe 1: 2->1 in 1. Releases 0@2 and 2@8.
	// Intake 1, deadline 20. At minute 1 both releases are still in the
	// future; closing pipe 0 blocks the departure from node 0 at minute 2,
	// but the future release at node 2 still seeds normally and reaches
	// node 1 at 9 (baseline: 7).
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 5}, {2, 1, 1}},
		Releases: []Release{{0, 2}, {2, 8}},
		Intakes:  []int{1},
		Deadline: 20,
	}
	in := mustIncident(t, spec)
	mustAdvance(t, in, 1)
	sim := mustSimulate(t, in, 0)

	if !reflect.DeepEqual(sim.ShutdownArrivals, map[int]int64{0: 2, 1: 9, 2: 8}) {
		t.Fatalf("shutdown arrivals=%v, want {0:2, 1:9, 2:8}", sim.ShutdownArrivals)
	}
	c := changeFor(sim, 1)
	if c == nil || *c.Baseline != 7 || *c.Shutdown != 9 {
		t.Fatalf("change node1=%+v, want 7 -> 9", c)
	}
	if sim.ShutdownStatus != Breached || sim.Protected {
		t.Fatalf("status=%s protected=%v, want breached/false (future release still reaches)",
			sim.ShutdownStatus, sim.Protected)
	}
}

func TestSimulateDeadlineBoundary(t *testing.T) {
	// Pipe 0: 0->1 direct in 8; pipes 1+2: 0->2->1 in 3+7=10, exactly the
	// deadline. Release 0@0, intake 1, deadline 10.
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 8}, {0, 2, 3}, {2, 1, 7}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 10,
	}

	// Close the direct pipe at minute 0 (before the departure): the detour
	// arrives exactly at the deadline, which still counts as breached.
	in := mustIncident(t, spec)
	mustAdvance(t, in, 0)
	sim := mustSimulate(t, in, 0)
	if sim.ShutdownArrivals[1] != 10 || sim.ShutdownStatus != Breached || sim.Protected {
		t.Fatalf("arrival at exactly the deadline: arrivals=%v status=%s protected=%v, want 10/breached/false",
			sim.ShutdownArrivals, sim.ShutdownStatus, sim.Protected)
	}

	// Same shape with an 11-minute detour: one minute past the deadline,
	// so the intake is protected and node 1 drops out of the shutdown map.
	spec2 := spec
	spec2.Pipes = []Pipe{{0, 1, 8}, {0, 2, 3}, {2, 1, 8}}
	in2 := mustIncident(t, spec2)
	mustAdvance(t, in2, 0)
	sim2 := mustSimulate(t, in2, 0)
	if sim2.ShutdownStatus != Contained || !sim2.Protected {
		t.Fatalf("detour past deadline: status=%s protected=%v, want contained/true",
			sim2.ShutdownStatus, sim2.Protected)
	}
	if _, ok := sim2.ShutdownArrivals[1]; ok {
		t.Fatalf("node 1 arrival %d leaked past the deadline", sim2.ShutdownArrivals[1])
	}
	c := changeFor(sim2, 1)
	if c == nil || *c.Baseline != 8 || c.Shutdown != nil {
		t.Fatalf("change node1=%+v, want 8 -> null", c)
	}
}

func TestSimulateRejectsBadIndices(t *testing.T) {
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 1}, {1, 2, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	mustAdvance(t, in, 1)
	for _, closed := range [][]int64{{0, 0}, {1, 0, 1}, {-1}, {2}, {100}} {
		if _, err := in.SimulateShutdown(closed); err == nil {
			t.Fatalf("simulate(%v) unexpectedly succeeded", closed)
		} else {
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("simulate(%v) error %v is not a *ValidationError", closed, err)
			}
		}
	}
	// Rejections never mutate the committed snapshot.
	snap := in.Snapshot()
	if snap.CurrentMinute != 1 || snap.Status != Propagating {
		t.Fatalf("snapshot changed after rejected simulations: %+v", snap)
	}
}

func TestSimulateRejectsTerminalIncident(t *testing.T) {
	spec := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 1}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 5,
	}
	in := mustIncident(t, spec)
	mustAdvance(t, in, 5) // breached at minute 1
	if _, err := in.SimulateShutdown(nil); !errors.Is(err, ErrTerminal) {
		t.Fatalf("simulate on breached incident: err=%v, want incident_terminal", err)
	}

	spec2 := Spec{
		N:        2,
		Pipes:    []Pipe{{0, 1, 9}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{1},
		Deadline: 5,
	}
	in2 := mustIncident(t, spec2)
	mustAdvance(t, in2, 5) // contained at the deadline
	if _, err := in2.SimulateShutdown([]int64{0}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("simulate on contained incident: err=%v, want incident_terminal", err)
	}
}

func TestSimulateIsReadOnly(t *testing.T) {
	spec := Spec{
		N:        3,
		Pipes:    []Pipe{{0, 1, 2}, {1, 2, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{2},
		Deadline: 10,
	}
	in := mustIncident(t, spec)
	mustAdvance(t, in, 1)
	before := in.Snapshot()
	for i := 0; i < 3; i++ {
		mustSimulate(t, in, 0, 1)
	}
	after := in.Snapshot()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("simulation mutated the snapshot: %+v -> %+v", before, after)
	}
	// The original advance interface is unaffected: the next tick still
	// reports exactly the arrivals between minute 1 and 3.
	ar, snap, err := in.Advance(3)
	if err != nil {
		t.Fatalf("advance after simulations: %v", err)
	}
	if len(ar) != 1 || ar[0] != (Arrival{Node: 1, AtMinute: 2}) {
		t.Fatalf("new arrivals after simulation=%v, want [{1 2}]", ar)
	}
	if snap.CurrentMinute != 3 || snap.Status != Propagating {
		t.Fatalf("snapshot after advance=%+v", snap)
	}
}

// TestSimulateConcurrentSnapshotsAreConsistent races rehearsals against
// advances. Every rehearsal response must be internally consistent with
// exactly one committed snapshot: the recorded minute, the recorded arrivals
// and the recomputed changes must all line up, never mixing two states.
func TestSimulateConcurrentSnapshotsAreConsistent(t *testing.T) {
	spec := Spec{
		N:        6,
		Pipes:    []Pipe{{0, 1, 2}, {1, 2, 2}, {2, 3, 2}, {3, 4, 2}, {4, 5, 2}},
		Releases: []Release{{0, 0}},
		Intakes:  []int{5},
		Deadline: 20,
	}
	in := mustIncident(t, spec)
	var wg sync.WaitGroup
	for m := int64(1); m <= 9; m++ {
		wg.Add(1)
		go func(target int64) {
			defer wg.Done()
			_, _, _ = in.Advance(target) // conflicts are expected and fine
		}(m)
	}
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sim, err := in.SimulateShutdown([]int64{2})
			if err != nil {
				return // terminal state reached mid-race: a legal rejection
			}
			cur := sim.Snapshot.CurrentMinute
			for node, d := range sim.Snapshot.EarliestArrivals {
				if d > cur {
					t.Errorf("snapshot records node %d at %d, ahead of its minute %d", node, d, cur)
				}
				if sim.BaselineArrivals[node] != d {
					t.Errorf("baseline arrival for node %d = %d, snapshot records %d",
						node, sim.BaselineArrivals[node], d)
				}
			}
			for _, c := range sim.Changes {
				if c.Baseline != nil && *c.Baseline != sim.BaselineArrivals[c.Node] {
					t.Errorf("change baseline for node %d inconsistent with baseline map", c.Node)
				}
				if c.Shutdown != nil && *c.Shutdown != sim.ShutdownArrivals[c.Node] {
					t.Errorf("change shutdown for node %d inconsistent with shutdown map", c.Node)
				}
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Minute-by-minute oracle cross-check on random small graphs
// ---------------------------------------------------------------------------

// oracleShutdownArrivals simulates the shutdown minute by minute (it never
// runs Dijkstra): at every minute from current to the deadline it applies
// releases and scheduled pipe arrivals, then lets every contaminated node
// depart along the still-open pipes. It returns the earliest arrival minute
// per node, tracking only arrivals at or before the deadline (MaxInt64
// otherwise). This is the independent reference for shutdownArrivals.
func oracleShutdownArrivals(spec Spec, base []int64, current int64, closed []bool) []int64 {
	inf := int64(math.MaxInt64)
	arr := make([]int64, spec.N)
	for i := range arr {
		arr[i] = inf
	}
	type traversal struct {
		to int
		at int64
	}
	pending := make([]traversal, 0)

	// Pollution already present at or before the shutdown minute.
	for v, d := range base {
		if d <= current {
			arr[v] = d
		}
	}
	// Traversals that entered any pipe strictly before the shutdown minute
	// are already scheduled with their original traversal time; closed
	// pipes only refuse departures from the shutdown minute on.
	for _, p := range spec.Pipes {
		if p.From == p.To {
			continue
		}
		dep := base[p.From]
		if dep < current && dep+p.Minutes > current && dep+p.Minutes <= spec.Deadline {
			pending = append(pending, traversal{p.To, dep + p.Minutes})
		}
	}

	for tm := current; tm <= spec.Deadline; tm++ {
		// Releases happening exactly now.
		for _, r := range spec.Releases {
			if r.At == tm && tm < arr[r.Node] {
				arr[r.Node] = tm
			}
		}
		// Pipe traversals completing exactly now.
		for _, tr := range pending {
			if tr.at == tm && tm < arr[tr.to] {
				arr[tr.to] = tm
			}
		}
		// Every node whose earliest arrival is exactly now departs along
		// each open pipe (a closed pipe refuses the departure because it
		// starts at or after the shutdown minute).
		for v := 0; v < spec.N; v++ {
			if arr[v] != tm {
				continue
			}
			for i, p := range spec.Pipes {
				if p.From != v || p.From == p.To || closed[i] {
					continue
				}
				at := tm + p.Minutes
				if at <= spec.Deadline {
					pending = append(pending, traversal{p.To, at})
				}
			}
		}
	}
	return arr
}

func TestSimulateAgainstMinuteOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260924))
	const wantCases = 400
	tested := 0
	for tested < wantCases {
		n := 2 + rng.Intn(7)
		m := rng.Intn(13)
		pipes := make([]Pipe, m)
		for i := range pipes {
			pipes[i] = Pipe{
				From:    rng.Intn(n),
				To:      rng.Intn(n),
				Minutes: int64(1 + rng.Intn(6)),
			}
		}
		releases := make([]Release, 1+rng.Intn(3))
		for i := range releases {
			releases[i] = Release{Node: rng.Intn(n), At: int64(rng.Intn(11))}
		}
		deadline := int64(5 + rng.Intn(21))
		for i := range releases {
			if releases[i].At > deadline {
				releases[i].At = deadline
			}
		}
		intakes := []int{rng.Intn(n)}
		spec := Spec{N: n, Pipes: pipes, Releases: releases, Intakes: intakes, Deadline: deadline}
		in, err := NewIncident(spec)
		if err != nil {
			t.Fatalf("random spec rejected: %v", err)
		}

		// Pick a rehearsal minute that keeps the event non-terminal:
		// strictly before the deadline and strictly before any intake is
		// reached.
		limit := deadline
		for _, id := range in.intakes {
			if in.dist[id] < limit {
				limit = in.dist[id]
			}
		}
		if limit < 1 {
			continue // every legal minute is already terminal; resample
		}
		current := int64(rng.Intn(int(limit)))
		if current > 0 {
			if _, _, err := in.Advance(current); err != nil {
				t.Fatalf("advance(%d): %v", current, err)
			}
		}

		closedFlags := make([]bool, m)
		closedList := make([]int64, 0)
		for i := range pipes {
			if rng.Intn(4) == 0 {
				closedFlags[i] = true
				closedList = append(closedList, int64(i))
			}
		}

		sim, err := in.SimulateShutdown(closedList)
		if err != nil {
			t.Fatalf("simulate(%v) at minute %d: %v", closedList, current, err)
		}
		want := oracleShutdownArrivals(spec, in.dist, current, closedFlags)

		// Recompute the full shutdown distances through the same entry
		// point the rehearsal uses, then compare against the oracle.
		got := shutdownArrivals(spec, in.dist, current, closedFlags)
		for v := 0; v < n; v++ {
			switch {
			case want[v] <= deadline:
				if got[v] != want[v] {
					t.Fatalf("case %d node %d: got %d, oracle %d\nspec=%+v\nclosed=%v current=%d",
						tested, v, got[v], want[v], spec, closedList, current)
				}
			default:
				if got[v] <= deadline {
					t.Fatalf("case %d node %d: got %d within deadline, oracle unreachable\nspec=%+v\nclosed=%v current=%d",
						tested, v, got[v], spec, closedList, current)
				}
			}
		}

		// The rehearsal's public result must agree with the oracle too.
		for v := 0; v < n; v++ {
			gotV, ok := sim.ShutdownArrivals[v]
			if want[v] <= deadline {
				if !ok || gotV != want[v] {
					t.Fatalf("case %d shutdown arrivals node %d = %d(ok=%v), oracle %d",
						tested, v, gotV, ok, want[v])
				}
			} else if ok {
				t.Fatalf("case %d node %d present at %d, oracle has no arrival by the deadline",
					tested, v, gotV)
			}
		}
		// Monotonicity: closing pipes can never make any arrival earlier.
		for v := 0; v < n; v++ {
			if got[v] < in.dist[v] {
				t.Fatalf("case %d node %d: shutdown arrival %d earlier than baseline %d",
					tested, v, got[v], in.dist[v])
			}
		}
		// The recorded snapshot must match the rehearsal minute.
		if sim.Snapshot.CurrentMinute != current {
			t.Fatalf("case %d snapshot minute %d, want %d", tested, sim.Snapshot.CurrentMinute, current)
		}
		tested++
	}
}
