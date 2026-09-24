// Package incident models a contamination propagation event.
//
// Pollution is released at specified nodes at specified minutes and travels
// along directed pipes, each of which takes a whole number of minutes to
// traverse. The earliest minute at which contamination can reach every node
// is the multi-source shortest-path distance: every release seeds its node
// with its release minute and every directed pipe relaxes arrival by its
// traversal time. Parallel pipes are both traversable, so the shorter route
// wins naturally; self-loops are never traversed and a pipe u -> v only ever
// carries contamination forward, so reverse edges never produce upstream
// propagation.
//
// An event advances monotonically in minute ticks. Its status starts as
// "scheduled", becomes "propagating" once the first release has happened,
// "breached" if a key intake is reached at or before the deadline, and
// "contained" once the clock reaches the deadline without any intake being
// reached. Advancing to the current minute is an idempotent retry that
// reports no new arrivals and the same snapshot; going backwards, past the
// deadline, or advancing a terminal event is rejected and never mutates the
// event.
//
// A non-terminal event also supports read-only shutdown rehearsals
// (SimulateShutdown): against one committed snapshot, the rehearsal computes
// how immediately closing a set of pipes (by creation-time index) would
// change earliest arrivals within the deadline and whether the key intakes
// would stay dry — without ever mutating the event.
package incident

import (
	"container/heap"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

const (
	// MaxNodes and MaxPipes bound the accepted topology.
	MaxNodes = 20000
	MaxPipes = 100000
	// MaxMinutes bounds traversal times, release minutes and the deadline.
	MaxMinutes = int64(1_000_000_000)
)

// Status is the lifecycle state of an incident.
type Status string

const (
	// Scheduled means the clock has not reached the first release yet.
	Scheduled Status = "scheduled"
	// Propagating means at least one release has happened and no terminal
	// condition has been reached.
	Propagating Status = "propagating"
	// Breached is terminal: contamination reached a key intake at or before
	// the deadline.
	Breached Status = "breached"
	// Contained is terminal: the deadline elapsed without any key intake
	// being reached.
	Contained Status = "contained"
)

// Terminal reports whether the status can never change again.
func (s Status) Terminal() bool { return s == Breached || s == Contained }

// Pipe is a directed pipe whose traversal takes Minutes minutes.
type Pipe struct {
	From    int
	To      int
	Minutes int64
}

// Release places contamination at Node starting at minute At.
type Release struct {
	Node int
	At   int64
}

// Spec fully describes the topology and schedule of an incident.
type Spec struct {
	N        int
	Pipes    []Pipe
	Releases []Release
	Intakes  []int
	Deadline int64
}

// Arrival records that contamination first reached Node at minute AtMinute.
type Arrival struct {
	Node     int   `json:"node"`
	AtMinute int64 `json:"at_minute"`
}

// Snapshot is the immutable-at-return-time view of an incident: the current
// clock minute, the status and the earliest arrival minute of every node
// reached so far.
type Snapshot struct {
	CurrentMinute    int64
	Status           Status
	EarliestArrivals map[int]int64
}

// ArrivalChange describes how one node's earliest arrival minute (within the
// deadline) moves between the baseline plan and the shutdown plan. A nil
// Baseline or Shutdown means the plan does not reach the node by the
// deadline at all.
type ArrivalChange struct {
	Node     int
	Baseline *int64
	Shutdown *int64
}

// IntakeSimulation is the per-intake conclusion of a shutdown rehearsal:
// when each plan first reaches the intake (nil = not by the deadline) and
// whether that counts as a breach.
type IntakeSimulation struct {
	Node             int
	BaselineAt       *int64
	ShutdownAt       *int64
	BaselineBreached bool
	ShutdownBreached bool
}

// Simulation is the read-only result of rehearsing an immediate pipe
// shutdown against one committed incident snapshot. It records the snapshot
// the rehearsal was taken against (clock minute, status, nodes already
// reached), both plans' earliest arrivals within the deadline, the per-node
// changes and the intake conclusions. Nothing in a Simulation ever mutates
// the incident.
type Simulation struct {
	// Snapshot is the committed incident state the rehearsal is based on.
	Snapshot Snapshot
	// ClosedPipes is the validated, sorted list of pipe indices that were
	// hypothetically closed.
	ClosedPipes []int
	// BaselineArrivals and ShutdownArrivals are the earliest arrival
	// minutes at or before the deadline under the original plan and under
	// the shutdown plan (unreached nodes are absent).
	BaselineArrivals map[int]int64
	ShutdownArrivals map[int]int64
	// Changes lists every node whose within-deadline arrival differs
	// between the two plans, ordered by node id.
	Changes []ArrivalChange
	// Intakes lists the per-intake conclusions, ordered by node id.
	Intakes []IntakeSimulation
	// BaselineStatus and ShutdownStatus are the intake conclusions of each
	// plan: Breached if any intake is reached by the deadline, Contained
	// otherwise.
	BaselineStatus Status
	ShutdownStatus Status
	// Protected reports whether the shutdown plan keeps every intake
	// unreached until the deadline.
	Protected bool
}

// ValidationError marks a spec that must never be accepted; the HTTP layer
// maps it to a stable 422 response.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// Advance-time conflict codes, mapped by the HTTP layer to stable 409
// responses.
var (
	// ErrClockRegression is returned when the target minute is behind the
	// committed clock.
	ErrClockRegression = errors.New("clock_regression")
	// ErrPastDeadline is returned when the target minute exceeds the
	// deadline.
	ErrPastDeadline = errors.New("past_deadline")
	// ErrTerminal is returned when an advance is requested on an event that
	// has already reached a terminal status.
	ErrTerminal = errors.New("incident_terminal")
)

// ConflictError is a rejected advance: it matches one of the sentinel codes
// for errors.Is while carrying a human-readable detail.
type ConflictError struct {
	code   error
	Detail string
}

func (e *ConflictError) Error() string { return e.code.Error() + ": " + e.Detail }

// Is makes errors.Is(err, ErrClockRegression|ErrPastDeadline|ErrTerminal)
// work without unwrap.
func (e *ConflictError) Is(target error) bool { return target == e.code }

func conflict(code error, detail string) error {
	return &ConflictError{code: code, Detail: detail}
}

// Validate checks every constraint of the spec. It returns a
// *ValidationError describing the first violation, or nil.
func Validate(s Spec) error {
	if s.N < 2 || s.N > MaxNodes {
		return &ValidationError{fmt.Sprintf("n must satisfy 2 <= n <= %d, got %d", MaxNodes, s.N)}
	}
	if s.Deadline < 0 || s.Deadline > MaxMinutes {
		return &ValidationError{fmt.Sprintf("deadline must satisfy 0 <= deadline <= %d, got %d", MaxMinutes, s.Deadline)}
	}
	if len(s.Pipes) > MaxPipes {
		return &ValidationError{fmt.Sprintf("at most %d pipes allowed, got %d", MaxPipes, len(s.Pipes))}
	}
	for i, p := range s.Pipes {
		if p.From < 0 || p.From >= s.N {
			return &ValidationError{fmt.Sprintf("pipes[%d].from=%d is out of range [0,%d)", i, p.From, s.N)}
		}
		if p.To < 0 || p.To >= s.N {
			return &ValidationError{fmt.Sprintf("pipes[%d].to=%d is out of range [0,%d)", i, p.To, s.N)}
		}
		if p.Minutes < 1 || p.Minutes > MaxMinutes {
			return &ValidationError{fmt.Sprintf("pipes[%d].minutes=%d must satisfy 1 <= minutes <= %d", i, p.Minutes, MaxMinutes)}
		}
	}
	if len(s.Releases) == 0 {
		return &ValidationError{"releases must be a non-empty array of {node, at} objects"}
	}
	for i, r := range s.Releases {
		if r.Node < 0 || r.Node >= s.N {
			return &ValidationError{fmt.Sprintf("releases[%d].node=%d is out of range [0,%d)", i, r.Node, s.N)}
		}
		if r.At < 0 || r.At > s.Deadline {
			return &ValidationError{fmt.Sprintf("releases[%d].at=%d must satisfy 0 <= at <= deadline %d", i, r.At, s.Deadline)}
		}
	}
	if len(s.Intakes) == 0 {
		return &ValidationError{"intakes must be a non-empty array of node ids"}
	}
	for i, id := range s.Intakes {
		if id < 0 || id >= s.N {
			return &ValidationError{fmt.Sprintf("intakes[%d]=%d is out of range [0,%d)", i, id, s.N)}
		}
	}
	return nil
}

// adjEdge is a forward traversal arc. Only forward arcs are ever built.
type adjEdge struct {
	to int
	w  int64
}

// pqItem is a priority-queue candidate: node v at tentative distance d.
type pqItem struct {
	v int
	d int64
}

type pq []pqItem

func (p pq) Len() int           { return len(p) }
func (p pq) Less(i, j int) bool { return p[i].d < p[j].d }
func (p pq) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x any)        { *p = append(*p, x.(pqItem)) }
func (p *pq) Pop() any {
	old := *p
	last := old[len(old)-1]
	*p = old[:len(old)-1]
	return last
}

// earliestArrivals runs the multi-source Dijkstra relaxation. Each release
// seeds its node with its release minute (the earliest seed at a node wins),
// then every directed pipe relaxes arrival by traversal time. Self-loops are
// not inserted (they can never move contamination anywhere) and there are no
// reverse arcs, so neither can produce propagation. It returns the earliest
// arrival minute per node (math.MaxInt64 when unreachable) and the minute of
// the first release.
func earliestArrivals(s Spec) ([]int64, int64) {
	inf := int64(math.MaxInt64)
	dist := make([]int64, s.N)
	for i := range dist {
		dist[i] = inf
	}
	adj := make([][]adjEdge, s.N)
	for _, p := range s.Pipes {
		if p.From == p.To {
			continue // a self-loop can never carry contamination to a new node
		}
		adj[p.From] = append(adj[p.From], adjEdge{p.To, p.Minutes})
	}
	first := inf
	queue := make(pq, 0, len(s.Releases))
	for _, r := range s.Releases {
		if r.At < first {
			first = r.At
		}
		if r.At < dist[r.Node] {
			dist[r.Node] = r.At
		}
	}
	for v := 0; v < s.N; v++ {
		if dist[v] != inf {
			queue = append(queue, pqItem{v, dist[v]})
		}
	}
	heap.Init(&queue)
	for queue.Len() > 0 {
		cur := heap.Pop(&queue).(pqItem)
		if cur.d != dist[cur.v] {
			continue // stale queue entry after a better relaxation
		}
		for _, e := range adj[cur.v] {
			nd := cur.d + e.w
			if nd < dist[e.to] {
				dist[e.to] = nd
				heap.Push(&queue, pqItem{e.to, nd})
			}
		}
	}
	return dist, first
}

// Incident is a stored event. Its methods are safe for concurrent use: every
// advance holds the incident lock while it validates the clock, computes the
// arrival increment and commits the clock and status together, so concurrent
// requests take effect one at a time in a strictly monotonic order.
type Incident struct {
	id       string
	spec     Spec
	dist     []int64 // static earliest arrival per node
	isIntake []bool
	intakes  []int // de-duplicated intake nodes
	first    int64 // minute of the first release

	mu      sync.Mutex
	started bool   // false until the first successful advance
	current int64  // committed clock minute (0 before start)
	status  Status // committed status
}

// NewIncident validates the spec, pre-computes all earliest arrivals and
// returns the event in its initial scheduled state.
func NewIncident(spec Spec) (*Incident, error) {
	if err := Validate(spec); err != nil {
		return nil, err
	}
	dist, first := earliestArrivals(spec)
	isIntake := make([]bool, spec.N)
	intakes := make([]int, 0, len(spec.Intakes))
	for _, id := range spec.Intakes {
		if !isIntake[id] {
			isIntake[id] = true
			intakes = append(intakes, id)
		}
	}
	return &Incident{
		spec:     spec,
		dist:     dist,
		isIntake: isIntake,
		intakes:  intakes,
		first:    first,
		current:  0,
		status:   Scheduled,
	}, nil
}

// ID returns the assigned identifier.
func (in *Incident) ID() string { return in.id }

// Deadline returns the cutoff minute.
func (in *Incident) Deadline() int64 { return in.spec.Deadline }

// Snapshot returns the current snapshot under the incident lock.
func (in *Incident) Snapshot() Snapshot {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.snapshotLocked()
}

func (in *Incident) snapshotLocked() Snapshot {
	arrivals := make(map[int]int64)
	if in.started {
		for v, d := range in.dist {
			if d <= in.current {
				arrivals[v] = d
			}
		}
	}
	return Snapshot{CurrentMinute: in.current, Status: in.status, EarliestArrivals: arrivals}
}

// statusAt computes the status the event has once the clock is at t.
func (in *Incident) statusAt(t int64) Status {
	for _, id := range in.intakes {
		if in.dist[id] <= t { // reached at or before the cutoff minute
			return Breached
		}
	}
	if t >= in.spec.Deadline {
		return Contained
	}
	if t >= in.first {
		return Propagating
	}
	return Scheduled
}

// Advance validates the target minute and, if legal, atomically commits the
// new clock, the arrivals newly revealed by this tick and the resulting
// status. It returns the newly arrived nodes (sorted by minute then node),
// the fresh snapshot, and nil. A same-minute retry returns an empty increment
// and the identical snapshot. Every rejection leaves the event untouched.
func (in *Incident) Advance(target int64) ([]Arrival, Snapshot, error) {
	in.mu.Lock()
	defer in.mu.Unlock()

	// Idempotent retry: the committed minute is reported again with no
	// increment and an identical snapshot, even after termination.
	if in.started && target == in.current {
		return []Arrival{}, in.snapshotLocked(), nil
	}
	if (in.started && target < in.current) || (!in.started && target < 0) {
		return nil, Snapshot{}, conflict(ErrClockRegression,
			fmt.Sprintf("target minute %d is behind the committed clock at minute %d", target, in.current))
	}
	if in.status.Terminal() {
		return nil, Snapshot{}, conflict(ErrTerminal,
			fmt.Sprintf("incident already reached terminal status %q", in.status))
	}
	if target > in.spec.Deadline {
		return nil, Snapshot{}, conflict(ErrPastDeadline,
			fmt.Sprintf("target minute %d exceeds the deadline %d", target, in.spec.Deadline))
	}

	// Commit section: compute the increment from static arrival times, then
	// publish the clock and status together while still holding the lock.
	lower := in.current
	if !in.started {
		lower = -1 // the first tick reveals every arrival at or before target
	}
	newArrivals := make([]Arrival, 0)
	for v, d := range in.dist {
		if d > lower && d <= target {
			newArrivals = append(newArrivals, Arrival{Node: v, AtMinute: d})
		}
	}
	sort.Slice(newArrivals, func(i, j int) bool {
		if newArrivals[i].AtMinute != newArrivals[j].AtMinute {
			return newArrivals[i].AtMinute < newArrivals[j].AtMinute
		}
		return newArrivals[i].Node < newArrivals[j].Node
	})

	in.started = true
	in.current = target
	in.status = in.statusAt(target)
	return newArrivals, in.snapshotLocked(), nil
}

// SimulateShutdown runs a read-only rehearsal of immediately closing the
// pipes with the given creation-time pipe indices, evaluated against the
// incident's committed snapshot. Closing a pipe only stops traversals that
// would start at the snapshot minute or later: pollution that entered a
// closed pipe strictly earlier keeps travelling with its original traversal
// time and arrives as before. Releases at or after the snapshot minute seed
// normally; parallel pipes are handled by their own indices and closing one
// never closes the others.
//
// Repeated or out-of-range pipe indices return a *ValidationError, and a
// terminal event returns ErrTerminal; every rejection leaves the incident
// untouched. The incident is never mutated by a successful rehearsal either.
func (in *Incident) SimulateShutdown(closedIndices []int64) (Simulation, error) {
	closed := make([]bool, len(in.spec.Pipes))
	firstUse := make(map[int]int, len(closedIndices))
	sorted := make([]int, 0, len(closedIndices))
	for i, raw := range closedIndices {
		if raw < 0 || raw >= int64(len(in.spec.Pipes)) {
			return Simulation{}, &ValidationError{fmt.Sprintf(
				"closed_pipes[%d]=%d is out of range [0,%d)", i, raw, len(in.spec.Pipes))}
		}
		idx := int(raw)
		if closed[idx] {
			return Simulation{}, &ValidationError{fmt.Sprintf(
				"closed_pipes[%d]=%d duplicates closed_pipes[%d]", i, idx, firstUse[idx])}
		}
		closed[idx] = true
		firstUse[idx] = i
		sorted = append(sorted, idx)
	}
	sort.Ints(sorted)

	// Capture the committed snapshot under the incident lock so a rehearsal
	// can never combine two different advancing states. The read-only
	// computation itself runs outside the lock, over immutable spec/static
	// distance data and local copies.
	in.mu.Lock()
	if in.status.Terminal() {
		status := in.status
		in.mu.Unlock()
		return Simulation{}, conflict(ErrTerminal,
			fmt.Sprintf("incident already reached terminal status %q", status))
	}
	snap := in.snapshotLocked()
	current := in.current
	in.mu.Unlock()

	simDist := shutdownArrivals(in.spec, in.dist, current, closed)
	baseline := arrivalsAtOrBefore(in.dist, in.spec.Deadline)
	shutdown := arrivalsAtOrBefore(simDist, in.spec.Deadline)

	changes := make([]ArrivalChange, 0)
	for v := 0; v < in.spec.N; v++ {
		b, bok := baseline[v]
		s, sok := shutdown[v]
		if bok && sok && b == s {
			continue
		}
		change := ArrivalChange{Node: v}
		if bok {
			bv := b
			change.Baseline = &bv
		}
		if sok {
			sv := s
			change.Shutdown = &sv
		}
		changes = append(changes, change)
	}

	intakes := append([]int(nil), in.intakes...)
	sort.Ints(intakes)
	results := make([]IntakeSimulation, 0, len(intakes))
	anyShutdownBreach := false
	for _, id := range intakes {
		r := IntakeSimulation{Node: id}
		if d := in.dist[id]; d <= in.spec.Deadline {
			dv := d
			r.BaselineAt = &dv
			r.BaselineBreached = true
		}
		if d := simDist[id]; d <= in.spec.Deadline {
			dv := d
			r.ShutdownAt = &dv
			r.ShutdownBreached = true
			anyShutdownBreach = true
		}
		results = append(results, r)
	}

	return Simulation{
		Snapshot:         snap,
		ClosedPipes:      sorted,
		BaselineArrivals: baseline,
		ShutdownArrivals: shutdown,
		Changes:          changes,
		Intakes:          results,
		BaselineStatus:   statusOutcome(in.dist, in.intakes, in.spec.Deadline),
		ShutdownStatus:   statusOutcome(simDist, in.intakes, in.spec.Deadline),
		Protected:        !anyShutdownBreach,
	}, nil
}

// statusOutcome reports the deadline conclusion for one plan: Breached if any
// intake is reached at or before the deadline, Contained otherwise.
func statusOutcome(dist []int64, intakes []int, deadline int64) Status {
	for _, id := range intakes {
		if dist[id] <= deadline {
			return Breached
		}
	}
	return Contained
}

// arrivalsAtOrBefore returns the arrival entries with d <= cutoff. Entries
// with d == math.MaxInt64 (unreachable) never satisfy the bound because the
// deadline itself is capped at MaxMinutes.
func arrivalsAtOrBefore(dist []int64, cutoff int64) map[int]int64 {
	out := make(map[int]int64)
	for v, d := range dist {
		if d <= cutoff {
			out[v] = d
		}
	}
	return out
}

// shutdownArrivals computes earliest arrivals under a hypothetical pipe
// shutdown starting at minute current.
//
// Seeds are threefold:
//  1. Every node the baseline already reached by current (pollution present
//     at that node at the recorded minute).
//  2. The in-transit payload of each closed pipe whose traversal started
//     strictly before current: it still arrives at base[from]+minutes, even
//     though the pipe is now closed.
//  3. Every release happening at or after current; future releases behave
//     exactly as in the original plan.
//
// Closed pipes are then removed from the graph and multi-source Dijkstra
// propagates the seeds along the still-open pipes. Self-loops are never
// traversed, and parallel pipes are separate arcs, so closing an index
// affects only that single pipe.
func shutdownArrivals(spec Spec, base []int64, current int64, closed []bool) []int64 {
	inf := int64(math.MaxInt64)
	dist := make([]int64, spec.N)
	for i := range dist {
		dist[i] = inf
	}
	adj := make([][]adjEdge, spec.N)
	for i, p := range spec.Pipes {
		if p.From == p.To || closed[i] {
			continue
		}
		adj[p.From] = append(adj[p.From], adjEdge{p.To, p.Minutes})
	}

	queue := make(pq, 0)
	seed := func(v int, d int64) {
		if d < dist[v] {
			dist[v] = d
			queue = append(queue, pqItem{v, d})
		}
	}

	// 1. Pollution already present at a node at or before the shutdown
	// minute. Such a node's outgoing traversals starting at current are
	// governed by the open/closed graph below (an arrival exactly at current
	// departs exactly at current, and closed pipes block that departure).
	for v, d := range base {
		if d <= current {
			seed(v, d)
		}
	}

	// 2. Traversals that entered a now-closed pipe strictly before current
	// are already in flight and still arrive with their original traversal
	// time. Arrivals at or before current are already covered by seed set 1.
	for i, p := range spec.Pipes {
		if !closed[i] || p.From == p.To {
			continue
		}
		departed := base[p.From]
		if departed >= current {
			continue // departure at current or later is blocked
		}
		seed(p.To, departed+p.Minutes)
	}

	// 3. Future releases seed normally.
	for _, r := range spec.Releases {
		if r.At >= current {
			seed(r.Node, r.At)
		}
	}

	heap.Init(&queue)
	for queue.Len() > 0 {
		cur := heap.Pop(&queue).(pqItem)
		if cur.d != dist[cur.v] {
			continue
		}
		for _, e := range adj[cur.v] {
			nd := cur.d + e.w
			if nd < dist[e.to] {
				dist[e.to] = nd
				heap.Push(&queue, pqItem{e.to, nd})
			}
		}
	}
	return dist
}

// Store is the in-memory collection of incidents. The map lock only guards
// map membership; per-incident ordering is handled by the Incident lock.
type Store struct {
	mu   sync.Mutex
	byID map[string]*Incident
}

// NewStore creates an empty store.
func NewStore() *Store {
	return &Store{byID: make(map[string]*Incident)}
}

// Create validates the spec, computes propagation and stores a new incident
// with a random identifier.
func (s *Store) Create(spec Spec) (*Incident, error) {
	in, err := NewIncident(spec)
	if err != nil {
		return nil, err
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}
	in.id = id
	s.mu.Lock()
	s.byID[id] = in
	s.mu.Unlock()
	return in, nil
}

// Get returns the incident with the given id.
func (s *Store) Get(id string) (*Incident, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.byID[id]
	return in, ok
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "inc_" + hex.EncodeToString(b[:]), nil
}
