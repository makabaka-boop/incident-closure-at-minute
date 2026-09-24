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
