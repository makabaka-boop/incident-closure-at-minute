// Package flow provides a self-contained maximum-flow / minimum-cut solver.
//
// The minimum shutdown cost of a rain-pollution network is the minimum
// s-t cut: the cheapest set of pipes whose removal blocks every path from
// the supply inlets (sources) to the water intakes (sinks). By the
// max-flow min-cut theorem this equals the maximum flow in a network where
// a super source feeds every source and every sink drains into a super
// sink, both through arcs of capacity (total edge cost + 1) so that an
// optimal cut never severs them. The solver is Dinic's algorithm
// implemented from scratch on 64-bit capacities; no external solver and
// no cut-set enumeration is used.
package flow

import "math"

// Edge is a directed pipe with a shutdown cost.
type Edge struct {
	From int
	To   int
	Cost int64
}

type arc struct {
	to  int
	rev int // index of the reverse arc in g[to]
	cap int64
}

// Dinic is a max-flow solver using Dinic's algorithm with the
// current-arc optimisation.
type Dinic struct {
	g     [][]arc
	level []int
	next  []int
}

// NewDinic returns a solver for a graph with n nodes (ids 0..n-1).
func NewDinic(n int) *Dinic {
	return &Dinic{
		g:     make([][]arc, n),
		level: make([]int, n),
		next:  make([]int, n),
	}
}

// AddEdge inserts a directed edge u -> v with capacity c.
func (d *Dinic) AddEdge(u, v int, c int64) {
	d.g[u] = append(d.g[u], arc{to: v, rev: len(d.g[v]), cap: c})
	d.g[v] = append(d.g[v], arc{to: u, rev: len(d.g[u]) - 1, cap: 0})
}

// MaxFlow computes the maximum flow from s to t.
func (d *Dinic) MaxFlow(s, t int) int64 {
	var flow int64
	for d.bfs(s, t) {
		for i := range d.next {
			d.next[i] = 0
		}
		for {
			pushed := d.dfs(s, t, math.MaxInt64)
			if pushed == 0 {
				break
			}
			flow += pushed
		}
	}
	return flow
}

// bfs builds the level graph and reports whether t is reachable from s in
// the residual network.
func (d *Dinic) bfs(s, t int) bool {
	for i := range d.level {
		d.level[i] = -1
	}
	d.level[s] = 0
	queue := make([]int, 0, len(d.g))
	queue = append(queue, s)
	for head := 0; head < len(queue); head++ {
		u := queue[head]
		for _, a := range d.g[u] {
			if a.cap > 0 && d.level[a.to] < 0 {
				d.level[a.to] = d.level[u] + 1
				queue = append(queue, a.to)
			}
		}
	}
	return d.level[t] >= 0
}

// dfs pushes flow along admissible arcs of the level graph.
func (d *Dinic) dfs(u, t int, f int64) int64 {
	if u == t {
		return f
	}
	for ; d.next[u] < len(d.g[u]); d.next[u]++ {
		i := d.next[u]
		a := &d.g[u][i]
		if a.cap <= 0 || d.level[a.to] != d.level[u]+1 {
			continue
		}
		pushed := d.dfs(a.to, t, min(f, a.cap))
		if pushed > 0 {
			a.cap -= pushed
			d.g[a.to][a.rev].cap += pushed
			return pushed
		}
	}
	return 0
}

// MinShutdownCost returns the minimum total cost of pipes whose removal
// blocks every path from any source to any sink. It is 0 when no such path
// exists. Self-loops never cross a cut and parallel edges are charged
// individually, so both are handled naturally by the reduction.
func MinShutdownCost(n int, edges []Edge, sources, sinks []int) int64 {
	var total int64
	for _, e := range edges {
		total += e.Cost
	}
	// Any cut severing a super arc costs at least total+1, while cutting
	// every original edge out of the sources costs at most total, so an
	// optimal cut only ever severs original edges.
	big := total + 1
	superSource, superSink := n, n+1
	d := NewDinic(n + 2)
	for _, e := range edges {
		d.AddEdge(e.From, e.To, e.Cost)
	}
	for _, s := range sources {
		d.AddEdge(superSource, s, big)
	}
	for _, t := range sinks {
		d.AddEdge(t, superSink, big)
	}
	return d.MaxFlow(superSource, superSink)
}
