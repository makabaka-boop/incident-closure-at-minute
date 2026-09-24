package flow

import (
	"math"
	"math/rand"
	"testing"
)

func TestMinShutdownCost(t *testing.T) {
	cases := []struct {
		name           string
		n              int
		edges          []Edge
		sources, sinks []int
		want           int64
	}{
		{"single edge", 2, []Edge{{0, 1, 7}}, []int{0}, []int{1}, 7},
		{"parallel edges charged individually", 2, []Edge{{0, 1, 3}, {0, 1, 4}}, []int{0}, []int{1}, 7},
		{"self loops ignored", 3, []Edge{{0, 0, 100}, {0, 1, 5}, {1, 1, 9}, {1, 2, 2}}, []int{0}, []int{2}, 2},
		{"directed graph", 2, []Edge{{1, 0, 5}}, []int{0}, []int{1}, 0},
		{"no path means zero", 4, []Edge{{0, 1, 5}, {2, 3, 6}}, []int{0}, []int{3}, 0},
		{"bottleneck of a chain", 5, []Edge{{0, 1, 9}, {1, 2, 4}, {2, 3, 6}, {3, 4, 3}}, []int{0}, []int{4}, 3},
		{"two disjoint paths", 4, []Edge{{0, 1, 5}, {1, 3, 5}, {0, 2, 8}, {2, 3, 8}}, []int{0}, []int{3}, 13},
		{"multi source multi sink", 6, []Edge{
			{0, 2, 10}, {1, 2, 1}, {2, 3, 4}, {3, 4, 3}, {3, 5, 2}, {0, 4, 12}, {1, 5, 8},
		}, []int{0, 1}, []int{4, 5}, 24},
		{"must sever every branch", 3, []Edge{{0, 1, 4}, {0, 2, 6}}, []int{0}, []int{1, 2}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MinShutdownCost(tc.n, tc.edges, tc.sources, tc.sinks); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// bruteForce enumerates every s-t cut. It is a test oracle for tiny graphs
// only and is never used by the solver itself.
func bruteForce(n int, edges []Edge, sources, sinks []int) int64 {
	isEndpoint := make([]bool, n)
	for _, s := range sources {
		isEndpoint[s] = true
	}
	for _, t := range sinks {
		isEndpoint[t] = true
	}
	var free []int
	for v := 0; v < n; v++ {
		if !isEndpoint[v] {
			free = append(free, v)
		}
	}
	best := int64(math.MaxInt64)
	for mask := 0; mask < 1<<uint(len(free)); mask++ {
		side := make([]bool, n) // true = source side of the cut
		for _, s := range sources {
			side[s] = true
		}
		for i, v := range free {
			if mask>>uint(i)&1 == 1 {
				side[v] = true
			}
		}
		var cost int64
		for _, e := range edges {
			if side[e.From] && !side[e.To] {
				cost += e.Cost
			}
		}
		if cost < best {
			best = cost
		}
	}
	return best
}

func TestAgainstBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 300; trial++ {
		n := 2 + rng.Intn(7) // 2..8 nodes
		edges := make([]Edge, rng.Intn(15))
		for i := range edges {
			edges[i] = Edge{rng.Intn(n), rng.Intn(n), int64(1 + rng.Intn(20))}
		}
		// Random disjoint non-empty endpoint sets.
		perm := rng.Perm(n)
		ns := 1 + rng.Intn(n-1)
		nk := 1 + rng.Intn(n-ns)
		sources := append([]int(nil), perm[:ns]...)
		sinks := append([]int(nil), perm[ns:ns+nk]...)
		got := MinShutdownCost(n, edges, sources, sinks)
		want := bruteForce(n, edges, sources, sinks)
		if got != want {
			t.Fatalf("trial %d: got %d, want %d (n=%d edges=%v sources=%v sinks=%v)",
				trial, got, want, n, edges, sources, sinks)
		}
	}
}

// TestLargeNetwork exercises the upper constraint bounds: 20000 nodes and
// 100000 edges. The minimum cut is exactly the sum of the small edges into
// node 1: every edge into the sink is one of them, and every other edge
// costs 1e9, more than that sum, so no cheaper cut exists.
func TestLargeNetwork(t *testing.T) {
	const n = 20000
	const m = 100000
	rng := rand.New(rand.NewSource(42))
	edges := make([]Edge, 0, m)
	var want int64
	for i := 2; i <= 5000; i++ {
		edges = append(edges, Edge{0, i, 1_000_000_000})
		c := int64(1 + rng.Intn(1000))
		edges = append(edges, Edge{i, 1, c})
		want += c
	}
	for len(edges) < m {
		edges = append(edges, Edge{2 + rng.Intn(n-2), 2 + rng.Intn(n-2), 1_000_000_000})
	}
	if got := MinShutdownCost(n, edges, []int{0}, []int{1}); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}
