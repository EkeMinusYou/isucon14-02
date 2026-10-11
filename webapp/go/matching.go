package main

import (
	"container/heap"
	"math"
	"sort"
)

type matchingChair struct {
	ID        string `db:"id"`
	Latitude  int    `db:"latitude"`
	Longitude int    `db:"longitude"`
	Speed     int    `db:"speed"`
}

type matchingAssignment struct {
	rideIndex  int
	chairIndex int
}

func matchingTravelTime(ride Ride, chair matchingChair) float64 {
	// A nonpositive speed denotes missing position or model metadata. Keep
	// these chairs available as fallbacks without treating them as nearby.
	if chair.Speed <= 0 {
		return math.Inf(1)
	}
	distance := matchingDistance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude) +
		matchingDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	return float64(distance) / float64(chair.Speed)
}

func matchingDistance(aLat, aLon, bLat, bLon int) int64 {
	lat, lon := int64(aLat)-int64(bLat), int64(aLon)-int64(bLon)
	if lat < 0 {
		lat = -lat
	}
	if lon < 0 {
		lon = -lon
	}
	return lat + lon
}

type matchingCandidate struct {
	chairIndex int
	cost       float64
}

// Keep the least promising candidate at the root, including stable ties.
type matchingCandidates []matchingCandidate

func (h matchingCandidates) Len() int { return len(h) }
func (h matchingCandidates) Less(i, j int) bool {
	if h[i].cost == h[j].cost {
		return h[i].chairIndex > h[j].chairIndex
	}
	return h[i].cost > h[j].cost
}
func (h matchingCandidates) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *matchingCandidates) Push(x any)   { *h = append(*h, x.(matchingCandidate)) }
func (h *matchingCandidates) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// The caller selects rides. Each row retains its best n chairs: at most n-1
// can be occupied, so an omitted chair can always be replaced without increasing
// cost. Pruning preserves a global optimum.
// Candidate extraction costs O(n*C*log(n)); the sparse graph has E <= n*n
// assignment edges instead of a dense n-by-union matrix.
func assignMatchingChairs(rides []Ride, chairs []matchingChair) []matchingAssignment {
	n := min(len(rides), len(chairs))
	if n == 0 {
		return nil
	}
	rows := make([]matchingCandidates, n)
	selected := make([]bool, len(chairs))
	maxKnownCost := 0.0
	for i, ride := range rides[:n] {
		best := make(matchingCandidates, 0, n)
		for j, chair := range chairs {
			candidate := matchingCandidate{chairIndex: j, cost: matchingTravelTime(ride, chair)}
			if len(best) < n {
				best = append(best, candidate)
				if len(best) == n {
					heap.Init(&best)
				}
			} else if candidate.cost < best[0].cost {
				best[0] = candidate
				heap.Fix(&best, 0)
			}
		}
		// Stable adjacency order also makes equal-cost assignments reproducible.
		sort.Slice(best, func(a, b int) bool { return best[a].chairIndex < best[b].chairIndex })
		rows[i] = best
		for _, candidate := range best {
			selected[candidate.chairIndex] = true
			if !math.IsInf(candidate.cost, 1) {
				maxKnownCost = math.Max(maxKnownCost, candidate.cost)
			}
		}
	}
	indices := make([]int, 0, min(n*n, len(chairs)))
	vertices := make([]int, len(chairs))
	for j, included := range selected {
		if included {
			vertices[j] = n + len(indices)
			indices = append(indices, j)
		}
	}
	source, sink := n+len(indices), n+len(indices)+1
	g := make(matchingGraph, sink+1)
	degrees := make([]int, len(g))
	degrees[source], degrees[sink] = n, len(indices)
	for j := range indices {
		degrees[n+j] = 1
	}
	for i, candidates := range rows {
		degrees[i] = len(candidates) + 1
		for _, candidate := range candidates {
			degrees[vertices[candidate.chairIndex]]++
		}
	}
	for v, degree := range degrees {
		g[v] = make([]matchingEdge, 0, degree)
	}
	// Minimize fallback count first, then total known pickup + travel time.
	fallbackCost := float64(n+1) * (maxKnownCost + 1)
	for i, candidates := range rows {
		g.add(source, i, 0)
		for _, candidate := range candidates {
			cost := candidate.cost
			if math.IsInf(cost, 1) {
				cost = fallbackCost
			}
			g.add(i, vertices[candidate.chairIndex], cost)
		}
	}
	for j := range indices {
		g.add(n+j, sink, 0)
	}
	g.assign(source, sink, n)
	assignments := make([]matchingAssignment, n)
	for i := range rows {
		for _, edge := range g[i] {
			if edge.to >= n && edge.to < source && !edge.available {
				assignments[i] = matchingAssignment{rideIndex: i, chairIndex: indices[edge.to-n]}
				break
			}
		}
	}
	return assignments
}

type matchingEdge struct {
	to, reverse int
	cost        float64
	available   bool
}

type matchingGraph [][]matchingEdge

func (g matchingGraph) add(from, to int, cost float64) {
	forward := matchingEdge{to: to, reverse: len(g[to]), cost: cost, available: true}
	backward := matchingEdge{to: from, reverse: len(g[from]), cost: -cost}
	g[from] = append(g[from], forward)
	g[to] = append(g[to], backward)
}

// Successive shortest augmenting paths with potentials. Reverse edges can
// change assignments across the entire plan. O(n*(E+V)*log(V)) time and
// O(E+V) solver memory; V <= n + min(C,n*n) + 2. No shared state is accessed.
func (g matchingGraph) assign(source, sink, count int) {
	potential := make([]float64, len(g))
	distance := make([]float64, len(g))
	previous, via := make([]int, len(g)), make([]int, len(g))
	queue := make(matchingPathHeap, 0, len(g))
	for flow := 0; flow < count; flow++ {
		for v := range g {
			distance[v] = math.Inf(1)
		}
		distance[source] = 0
		queue = queue[:0]
		queue.push(matchingPath{vertex: source})
		for len(queue) > 0 {
			path := queue.pop()
			v := path.vertex
			if path.cost != distance[v] {
				continue
			}
			if v == sink {
				break
			}
			for j, edge := range g[v] {
				if !edge.available {
					continue
				}
				// Roundoff can make a theoretically nonnegative reduced cost tiny and negative.
				cost := path.cost + math.Max(0, edge.cost+potential[v]-potential[edge.to])
				if cost < distance[edge.to] {
					distance[edge.to], previous[edge.to], via[edge.to] = cost, v, j
					queue.push(matchingPath{vertex: edge.to, cost: cost})
				}
			}
		}
		// Every row has n distinct neighbors, so all n rows have a matching.
		for v := range g {
			potential[v] += math.Min(distance[v], distance[sink])
		}
		for v := sink; v != source; v = previous[v] {
			edge := &g[previous[v]][via[v]]
			edge.available = false
			g[v][edge.reverse].available = true
		}
	}
}

type matchingPath struct {
	vertex int
	cost   float64
}

// Typed heap avoids allocating an interface value for every relaxed edge.
type matchingPathHeap []matchingPath

func (a matchingPath) before(b matchingPath) bool {
	return a.cost < b.cost || (a.cost == b.cost && a.vertex < b.vertex)
}

func (h *matchingPathHeap) push(path matchingPath) {
	*h = append(*h, path)
	i := len(*h) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !path.before((*h)[parent]) {
			break
		}
		(*h)[i] = (*h)[parent]
		i = parent
	}
	(*h)[i] = path
}

func (h *matchingPathHeap) pop() matchingPath {
	result := (*h)[0]
	last := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	if len(*h) == 0 {
		return result
	}
	i := 0
	for 2*i+1 < len(*h) {
		child := 2*i + 1
		if child+1 < len(*h) && (*h)[child+1].before((*h)[child]) {
			child++
		}
		if !(*h)[child].before(last) {
			break
		}
		(*h)[i] = (*h)[child]
		i = child
	}
	(*h)[i] = last
	return result
}
