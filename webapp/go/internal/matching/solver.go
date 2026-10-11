package matching

import (
	"context"
	"errors"
	"math"
)

// Values contain no identities or mutable application state. Slice order is
// the implicit index used by the assignment response.
type Ride struct {
	PickupLatitude       int `json:"p"`
	PickupLongitude      int `json:"q"`
	DestinationLatitude  int `json:"d"`
	DestinationLongitude int `json:"e"`
}
type Chair struct {
	Latitude  int `json:"a"`
	Longitude int `json:"o"`
	Speed     int `json:"s"`
}
type Assignment struct{ RideIndex, ChairIndex int }

func TravelTime(ride Ride, chair Chair) float64 {
	// A nonpositive speed denotes missing position or model metadata. Keep
	// these chairs available as fallbacks without treating them as nearby.
	if chair.Speed <= 0 {
		return math.Inf(1)
	}
	distance := Distance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude) +
		Distance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	return float64(distance) / float64(chair.Speed)
}

func Distance(aLat, aLon, bLat, bLon int) int64 {
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
// O(E+V) solver memory. No shared state is accessed.
func (g matchingGraph) assign(ctx context.Context, source, sink, count int) error {
	potential := make([]float64, len(g))
	distance := make([]float64, len(g))
	previous, via := make([]int, len(g)), make([]int, len(g))
	queue := make(matchingPathHeap, 0, len(g))
	for flow := 0; flow < count; flow++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		for v := range g {
			distance[v] = math.Inf(1)
		}
		distance[source] = 0
		queue = queue[:0]
		queue.push(matchingPath{vertex: source})
		visited := 0
		for len(queue) > 0 {
			if visited%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			visited++
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
		if math.IsInf(distance[sink], 1) {
			return errors.New("matching graph has insufficient capacity")
		}
		// The FIFO backbone guarantees the requested cardinality.
		for v := range g {
			potential[v] += math.Min(distance[v], distance[sink])
		}
		for v := sink; v != source; v = previous[v] {
			edge := &g[previous[v]][via[v]]
			edge.available = false
			g[v][edge.reverse].available = true
		}
	}
	return ctx.Err()
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
