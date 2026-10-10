package main

import (
	"container/heap"
	"math"
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

// Rides arrive in FIFO order; never skip an older ride to favor a short trip.
// This function only plans assignments. Its caller validates and persists them.
func assignMatchingChairs(rides []Ride, chairs []matchingChair) []matchingAssignment {
	n := min(len(rides), len(chairs))
	if n == 0 {
		return nil
	}
	rides = rides[:n]

	// For n rides, the union of each ride's best n chairs contains an optimum:
	// at most n-1 of those chairs can be occupied by other assignments. A worse
	// chair can therefore always be replaced without increasing total cost.
	// Bound the solver to at most n*n chairs while scanning the full pool once
	// per ride, in O(n * chairs * log(n)) time.
	selected := make([]bool, len(chairs))
	for _, ride := range rides {
		best := make(matchingCandidates, 0, n)
		for j, chair := range chairs {
			candidate := matchingCandidate{chairIndex: j, cost: matchingTravelTime(ride, chair)}
			if len(best) < n {
				heap.Push(&best, candidate)
			} else if candidate.cost < best[0].cost {
				best[0] = candidate
				heap.Fix(&best, 0)
			}
		}
		for _, candidate := range best {
			selected[candidate.chairIndex] = true
		}
	}
	indices := make([]int, 0, min(n*n, len(chairs)))
	for j, included := range selected {
		if included {
			indices = append(indices, j)
		}
	}
	m := len(indices)
	costs := make([][]float64, n)
	maxKnownCost := 0.0
	for i, ride := range rides {
		costs[i] = make([]float64, m)
		for j, chairIndex := range indices {
			costs[i][j] = matchingTravelTime(ride, chairs[chairIndex])
			if !math.IsInf(costs[i][j], 1) {
				maxKnownCost = math.Max(maxKnownCost, costs[i][j])
			}
		}
	}
	// A finite penalty keeps augmenting paths valid when every remaining
	// chair has unknown metadata. Prefer fewer fallback chairs before time:
	// the penalty exceeds the entire known-cost range of this batch.
	fallbackCost := float64(n+1) * (maxKnownCost + 1)
	for _, row := range costs {
		for j, cost := range row {
			if math.IsInf(cost, 1) {
				row[j] = fallbackCost
			}
		}
	}

	// Rectangular Hungarian assignment, O(n*n*m). Each augmenting path may
	// replace earlier choices, unlike a greedy nearest-chair matcher.
	u, v := make([]float64, n+1), make([]float64, m+1)
	p, way := make([]int, m+1), make([]int, m+1)
	minimum, used := make([]float64, m+1), make([]bool, m+1)
	for i := 1; i <= n; i++ {
		p[0] = i
		j0 := 0
		for j := 0; j <= m; j++ {
			minimum[j], used[j] = math.Inf(1), false
		}
		for {
			used[j0] = true
			i0, j1, delta := p[j0], 0, math.Inf(1)
			for j := 1; j <= m; j++ {
				if used[j] {
					continue
				}
				cur := costs[i0-1][j-1] - u[i0] - v[j]
				if cur < minimum[j] {
					minimum[j], way[j] = cur, j0
				}
				if minimum[j] < delta {
					delta, j1 = minimum[j], j
				}
			}
			for j := 0; j <= m; j++ {
				if used[j] {
					u[p[j]] += delta
					v[j] -= delta
				} else {
					minimum[j] -= delta
				}
			}
			j0 = j1
			if p[j0] == 0 {
				break
			}
		}
		for j0 != 0 {
			j1 := way[j0]
			p[j0] = p[j1]
			j0 = j1
		}
	}
	assignments := make([]matchingAssignment, n)
	for j := 1; j <= m; j++ {
		if p[j] != 0 {
			assignments[p[j]-1] = matchingAssignment{rideIndex: p[j] - 1, chairIndex: indices[j-1]}
		}
	}
	return assignments
}
