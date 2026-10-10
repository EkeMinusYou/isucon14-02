package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

// The base implementation is a test-only reference for quality and CPU comparisons.
func legacyHungarianAssignment(rides []Ride, chairs []matchingChair) []matchingAssignment {
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

// Consume exactly the same FIFO ride set and chair pool, 64 rides at a time.
// This isolates assignment quality from the number of external matcher calls.
func legacyMatchingBatches(rides []Ride, chairs []matchingChair) []matchingAssignment {
	remaining := append([]matchingChair(nil), chairs...)
	indices := make([]int, len(chairs))
	for j := range indices {
		indices[j] = j
	}
	var result []matchingAssignment
	for start := 0; start < len(rides) && len(remaining) > 0; {
		end := min(start+64, len(rides))
		batch := legacyHungarianAssignment(rides[start:end], remaining)
		used := make([]bool, len(remaining))
		for _, a := range batch {
			result = append(result, matchingAssignment{start + a.rideIndex, indices[a.chairIndex]})
			used[a.chairIndex] = true
		}
		next, nextIndices := remaining[:0], indices[:0]
		for j, c := range remaining {
			if !used[j] {
				next = append(next, c)
				nextIndices = append(nextIndices, indices[j])
			}
		}
		remaining, indices = next, nextIndices
		start += len(batch)
	}
	return result
}

func matchingEstimate(rides []Ride, chairs []matchingChair, assignments []matchingAssignment) (pickup, travel float64) {
	for _, a := range assignments {
		r, c := rides[a.rideIndex], chairs[a.chairIndex]
		pickup += float64(matchingDistance(c.Latitude, c.Longitude, r.PickupLatitude, r.PickupLongitude)) / float64(c.Speed)
		travel += float64(matchingDistance(r.PickupLatitude, r.PickupLongitude, r.DestinationLatitude, r.DestinationLongitude)) / float64(c.Speed)
	}
	return
}

func TestMatchingAcross64Boundary(t *testing.T) {
	for _, name := range []string{"PickupCompetition", "FastChairsForLaterLongRides"} {
		t.Run(name, func(t *testing.T) {
			rides, chairs := make([]Ride, 128), make([]matchingChair, 128)
			for i := range rides {
				if name == "PickupCompetition" {
					chairs[i] = matchingChair{Speed: 2}
					rides[i] = Ride{DestinationLatitude: 1}
					if i < 64 {
						rides[i] = Ride{PickupLatitude: 4, DestinationLatitude: 5}
					} else {
						chairs[i].Latitude = 10
					}
				} else {
					chairs[i] = matchingChair{Speed: 1}
					rides[i] = Ride{DestinationLatitude: 1000}
					if i < 64 {
						chairs[i].Speed = 10
						rides[i].DestinationLatitude = 1
					}
				}
			}
			old, new := legacyMatchingBatches(rides, chairs), assignMatchingChairs(rides, chairs)
			op, ot := matchingEstimate(rides, chairs, old)
			np, nt := matchingEstimate(rides, chairs, new)
			if len(old) != 128 || len(new) != 128 || np+nt >= op+ot {
				t.Fatalf("old=(%g,%g), new=(%g,%g)", op, ot, np, nt)
			}
			t.Logf("same 128 rides/chairs: old pickup=%.3fs travel=%.3fs cost=%.3fs; new pickup=%.3fs travel=%.3fs cost=%.3fs", op, ot, op+ot, np, nt, np+nt)
		})
	}
}

func localMatchingInput(n, m int) ([]Ride, []matchingChair) {
	rng := rand.New(rand.NewPCG(17, 31))
	rides, chairs := make([]Ride, n), make([]matchingChair, m)
	for i := range rides {
		rides[i] = Ride{PickupLatitude: rng.IntN(1000), PickupLongitude: rng.IntN(1000), DestinationLatitude: rng.IntN(1000), DestinationLongitude: rng.IntN(1000)}
	}
	for j := range chairs {
		chairs[j] = matchingChair{Latitude: rng.IntN(1000), Longitude: rng.IntN(1000), Speed: 1 + rng.IntN(7)}
	}
	return rides, chairs
}

func TestMatchingLargePlanQuality(t *testing.T) {
	for _, n := range []int{65, 128, 256} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			rides, chairs := localMatchingInput(n, 1024)
			got := assignMatchingChairs(rides, chairs)
			exact := legacyHungarianAssignment(rides, chairs)
			batches := legacyMatchingBatches(rides, chairs)
			gp, gt := matchingEstimate(rides, chairs, got)
			ep, et := matchingEstimate(rides, chairs, exact)
			bp, bt := matchingEstimate(rides, chairs, batches)
			used := map[int]bool{}
			for i, a := range got {
				if a.rideIndex != i || used[a.chairIndex] {
					t.Fatal("FIFO or uniqueness violation")
				}
				used[a.chairIndex] = true
			}
			if len(got) != n || math.Abs(gp+gt-ep-et) > 1e-7 || gp+gt > bp+bt+1e-7 {
				t.Fatalf("cost new=%g exact=%g batches=%g", gp+gt, ep+et, bp+bt)
			}
			t.Logf("same %d rides/1024 chairs: old pickup=%.3fs travel=%.3fs cost=%.3fs; new pickup=%.3fs travel=%.3fs cost=%.3fs", n, bp, bt, bp+bt, gp, gt, gp+gt)
		})
	}
}

// This benchmark uses only local synthetic inputs, never the contest benchmarker.
func BenchmarkLocalMatchingComparison(b *testing.B) {
	for _, n := range []int{64, 128, 256} {
		rides, chairs := localMatchingInput(n, 1024)
		for _, method := range []string{"Legacy64", "SparsePlan"} {
			b.Run(fmt.Sprintf("%d/%s", n, method), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if method == "Legacy64" {
						legacyMatchingBatches(rides, chairs)
					} else {
						assignMatchingChairs(rides, chairs)
					}
				}
			})
		}
	}
}
