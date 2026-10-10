package main

import (
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestMatchingAssignment(t *testing.T) {
	tests := []struct {
		name   string
		rides  []Ride
		chairs []matchingChair
		want   []matchingAssignment
	}{
		{
			name: "FastChairServesLongRide",
			rides: []Ride{
				{DestinationLatitude: 2},
				{DestinationLatitude: 100},
			},
			chairs: []matchingChair{{Speed: 10}, {Speed: 2}},
			want:   []matchingAssignment{{0, 1}, {1, 0}},
		},
		{
			name:  "PickupDistanceCanOutweighSpeed",
			rides: []Ride{{DestinationLatitude: 2}},
			chairs: []matchingChair{
				{Latitude: 100, Speed: 10},
				{Speed: 2},
			},
			want: []matchingAssignment{{0, 1}},
		},
		{
			name: "GlobalAssignmentBeatsGreedy",
			rides: []Ride{
				{PickupLatitude: 4, DestinationLatitude: 5},
				{DestinationLatitude: 1},
			},
			chairs: []matchingChair{{Speed: 2}, {Latitude: 10, Speed: 2}},
			want:   []matchingAssignment{{0, 1}, {1, 0}},
		},
		{
			name: "OldLongRidePrecedesNewShortRide",
			rides: []Ride{
				{DestinationLatitude: 100},
				{DestinationLatitude: 1},
			},
			chairs: []matchingChair{{Speed: 2}},
			want:   []matchingAssignment{{0, 0}},
		},
		{name: "NoChairs", rides: []Ride{{}}},
		{name: "NoRides", chairs: []matchingChair{{Speed: 2}}},
		{
			name:   "KnownChairPrecedesUnknownMetadata",
			rides:  []Ride{{DestinationLatitude: 1}},
			chairs: []matchingChair{{Speed: 0}, {Latitude: 1000000, Speed: 2}},
			want:   []matchingAssignment{{0, 1}},
		},
		{
			name:   "AllUnknownChairsRemainUsable",
			rides:  []Ride{{DestinationLatitude: 100}, {DestinationLatitude: 1}},
			chairs: []matchingChair{{Speed: 0}, {Speed: 0}},
			want:   []matchingAssignment{{0, 0}, {1, 1}},
		},
		{
			name:   "UnknownChairFillsRemainingAssignment",
			rides:  []Ride{{DestinationLatitude: 100}, {DestinationLatitude: 1}},
			chairs: []matchingChair{{Speed: 0}, {Speed: 2}},
			want:   []matchingAssignment{{0, 0}, {1, 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := assignMatchingChairs(tt.rides, tt.chairs)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("assignments = %v, want %v", got, tt.want)
			}
		})
	}
}

// Exhaustive search is an independent oracle for both candidate pruning and
// the solver, including competition for the same chair and fractional costs.
func TestMatchingAssignmentMinimumAgainstExhaustiveSearch(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 19))
	for trial := 0; trial < 120; trial++ {
		rides := make([]Ride, 1+rng.IntN(4))
		chairs := make([]matchingChair, 1+rng.IntN(7))
		for i := range rides {
			rides[i] = Ride{
				PickupLatitude: rng.IntN(40) - 20, PickupLongitude: rng.IntN(40) - 20,
				DestinationLatitude: rng.IntN(40) - 20, DestinationLongitude: rng.IntN(40) - 20,
			}
		}
		for j := range chairs {
			chairs[j] = matchingChair{Latitude: rng.IntN(40) - 20, Longitude: rng.IntN(40) - 20, Speed: 1 + rng.IntN(7)}
		}
		n := min(len(rides), len(chairs))
		used := make([]bool, len(chairs))
		best := math.Inf(1)
		var search func(int, float64)
		search = func(i int, cost float64) {
			if i == n {
				best = math.Min(best, cost)
				return
			}
			for j, chair := range chairs {
				if used[j] {
					continue
				}
				used[j] = true
				pickup := math.Abs(float64(chair.Latitude-rides[i].PickupLatitude)) + math.Abs(float64(chair.Longitude-rides[i].PickupLongitude))
				travel := math.Abs(float64(rides[i].PickupLatitude-rides[i].DestinationLatitude)) + math.Abs(float64(rides[i].PickupLongitude-rides[i].DestinationLongitude))
				search(i+1, cost+(pickup+travel)/float64(chair.Speed))
				used[j] = false
			}
		}
		search(0, 0)
		got := assignMatchingChairs(rides, chairs)
		if len(got) != n {
			t.Fatalf("trial %d: assigned %d rides, want %d", trial, len(got), n)
		}
		cost := 0.0
		for i, assignment := range got {
			if assignment.rideIndex != i || used[assignment.chairIndex] {
				t.Fatalf("trial %d: skipped old ride or reused chair: %v", trial, got)
			}
			used[assignment.chairIndex] = true
			cost += matchingTravelTime(rides[i], chairs[assignment.chairIndex])
		}
		if math.Abs(cost-best) > 1e-9 {
			t.Fatalf("trial %d: total time %f, want optimum %f", trial, cost, best)
		}
		if again := assignMatchingChairs(rides, chairs); !reflect.DeepEqual(got, again) {
			t.Fatalf("trial %d: assignment is not deterministic", trial)
		}
	}
}

func TestMatchingDistanceLargeCoordinates(t *testing.T) {
	const low, high = -2147483648, 2147483647
	if got := matchingDistance(low, high, high, low); got != 8589934590 {
		t.Fatalf("distance = %d, want 8589934590", got)
	}
}
