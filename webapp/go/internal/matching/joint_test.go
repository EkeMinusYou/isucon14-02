package matching

import (
	"context"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
)

// Enumerate ride subsets and chair permutations independently of graph costs,
// pruning and Efficiency. Small optional pools fit entirely in each query.
func TestMatchingJointAgainstExhaustiveOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(43, 97))
	for trial := 0; trial < 180; trial++ {
		n := 1 + rng.IntN(4)
		request := Request{Count: n, Required: (n+3)/4 + rng.IntN(n-(n+3)/4+1),
			Rides: make([]Ride, n+1+rng.IntN(3)), Chairs: make([]Chair, n+rng.IntN(3))}
		for i := range request.Rides {
			request.Rides[i] = Ride{rng.IntN(20) - 10, rng.IntN(20) - 10, rng.IntN(20) - 10, rng.IntN(20) - 10}
		}
		for j := range request.Chairs {
			request.Chairs[j] = Chair{rng.IntN(20) - 10, rng.IntN(20) - 10, rng.IntN(7)}
		}
		// Include zero distances, equal rates, unknown and excess chairs.
		if trial%15 == 0 {
			for i := range request.Rides {
				request.Rides[i] = Ride{}
			}
		}
		if trial%17 == 0 {
			for j := range request.Chairs {
				request.Chairs[j].Speed = 0
			}
		}
		abs := func(x int) float64 { return math.Abs(float64(x)) }
		value := func(i, j int) (int, float64) {
			ride, chair := request.Rides[i], request.Chairs[j]
			if chair.Speed == 0 {
				return 1, 0
			}
			pickup := abs(chair.Latitude-ride.PickupLatitude) + abs(chair.Longitude-ride.PickupLongitude)
			trip := abs(ride.PickupLatitude-ride.DestinationLatitude) + abs(ride.PickupLongitude-ride.DestinationLongitude)
			return 0, trip / ((pickup+trip)/float64(chair.Speed) + 5)
		}
		bestFallback, bestRate := n+1, -1.0
		used := make([]bool, len(request.Chairs))
		var enumerate func(int, int, int, float64)
		enumerate = func(i, count, fallback int, rate float64) {
			if count == n {
				if i < request.Required {
					return
				}
				if fallback < bestFallback || (fallback == bestFallback && rate > bestRate) {
					bestFallback, bestRate = fallback, rate
				}
				return
			}
			if i == len(request.Rides) || len(request.Rides)-i < n-count {
				return
			}
			if i >= request.Required {
				enumerate(i+1, count, fallback, rate)
			}
			for j := range request.Chairs {
				if used[j] {
					continue
				}
				used[j] = true
				f, v := value(i, j)
				enumerate(i+1, count+1, fallback+f, rate+v)
				used[j] = false
			}
		}
		enumerate(0, 0, 0, 0)
		result, err := Solve(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		response := Response{Rides: make([]int, len(result)), Chairs: make([]int, len(result))}
		fallback, rate := 0, 0.0
		for i, a := range result {
			response.Rides[i], response.Chairs[i] = a.RideIndex, a.ChairIndex
			f, v := value(a.RideIndex, a.ChairIndex)
			fallback, rate = fallback+f, rate+v
		}
		if err := response.Validate(request); err != nil {
			t.Fatalf("trial %d: %v (%v)", trial, err, result)
		}
		if fallback != bestFallback || math.Abs(rate-bestRate) > 1e-8 {
			t.Fatalf("trial %d: fallback/rate=%d/%g want %d/%g request=%+v result=%v", trial, fallback, rate, bestFallback, bestRate, request, result)
		}
		again, err := Solve(context.Background(), request)
		if err != nil || !reflect.DeepEqual(result, again) {
			t.Fatalf("unstable tie: %v %v", result, again)
		}
	}
}

func TestMatchingJointChoosesRideAndChairTogether(t *testing.T) {
	request := Request{Count: 2, Required: 1,
		Rides:  []Ride{{PickupLatitude: 10, DestinationLatitude: 110}, {DestinationLatitude: 1}, {DestinationLatitude: 100}},
		Chairs: []Chair{{Speed: 10}, {Latitude: 10, Speed: 1}}}
	got, err := Solve(context.Background(), request)
	want := []Assignment{{0, 1}, {2, 0}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("joint=%v want=%v err=%v", got, want, err)
	}
}

func TestMatchingJointGraphBoundsAndFallbackCapacity(t *testing.T) {
	request := Request{Count: PlanLimit, Required: PlanLimit / 4, Rides: make([]Ride, MaxRides), Chairs: make([]Chair, MaxChairs)}
	for j := range request.Chairs {
		request.Chairs[j] = Chair{Latitude: j, Speed: 1}
	}
	rows, err := candidateRows(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	edges, rides := 0, 0
	for i, row := range rows {
		edges += len(row)
		if len(row) != 0 {
			rides++
		}
		if i < request.Count && len(row) != request.Count {
			t.Fatal("FIFO backbone lost")
		}
		seen := map[int]bool{}
		for _, c := range row {
			if seen[c.chairIndex] {
				t.Fatal("duplicate edge")
			}
			seen[c.chairIndex] = true
		}
	}
	if edges > request.Count*request.Count+NearbyLimit*len(request.Chairs) || rides > request.Count+NearbyLimit*len(request.Chairs) {
		t.Fatalf("graph too large: E=%d R=%d", edges, rides)
	}
	// All nearby choices collide; only a few chairs have metadata. Full count
	// and the rescued prefix must still survive through unknown fallback edges.
	request.Chairs = request.Chairs[:PlanLimit+3]
	for j := range request.Chairs {
		if j >= 3 {
			request.Chairs[j].Speed = 0
		}
	}
	request.Rides = request.Rides[:2*PlanLimit]
	result, err := Solve(context.Background(), request)
	if err != nil || len(result) != PlanLimit {
		t.Fatalf("capacity=%d err=%v", len(result), err)
	}
	response := Response{Rides: make([]int, len(result)), Chairs: make([]int, len(result))}
	known := 0
	for i, a := range result {
		response.Rides[i], response.Chairs[i] = a.RideIndex, a.ChairIndex
		if request.Chairs[a.ChairIndex].Speed > 0 {
			known++
		}
	}
	if err := response.Validate(request); err != nil || known != 3 {
		t.Fatalf("fallback/required: known=%d err=%v", known, err)
	}
}

func TestMatchingJointNearbyAgainstFullScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(13, 29))
	rides, indices := make([]Ride, 200), make([]int, 200)
	for i := range rides {
		rides[i] = Ride{PickupLatitude: rng.IntN(30) - 15, PickupLongitude: rng.IntN(30) - 15}
		indices[i] = i
	}
	root := buildPickups(rides, indices, 0)
	for q := 0; q < 60; q++ {
		chair := Chair{Latitude: rng.IntN(50) - 25, Longitude: rng.IntN(50) - 25, Speed: 1}
		want := append([]int(nil), indices...)
		sort.Slice(want, func(a, b int) bool {
			x, y := rides[want[a]], rides[want[b]]
			dx, dy := Distance(chair.Latitude, chair.Longitude, x.PickupLatitude, x.PickupLongitude), Distance(chair.Latitude, chair.Longitude, y.PickupLatitude, y.PickupLongitude)
			return dx < dy || (dx == dy && want[a] < want[b])
		})
		if got := nearbyPickups(root, rides, chair, 0); !reflect.DeepEqual(got, want[:NearbyLimit]) {
			t.Fatalf("nearby=%v want=%v", got, want[:NearbyLimit])
		}
	}
}
