package main

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMatchingPickupIndexAgainstFullScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 23))
	s := memoryTestState(t)
	at := time.Now().UTC()
	for i := 0; i < 400; i++ {
		// Duplicate positions/timestamps, negative and widely separated points.
		row := Ride{ID: fmt.Sprintf("r-%03d", i), UserID: "u", CreatedAt: at.Add(time.Duration(rng.IntN(10)) * time.Microsecond),
			PickupLatitude: (rng.IntN(30) - 15) * 10000, PickupLongitude: rng.IntN(30) - 15}
		s.addRide(row, initialFare)
	}
	for phase := 0; phase < 3; phase++ {
		for q := 0; q < 100; q++ {
			chair := matchingChair{Latitude: rng.IntN(400000) - 200000, Longitude: rng.IntN(40) - 20}
			selected := map[string]bool{}
			for _, ride := range s.waitingRides {
				selected[ride.ID] = rng.IntN(3) == 0
			}
			var want *memoryRide
			var best int64
			for _, ride := range s.waitingRides {
				if selected[ride.ID] {
					continue
				}
				d := matchingDistance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
				if want == nil || d < best || (d == best && ridePrecedes(ride, want)) {
					want, best = ride, d
				}
			}
			if got := nearestWaitingPickup(s.waitingPickups, chair, selected); got != want {
				t.Fatalf("phase=%d query=%d nearest=%v want=%v", phase, q, got, want)
			}
		}
		// Remove through the publication path, exercising bucket and node removal.
		for i, ride := range append([]*memoryRide(nil), s.waitingRides...) {
			if phase == 2 || i%2 == 0 {
				row := ride.Ride
				row.ChairID = sql.NullString{String: "c", Valid: true}
				s.apply(stateRecord{Rides: []Ride{row}})
			}
		}
		assertCurrentIndexes(t, s, at)
	}
	if s.waitingPickups != nil {
		t.Fatal("empty waiting set retained pickup nodes")
	}
}

func addGeographicTestChair(s *rideState, id string, latitude int) {
	s.modelSpeeds["model"] = 2
	s.addChair(Chair{ID: id, Model: "model", IsActive: true})
	s.seedCoordinateDistance(id, distanceTotal{ChairID: id, Latitude: sql.NullInt64{Int64: int64(latitude), Valid: true}, Longitude: sql.NullInt64{Valid: true}})
}

func createGeographicTestRide(t *testing.T, s *rideState, id string, at time.Time, pickup, trip int) {
	t.Helper()
	row := Ride{ID: id, UserID: id, CreatedAt: at, UpdatedAt: at, PickupLatitude: pickup, DestinationLatitude: pickup + trip}
	if err := s.commit(stateRecord{CreatedRide: &createdRide{Ride: row, Fare: calculateSale(row)},
		Statuses: []RideStatus{{ID: id + "-matching", RideID: id, Status: "MATCHING", CreatedAt: at}}}); err != nil {
		t.Fatal(err)
	}
}

func TestMatchingGeographicSelectionAndRecovery(t *testing.T) {
	s := memoryTestState(t)
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	at := time.Now().UTC().Add(-time.Second)
	for i := 0; i < 8; i++ {
		addGeographicTestChair(s, fmt.Sprintf("c-%d", i), i*100)
	}
	for i := 0; i < 16; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("far-%02d", i), at.Add(time.Duration(i)*time.Microsecond), 10000+i, 1)
	}
	for i := 0; i < 8; i++ {
		// Nearby long passenger trips must beat distant short passenger trips.
		createGeographicTestRide(t, s, fmt.Sprintf("near-%d", i), at.Add(time.Millisecond), i*100, 100000)
	}
	plan, assignments := testMatchingPlan(t, s)
	if len(plan.rides) != 8 || len(assignments) != 8 {
		t.Fatal("candidate count differs from available supply")
	}
	for i, ride := range plan.rides {
		if i < 2 && ride.ID != fmt.Sprintf("far-%02d", i) || i >= 2 && ride.DestinationLatitude-ride.PickupLatitude != 100000 {
			t.Fatalf("oldest rescue or geographic long-trip selection lost: %+v", plan.rides)
		}
	}
	// A new earlier request changes FIFO but must not invalidate selected rows.
	createGeographicTestRide(t, s, "late-older", at.Add(-time.Second), 20000, 10)
	if retry, err := s.commitMatching(plan, assignments); retry || err != nil {
		t.Fatalf("waiting insertion invalidated a valid plan: retry=%v err=%v", retry, err)
	}
	assertCurrentIndexes(t, s, time.Now())
	if s.rides["late-older"].ChairID.Valid || len(s.matchingChairs) != 0 || s.metrics.series["dispatch_wait"].count != 8 {
		t.Fatal("selection, ACK gating or existing assignment metrics lost")
	}
	// Exercise normal close/reopen from WAL-only creations and arbitrary removals.
	path := s.wal.Name()
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	restored := memoryTestState(t)
	for id, chair := range s.chairs {
		restored.addChair(chair)
		restored.seedCoordinateDistance(id, s.chairDistance(id))
	}
	restored.modelSpeeds = s.modelSpeeds
	restored.wal.Close()
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	restored.wal = f
	if err := restored.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	restored.rebuildIndexes()
	assertCurrentIndexes(t, restored, time.Now())
	for id, ride := range s.rides {
		if !reflect.DeepEqual(ride.Ride, restored.rides[id].Ride) {
			t.Fatalf("recovery changed %s", id)
		}
	}
	if len(restored.matchingChairs) != 0 {
		t.Fatal("recovery released chairs before notification ACK")
	}
	// Completion is still blocked by pending notifications after restart.
	assigned := restored.rides[plan.rides[0].ID]
	restored.addStatus(restored.newStatus(assigned, "COMPLETED", time.Now()))
	if _, ready := restored.matchingChairs[assigned.ChairID.String]; ready {
		t.Fatal("completion without notification ACK released a chair")
	}
	// The derived index must reset on initialize, with no old-generation rides.
	restored.generation, restored.sequence = "initialized", 0
	restored.rides = map[string]*memoryRide{}
	restored.statuses = map[string]RideStatus{}
	restored.rebuildIndexes()
	if err := restored.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	if restored.waitingPickups != nil || len(restored.waitingRides) != 0 {
		t.Fatal("initialize rebuild retained old waiting positions")
	}
	createGeographicTestRide(t, restored, "initialized-ride", time.Now().UTC(), -100, 100)
	if nearestWaitingPickup(restored.waitingPickups, matchingChair{}, nil).ID != "initialized-ride" {
		t.Fatal("new generation pickup index did not accept a request")
	}
}

func TestMatchingGeographicRescue(t *testing.T) {
	now := time.Now().UTC()
	s := memoryTestState(t)
	chairs := make([]matchingChair, 8)
	for i := range chairs {
		chairs[i] = matchingChair{ID: fmt.Sprint(i), Speed: 1}
	}
	for i := 0; i < 6; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("old-%d", i), now.Add(-matchingRideRescueAge), 10000+i, 1000)
	}
	for i := 0; i < 10; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("new-%02d", i), now.Add(-time.Second), i, 10)
	}
	rows := s.selectWaitingRides(8, chairs, now)
	for i := 0; i < 6; i++ {
		if rows[i].ID != fmt.Sprintf("old-%d", i) {
			t.Fatalf("age rescue skipped %d: %+v", i, rows)
		}
	}
	if rows := s.selectWaitingRides(1, chairs[:1], now); rows[0].ID != "old-0" {
		t.Fatal("single-chair plans must make FIFO progress")
	}
	// Sustained nearby arrivals cannot starve distant requests below the age
	// threshold: each four-chair plan still commits at least one oldest request.
	s = memoryTestState(t)
	for i := 0; i < 6; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("far-%d", i), now.Add(-time.Second), 10000+i, 100)
	}
	for batch := 0; batch < 6; batch++ {
		for i := 0; i < 4; i++ {
			addGeographicTestChair(s, fmt.Sprintf("c-%d-%d", batch, i), i)
			createGeographicTestRide(t, s, fmt.Sprintf("near-%d-%d", batch, i), now, i, 1)
		}
		if err := s.match(); err != nil {
			t.Fatal(err)
		}
		if !s.rides[fmt.Sprintf("far-%d", batch)].ChairID.Valid {
			t.Fatalf("nearby arrivals starved FIFO request in batch %d", batch)
		}
		assertCurrentIndexes(t, s, time.Now())
	}
}

func TestMatchingGeographicLimitAndFallback(t *testing.T) {
	now := time.Now().UTC()
	s := memoryTestState(t)
	for i := 0; i < 2*matchingPlanLimit; i++ {
		addGeographicTestChair(s, fmt.Sprintf("c-%03d", i), 0)
		pickup, trip := 10000, 1
		if i >= matchingPlanLimit {
			pickup, trip = 0, 100000
		}
		// Tied pickup positions and ages retain request order, not trip length.
		row := Ride{ID: fmt.Sprintf("r-%03d", i), UserID: "u", CreatedAt: now,
			PickupLatitude: pickup, DestinationLatitude: pickup + trip}
		s.addRide(row, calculateSale(row))
		s.addStatus(RideStatus{ID: row.ID + "-matching", RideID: row.ID, Status: "MATCHING", CreatedAt: now})
	}
	plan, err := s.snapshotMatching("", 0)
	if err != nil || len(plan.rides) != matchingPlanLimit {
		t.Fatalf("geographic solver horizon: %d %v", len(plan.rides), err)
	}
	seen := map[string]bool{}
	for i, ride := range plan.rides {
		want := i
		if i >= matchingPlanLimit/4 {
			want = matchingPlanLimit + i - matchingPlanLimit/4
		}
		if seen[ride.ID] || ride.ID != fmt.Sprintf("r-%03d", want) {
			t.Fatalf("quota, tie order or uniqueness lost at %d: %s", i, ride.ID)
		}
		seen[ride.ID] = true
	}
	// Missing metadata must fill capacity from FIFO rather than locating a
	// nonexistent position at the origin or silently dropping available chairs.
	for id := range s.chairs {
		s.seedCoordinateDistance(id, distanceTotal{})
	}
	plan, err = s.snapshotMatching("", 0)
	if err != nil || len(plan.rides) != matchingPlanLimit {
		t.Fatal("unknown positions lost capacity")
	}
	for i, ride := range plan.rides {
		if ride.ID != fmt.Sprintf("r-%03d", i) {
			t.Fatal("unknown positions displaced FIFO requests")
		}
	}
}

func TestMatchingPlanRejectsIncompleteOrDuplicatePairs(t *testing.T) {
	for _, change := range []string{"partial", "ride-duplicate", "chair-duplicate", "ride-index", "chair-index", "status"} {
		t.Run(change, func(t *testing.T) {
			s := matchingBatchTestState(t, 2, 2)
			plan, assignments := testMatchingPlan(t, s)
			switch change {
			case "partial":
				assignments = assignments[:1]
			case "ride-duplicate":
				assignments[1].rideIndex = assignments[0].rideIndex
			case "chair-duplicate":
				assignments[1].chairIndex = assignments[0].chairIndex
			case "ride-index":
				assignments[0].rideIndex = -1
			case "chair-index":
				assignments[0].chairIndex = len(plan.chairs)
			case "status":
				s.addStatus(s.newStatus(s.rides[plan.rides[0].ID], "COMPLETED", time.Now()))
			}
			if retry, err := s.commitMatching(plan, assignments); !retry || err != nil || s.sequence != 0 {
				t.Fatalf("invalid plan published: retry=%v err=%v sequence=%d", retry, err, s.sequence)
			}
		})
	}
}
