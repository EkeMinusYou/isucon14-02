package main

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMatchingRescueThresholdAndQuarterQuota(t *testing.T) {
	now := time.Unix(1700000000, 0)
	rides := make([]Ride, 12)
	for i := range rides {
		rides[i].CreatedAt = now.Add(-time.Second)
		if i < 6 {
			rides[i].CreatedAt = now.Add(-matchingRideRescueAge)
		}
	}
	for _, test := range []struct {
		n        int
		at       time.Time
		required int
	}{
		{0, now, 0}, {1, now, 1}, {8, now.Add(-time.Nanosecond), 2},
		{8, now, 6}, {8, now.Add(time.Nanosecond), 6}, {4, now, 4}, {12, now, 6},
	} {
		if got := requiredMatchingRides(rides, test.n, test.at); got != test.required {
			t.Fatalf("n=%d at=%v required=%d want=%d", test.n, test.at, got, test.required)
		}
	}
	for i := range rides {
		rides[i].CreatedAt = now
	}
	for n := 1; n <= len(rides); n++ {
		if got := requiredMatchingRides(rides, n, now); got != (n+3)/4 {
			t.Fatalf("quarter quota n=%d got=%d", n, got)
		}
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
	if plan.count != 8 || len(plan.rides) != 24 || len(assignments) != 8 {
		t.Fatal("candidate count differs from available supply")
	}
	for i, assignment := range assignments {
		ride := plan.rides[assignment.rideIndex]
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
	assigned := restored.rides[plan.rides[assignments[0].rideIndex].ID]
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
	if len(restored.waitingRides) != 0 {
		t.Fatal("initialize rebuild retained old waiting positions")
	}
	createGeographicTestRide(t, restored, "initialized-ride", time.Now().UTC(), -100, 100)
	if len(restored.waitingRides) != 1 || restored.waitingRides[0].ID != "initialized-ride" {
		t.Fatal("new generation pickup index did not accept a request")
	}
}

func TestMatchingGeographicRescue(t *testing.T) {
	now := time.Now().UTC()
	s := memoryTestState(t)
	for i := 0; i < 8; i++ {
		addGeographicTestChair(s, fmt.Sprint(i), 0)
	}
	for i := 0; i < 6; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("old-%d", i), now.Add(-matchingRideRescueAge), 10000+i, 1)
	}
	for i := 0; i < 10; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("new-%02d", i), now.Add(-time.Second), i, 1000)
	}
	plan, assignments := testMatchingPlan(t, s)
	rows := make([]Ride, len(assignments))
	for i, a := range assignments {
		rows[i] = plan.rides[a.rideIndex]
	}
	if plan.required != 6 {
		t.Fatalf("age rescue count=%d", plan.required)
	}
	for i := 0; i < 6; i++ {
		if rows[i].ID != fmt.Sprintf("old-%d", i) {
			t.Fatalf("age rescue skipped %d: %+v", i, rows)
		}
	}
	plan.count, plan.required = 1, 1
	if a := assignMatchingPlan(plan); plan.rides[a[0].rideIndex].ID != "old-0" {
		t.Fatal("single-chair plans must make FIFO progress")
	}
	// Sustained nearby arrivals cannot starve distant requests below the age
	// threshold: each four-chair plan still commits at least one oldest request.
	s = memoryTestState(t)
	for i := 0; i < 6; i++ {
		createGeographicTestRide(t, s, fmt.Sprintf("far-%d", i), now.Add(-time.Second), 10000+i, 1)
	}
	for batch := 0; batch < 6; batch++ {
		for i := 0; i < 4; i++ {
			addGeographicTestChair(s, fmt.Sprintf("c-%d-%d", batch, i), i)
			createGeographicTestRide(t, s, fmt.Sprintf("near-%d-%d", batch, i), now, i, 1000)
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
		// Equal efficiency at tied pickup positions retains request order.
		row := Ride{ID: fmt.Sprintf("r-%03d", i), UserID: "u", CreatedAt: now,
			PickupLatitude: pickup, DestinationLatitude: pickup + trip}
		s.addRide(row, calculateSale(row))
		s.addStatus(RideStatus{ID: row.ID + "-matching", RideID: row.ID, Status: "MATCHING", CreatedAt: now})
	}
	plan, assignments := testMatchingPlan(t, s)
	if plan.count != matchingPlanLimit || len(plan.rides) != 2*matchingPlanLimit || len(assignments) != matchingPlanLimit {
		t.Fatal("candidate pool was prematurely reduced to output horizon")
	}
	for i, a := range assignments {
		ride := plan.rides[a.rideIndex]
		if i < matchingPlanLimit/4 && ride.ID != fmt.Sprintf("r-%03d", i) ||
			i >= matchingPlanLimit/4 && ride.PickupLatitude != 0 {
			t.Fatalf("quota or long-trip joint choice lost at %d: %+v", i, ride)
		}
	}
	// Unknown coordinates are usable but do not create nearby choices.
	for id := range s.chairs {
		s.seedCoordinateDistance(id, distanceTotal{})
	}
	plan, assignments = testMatchingPlan(t, s)
	if len(assignments) != matchingPlanLimit {
		t.Fatal("fallback lost cardinality")
	}
	for i, a := range assignments {
		if plan.rides[a.rideIndex].ID != fmt.Sprintf("r-%03d", i) {
			t.Fatal("unknown positions displaced FIFO")
		}
	}
}

func TestMatchingPlanRejectsIncompleteOrDuplicatePairs(t *testing.T) {
	for _, change := range []string{"partial", "ride-duplicate", "chair-duplicate", "ride-index", "chair-index", "status", "rescue-omitted"} {
		t.Run(change, func(t *testing.T) {
			s := matchingBatchTestState(t, 3, 2)
			plan, assignments := testMatchingPlan(t, s)
			switch change {
			case "rescue-omitted":
				assignments[1].rideIndex = 2
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
