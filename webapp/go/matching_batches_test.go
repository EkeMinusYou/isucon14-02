package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func matchingBatchTestState(t *testing.T, rides, chairs int) *rideState {
	t.Helper()
	s := memoryTestState(t)
	s.modelSpeeds["model"] = 1
	for i := 0; i < chairs; i++ {
		id := fmt.Sprintf("chair-%03d", i)
		s.addChair(Chair{ID: id, Model: "model", IsActive: true})
		s.seedCoordinateDistance(id, distanceTotal{ChairID: id, Latitude: sql.NullInt64{Int64: 1000, Valid: true}, Longitude: sql.NullInt64{Int64: 1000, Valid: true}})
	}
	// Arrival order differs from FIFO order.
	for i := rides - 1; i >= 0; i-- {
		seedMemoryRide(s, fmt.Sprintf("ride-%03d", i), "", time.Unix(1700000000+int64(i), 0))
	}
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	return s
}

func TestMatchingBatchSaturation(t *testing.T) {
	for _, test := range []struct {
		rides, chairs, assigned, batches int
	}{
		{0, 3 * matchingPlanLimit, 0, 0},
		{3 * matchingPlanLimit, 0, 0, 0},
		{63, 3 * matchingPlanLimit, 63, 1},
		{127, 3 * matchingPlanLimit, 127, 1},
		{matchingPlanLimit - 1, 3 * matchingPlanLimit, matchingPlanLimit - 1, 1},
		{matchingPlanLimit, 3 * matchingPlanLimit, matchingPlanLimit, 1},
		{2*matchingPlanLimit - 1, 3 * matchingPlanLimit, matchingPlanLimit, 1},
		{3 * matchingPlanLimit, matchingPlanLimit - 1, matchingPlanLimit - 1, 1},
		{3 * matchingPlanLimit, matchingPlanLimit, matchingPlanLimit, 1},
		{3 * matchingPlanLimit, 2*matchingPlanLimit - 1, matchingPlanLimit, 1},
		{2 * matchingPlanLimit, 2 * matchingPlanLimit, 2 * matchingPlanLimit, 2},
		{3 * matchingPlanLimit, 3 * matchingPlanLimit, 2 * matchingPlanLimit, 2},
	} {
		t.Run(fmt.Sprintf("Rides%dChairs%d", test.rides, test.chairs), func(t *testing.T) {
			s := matchingBatchTestState(t, test.rides, test.chairs)
			if err := s.match(); err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for i := 0; i < test.rides; i++ {
				ride := s.rides.get(fmt.Sprintf("ride-%03d", i))
				if ride.ChairID.Valid != (i < test.assigned) || (ride.ChairID.Valid && seen[ride.ChairID.String]) {
					t.Fatalf("FIFO or unique assignment violated at ride %d: %+v", i, ride.Ride)
				}
				if ride.ChairID.Valid {
					seen[ride.ChairID.String] = true
				}
			}
			batch := s.metrics.series["dispatch_batch"]
			if batch.count != uint64(test.batches) || batch.sum != float64(test.assigned) || batch.buckets[4] != batch.count || s.metrics.series["dispatch_wait"].count != uint64(test.assigned) {
				t.Fatalf("metrics must count each batch separately: %+v", batch)
			}
			if len(readTestWAL(t, s)) != test.batches || len(s.waitingRides) != test.rides-test.assigned || len(s.matchingChairs) != test.chairs-test.assigned {
				t.Fatal("WAL records or current indexes disagree with assignments")
			}
		})
	}
}

func TestMatchingAdditionalBatchRevalidates(t *testing.T) {
	for _, change := range []string{"inactive", "ack-pending", "waiting-drained", "generation", "not-ready", "failed-wal", "coordinate"} {
		t.Run(change, func(t *testing.T) {
			chairs := 2 * matchingPlanLimit
			if change == "coordinate" {
				chairs++
			}
			s := matchingBatchTestState(t, 2*matchingPlanLimit, chairs)
			generation, err := s.matchBatch("", 0)
			if err != nil {
				t.Fatal(err)
			}
			var remaining string
			for id := range s.matchingChairs {
				remaining = id
				break
			}
			switch change {
			case "inactive":
				if err := s.activity(remaining, false); err != nil {
					t.Fatal(err)
				}
			case "ack-pending":
				// A completed ride with an unacknowledged notification blocks reuse.
				s.walMu.Lock()
				seedMemoryRide(s, "completed", remaining, time.Unix(1600000000, 0))
				s.addStatus(s.newStatus(s.rides.get("completed"), "COMPLETED", time.Unix(1600000001, 0)))
				s.walMu.Unlock()
			case "waiting-drained":
				if err := s.match(); err != nil {
					t.Fatal(err)
				}
			case "generation":
				s.walMu.Lock()
				s.generation = "initialized"
				s.walMu.Unlock()
			case "not-ready":
				s.walMu.Lock()
				s.ready = false
				s.walMu.Unlock()
			case "failed-wal":
				if err := s.wal.Close(); err != nil {
					t.Fatal(err)
				}
			case "coordinate":
				if _, available := s.matchingChairs[fmt.Sprintf("chair-%03d", 2*matchingPlanLimit)]; !available {
					t.Fatal("fixture did not leave the last candidate unused")
				}
				if _, err := s.coordinate(fmt.Sprintf("chair-%03d", 2*matchingPlanLimit), Coordinate{10, 20}); err != nil {
					t.Fatal(err)
				}
			}
			sequence, batches := s.sequence, s.metrics.series["dispatch_batch"].count
			_, err = s.matchBatch(generation, matchingPlanLimit)
			if wantError := change == "not-ready" || change == "failed-wal"; (err != nil) != wantError {
				t.Fatalf("additional batch error: %v", err)
			}
			if change == "coordinate" {
				if s.sequence != sequence+1 || len(s.waitingRides) != 0 || len(s.chairRides.get(fmt.Sprintf("chair-%03d", 2*matchingPlanLimit))) != 1 {
					t.Fatal("additional batch did not use the updated coordinate")
				}
			} else if s.sequence != sequence || s.metrics.series["dispatch_batch"].count != batches {
				t.Fatal("ineligible or failed additional batch published assignments")
			}
		})
	}
}

func TestMatchingTwoBatchesRecoverWAL(t *testing.T) {
	s := matchingBatchTestState(t, 3*matchingPlanLimit, 3*matchingPlanLimit)
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	path := s.wal.Name()
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	restored := matchingBatchTestState(t, 3*matchingPlanLimit, 3*matchingPlanLimit)
	restored.wal.Close()
	wal, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	restored.wal = wal
	if err := restored.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	if restored.sequence != 2 || len(restored.waitingRides) != matchingPlanLimit || len(restored.matchingChairs) != matchingPlanLimit {
		t.Fatal("two committed batches were not recovered")
	}
	for id, ride := range s.rides.all() {
		if restored.rides.get(id).ChairID != ride.ChairID || len(restored.rides.get(id).Statuses) != len(ride.Statuses) {
			t.Fatalf("recovered assignment or notification changed for %s", id)
		}
	}
}
