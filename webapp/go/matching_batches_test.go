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
		s.distances[id] = distanceTotal{ChairID: id, Latitude: sql.NullInt64{Int64: 1000, Valid: true}, Longitude: sql.NullInt64{Int64: 1000, Valid: true}}
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
		{0, 192, 0, 0},
		{192, 0, 0, 0},
		{63, 192, 63, 1},
		{64, 192, 64, 1},
		{127, 192, 64, 1},
		{192, 63, 63, 1},
		{192, 64, 64, 1},
		{192, 127, 64, 1},
		{128, 128, 128, 2},
		{192, 192, 128, 2},
	} {
		t.Run(fmt.Sprintf("Rides%dChairs%d", test.rides, test.chairs), func(t *testing.T) {
			s := matchingBatchTestState(t, test.rides, test.chairs)
			if err := s.match(); err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for i := 0; i < test.rides; i++ {
				ride := s.rides[fmt.Sprintf("ride-%03d", i)]
				if ride.ChairID.Valid != (i < test.assigned) || (ride.ChairID.Valid && seen[ride.ChairID.String]) {
					t.Fatalf("FIFO or unique assignment violated at ride %d: %+v", i, ride.Ride)
				}
				if ride.ChairID.Valid {
					seen[ride.ChairID.String] = true
				}
			}
			batch := s.metrics.series["dispatch_batch"]
			if batch.count != uint64(test.batches) || batch.sum != float64(test.assigned) || batch.buckets[3] != batch.count || s.metrics.series["dispatch_wait"].count != uint64(test.assigned) {
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
			chairs := 128
			if change == "coordinate" {
				chairs++
			}
			s := matchingBatchTestState(t, 128, chairs)
			s.mu.Lock()
			generation := s.generation
			err := s.matchBatchLocked()
			s.mu.Unlock()
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
				s.mu.Lock()
				seedMemoryRide(s, "completed", remaining, time.Unix(1600000000, 0))
				s.addStatus(s.newStatus(s.rides["completed"], "COMPLETED", time.Unix(1600000001, 0)))
				s.mu.Unlock()
			case "waiting-drained":
				if err := s.match(); err != nil {
					t.Fatal(err)
				}
			case "generation":
				s.mu.Lock()
				s.generation = "initialized"
				s.mu.Unlock()
			case "not-ready":
				s.mu.Lock()
				s.ready = false
				s.mu.Unlock()
			case "failed-wal":
				if err := s.wal.Close(); err != nil {
					t.Fatal(err)
				}
			case "coordinate":
				if _, available := s.matchingChairs["chair-128"]; !available {
					t.Fatal("fixture did not leave the last candidate unused")
				}
				if _, err := s.coordinate("chair-128", Coordinate{10, 20}); err != nil {
					t.Fatal(err)
				}
			}
			sequence, batches := s.sequence, s.metrics.series["dispatch_batch"].count
			err = s.matchAdditionalBatch(generation)
			if wantError := change == "not-ready" || change == "failed-wal"; (err != nil) != wantError {
				t.Fatalf("additional batch error: %v", err)
			}
			if change == "coordinate" {
				if s.sequence != sequence+1 || len(s.waitingRides) != 0 || len(s.chairRides["chair-128"]) != 1 {
					t.Fatal("additional batch did not use the updated coordinate")
				}
			} else if s.sequence != sequence || s.metrics.series["dispatch_batch"].count != batches {
				t.Fatal("ineligible or failed additional batch published assignments")
			}
		})
	}
}

func TestMatchingTwoBatchesRecoverWAL(t *testing.T) {
	s := matchingBatchTestState(t, 192, 192)
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	path := s.wal.Name()
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	restored := matchingBatchTestState(t, 192, 192)
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
	if restored.sequence != 2 || len(restored.waitingRides) != 64 || len(restored.matchingChairs) != 64 {
		t.Fatal("two committed batches were not recovered")
	}
	for id, ride := range s.rides {
		if restored.rides[id].ChairID != ride.ChairID || len(restored.rides[id].Statuses) != len(ride.Statuses) {
			t.Fatalf("recovered assignment or notification changed for %s", id)
		}
	}
}
