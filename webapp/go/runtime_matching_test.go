package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func testMatchingPlan(t *testing.T, s *rideState) (matchingPlan, []matchingAssignment) {
	t.Helper()
	plan, err := s.snapshotMatching("", 0)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(plan.chairs, func(i, j int) bool { return plan.chairs[i].ID < plan.chairs[j].ID })
	return plan, assignMatchingChairs(plan.rides, plan.chairs)
}

func TestMatchingPlanRevalidation(t *testing.T) {
	for _, reason := range []string{"Generation", "OlderRide", "Inactive", "Unfinished", "PendingACK", "AlreadyAssigned"} {
		t.Run(reason, func(t *testing.T) {
			s := memoryTestState(t)
			s.addChair(Chair{ID: "c", IsActive: true})
			seedMemoryRide(s, "r", "", time.Unix(1700000000, 0))
			plan, assignments := testMatchingPlan(t, s)
			switch reason {
			case "Generation":
				s.generation = "new"
			case "OlderRide":
				seedMemoryRide(s, "older", "", time.Unix(1699999999, 0))
			case "Inactive":
				if err := s.activity("c", false); err != nil {
					t.Fatal(err)
				}
			case "Unfinished", "PendingACK":
				seedMemoryRide(s, "old", "c", time.Unix(1699999999, 0))
				if reason == "PendingACK" {
					s.addStatus(s.newStatus(s.rides["old"], "COMPLETED", time.Now()))
					if len(s.chairCurrent["c"].unfinished) != 0 {
						t.Fatal("test requires only pending notification ACKs")
					}
				}
			case "AlreadyAssigned":
				if err := s.match(); err != nil {
					t.Fatal(err)
				}
			}
			sequence := s.sequence
			retry, err := s.commitMatching(plan, assignments)
			if err != nil || retry != (reason != "Generation") || sequence != s.sequence {
				t.Fatalf("stale plan: retry=%v err=%v sequence=%d->%d", retry, err, sequence, s.sequence)
			}
			if reason != "AlreadyAssigned" && s.rides["r"].ChairID.Valid {
				t.Fatal("stale plan published an assignment")
			}
			if reason == "OlderRide" {
				if err := s.match(); err != nil || !s.rides["older"].ChairID.Valid || s.rides["r"].ChairID.Valid {
					t.Fatal("replanning did not preserve FIFO")
				}
			}
		})
	}
}

func TestMatchingPlanConcurrentCommit(t *testing.T) {
	s := memoryTestState(t)
	for i := 0; i < 128; i++ {
		id := fmt.Sprintf("%03d", i)
		s.addChair(Chair{ID: id, IsActive: true})
		seedMemoryRide(s, id, "", time.Unix(1700000000+int64(i), 0))
	}
	plan, assignments := testMatchingPlan(t, s)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.commitMatching(plan, assignments); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if s.sequence != 1 || len(readTestWAL(t, s)) != 1 || len(s.waitingRides) != 0 {
		t.Fatal("overlapping plans committed twice or lost assignments")
	}
}

func TestMatchingSnapshotAllowsCoordinateUpdates(t *testing.T) {
	s := memoryTestState(t)
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	s.modelSpeeds["model"] = 2
	s.addChair(Chair{ID: "c", Model: "model", IsActive: true})
	s.distances["c"] = distanceTotal{Latitude: sql.NullInt64{Valid: true}, Longitude: sql.NullInt64{Valid: true}}
	seedMemoryRide(s, "r", "", time.Unix(1700000000, 0))
	plan, err := s.snapshotMatching("", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Hold only the planner lock while both the solver and coordinate writes run.
	s.matchingMu.Lock()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := s.coordinate("c", Coordinate{Latitude: 100}); err != nil {
			t.Error(err)
		}
	}()
	assignments := assignMatchingChairs(plan.rides, plan.chairs)
	wg.Wait()
	s.matchingMu.Unlock()
	if plan.chairs[0].Latitude != 0 {
		t.Fatal("snapshot aliases mutable state")
	}
	if retry, err := s.commitMatching(plan, assignments); retry || err != nil {
		t.Fatalf("coordinate updates must not cause perpetual replanning: %v %v", retry, err)
	}
	if got := s.metrics.series["pickup_b1"].sum; got != 55000 {
		t.Fatalf("commit-position pickup = %g, want 55000ms", got)
	}
}

func TestLargeMatchingWALReplayAndMetricContract(t *testing.T) {
	s, restored := memoryTestState(t), memoryTestState(t)
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	for i := 0; i < matchingPlanLimit+1; i++ {
		id := fmt.Sprintf("%03d", i)
		for _, state := range []*rideState{s, restored} {
			state.addChair(Chair{ID: id, IsActive: true, Model: "model"})
			state.modelSpeeds["model"] = 2
			seedMemoryRide(state, id, "", time.Unix(1700000000+int64(i), 0))
			if i != 0 {
				state.distances[id] = distanceTotal{Latitude: sql.NullInt64{Valid: true}, Longitude: sql.NullInt64{Valid: true}}
			}
		}
	}
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	if len(s.waitingRides) != 1 || len(readTestWAL(t, s)[0].Rides) != matchingPlanLimit {
		t.Fatal("plan limit or atomic WAL batch violated")
	}
	batch := s.metrics.series["dispatch_batch"]
	if batch.count != 1 || batch.sum != 256 || batch.buckets[3] != 0 || batch.buckets[4] != 1 ||
		s.metrics.series["pickup_b9_plus"].count != 256 || s.dispatch.unknown[2] != 0 {
		t.Fatal("large known batch was counted as <=64 or unknown")
	}
	// Only the unknown chair remains; it is still usable and never reports zero time.
	if err := s.match(); err != nil || s.dispatch.unknown[0] != 1 || s.metrics.series["pickup_b1"].count != 0 {
		t.Fatal("fallback assignment or unknown estimate violated")
	}
	values := map[string]any{}
	s.metrics.export(values)
	if values["ride_metrics_version"] != 2 || values["pickup_b9_plus_n"] != uint64(256) {
		t.Fatal("new metric contract missing")
	}
	if _, exists := values["pickup_b9_64_n"]; exists {
		t.Fatal("old cohort name misrepresents a large plan")
	}
	// Reopen the WAL as on normal restart, with the same SQL-seeded base state.
	if err := s.wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStateWAL(filepath.Dir(s.wal.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored.wal = reopened
	if err := restored.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	restored.rebuildIndexes()
	for id, ride := range s.rides {
		if !reflect.DeepEqual(ride.Ride, restored.rides[id].Ride) || restored.chairEvents[ride.ChairID.String].pending.Len() == 0 {
			t.Fatalf("WAL replay lost ride or assignment notification: %s", id)
		}
	}
	if restored.sequence != 2 || len(restored.waitingRides) != 0 || len(restored.matchingChairs) != 0 {
		t.Fatal("WAL replay lost reuse gating or assignments")
	}
}
