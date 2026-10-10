package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestMatchingTravelTime(t *testing.T) {
	type candidate struct {
		id, model           string
		latitude, longitude int64
		positioned          bool
	}
	for _, test := range []struct {
		name    string
		chairs  []candidate
		pickups []Coordinate
		want    []string
	}{
		{"BatchUsesAllKnownCandidates", []candidate{{"a-near", "model", 0, 3, true}, {"z-fast", "fast", 4, 4, true}, {"unknown-position", "model", 0, 0, false}, {"unknown-model", "missing", 0, 0, true}}, []Coordinate{{0, 0}, {4, 4}}, []string{"a-near", "z-fast"}},
		{"ManhattanFractionalTime", []candidate{{"a-diagonal", "fast", -13, -23, true}, {"z-nearest", "fast", -10, -25, true}}, []Coordinate{{-10, -20}}, []string{"z-nearest"}},
		{"UnknownMetadataFallback", []candidate{{"no-position", "model", 0, 0, false}, {"no-model", "missing", 0, 0, true}, {"zero-speed", "stopped", 0, 0, true}}, []Coordinate{{0, 0}, {0, 0}, {0, 0}}, []string{"no-model", "no-position", "zero-speed"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := memoryTestState(t)
			s.modelSpeeds = map[string]int{"model": 1, "fast": 4, "stopped": 0}
			for _, c := range test.chairs {
				s.addChair(Chair{ID: c.id, Model: c.model, IsActive: true})
				s.distances[c.id] = distanceTotal{ChairID: c.id, Latitude: sql.NullInt64{Int64: c.latitude, Valid: c.positioned}, Longitude: sql.NullInt64{Int64: c.longitude, Valid: c.positioned}}
			}
			for i, pickup := range test.pickups {
				id := fmt.Sprintf("ride-%d", i)
				seedMemoryRide(s, id, "", time.Unix(1700000000+int64(i), 0))
				s.rides[id].PickupLatitude, s.rides[id].PickupLongitude = pickup.Latitude, pickup.Longitude
				s.rides[id].DestinationLatitude, s.rides[id].DestinationLongitude = pickup.Latitude, pickup.Longitude
			}
			if err := s.match(); err != nil {
				t.Fatal(err)
			}
			for i, want := range test.want {
				if got := s.rides[fmt.Sprintf("ride-%d", i)].ChairID.String; got != want {
					t.Fatalf("ride %d chair=%s want=%s", i, got, want)
				}
			}
		})
	}
}

func TestMatching(t *testing.T) {
	t.Run("TravelTimeAssignmentPreservesRideCoordinates", func(t *testing.T) {
		s := memoryTestState(t)
		s.modelSpeeds = map[string]int{"fast": 10, "slow": 2}
		for _, model := range []string{"fast", "slow"} {
			s.addChair(Chair{ID: model, Model: model, IsActive: true})
			s.distances[model] = distanceTotal{ChairID: model, Latitude: sql.NullInt64{Valid: true}, Longitude: sql.NullInt64{Valid: true}}
		}
		seedMemoryRide(s, "short", "", time.Unix(1700000000, 0))
		seedMemoryRide(s, "long", "", time.Unix(1700000001, 0))
		s.rides["short"].Ride.PickupLatitude, s.rides["short"].Ride.PickupLongitude = 0, 0
		s.rides["short"].Ride.DestinationLatitude, s.rides["short"].Ride.DestinationLongitude = 1, 1
		s.rides["long"].Ride.PickupLatitude, s.rides["long"].Ride.PickupLongitude = 0, 0
		s.rides["long"].Ride.DestinationLatitude, s.rides["long"].Ride.DestinationLongitude = 100, 0
		beforeShort, beforeLong := s.rides["short"].Ride, s.rides["long"].Ride
		if err := s.match(); err != nil {
			t.Fatal(err)
		}
		for _, pair := range []struct {
			before Ride
			chair  string
		}{{beforeShort, "slow"}, {beforeLong, "fast"}} {
			got := s.rides[pair.before.ID].Ride
			if got.ChairID.String != pair.chair || got.PickupLatitude != pair.before.PickupLatitude || got.PickupLongitude != pair.before.PickupLongitude || got.DestinationLatitude != pair.before.DestinationLatitude || got.DestinationLongitude != pair.before.DestinationLongitude {
				t.Fatalf("assignment changed coordinates or selected wrong chair: %+v", got)
			}
		}
	})
	t.Run("OldestFirstBoundedBatchAndConcurrentRequests", func(t *testing.T) {
		s := memoryTestState(t)
		previous := runtimeState
		runtimeState = s
		t.Cleanup(func() { notifications.reset(); runtimeState = previous })
		for i := 0; i < 2*matchingPlanLimit+2; i++ {
			id := fmt.Sprintf("chair-%03d", i)
			s.addChair(Chair{ID: id, IsActive: true})
			seedMemoryRide(s, fmt.Sprintf("ride-%03d", i), "", time.Unix(1700000000+int64(i), 0))
		}
		appSub, stop := notifications.subscribe("app", "u")
		defer stop()
		request := func() int {
			res := httptest.NewRecorder()
			internalGetMatching(res, httptest.NewRequest(http.MethodGet, "/api/internal/matching", nil))
			return res.Code
		}
		if code := request(); code != http.StatusNoContent {
			t.Fatal(code)
		}
		for i := 0; i < 2*matchingPlanLimit+2; i++ {
			assigned := s.rides[fmt.Sprintf("ride-%03d", i)].ChairID.Valid
			if assigned != (i < 2*matchingPlanLimit) {
				t.Fatal("batch must match oldest rides first")
			}
		}
		if len(appSub.wake) != 1 {
			t.Fatal("assignment did not wake the app")
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if code := request(); code != http.StatusNoContent {
					t.Error(code)
				}
			}()
		}
		wg.Wait()
		assigned := map[string]bool{}
		for _, ride := range s.rides {
			if !ride.ChairID.Valid || assigned[ride.ChairID.String] {
				t.Fatal("missing or duplicate assignment")
			}
			assigned[ride.ChairID.String] = true
			if len(ride.Statuses) != 1 {
				t.Fatal("assignment invented a transition")
			}
		}
	})
	t.Run("CompletionNotificationGatesReuse", func(t *testing.T) {
		s := memoryTestState(t)
		previous := runtimeState
		runtimeState = s
		t.Cleanup(func() { runtimeState = previous })
		s.addChair(Chair{ID: "c", IsActive: true})
		seedMemoryRide(s, "old", "c", time.Now().Add(-time.Hour))
		for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"} {
			s.addStatus(s.newStatus(s.rides["old"], status, time.Now().UTC()))
		}
		seedMemoryRide(s, "next", "", time.Now().UTC())
		if err := s.match(); err != nil {
			t.Fatal(err)
		}
		if s.rides["next"].ChairID.Valid {
			t.Fatal("chair reused before consuming completion")
		}
		events, err := loadNotifications(context.Background(), "chair", "c", "")
		if err != nil || len(events) != 6 {
			t.Fatalf("completion history %v %v", events, err)
		}
		for _, event := range events {
			if err := acknowledgeNotification(context.Background(), "chair", event.statusID); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.match(); err != nil {
			t.Fatal(err)
		}
		if s.rides["next"].ChairID.String != "c" {
			t.Fatal("chair not reused after completion was consumed")
		}
	})
	t.Run("InactiveAndFailedAppendCannotAssign", func(t *testing.T) {
		s := memoryTestState(t)
		s.addChair(Chair{ID: "c", IsActive: true})
		seedMemoryRide(s, "r", "", time.Now().UTC())
		if err := s.activity("c", false); err != nil {
			t.Fatal(err)
		}
		if err := s.match(); err != nil {
			t.Fatal(err)
		}
		if s.rides["r"].ChairID.Valid {
			t.Fatal("inactive chair assigned")
		}
		if err := s.activity("c", true); err != nil {
			t.Fatal(err)
		}
		s.wal.Close()
		notifications.reset()
		sub, stop := notifications.subscribe("chair", "c")
		defer stop()
		if err := s.match(); err == nil {
			t.Fatal("assignment accepted without WAL")
		}
		if s.rides["r"].ChairID.Valid || len(sub.wake) != 0 {
			t.Fatal("failed WAL published assignment")
		}
	})
}
