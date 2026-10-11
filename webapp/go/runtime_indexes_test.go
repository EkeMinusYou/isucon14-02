package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

// Full history is an independent oracle for the incremental working sets.
func assertCurrentIndexes(t *testing.T, s *rideState, at time.Time) {
	t.Helper()
	users := map[string]int{}
	var waiting []*memoryRide
	for _, ride := range s.rides.all() {
		if !ride.ChairID.Valid {
			waiting = append(waiting, ride)
		}
		if s.latest(ride) != "COMPLETED" {
			users[ride.UserID]++
		}
	}
	sort.Slice(waiting, func(i, j int) bool {
		if waiting[i].CreatedAt.Equal(waiting[j].CreatedAt) {
			return waiting[i].ID < waiting[j].ID
		}
		return waiting[i].CreatedAt.Before(waiting[j].CreatedAt)
	})
	if len(waiting) != len(s.waitingRides) {
		t.Fatal("waiting count differs from history")
	}
	for i, ride := range waiting {
		if s.waitingRides[i].ID != ride.ID {
			t.Fatal("waiting FIFO differs from history")
		}
	}
	for id := range s.userRides.all() {
		if s.userActiveRides.get(id) != users[id] || s.continuingRide(id) != (users[id] != 0) {
			t.Fatal("active user rides differ from history")
		}
	}
	for id, chair := range s.chairs.all() {
		available, nearby := chair.IsActive, chair.IsActive
		var latestUnfinished *memoryRide
		for _, rideID := range s.chairRides.get(id) {
			ride := s.rides.get(rideID)
			if s.latest(ride) != "COMPLETED" {
				available, nearby = false, false
				if latestUnfinished == nil || ride.CreatedAt.After(latestUnfinished.CreatedAt) ||
					(ride.CreatedAt.Equal(latestUnfinished.CreatedAt) && ride.ID > latestUnfinished.ID) {
					latestUnfinished = ride
				}
			} else {
				completed := s.statuses.get(ride.Statuses[len(ride.Statuses)-1])
				if ride.CompletionResponsePending || at.Before(ride.NearbyAvailableAt) || at.Before(completed.CreatedAt.Add(nearbyCompletionGrace)) {
					nearby = false
				}
			}
			for _, statusID := range ride.Statuses {
				if s.statuses.get(statusID).ChairSentAt == nil {
					available = false
				}
			}
		}
		_, indexedAvailable := s.matchingChairs[id]
		gotNearby, err := s.nearbyAvailable(id, at)
		missing := chair.IsActive && latestUnfinished != nil && s.latest(latestUnfinished) == ""
		if available != indexedAvailable || nearby != gotNearby || (err != nil) != missing {
			t.Fatalf("chair %s: match=%v/%v nearby=%v/%v missing=%v err=%v", id, indexedAvailable, available, gotNearby, nearby, missing, err)
		}
	}
}

func TestCurrentIndexesFIFOAndReplay(t *testing.T) {
	s := memoryTestState(t)
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState = previous })
	s.addChair(Chair{ID: "c", IsActive: true})
	at := time.Now().UTC().Add(-time.Hour)
	// An older SQL transaction can publish after a younger request. Tied
	// timestamps must retain the existing ID tie break as well.
	for _, seed := range []struct {
		id string
		at time.Time
	}{{"new", at.Add(time.Second)}, {"z-old", at}, {"a-old", at}} {
		row := Ride{ID: seed.id, UserID: seed.id, CreatedAt: seed.at, UpdatedAt: seed.at}
		if err := s.commit(stateRecord{CreatedRide: &createdRide{Ride: row, Fare: initialFare}, Statuses: []RideStatus{{ID: seed.id + "-0", RideID: seed.id, Status: "MATCHING", CreatedAt: seed.at}}}); err != nil {
			t.Fatal(err)
		}
		assertCurrentIndexes(t, s, at)
	}
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	if s.rides.get("a-old").ChairID.String != "c" || s.rides.get("z-old").ChairID.Valid || s.rides.get("new").ChairID.Valid {
		t.Fatal("late publication changed FIFO")
	}
	for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
		if err := s.commit(stateRecord{Statuses: []RideStatus{s.newStatus(s.rides.get("a-old"), status, time.Now().UTC())}}); err != nil {
			t.Fatal(err)
		}
		assertCurrentIndexes(t, s, time.Now())
	}
	if _, err := s.complete("a-old", 5); err != nil {
		t.Fatal(err)
	}
	assertCurrentIndexes(t, s, time.Now().Add(time.Hour))
	s.finishCompletionResponse("a-old")
	s.finishCompletionResponse("a-old")
	ready := s.rides.get("a-old").NearbyAvailableAt
	assertCurrentIndexes(t, s, ready.Add(-time.Nanosecond))
	assertCurrentIndexes(t, s, ready)
	ids := s.rides.get("a-old").Statuses
	// Completion ACK alone must not allow reuse while an older event is pending.
	for i := len(ids) - 1; i > 0; i-- {
		if err := acknowledgeNotification(context.Background(), "chair", ids[i]); err != nil {
			t.Fatal(err)
		}
		assertCurrentIndexes(t, s, ready)
	}
	if err := s.match(); err != nil || s.rides.get("z-old").ChairID.Valid {
		t.Fatal("completion ACK bypassed pending older events")
	}
	for i := 0; i < 2; i++ {
		if err := acknowledgeNotification(context.Background(), "chair", ids[0]); err != nil {
			t.Fatal(err)
		}
	}
	assertCurrentIndexes(t, s, ready)
	// Exercise unprojected replay with SQL already containing one request.
	// Its CreatedRide and MATCHING records must not duplicate any index entry.
	restored := memoryTestState(t)
	restored.addChair(s.chairs.get("c"))
	row := s.rides.get("new").Ride
	restored.addRide(row, initialFare)
	restored.addStatus(s.statuses.get("new-0"))
	restored.wal = s.wal
	if err := restored.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	restored.rebuildIndexes()
	assertCurrentIndexes(t, restored, ready)
	if restored.sequence != s.sequence || restored.rides.len() != 3 || restored.chairEvents.get("c").pending.Len() != 0 {
		t.Fatal("replay lost or duplicated state")
	}
	if err := restored.match(); err != nil || restored.rides.get("z-old").ChairID.String != "c" {
		t.Fatal("replayed ACKs did not reopen FIFO matching")
	}
	assertCurrentIndexes(t, restored, ready)
}

func TestIndexedNotificationsSparseACKAndCursor(t *testing.T) {
	s := memoryTestState(t)
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState = previous })
	s.addChair(Chair{ID: "c", IsActive: true})
	at := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("r%d", i)
		seedMemoryRide(s, id, "c", at.Add(time.Duration(i)*time.Minute))
		for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"} {
			s.addStatus(s.newStatus(s.rides.get(id), status, at.Add(time.Duration(i)*time.Minute)))
		}
	}
	for _, kind := range []string{"app", "chair"} {
		id := "u"
		if kind == "chair" {
			id = "c"
		}
		queue := s.notificationQueue(kind, id)
		for i := len(queue.ids) - 1; i >= 0; i-- {
			if i%5 == 0 {
				continue
			}
			if err := acknowledgeNotification(context.Background(), kind, queue.ids[i]); err != nil {
				t.Fatal(err)
			}
		}
		check := func() {
			t.Helper()
			for _, cursor := range append([]string{"", "unknown", "foreign-recipient"}, queue.ids...) {
				position := -1
				for i, statusID := range queue.ids {
					if statusID == cursor {
						position = i
					}
				}
				var want []string
				for i, statusID := range queue.ids {
					status := s.statuses.get(statusID)
					pending := status.AppSentAt == nil
					if kind == "chair" {
						pending = status.ChairSentAt == nil
					}
					if pending || (position >= 0 && i > position) {
						want = append(want, statusID)
					}
				}
				events, err := loadNotifications(context.Background(), kind, id, cursor)
				var got []string
				for _, event := range events {
					if event.statusID != "" {
						got = append(got, event.statusID)
					}
				}
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s cursor=%q: %v != %v (%v)", kind, cursor, got, want, err)
				}
			}
		}
		check()
		s.rebuildIndexes()
		queue = s.notificationQueue(kind, id)
		check()
		for _, statusID := range queue.ids {
			if err := acknowledgeNotification(context.Background(), kind, statusID); err != nil {
				t.Fatal(err)
			}
		}
		check()
		events, err := loadNotifications(context.Background(), kind, id, "")
		if err != nil || len(events) != 1 || events[0].statusID != "" || events[0].eventID != queue.ids[len(queue.ids)-1] {
			t.Fatal("fully ACKed reconnect did not return latest state")
		}
	}
	assertCurrentIndexes(t, s, time.Now())
}

func TestCurrentIndexesMultipleUnfinishedAndPublication(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	at := time.Now().UTC().Add(-time.Hour)
	seedMemoryRide(s, "old", "c", at)
	seedMemoryRide(s, "new", "c", at.Add(time.Minute))
	s.addRide(Ride{ID: "missing", UserID: "u", ChairID: sql.NullString{String: "c", Valid: true}, CreatedAt: at.Add(-time.Minute)}, initialFare)
	for _, id := range []string{"new", "old"} {
		for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
			s.addStatus(s.newStatus(s.rides.get(id), status, at.Add(2*time.Minute)))
		}
		if _, err := s.complete(id, 5); err != nil {
			t.Fatal(err)
		}
		assertCurrentIndexes(t, s, time.Now().Add(time.Hour))
	}
	// Finishing one response must not release another pending completion.
	s.finishCompletionResponse("new")
	s.addStatus(s.newStatus(s.rides.get("missing"), "COMPLETED", at.Add(3*time.Minute)))
	assertCurrentIndexes(t, s, time.Now().Add(time.Hour))
	s.rebuildIndexes()
	assertCurrentIndexes(t, s, time.Now().Add(time.Hour))
	s.finishCompletionResponse("old")
	assertCurrentIndexes(t, s, s.rides.get("old").NearbyAvailableAt)
	if err := s.activity("c", false); err != nil {
		t.Fatal(err)
	}
	assertCurrentIndexes(t, s, time.Now().Add(time.Hour))
}

func TestIndexedACKRejectsOldGenerationAndFailedWAL(t *testing.T) {
	s := memoryTestState(t)
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState = previous })
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "r", "c", time.Now().Add(-time.Hour))
	s.addStatus(s.newStatus(s.rides.get("r"), "COMPLETED", time.Now().Add(-time.Minute)))
	queue := s.chairEvents.get("c")
	ids := s.rides.get("r").Statuses
	if err := acknowledgeNotificationGeneration(context.Background(), "chair", ids[1], "old-generation"); err == nil {
		t.Fatal("old generation ACK accepted")
	}
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeNotificationGeneration(context.Background(), "chair", ids[1], s.generation); err == nil {
		t.Fatal("ACK accepted without WAL")
	}
	if queue.pending.Len() != 2 || s.statuses.get(ids[1]).ChairSentAt != nil {
		t.Fatal("rejected ACK consumed an indexed notification")
	}
	assertCurrentIndexes(t, s, time.Now())
}
