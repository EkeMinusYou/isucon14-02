package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

func partitionSeed(s *rideState, n int) {
	s.addOwner(Owner{ID: "owner", Name: "Owner", ChairRegisterToken: "register"})
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%02d", i)
		s.addUser(User{ID: "u" + id, Firstname: id, AccessToken: "token" + id})
		s.addCoupon(Coupon{UserID: "u" + id, Code: "CP_NEW2024", Discount: 3000, CreatedAt: time.Unix(1, 0)})
		s.addChair(Chair{ID: "c" + id, OwnerID: "owner", IsActive: true, Model: "model"})
	}
	s.modelSpeeds["model"] = 2
}

func awaitPartition(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated business state blocked progress")
	}
}

func TestStatePartitionsIndependentTargets(t *testing.T) {
	s := observedState(t)
	partitionSeed(s, 3)
	r0, _, err := s.createRide("u00", Coordinate{1, 2}, Coordinate{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	r1, _, err := s.createRide("u01", Coordinate{1, 2}, Coordinate{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	_, release := s.lockRide(r0.ID, false)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	done := make(chan error, 1)
	go func() {
		r := s.rides.get(r1.ID)
		if err := s.chairStatus(r.ChairID.String, r.ID, "ENROUTE"); err != nil {
			done <- err
			return
		}
		if _, err := s.coordinate(r.ChairID.String, Coordinate{1, 2}); err != nil {
			done <- err
			return
		}
		if _, err := loadNotifications(context.Background(), "app", r.UserID, ""); err != nil {
			done <- err
			return
		}
		if err := s.registerPaymentToken("u02", "payment"); err != nil {
			done <- err
			return
		}
		if _, _, err := s.createRide("u02", Coordinate{1, 2}, Coordinate{3, 4}); err != nil {
			done <- err
			return
		}
		done <- s.match()
	}()
	awaitPartition(t, done)
	release()
	released = true
	if s.latest(s.rides.get(r1.ID)) != "PICKUP" || s.rides.get(s.userRides.get("u02")[0]).ChairID.Valid == false {
		t.Fatal("independent transitions or assignment were lost")
	}
}

func TestStatePartitionsJointMatchingLocksOnlySelectedRides(t *testing.T) {
	s := observedState(t)
	partitionSeed(s, 4)
	at := time.Now().UTC().Add(-time.Second)
	for i := 0; i < 12; i++ {
		row := Ride{ID: fmt.Sprintf("ride-%02d", i), UserID: fmt.Sprintf("user-%02d", i),
			CreatedAt: at, UpdatedAt: at, DestinationLatitude: i + 1}
		s.addRide(row, calculateSale(row))
		s.addStatus(RideStatus{ID: row.ID + "-matching", RideID: row.ID, Status: "MATCHING", CreatedAt: at})
	}
	plan, assignments := testMatchingPlan(t, s)
	selected := make(map[int]bool, len(assignments))
	for _, a := range assignments {
		selected[a.rideIndex] = true
	}
	omitted := -1
	for i := range plan.rides {
		if !selected[i] {
			omitted = i
			break
		}
	}
	if omitted < 0 || plan.count != 4 || len(assignments) != 4 {
		t.Fatal("fixture requires an unselected candidate")
	}
	// The larger joint candidate pool must not expand business lock ownership.
	_, release := s.lockRide(plan.rides[omitted].ID, false)
	defer release()
	done := make(chan error, 1)
	go func() {
		retry, err := s.commitMatching(plan, assignments)
		if retry && err == nil {
			err = fmt.Errorf("unselected candidate invalidated the plan")
		}
		done <- err
	}()
	awaitPartition(t, done)
	if s.sequence != 1 || s.rides.get(plan.rides[omitted].ID).ChairID.Valid {
		t.Fatal("joint commit changed an unselected ride")
	}
}

func TestStatePartitionsAppendDoesNotSerializePublication(t *testing.T) {
	directory := t.TempDir()
	seed := func(s *rideState) { partitionSeed(s, 2) }
	s := memoryTestStateIn(t, directory, "partitions", seed)
	// Delay one user's publication after its WAL write using its directory shard.
	// Another user's validation, append and publication must still finish.
	if stateMapBucket("u00") == stateMapBucket("u01") {
		t.Fatal("fixture requires separate directory shards")
	}
	shard := &s.appEvents.shards[stateMapBucket("u00")]
	shard.mu.Lock()
	released, firstFinished := false, false
	first := make(chan error, 1)
	defer func() {
		if !released {
			shard.mu.Unlock()
		}
		if !firstFinished {
			<-first
		}
	}()
	go func() { _, _, err := s.createRide("u00", Coordinate{1, 2}, Coordinate{3, 4}); first <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.walMu.RLock()
		sequence := s.sequence
		s.walMu.RUnlock()
		if sequence == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first mutation did not append before publication")
		}
		time.Sleep(time.Millisecond)
	}
	second := make(chan error, 1)
	go func() { _, _, err := s.createRide("u01", Coordinate{1, 2}, Coordinate{3, 4}); second <- err }()
	awaitPartition(t, second)
	select {
	case <-first:
		t.Fatal("first publication bypassed its target lock")
	default:
	}
	shard.mu.Unlock()
	released = true
	awaitPartition(t, first)
	firstFinished = true
	records := readTestWAL(t, s)
	if len(records) != 2 || records[0].Sequence != 1 || records[1].Sequence != 2 {
		t.Fatal("append order is not contiguous")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	restored := memoryTestStateIn(t, directory, "partitions", seed)
	for _, id := range []string{"u00", "u01"} {
		ride := restored.rides.get(restored.userRides.get(id)[0])
		if restored.latest(ride) != "MATCHING" || ride.Fare != 500 || len(restored.unusedCoupons.get(id)) != 0 {
			t.Fatal("restart lost accepted ride or coupon consumption")
		}
	}
}

func TestStatePartitionsConcurrentLifecycleNotificationsAndRecovery(t *testing.T) {
	const n = 8
	directory := t.TempDir()
	seed := func(s *rideState) { partitionSeed(s, n) }
	s := memoryTestStateIn(t, directory, "concurrent-partitions", seed)
	previous := runtimeState
	runtimeState = s
	defer func() { runtimeState = previous }()
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	stop, readDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			rideMeasurementSnapshot()
			_, err := s.searchNearbyChairs(0, 0, 100)
			if err != nil {
				t.Error(err)
				return
			}
			req := httptest.NewRequest(http.MethodGet, "/api/owner/sales", nil)
			req = req.WithContext(context.WithValue(req.Context(), "owner", &Owner{ID: "owner"}))
			ownerGetSales(httptest.NewRecorder(), req)
			req = httptest.NewRequest(http.MethodGet, "/api/app/rides", nil)
			req = req.WithContext(context.WithValue(req.Context(), "user", &User{ID: "u00"}))
			appGetRides(httptest.NewRecorder(), req)
		}
	}()
	readersStopped := false
	defer func() {
		if !readersStopped {
			close(stop)
			<-readDone
		}
	}()
	for round := 0; round < 2; round++ {
		var wg sync.WaitGroup
		ids := make([]string, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				r, _, err := s.createRide(fmt.Sprintf("u%02d", i), Coordinate{1, 2}, Coordinate{3, 4})
				if err != nil {
					t.Error(err)
					return
				}
				ids[i] = r.ID
			}(i)
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		if err := s.match(); err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				r := s.rides.get(id)
				chair, user := r.ChairID.String, r.ownerID
				if chair == "" {
					t.Error("ride not assigned")
					return
				}
				if err := s.chairStatus(chair, id, "ENROUTE"); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.coordinate(chair, Coordinate{1, 2}); err != nil {
					t.Error(err)
					return
				}
				if err := s.chairStatus(chair, id, "CARRYING"); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.coordinate(chair, Coordinate{3, 4}); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.complete(id, 5); err != nil {
					t.Error(err)
					return
				}
				lock := s.chairMutex(chair)
				lock.RLock()
				if s.chairCurrent.get(chair).completionPending != 1 {
					t.Error("completion response was published early")
				}
				lock.RUnlock()
				// App and chair ACKs mutate the same history rows concurrently.
				var acks sync.WaitGroup
				for kind, recipient := range map[string]string{"app": user, "chair": chair} {
					acks.Add(1)
					go func(kind, recipient string) {
						defer acks.Done()
						events, err := loadNotifications(context.Background(), kind, recipient, "")
						if err != nil {
							t.Error(err)
							return
						}
						want := []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"}
						if len(events) != 6 {
							t.Errorf("pending history length: %d", len(events))
							return
						}
						for i, event := range events {
							status := ""
							if kind == "chair" {
								status = event.data.(chairGetNotificationResponseData).Status
							} else {
								status = event.data.(appGetNotificationResponseData).Status
							}
							if status != want[i] {
								t.Errorf("out of order: %s != %s", status, want[i])
							}
							for j := 0; j < 2; j++ {
								if err := acknowledgeNotificationGeneration(context.Background(), kind, event.statusID, event.generation); err != nil {
									t.Error(err)
								}
							}
						}
						replay, err := loadNotifications(context.Background(), kind, recipient, events[0].eventID)
						if err != nil || len(replay) != 5 {
							t.Errorf("Last-Event-ID suffix: %d %v", len(replay), err)
						}
					}(kind, recipient)
				}
				acks.Wait()
				s.finishCompletionResponse(id)
			}(id)
		}
		wg.Wait()
		if t.Failed() {
			return
		}
	}
	close(stop)
	<-readDone
	readersStopped = true
	assertCurrentIndexes(t, s, time.Now().Add(time.Second))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	restored := memoryTestStateIn(t, directory, "concurrent-partitions", seed)
	runtimeState = restored
	for i := 0; i < n; i++ {
		user, chair := fmt.Sprintf("u%02d", i), fmt.Sprintf("c%02d", i)
		if restored.stats.get(chair).Count != 2 || restored.stats.get(chair).Evaluation != 10 || restored.continuingRide(user) {
			t.Fatal("restart lost completed state")
		}
		events, err := loadNotifications(context.Background(), "app", user, "")
		if err != nil || len(events) != 1 {
			t.Fatal("restored ACK state is inconsistent")
		}
		first := s.appEvents.get(user).ids[0]
		replay, err := loadNotifications(context.Background(), "app", user, first)
		if err != nil || len(replay) != 11 {
			t.Fatalf("restored full ordered history: %d %v", len(replay), err)
		}
		for j, event := range replay {
			if event.data.(appGetNotificationResponseData).Status != []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED", "MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"}[j] {
				t.Fatal("restored cross-ride event order changed")
			}
		}
		if !reflect.DeepEqual(s.chairRides.get(chair), restored.chairRides.get(chair)) {
			t.Fatal("restart changed chair history order")
		}
	}
}

func TestStatePartitionsRegistrationAndAuthentication(t *testing.T) {
	s := observedState(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u, err := s.registerUser(appPostUsersRequest{Username: fmt.Sprintf("user%d", i), FirstName: "First", LastName: "Last", DateOfBirth: "2000-01-01"})
			if err != nil {
				t.Error(err)
				return
			}
			req := httptest.NewRequest(http.MethodGet, "/api/app/rides", nil)
			req.AddCookie(&http.Cookie{Name: "app_session", Value: u.AccessToken})
			res := httptest.NewRecorder()
			runtimeStateMiddleware(appAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Context().Value("user").(*User).ID != u.ID {
					t.Error("auth returned another user")
				}
				w.WriteHeader(http.StatusNoContent)
			}))).ServeHTTP(res, req)
			if res.Code != http.StatusNoContent {
				t.Errorf("auth during registration: %d", res.Code)
			}
			if _, _, err := s.createRide(u.ID, Coordinate{1, 2}, Coordinate{3, 4}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if s.users.len() != 16 || s.rides.len() != 16 || s.couponCounts.get(s.accountKey("CP_NEW2024")) != 16 {
		t.Fatal("concurrent registration lost accounts or coupon counts")
	}
}
