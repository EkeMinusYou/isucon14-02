package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppGetNearbyChairs(t *testing.T) {
	s := memoryTestState(t)
	previousState, previousDB := runtimeState, db
	runtimeState, db = s, nil
	t.Cleanup(func() { runtimeState, db = previousState, previousDB })
	chair := func(id string, active bool, latitude, longitude int) {
		s.addChair(Chair{ID: id, Name: "name-" + id, Model: "model", IsActive: active})
		s.distances[id] = distanceTotal{ChairID: id, Latitude: sql.NullInt64{Int64: int64(latitude), Valid: true}, Longitude: sql.NullInt64{Int64: int64(longitude), Valid: true}, UpdatedAt: sql.NullTime{Time: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC), Valid: true}}
	}
	coordinate := func(id string, latitude, longitude int) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/api/chair/coordinate",
			strings.NewReader(fmt.Sprintf(`{"latitude":%d,"longitude":%d}`, latitude, longitude)))
		request = request.WithContext(context.WithValue(request.Context(), "chair", &Chair{ID: id}))
		response := httptest.NewRecorder()
		chairPostCoordinate(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("coordinate: %d %s", response.Code, response.Body.String())
		}
	}
	ride := func(id, chairID, status string, second int) {
		at := time.Date(2026, 1, 1, 0, 0, second, 0, time.UTC)
		s.addRide(Ride{ID: id, ChairID: sql.NullString{String: chairID, Valid: true}, CreatedAt: at, UpdatedAt: at}, initialFare)
		if status != "" {
			s.addStatus(RideStatus{ID: "status-" + id, RideID: id, Status: status, CreatedAt: at})
		}
	}
	check := func(query string, expectedStatus int, expected map[string]Coordinate) {
		t.Helper()
		response := httptest.NewRecorder()
		appGetNearbyChairs(response, httptest.NewRequest(http.MethodGet, "/api/app/nearby-chairs?"+query, nil))
		if response.Code != expectedStatus {
			t.Fatalf("status = %d, want %d: %s", response.Code, expectedStatus, response.Body.String())
		}
		if expectedStatus != http.StatusOK {
			return
		}
		var result appGetNearbyChairsResponse
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Chairs == nil || result.RetrievedAt <= 0 || len(result.Chairs) != len(expected) {
			t.Fatalf("unexpected response: %+v", result)
		}
		seen := make(map[string]bool)
		for _, got := range result.Chairs {
			want, ok := expected[got.ID]
			if !ok || seen[got.ID] || got.CurrentCoordinate != want || got.Name != "name-"+got.ID || got.Model != "model" {
				t.Fatalf("unexpected chair: %+v", got)
			}
			seen[got.ID] = true
		}
	}

	t.Run("availability and latest coordinates", func(t *testing.T) {
		chair("idle", true, 0, 0)
		chair("boundary", true, -20, 30)
		chair("outside", true, 20, 31)
		chair("inactive", false, 0, 0)
		chair("finished", true, 0, 0)
		ride("finished-old", "finished", "ARRIVED", 1)
		s.addStatus(RideStatus{ID: "finished-new", RideID: "finished-old", Status: "COMPLETED", CreatedAt: time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)})
		ride("finished-next", "finished", "COMPLETED", 3)
		// A completed newer ride must not hide an older unfinished ride.
		chair("busy", true, 0, 0)
		ride("busy-old", "busy", "CARRYING", 1)
		ride("busy-new", "busy", "COMPLETED", 3)
		for i, status := range []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
			id := fmt.Sprintf("busy-%d", i)
			chair(id, true, 0, 0)
			ride("ride-"+id, id, status, 1)
		}
		s.addChair(Chair{ID: "no-location", Name: "name-no-location", Model: "model", IsActive: true})
		chair("moved", true, 100, 100)
		coordinate("moved", -1, -1)
		chair("moved-away", true, 0, 0)
		coordinate("moved-away", 100, 100)
		check("latitude=0&longitude=0", http.StatusOK, map[string]Coordinate{
			"idle": {}, "boundary": {-20, 30}, "finished": {}, "moved": {-1, -1},
		})
		check("latitude=0&longitude=0&distance=0", http.StatusOK, map[string]Coordinate{"idle": {}, "finished": {}})
		check("latitude=0&longitude=0&distance=-1", http.StatusOK, map[string]Coordinate{})
	})
	t.Run("fresh state without SQL", func(t *testing.T) {
		coordinate("idle", 200, 200)
		ride("moved-busy", "moved", "ENROUTE", 4)
		for i := 0; i < 100; i++ {
			chair(fmt.Sprintf("extra-%d", i), true, 500, 500)
		}
		check("latitude=0&longitude=0", http.StatusOK, map[string]Coordinate{"boundary": {-20, 30}, "finished": {}})
	})
	t.Run("missing status keeps existing error behavior", func(t *testing.T) {
		// The old loop skips older missing states after finding a busy ride.
		ride("busy-missing", "busy", "", 0)
		check("latitude=0&longitude=0", http.StatusOK, map[string]Coordinate{"boundary": {-20, 30}, "finished": {}})
		chair("missing-status", true, 0, 0)
		ride("missing-status-ride", "missing-status", "", 1)
		check("latitude=0&longitude=0", http.StatusInternalServerError, nil)
	})
	for _, query := range []string{"", "latitude=x&longitude=0", "latitude=0&longitude=x", "latitude=0&longitude=0&distance=x"} {
		check(query, http.StatusBadRequest, nil)
	}
}

func TestNearbyCompletionPublication(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "r", "c", time.Now().Add(-time.Hour))
	for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
		s.addStatus(s.newStatus(s.rides["r"], status, time.Now().UTC()))
	}
	if _, err := s.complete("r", 5); err != nil {
		t.Fatal(err)
	}
	// A blocked evaluation response must remain hidden even beyond the grace.
	if available, err := s.nearbyAvailable("c", time.Now().Add(time.Hour)); available || err != nil {
		t.Fatalf("unflushed completion became available: %v %v", available, err)
	}
	s.finishCompletionResponse("r")
	ready := s.rides["r"].NearbyAvailableAt
	for _, elapsed := range []time.Duration{time.Millisecond, 6 * time.Millisecond, nearbyCompletionGrace - time.Nanosecond} {
		if available, err := s.nearbyAvailable("c", ready.Add(-nearbyCompletionGrace+elapsed)); available || err != nil {
			t.Fatalf("completion published at %v: %v %v", elapsed, available, err)
		}
	}
	if available, err := s.nearbyAvailable("c", ready); !available || err != nil {
		t.Fatalf("completion stayed hidden after grace: %v %v", available, err)
	}
	// A newly assigned ride must never be hidden by the old completion grace.
	seedMemoryRide(s, "next", "c", time.Now().UTC())
	if available, err := s.nearbyAvailable("c", ready.Add(time.Hour)); available || err != nil {
		t.Fatalf("busy chair became available: %v %v", available, err)
	}
}

type heldEvaluationResponse struct {
	*httptest.ResponseRecorder
	flushing chan struct{}
	release  chan struct{}
}

func (w *heldEvaluationResponse) FlushError() error {
	close(w.flushing)
	<-w.release
	w.ResponseRecorder.Flush()
	return nil
}

func TestNearbyWhileEvaluationResponseIsBlocked(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "r", "2026-01-01", true)
	for i, status := range []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
		insertTestStatus("r", fmt.Sprintf("s%d", i), status)
	}
	payment := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer payment.Close()
	db.MustExec(`INSERT INTO settings (name, value) VALUES ('payment_gateway_url', ?)`, payment.URL)
	db.MustExec(`INSERT INTO payment_tokens (user_id, token) VALUES ('u', 'payment')`)
	startTestRuntime(t)
	if _, err := runtimeState.coordinate("c", Coordinate{}); err != nil {
		t.Fatal(err)
	}
	response := &heldEvaluationResponse{ResponseRecorder: httptest.NewRecorder(), flushing: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(response.release) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"evaluation":5}`))
		req.SetPathValue("ride_id", "r")
		req = req.WithContext(context.WithValue(req.Context(), "user", &User{ID: "u"}))
		appPostRideEvaluatation(response, req)
	}()
	defer func() { release(); <-done }()
	select {
	case <-response.flushing:
	case <-done:
		t.Fatalf("evaluation returned before flush: %d %s", response.Code, response.Body.String())
	case <-time.After(5 * time.Second):
		t.Fatal("evaluation did not reach response flush")
	}
	res := httptest.NewRecorder()
	appGetNearbyChairs(res, httptest.NewRequest(http.MethodGet, "/api/app/nearby-chairs?latitude=0&longitude=0", nil))
	var nearby appGetNearbyChairsResponse
	if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &nearby) != nil || len(nearby.Chairs) != 0 {
		t.Fatalf("unflushed evaluation exposed chair: %d %s", res.Code, res.Body.String())
	}
	release()
	<-done
	if response.Code != http.StatusOK || !response.Flushed {
		t.Fatalf("evaluation response was not flushed: %+v", response)
	}
	s := runtimeState
	s.mu.RLock()
	ready := s.rides["r"].NearbyAvailableAt
	available, err := s.nearbyAvailable("c", ready)
	s.mu.RUnlock()
	if !available || err != nil {
		t.Fatalf("flushed completion did not reopen chair: %v %v", available, err)
	}
	if err := s.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	completed := s.statuses[s.rides["r"].Statuses[len(s.rides["r"].Statuses)-1]].CreatedAt
	if available, err := s.nearbyAvailable("c", completed.Add(6*time.Millisecond)); available || err != nil {
		t.Fatalf("recovery lost completion grace: %v %v", available, err)
	}
	if available, err := s.nearbyAvailable("c", completed.Add(nearbyCompletionGrace)); !available || err != nil {
		t.Fatalf("recovered completion stayed hidden: %v %v", available, err)
	}
}
