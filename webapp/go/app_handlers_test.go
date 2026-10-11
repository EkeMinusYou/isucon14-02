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
		s.seedCoordinateDistance(id, distanceTotal{ChairID: id, Latitude: sql.NullInt64{Int64: int64(latitude), Valid: true}, Longitude: sql.NullInt64{Int64: int64(longitude), Valid: true}, UpdatedAt: sql.NullTime{Time: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC), Valid: true}})
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

	t.Run("concurrent coordinates and recovered candidates", func(t *testing.T) {
		directory := t.TempDir()
		state := memoryTestStateIn(t, directory, "nearby-generation", nil)
		runtimeState = state
		defer func() { runtimeState = s }()
		chair := Chair{ID: "idle", Name: "name-idle", Model: "model", IsActive: true}
		if err := state.commit(stateRecord{Chair: &chair}); err != nil {
			t.Fatal(err)
		}
		coordinate("idle", 0, 0)

		done := make(chan struct{})
		var updateErr error
		go func() {
			defer close(done)
			for i := 1; i <= 128; i++ {
				if _, err := state.coordinate("idle", Coordinate{i, -i}); err != nil {
					updateErr = err
					return
				}
			}
		}()
		defer func() { <-done }()
		for i := 0; i < 128; i++ {
			response := httptest.NewRecorder()
			appGetNearbyChairs(response, httptest.NewRequest(http.MethodGet, "/api/app/nearby-chairs?latitude=0&longitude=0&distance=1000", nil))
			var result appGetNearbyChairsResponse
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Chairs) != 1 {
				t.Fatalf("concurrent nearby: %d %s", response.Code, response.Body.String())
			}
			position := result.Chairs[0].CurrentCoordinate
			if position.Latitude < 0 || position.Latitude > 128 || position.Longitude != -position.Latitude {
				t.Fatalf("inconsistent coordinate snapshot: %+v", position)
			}
		}
		<-done
		if updateErr != nil {
			t.Fatal(updateErr)
		}
		query := "latitude=128&longitude=-128&distance=0"
		want := map[string]Coordinate{"idle": {128, -128}}
		check(query, http.StatusOK, want)
		if err := state.activity("idle", false); err != nil {
			t.Fatal(err)
		}
		check(query, http.StatusOK, map[string]Coordinate{})
		if err := state.activity("idle", true); err != nil {
			t.Fatal(err)
		}
		check(query, http.StatusOK, want)
		if err := state.close(); err != nil {
			t.Fatal(err)
		}
		state = memoryTestStateIn(t, directory, "nearby-generation", nil)
		runtimeState = state
		check(query, http.StatusOK, want)
		coordinate("idle", 129, -129)
		check("latitude=129&longitude=-129&distance=0", http.StatusOK, map[string]Coordinate{"idle": {129, -129}})
		if err := state.close(); err != nil {
			t.Fatal(err)
		}
		runtimeState = memoryTestStateIn(t, directory, "next-generation", nil)
		check(query, http.StatusOK, map[string]Coordinate{})
	})
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

// Observe entry into the cancellable wait without adding hooks to the handler.
type nearbyWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *nearbyWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestAppGetNearbyChairsEmptyRetry(t *testing.T) {
	setup := func(t *testing.T) (*rideState, http.Handler, *http.Request, *nearbyWaitContext, context.CancelFunc) {
		t.Helper()
		s := memoryTestState(t)
		previous := runtimeState
		runtimeState = s
		t.Cleanup(func() { runtimeState = previous })
		s.addUser(User{ID: "u", AccessToken: "app-token"})
		s.addChair(Chair{ID: "c", Name: "chair", Model: "model"})
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		observed := &nearbyWaitContext{Context: ctx, waiting: make(chan struct{})}
		req := httptest.NewRequest(http.MethodGet, "/api/app/nearby-chairs?latitude=10&longitude=20&distance=0", nil).WithContext(observed)
		req.AddCookie(&http.Cookie{Name: "app_session", Value: "app-token"})
		return s, runtimeStateMiddleware(appAuthMiddleware(http.HandlerFunc(appGetNearbyChairs))), req, observed, cancel
	}
	start := func(t *testing.T, handler http.Handler, req *http.Request, ctx *nearbyWaitContext, res *httptest.ResponseRecorder) <-chan struct{} {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer close(done)
			handler.ServeHTTP(res, req)
		}()
		t.Cleanup(func() { <-done })
		select {
		case <-ctx.waiting:
		case <-done:
			t.Fatalf("request did not wait: %d %s", res.Code, res.Body.String())
		case <-time.After(time.Second):
			t.Fatal("request did not enter wait")
		}
		return done
	}
	await := func(t *testing.T, done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("request did not finish after one retry")
		}
	}
	t.Run("latest coordinates and availability after unlocked wait", func(t *testing.T) {
		s, handler, req, ctx, _ := setup(t)
		res := httptest.NewRecorder()
		startedAt := time.Now()
		done := start(t, handler, req, ctx, res)
		if !s.mu.TryLock() {
			t.Fatal("wait retained control state lock")
		}
		s.mu.Unlock()
		if stateLifecycle.TryLock() {
			stateLifecycle.Unlock()
			t.Fatal("wait lost initialization generation protection")
		}
		if _, err := s.coordinate("c", Coordinate{10, 20}); err != nil {
			t.Fatal(err)
		}
		if err := s.activity("c", true); err != nil {
			t.Fatal(err)
		}
		updatedAt := time.Now()
		await(t, done)
		var result appGetNearbyChairsResponse
		if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &result) != nil || len(result.Chairs) != 1 || result.Chairs[0].CurrentCoordinate != (Coordinate{10, 20}) {
			t.Fatalf("retry lost new chair: %d %s", res.Code, res.Body.String())
		}
		if result.RetrievedAt < updatedAt.UnixMilli() || result.RetrievedAt < startedAt.Add(nearbyEmptyRetryDelay).UnixMilli() {
			t.Fatalf("retry returned old retrieved_at: %d", result.RetrievedAt)
		}
	})
	t.Run("second empty result finishes with fresh timestamp", func(t *testing.T) {
		_, handler, req, _, _ := setup(t)
		res := httptest.NewRecorder()
		startedAt := time.Now()
		handler.ServeHTTP(res, req)
		var result appGetNearbyChairsResponse
		if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &result) != nil || result.Chairs == nil || len(result.Chairs) != 0 || result.RetrievedAt < startedAt.Add(nearbyEmptyRetryDelay).UnixMilli() {
			t.Fatalf("empty retry response: %d %s", res.Code, res.Body.String())
		}
	})
	for _, when := range []string{"before search", "during wait"} {
		t.Run("cancel "+when, func(t *testing.T) {
			s, handler, req, ctx, cancel := setup(t)
			res := httptest.NewRecorder()
			res.Code = 0
			var done <-chan struct{}
			if when == "during wait" {
				done = start(t, handler, req, ctx, res)
			}
			// Holding the state lock makes any extra search after cancellation
			// block, while the cancelled request must return without writing.
			s.mu.Lock()
			cancel()
			if done == nil {
				completed := make(chan struct{})
				done = completed
				go func() {
					defer close(completed)
					appGetNearbyChairs(res, req)
				}()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				s.mu.Unlock()
				await(t, done)
				t.Fatal("cancelled request attempted another state read")
			}
			s.mu.Unlock()
			if res.Code != 0 || res.Body.Len() != 0 {
				t.Fatalf("cancelled request wrote response: %d %s", res.Code, res.Body.String())
			}
		})
	}
	for _, kind := range []string{"nonempty", "invalid", "unauthorized", "missing status", "not ready"} {
		t.Run(kind+" returns immediately", func(t *testing.T) {
			s, handler, req, ctx, _ := setup(t)
			status := http.StatusOK
			switch kind {
			case "nonempty":
				if _, err := s.coordinate("c", Coordinate{10, 20}); err != nil {
					t.Fatal(err)
				}
				if err := s.activity("c", true); err != nil {
					t.Fatal(err)
				}
			case "invalid":
				req.URL.RawQuery = "latitude=x&longitude=20"
				status = http.StatusBadRequest
			case "unauthorized":
				req.Header.Del("Cookie")
				status = http.StatusUnauthorized
			case "missing status":
				s.nearbyMissingStatus["c"] = struct{}{}
				status = http.StatusInternalServerError
			case "not ready":
				s.ready = false
				status = http.StatusServiceUnavailable
			}
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != status {
				t.Fatalf("status = %d, want %d: %s", res.Code, status, res.Body.String())
			}
			select {
			case <-ctx.waiting:
				t.Fatal("request entered empty retry wait")
			default:
			}
		})
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
