package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPaymentRetryCountAndStatusContract(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		failures int
		retries  int
		failed   bool
	}{
		{name: "success"},
		{name: "processing", status: 409, failures: 2, retries: 2},
		{name: "processing exhausted", status: 409, failures: 6, retries: 5, failed: true},
		{name: "upstream failure", status: 500, failures: 1, retries: 1},
		{name: "upstream exhausted", status: 503, failures: 6, retries: 5, failed: true},
		{name: "invalid payment", status: 400, failures: 6, failed: true},
		{name: "expired key", status: 422, failures: 6, failed: true},
		{name: "unexpected success status", status: 200, failures: 6, failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodPost || r.URL.Path != "/payments" ||
					r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Idempotency-Key") != "generation:ride" ||
					r.Header.Get("Content-Type") != "application/json" || string(body) != `{"amount":500}` {
					t.Errorf("changed payment request: %s %s %v %s", r.Method, r.URL.Path, r.Header, body)
				}
				if int(posts.Add(1)) <= tc.failures {
					w.WriteHeader(tc.status)
					io.WriteString(w, `{"message":"payment failure"}`)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			started := time.Now()
			retries, err := requestPaymentGatewayPostPayment(context.Background(), server.URL, "token", "generation:ride", &paymentGatewayPostPaymentRequest{Amount: 500})
			if retries != tc.retries || (err != nil) != tc.failed || int(posts.Load()) != tc.retries+1 {
				t.Fatalf("retries=%d posts=%d err=%v", retries, posts.Load(), err)
			}
			if time.Since(started) < time.Duration(tc.retries)*100*time.Millisecond {
				t.Fatal("retry delay was shortened")
			}
			if tc.failed && (!errors.Is(err, erroredUpstream) || !strings.Contains(err.Error(), fmt.Sprintf("(%d)", tc.status))) {
				t.Fatalf("lost upstream status: %v", err)
			}
		})
	}
}

func TestPaymentAcceptedResponseLostAcrossRestart(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "r", "c", time.Now().Add(-time.Second))
	arrived := s.newStatus(s.rides["r"], "ARRIVED", time.Now())
	if err := s.commit(stateRecord{CreatedRide: &createdRide{Ride: s.rides["r"].Ride, Fare: s.rides["r"].Fare}, Statuses: []RideStatus{s.statuses["r-matching"], arrived}}); err != nil {
		t.Fatal(err)
	}
	_, fare, key, err := s.completionCandidate("r", "u")
	if err != nil {
		t.Fatal(err)
	}
	var posts, charges atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.Header.Get("Idempotency-Key") != key || r.Header.Get("Authorization") != "Bearer token" || string(body) != fmt.Sprintf(`{"amount":%d}`, fare) {
			t.Errorf("payment identity changed: %s %v %s", r.Method, r.Header, body)
		}
		attempt := posts.Add(1)
		if attempt == 1 {
			charges.Add(1) // The upstream accepted this key before losing the response.
		}
		if attempt <= 6 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent) // Replay of the already accepted key.
	}))
	defer server.Close()
	if err := s.registerPaymentToken("u", "token"); err != nil {
		t.Fatal(err)
	}
	s.paymentGatewayURL, s.paymentGatewayConfigured = server.URL, true
	previousState, previousDB := runtimeState, db
	runtimeState, db = s, nil
	defer func() { runtimeState, db = previousState, previousDB }()
	evaluate := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/app/rides/r/evaluation", strings.NewReader(`{"evaluation":5}`))
		request.SetPathValue("ride_id", "r")
		request = request.WithContext(context.WithValue(request.Context(), "user", &User{ID: "u"}))
		response := httptest.NewRecorder()
		appPostRideEvaluatation(response, request)
		return response
	}
	response := evaluate()
	if response.Code != http.StatusBadGateway || posts.Load() != 6 || s.latest(s.rides["r"]) != "ARRIVED" || s.stats["c"].Count != 0 {
		t.Fatalf("premature completion: %d posts=%d status=%s", response.Code, posts.Load(), s.latest(s.rides["r"]))
	}
	// Restore the same persisted generation and replay the WAL after a restart.
	restored := memoryTestState(t)
	restored.generation = s.generation
	restored.paymentGatewayURL, restored.paymentGatewayConfigured = server.URL, true
	restored.addChair(s.chairs["c"])
	restored.wal = s.wal
	if err := restored.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	_, restoredFare, restoredKey, err := restored.completionCandidate("r", "u")
	if err != nil || restoredFare != fare || restoredKey != key {
		t.Fatalf("restart changed payment: fare=%d key=%q err=%v", restoredFare, restoredKey, err)
	}
	runtimeState = restored
	response = evaluate()
	if response.Code != http.StatusOK || posts.Load() != 7 || charges.Load() != 1 || restored.latest(restored.rides["r"]) != "COMPLETED" || restored.stats["c"].Count != 1 {
		t.Fatalf("replay failed: %d posts=%d charges=%d", response.Code, posts.Load(), charges.Load())
	}
	completed := restored.statuses[restored.rides["r"].Statuses[len(restored.rides["r"].Statuses)-1]]
	if completed.AppSentAt != nil || completed.ChairSentAt != nil {
		t.Fatal("completion acknowledged before SSE delivery")
	}
	response = evaluate()
	if response.Code != http.StatusBadRequest || posts.Load() != 7 {
		t.Fatal("completed ride was charged again")
	}
	// A different ride and a reused ride ID in a new generation need new keys.
	seedMemoryRide(restored, "other", "c", time.Now())
	restored.addStatus(restored.newStatus(restored.rides["other"], "ARRIVED", time.Now()))
	_, _, otherKey, _ := restored.completionCandidate("other", "u")
	fresh := memoryTestState(t)
	fresh.generation = "next-initialize"
	seedMemoryRide(fresh, "r", "c", time.Now())
	fresh.addStatus(fresh.newStatus(fresh.rides["r"], "ARRIVED", time.Now()))
	_, _, freshKey, _ := fresh.completionCandidate("r", "u")
	if key == "" || otherKey == "" || freshKey == "" || key == otherKey || key == freshKey {
		t.Fatal("payment keys collide across rides or initialize")
	}
}
