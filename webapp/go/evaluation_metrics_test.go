package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEvaluationMetricsHandlerStages(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failMetadata  bool
		paymentStatus int
		retries       int
	}{
		{name: "success", paymentStatus: 204},
		{name: "missing payment configuration", failMetadata: true, paymentStatus: 204},
		{name: "invalid payment", paymentStatus: 400},
		{name: "expired key", paymentStatus: 422},
		{name: "still processing", paymentStatus: 409, retries: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var payments atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payments.Add(1)
				w.WriteHeader(tc.paymentStatus)
			}))
			defer server.Close()
			s := memoryTestState(t)
			s.metrics = newRideMetrics()
			s.resetDispatchMetrics()
			s.addChair(Chair{ID: "c", IsActive: true})
			seedMemoryRide(s, "r", "c", time.Now().Add(-time.Second))
			s.addStatus(s.newStatus(s.rides.get("r"), "ARRIVED", time.Now()))
			previousState, previousDB := runtimeState, db
			runtimeState, db = s, nil
			s.paymentTokens.set("u", PaymentToken{UserID: "u", Token: "token"})
			s.paymentGatewayURL, s.paymentGatewayConfigured = server.URL, !tc.failMetadata
			defer func() { runtimeState, db = previousState, previousDB }()
			request := httptest.NewRequest(http.MethodPost, "/api/app/rides/r/evaluation", strings.NewReader(`{"evaluation":5}`))
			request.SetPathValue("ride_id", "r")
			request = request.WithContext(context.WithValue(request.Context(), "user", &User{ID: "u"}))
			response := httptest.NewRecorder()
			lock := s.userMutex("u")
			lock.Lock()
			done := make(chan struct{})
			go func() { appPostRideEvaluatation(response, request); close(done) }()
			time.Sleep(15 * time.Millisecond)
			lock.Unlock()
			<-done
			if h := s.metrics.series["eval_lock_ok"]; h.count != 1 || h.sum < 10 {
				t.Fatalf("mutex wait: %+v", h)
			}
			if tc.failMetadata {
				if response.Code != 500 || payments.Load() != 0 || s.metrics.series["eval_sql_fail"].count != 1 || s.metrics.series["eval_total_fail"].count != 1 || s.metrics.series["eval_payment_ok"].count != 0 {
					t.Fatal("metadata failure metrics")
				}
			} else if tc.paymentStatus != 204 {
				if response.Code != 502 || int(payments.Load()) != tc.retries+1 || s.latest(s.rides.get("r")) != "ARRIVED" || s.stats.get("c").Count != 0 {
					t.Fatalf("payment failure completed ride: %d %s", response.Code, response.Body)
				}
				if s.metrics.series["eval_payment_fail"].count != 1 || s.metrics.series["eval_total_fail"].count != 1 || s.metrics.series["eval_complete_fail"].count != 0 || s.metrics.series["eval_complete_ok"].count != 0 || s.metrics.paymentRetries[1] != uint64(tc.retries) {
					t.Fatal("payment failure metrics")
				}
			} else {
				if response.Code != 200 || payments.Load() != 1 || s.latest(s.rides.get("r")) != "COMPLETED" {
					t.Fatalf("completion: %d %s", response.Code, response.Body)
				}
				for _, name := range []string{"eval_total_ok", "eval_sql_ok", "eval_payment_ok", "eval_complete_ok"} {
					if s.metrics.series[name].count != 1 {
						t.Fatalf("missing %s", name)
					}
				}
			}
		})
	}
}

func TestEvaluationMetricsOutcomesAndRetries(t *testing.T) {
	m := newRideMetrics()
	e := evaluationMetrics{metrics: m, retries: 5}
	e.visited[evalPayment] = true
	e.elapsed[evalPayment] = 600 * time.Millisecond
	e.visited[evalComplete] = true
	e.elapsed[evalComplete] = 2 * time.Millisecond
	e.finish(time.Now())
	if m.paymentRetries[1] != 5 || m.paymentRetries[0] != 0 || m.series["eval_payment_fail"].count != 1 || m.series["eval_payment_fail"].buckets != [5]uint64{0, 0, 1, 1, 1} || m.series["eval_complete_fail"].count != 1 || m.series["eval_sql_fail"].count != 0 {
		t.Fatal("failed stage, bucket or omitted-stage accounting")
	}
	e = evaluationMetrics{metrics: m, retries: 2}
	e.visited[evalPayment], e.success[evalPayment] = true, true
	e.elapsed[evalPayment] = 220 * time.Millisecond
	e.finish(time.Now())
	if m.paymentRetries[0] != 2 || m.series["eval_payment_ok"].sum != 220 {
		t.Fatal("successful retries")
	}
}
