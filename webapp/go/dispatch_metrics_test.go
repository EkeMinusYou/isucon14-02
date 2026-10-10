package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDispatchMetricsTransitionsAndFirstFlush(t *testing.T) {
	s := memoryTestState(t)
	previous := runtimeState
	runtimeState = s
	defer func() { runtimeState = previous }()
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	s.addChair(Chair{ID: "c", IsActive: true, Model: "fast"})
	s.modelSpeeds["fast"] = 2
	s.distances["c"] = distanceTotal{Latitude: sql.NullInt64{Valid: true}, Longitude: sql.NullInt64{Valid: true}}
	seedMemoryRide(s, "r", "", time.Now().Add(-2*time.Second))
	check := func(wait, available, blocked int) {
		t.Helper()
		if len(s.waitingRides) != wait || len(s.matchingChairs) != available {
			t.Fatalf("metrics diverged from current indexes: waiting=%d available=%d", len(s.waitingRides), len(s.matchingChairs))
		}
		v := rideMeasurementSnapshot().Metrics
		if v["dispatch_waiting"] != wait || v["dispatch_available"] != available || v["dispatch_notify_blocked"] != blocked {
			t.Fatalf("gauges: %v", v)
		}
	}
	check(1, 1, 0)
	if rideMeasurementSnapshot().Metrics["dispatch_oldest_ms"].(float64) < 2000 {
		t.Fatal("oldest wait missing")
	}
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	check(0, 0, 0)
	if s.metrics.series["dispatch_wait"].count != 1 || s.metrics.series["dispatch_batch"].sum != 1 || s.metrics.series["pickup_b1"].sum != 15000 {
		t.Fatal("assignment aggregates")
	}
	events, err := loadNotifications(context.Background(), "chair", "c", "")
	if err != nil || len(events) != 1 || !events[0].measureAssignment {
		t.Fatalf("assignment observation missing from pending queue: %+v %v", events, err)
	}
	statusID := "r-matching"
	at := s.rides["r"].assignedAt.Add(150 * time.Millisecond)
	observeAssignmentFlush("chair", statusID, "old-generation", at)
	observeAssignmentFlush("chair", statusID, s.generation, at)
	// An ACK failure leaves the event pending; another flush must not count twice.
	observeAssignmentFlush("chair", statusID, s.generation, at.Add(time.Second))
	observeAssignmentFlush("app", statusID, s.generation, at.Add(time.Millisecond))
	events, err = loadNotifications(context.Background(), "chair", "c", "")
	if err != nil || len(events) != 1 || events[0].measureAssignment {
		t.Fatal("pending replay observed as a new first flush")
	}
	if h := s.metrics.series["assign_flush_chair"]; h.count != 1 || h.sum != 150 {
		t.Fatalf("first flush: %+v", h)
	}
	for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
		if err := s.commit(stateRecord{Statuses: []RideStatus{s.newStatus(s.rides["r"], status, time.Now())}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.complete("r", 5); err != nil {
		t.Fatal(err)
	}
	check(0, 0, 1)
	for _, id := range s.rides["r"].Statuses {
		if err := acknowledgeNotification(context.Background(), "chair", id); err != nil {
			t.Fatal(err)
		}
	}
	check(0, 1, 0)
	// Duplicate ACKs, activity toggles and a new assignment preserve the counts.
	if err := acknowledgeNotification(context.Background(), "chair", statusID); err != nil {
		t.Fatal(err)
	}
	if err := s.activity("c", false); err != nil {
		t.Fatal(err)
	}
	check(0, 0, 0)
	if err := s.activity("c", true); err != nil {
		t.Fatal(err)
	}
	check(0, 1, 0)
	oldEpoch := s.metrics.epoch
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	check(0, 1, 0)
	if s.metrics.epoch == oldEpoch || s.metrics.series["dispatch_wait"].count != 0 || !s.rides["r"].assignedAt.IsZero() {
		t.Fatal("metrics reset")
	}
	observeAssignmentFlush("chair", statusID, s.generation, at)
	if s.metrics.series["assign_flush_chair"].count != 0 {
		t.Fatal("replay counted as assignment")
	}
}

func TestDispatchMetricsUnknownAndFailedAssignment(t *testing.T) {
	s := memoryTestState(t)
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "r", "", time.Now())
	s.failed = context.Canceled
	if err := s.match(); err == nil {
		t.Fatal("expected WAL rejection")
	}
	if len(s.dispatch.waiting) != 1 || s.metrics.series["dispatch_wait"].count != 0 {
		t.Fatal("failed assignment counted")
	}
	s.failed = nil
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	if s.dispatch.unknown[0] != 1 || s.metrics.series["pickup_b1"].count != 0 {
		t.Fatal("unknown estimate treated as zero")
	}
}

// A successful Flush still counts if clearing its deadline fails afterward.
type assignmentFlushWriter struct {
	*httptest.ResponseRecorder
	flushError error
	clearError error
}

func (w *assignmentFlushWriter) FlushError() error { return w.flushError }
func (w *assignmentFlushWriter) SetWriteDeadline(at time.Time) error {
	if at.IsZero() {
		return w.clearError
	}
	return nil
}
func TestDispatchMetricsFlushSuccessBoundary(t *testing.T) {
	calls := 0
	w := &assignmentFlushWriter{ResponseRecorder: httptest.NewRecorder(), flushError: errors.New("flush failed")}
	callback := func() { calls++ }
	if err := writeSSEWithFlush(w, http.NewResponseController(w), "data: {}\n\n", callback); err == nil || calls != 0 {
		t.Fatal("failed flush counted")
	}
	w.flushError = nil
	w.clearError = errors.New("clear deadline failed")
	if err := writeSSEWithFlush(w, http.NewResponseController(w), "data: {}\n\n", callback); err == nil || calls != 1 {
		t.Fatal("successful flush not counted")
	}
}
