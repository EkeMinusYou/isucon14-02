package main

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func observedState(t *testing.T) *rideState {
	t.Helper()
	s := memoryTestState(t)
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState = previous })
	return s
}

func TestCoordinateMetricsSamplingAndFailedWrite(t *testing.T) {
	s := observedState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	for i := 0; i < 129; i++ {
		if _, err := s.coordinate("c", Coordinate{i, i}); err != nil {
			t.Fatal(err)
		}
	}
	m := s.metrics
	if coordinateCounter(t, s, "coord_calls_ok") != 129 || coordinateCounter(t, s, "coord_samples_ok") != 3 || coordinateCounter(t, s, "coord_wal_ok") != 129 || coordinateCounter(t, s, "coord_apply_ok") != 129 {
		t.Fatalf("coordinate populations: %v", m.counters)
	}
	for _, stage := range coordinateStages {
		h := coordinateHistogram(t, s, "coord_"+stage+"_ok")
		if h.count != 3 || h.buckets[4] != 3 {
			t.Fatalf("stage %s: %+v", stage, h)
		}
	}
	if coordinateCounter(t, s, "flow_coord_first") != 1 || coordinateCounter(t, s, "flow_coord_no_ride") != 128 {
		t.Fatal("first/no-ride boundary")
	}
	before, seq := s.chairDistance("c"), s.sequence
	if err := s.coordinates.stripes[coordinateStripeIndex("c")].wal.Close(); err != nil {
		t.Fatal(err)
	}
	s.coordinateCalls.Store(192) // The next call is sampled, independent of success.
	if _, err := s.coordinate("c", Coordinate{999, 999}); err == nil {
		t.Fatal("closed WAL accepted")
	}
	if s.chairDistance("c") != before || s.sequence != seq || coordinateCounter(t, s, "coord_apply_ok") != 129 || coordinateHistogram(t, s, "coord_write_fail").count != 1 || coordinateHistogram(t, s, "coord_apply_fail").count != 0 || coordinateCounter(t, s, "coord_wal_fail") != 1 {
		t.Fatal("failed WAL published or unreachable apply counted")
	}
	values := rideMeasurementSnapshot().Metrics
	if values["ride_metrics_ready"] != 1 || values["ride_state_available"] != 0 || values["coord_calls_fail"] != uint64(1) {
		t.Fatal("failed state hid failure counters")
	}
	if _, err := s.coordinate("missing", Coordinate{}); err == nil {
		t.Fatal("missing chair accepted")
	}
	if coordinateCounter(t, s, "coord_calls_fail") != 2 || coordinateCounter(t, s, "coord_wal_fail") != 1 {
		t.Fatal("validation failure mixed with WAL attempt")
	}
	wal, err := os.ReadFile(s.wal.Name())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wal), "coord_calls") || strings.Contains(string(wal), "flow_enroute") {
		t.Fatal("observations persisted in WAL")
	}
}

func TestFlowMetricsStagesDistanceAndReuse(t *testing.T) {
	s := observedState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	if _, err := s.coordinate("c", Coordinate{}); err != nil {
		t.Fatal(err)
	}
	seedMemoryRide(s, "r", "", time.Now())
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	if s.dispatch.chair["c"].flow.stage != flowAssigned {
		t.Fatal("assignment boundary")
	}
	if err := s.chairStatus("c", "r", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{10, 20}); err != nil {
		t.Fatal(err)
	}
	// Accepted retransmission has zero distance and cannot repeat a transition.
	if _, err := s.coordinate("c", Coordinate{10, 20}); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("c", "r", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("c", "r", "CARRYING"); err == nil {
		t.Fatal("duplicate status accepted")
	}
	if _, err := s.coordinate("c", Coordinate{30, 40}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("r", 5); err != nil {
		t.Fatal(err)
	}
	m := s.metrics
	if h := coordinateHistogram(t, s, "flow_dist_enroute"); h.count != 1 || h.sum != 30 {
		t.Fatalf("pickup endpoint omitted: %+v", h)
	}
	if h := coordinateHistogram(t, s, "flow_dist_carrying"); h.count != 1 || h.sum != 40 {
		t.Fatalf("arrival endpoint omitted: %+v", h)
	}
	for _, name := range []string{"assigned", "enroute", "pickup", "carrying", "arrived"} {
		if m.stateSeries["flow_"+name].count != 1 {
			t.Fatalf("duplicate/missing stage: %s", name)
		}
	}
	if m.flow.ongoing.chairs != 1 || m.stateSeries["flow_reuse"].count != 0 {
		t.Fatal("completion mistaken for reuse")
	}
	if err := s.activity("c", false); err != nil {
		t.Fatal(err)
	}
	for _, id := range s.rides["r"].Statuses {
		if err := acknowledgeNotification(context.Background(), "chair", id); err != nil {
			t.Fatal(err)
		}
		if err := acknowledgeNotification(context.Background(), "chair", id); err != nil {
			t.Fatal(err)
		}
	}
	if m.flow.ongoing.chairs != 1 {
		t.Fatal("inactive chair became reusable")
	}
	// Nearby response/grace is deliberately not the matching reuse condition.
	if err := s.activity("c", true); err != nil {
		t.Fatal(err)
	}
	if m.flow.ongoing.chairs != 0 || m.stateSeries["flow_reuse"].count != 1 || m.stateSeries["flow_cycle"].count != 1 {
		t.Fatal("ACK/activity reuse boundary")
	}
	s.finishCompletionResponse("r")
	if m.stateSeries["flow_cycle"].count != 1 {
		t.Fatal("nearby grace counted another matching cycle")
	}
}

func TestFlowMetricsResetAndOccupancy(t *testing.T) {
	s := observedState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "r", "c", time.Now())
	if err := s.chairStatus("c", "r", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	epoch := s.metrics.epoch
	// Same persisted ride/distance state, fresh process/initialize metrics epoch.
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	m := s.metrics
	if epoch == m.epoch || m.flow.known != 0 || m.flow.restored != 1 || m.flow.stage[flowEnroute].chairs != 1 || coordinateCounter(t, s, "coord_calls_ok") != 0 || !s.rides["r"].assignedAt.IsZero() {
		t.Fatal("reset replayed complete observations")
	}
	if _, err := s.coordinate("c", Coordinate{10, 20}); err != nil {
		t.Fatal(err)
	}
	if coordinateHistogram(t, s, "flow_dist_enroute").count != 0 || coordinateCounter(t, s, "flow_coord_first") != 1 || m.stateSeries["flow_enroute"].count != 0 || m.counters["flow_enroute_partial"] != 1 {
		t.Fatal("first coordinate or restored stage treated as full")
	}
	if err := s.chairStatus("c", "r", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{30, 40}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("r", 5); err != nil {
		t.Fatal(err)
	}
	for _, id := range s.rides["r"].Statuses {
		if err := acknowledgeNotification(context.Background(), "chair", id); err != nil {
			t.Fatal(err)
		}
	}
	if m.stateSeries["flow_cycle"].count != 0 || m.counters["flow_cycle_partial"] != 1 || m.stateSeries["flow_carrying"].count != 1 {
		t.Fatal("restored cycle bias")
	}
	// Exact chair-ms integration, including an unfinished stage at window end.
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	m = s.metrics
	r := s.rides["r"]
	r.Statuses = []string{"r-matching"}
	at := m.started.Add(time.Second)
	s.startFlow(r, at, true)
	s.changeFlow(s.dispatch.chair["c"], flowEnroute, at.Add(time.Second), true)
	v := map[string]any{}
	m.exportFlow(v, at.Add(2*time.Second))
	if v["flow_assigned_total_ms"] != float64(1000) || v["flow_enroute_total_ms"] != float64(1000) || v["flow_enroute_age_ms"] != float64(1000) || v["flow_ongoing_age_ms"] != float64(2000) {
		t.Fatalf("occupancy arithmetic: %v", v)
	}
}

func TestPlanningMetricsReadinessAndReasons(t *testing.T) {
	s := matchingBatchTestState(t, 1, 1)
	m := s.metrics
	if !m.opportunity.pending || m.counters["match_ready_open"] != 1 {
		t.Fatal("ready not opened")
	}
	if err := s.activity("chair-000", false); err != nil {
		t.Fatal(err)
	}
	if m.opportunity.ready || m.counters["match_ready_interrupted"] != 1 {
		t.Fatal("readiness interruption lost")
	}
	if err := s.activity("chair-000", true); err != nil {
		t.Fatal(err)
	}
	m.opportunity.pendingSince = time.Now().Add(-100 * time.Millisecond)
	timing := s.beginPlan()
	plan, err := s.snapshotMatching("", 0, timing)
	if err != nil {
		t.Fatal(err)
	}
	timing.rides, timing.candidates = len(plan.rides), len(plan.chairs)
	plan.observation = timing
	if err := s.activity("chair-000", false); err != nil {
		t.Fatal(err)
	}
	retry, err := s.commitMatching(plan, assignMatchingChairs(plan.rides, plan.chairs))
	if err != nil || !retry || m.counters["match_conflict_chairs"] != 1 || m.opportunity.planning != 0 || m.counters["match_assigned"] != 0 || m.stateSeries["match_ready_wait"].sum < 100 {
		t.Fatal("conflict/readiness populations")
	}
	if err := s.activity("chair-000", true); err != nil {
		t.Fatal(err)
	}
	timing = s.beginPlan()
	plan, err = s.snapshotMatching("", 0, timing)
	if err != nil {
		t.Fatal(err)
	}
	timing.rides, timing.candidates = len(plan.rides), len(plan.chairs)
	plan.observation = timing
	s.generation = "next"
	retry, err = s.commitMatching(plan, assignMatchingChairs(plan.rides, plan.chairs))
	if err != nil || retry || m.counters["match_generation"] != 1 || m.counters["match_ready_generation"] != 1 || !m.opportunity.pending {
		t.Fatal("generation mixed with retry/old opportunity")
	}
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
	if m.counters["match_calls"] != 1 || m.counters["match_batches"] != 1 || m.counters["match_assigned"] != 1 || m.counters["match_attempts"] != 4 || m.counters["match_empty_demand"] != 1 || m.stateSeries["match_rides"].count != 3 {
		t.Fatalf("calls/attempts/batches/rides mixed: %v", m.counters)
	}
}

func TestRideMetricsSnapshotMissingAndConcurrent(t *testing.T) {
	s := observedState(t)
	s.mu.Lock()
	v := rideMeasurementSnapshot().Metrics
	s.mu.Unlock()
	if v["ride_metrics_ready"] != 0 || v["snapshot_status"] != 3 || v["snapshot_acquire_ms"].(float64) < 20 {
		t.Fatal("busy sample was stale/zero-filled")
	}
	if _, ok := v["coord_calls_ok"]; ok {
		t.Fatal("invalid sample had counters")
	}
	s.metrics.mu.Lock()
	v = rideMeasurementSnapshot().Metrics
	s.metrics.mu.Unlock()
	if v["snapshot_status"] != 4 {
		t.Fatal("aggregation contention missing")
	}
	stateLifecycle.Lock()
	v = rideMeasurementSnapshot().Metrics
	stateLifecycle.Unlock()
	if v["snapshot_status"] != 1 {
		t.Fatal("initialize boundary missing")
	}
	s.addChair(Chair{ID: "c", IsActive: true})
	stripe := &s.coordinates.stripes[coordinateStripeIndex("c")]
	stripe.mu.Lock()
	v = rideMeasurementSnapshot().Metrics
	stripe.mu.Unlock()
	if v["snapshot_status"] != 4 || v["ride_metrics_ready"] != 0 {
		t.Fatal("busy stripe returned partial/zero-filled metrics")
	}
	s.seedCoordinateDistance("c", distanceTotal{ChairID: "c", Latitude: sql.NullInt64{Valid: true}, Longitude: sql.NullInt64{Valid: true}})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 128; i++ {
			if _, err := s.coordinate("c", Coordinate{i, i}); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			e := evaluationMetrics{metrics: s.metrics}
			e.finish(time.Now())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			rideMeasurementSnapshot()
		}
	}()
	wg.Wait()
	if coordinateCounter(t, s, "coord_calls_ok") != 128 {
		t.Fatal("concurrent aggregation lost coordinates")
	}
}

func TestPlanningMetricsEmptyAndNewEpoch(t *testing.T) {
	s := matchingBatchTestState(t, 1, 1)
	before := s.metrics.opportunity.pendingSince
	version := s.metrics.opportunity.transitions
	if _, err := s.matchBatch(s.generation, matchingPlanLimit); err != nil {
		t.Fatal(err)
	}
	if !s.metrics.opportunity.pending || s.metrics.opportunity.pendingSince != before || s.metrics.opportunity.transitions != version || s.metrics.stateSeries["match_ready_wait"].count != 0 || s.metrics.counters["match_below_min"] != 1 {
		t.Fatal("empty snapshot consumed readiness")
	}
	timing := s.beginPlan()
	plan, err := s.snapshotMatching("", 0, timing)
	if err != nil {
		t.Fatal(err)
	}
	plan.observation = timing
	timing.rides, timing.candidates = len(plan.rides), len(plan.chairs)
	old := s.metrics
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	assignments := assignMatchingChairs(plan.rides, plan.chairs)
	if _, err := s.commitMatching(plan, assignments); err != nil {
		t.Fatal(err)
	}
	if old == s.metrics || s.metrics.counters["match_attempts"] != 0 || s.metrics.counters["match_batches"] != 0 || s.metrics.opportunity.planning != 0 {
		t.Fatal("old attempt contaminated new epoch")
	}
}
