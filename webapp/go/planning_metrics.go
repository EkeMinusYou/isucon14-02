package main

import "time"

type matchingOpportunity struct {
	generation          string
	ready, pending      bool
	since, pendingSince time.Time
	closedMS            float64
	transitions         uint64
	planning            int
}

type planTiming struct {
	claimed                                   bool
	readySince                                time.Time
	readyVersion                              uint64
	assigned                                  int
	metrics                                   *rideMetrics
	generation                                string
	started                                   time.Time
	startLock, snapshotLock, snapshot, solver time.Duration
	rides, candidates                         int
}

func (m *rideMetrics) registerMatching() {
	for _, name := range []string{"match_mu", "match_ready_wait", "match_start_lock", "match_snapshot_lock", "match_snapshot", "match_solver", "match_commit_lock", "match_commit", "match_total_ok", "match_total_retry", "match_total_empty", "match_total_fail"} {
		m.registerState(name, durationLimits)
	}
	for _, name := range []string{"match_rides", "match_candidates"} {
		m.registerState(name, [4]float64{1, 8, 64, 256})
	}
	for _, name := range []string{"match_calls", "match_attempts", "match_batches", "match_assigned", "match_ready_open", "match_ready_interrupted", "match_ready_after_start", "match_conflict_rides", "match_conflict_chairs", "match_generation", "match_unavailable", "match_wal_fail", "match_empty_demand", "match_empty_supply", "match_below_min", "match_abandon_limit", "match_ready_generation"} {
		m.counters[name] = 0
	}
}

func (s *rideState) observeOpportunity(at time.Time) {
	if s.metrics == nil || s.dispatch == nil {
		return
	}
	m, o := s.metrics, &s.metrics.opportunity
	if o.generation != s.generation {
		if o.generation != "" {
			if o.ready {
				o.closedMS += milliseconds(at.Sub(o.since))
			}
			if o.pending {
				m.counters["match_ready_interrupted"]++
			}
			m.counters["match_ready_generation"]++
			o.transitions++
		}
		o.generation, o.ready, o.pending = s.generation, false, false
	}
	ready := s.ready && s.failed == nil && s.coordinateFailed.Load() == nil && len(s.waitingRides) > 0 && len(s.matchingChairs) > 0
	if ready == o.ready {
		return
	}
	o.transitions++
	if ready {
		o.since, o.pendingSince, o.pending = at, at, true
		m.counters["match_ready_open"]++
	} else {
		o.closedMS += milliseconds(at.Sub(o.since))
		if o.pending {
			m.counters["match_ready_interrupted"]++
		}
		o.pending = false
	}
	o.ready = ready
}

func (s *rideState) observeMatcherCall(wait time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics != nil {
		s.metrics.counters["match_calls"]++
		s.metrics.observeState("match_mu", milliseconds(wait))
	}
}

// A short observational lock claims the currently continuous opportunity;
// the existing snapshot and solver still run at their original boundaries.
func (s *rideState) beginPlan() *planTiming {
	t := &planTiming{started: time.Now()}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.startLock = time.Since(t.started)
	s.observeOpportunity(time.Now())
	t.metrics, t.generation = s.metrics, s.generation
	if t.metrics == nil {
		return t
	}
	m := t.metrics
	m.counters["match_attempts"]++
	o := &m.opportunity
	o.planning++
	if o.ready && o.pending {
		t.claimed, t.readySince, t.readyVersion = true, o.pendingSince, o.transitions
		o.pending = false
	}
	return t
}

// Called under s.mu, including the existing commit lock. Old epochs are never
// aggregated into a new one, even if a test changes generation mid-plan.
func (s *rideState) finishPlan(t *planTiming, reason string, commitLock, commit time.Duration, committed bool) {
	if t == nil || t.metrics == nil || t.metrics != s.metrics {
		return
	}
	m := t.metrics
	finished := time.Now()
	outcome := "fail"
	switch reason {
	case "ok":
		outcome = "ok"
		m.counters["match_batches"]++
		m.counters["match_assigned"] += uint64(t.assigned)
	case "conflict_rides", "conflict_chairs":
		outcome = "retry"
	case "empty_demand", "empty_supply", "below_min":
		outcome = "empty"
	case "generation":
		if t.rides == 0 {
			outcome = "empty"
		}
	}
	if reason != "ok" {
		m.counters["match_"+reason]++
	}
	m.observeState("match_total_"+outcome, milliseconds(finished.Sub(t.started)))
	m.observeState("match_start_lock", milliseconds(t.startLock))
	m.observeState("match_snapshot_lock", milliseconds(t.snapshotLock))
	m.observeState("match_snapshot", milliseconds(t.snapshot))
	if t.rides > 0 {
		if t.claimed {
			if t.readySince.After(t.started) {
				m.counters["match_ready_after_start"]++
			}
			m.observeState("match_ready_wait", milliseconds(t.started.Sub(t.readySince)))
		}
		m.observeState("match_rides", float64(t.rides))
		m.observeState("match_candidates", float64(t.candidates))
		m.observeState("match_solver", milliseconds(t.solver))
	}
	if committed {
		m.observeState("match_commit_lock", milliseconds(commitLock))
		m.observeState("match_commit", milliseconds(commit))
	}
	m.opportunity.planning--
	s.observeOpportunity(finished)
	if t.rides == 0 && t.claimed && m.opportunity.ready && m.opportunity.transitions == t.readyVersion {
		// An empty/below-minimum snapshot did not process this opportunity.
		m.opportunity.pending, m.opportunity.pendingSince = true, t.readySince
	}
	if t.rides > 0 && m.opportunity.ready && !m.opportunity.pending {
		// Demand/supply left after any plan are a new unprocessed opportunity,
		// even when the continuous ready interval never became false.
		m.opportunity.pending, m.opportunity.pendingSince = true, finished
	}
}

func (s *rideState) finishEmptyPlan(t *planTiming, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishPlan(t, reason, 0, 0, false)
}

func (m *rideMetrics) exportOpportunity(values map[string]any, at time.Time) {
	o := &m.opportunity
	age, pendingAge := 0.0, 0.0
	if o.ready {
		age = milliseconds(at.Sub(o.since))
	}
	if o.pending {
		pendingAge = milliseconds(at.Sub(o.pendingSince))
	}
	values["match_ready"] = 0
	if o.ready {
		values["match_ready"] = 1
	}
	values["match_pending"] = 0
	if o.pending {
		values["match_pending"] = 1
	}
	values["match_ready_age_ms"] = age
	values["match_pending_age_ms"] = pendingAge
	values["match_ready_total_ms"] = o.closedMS + age
	values["match_ready_version"] = o.transitions
	values["match_planning"] = o.planning
}
