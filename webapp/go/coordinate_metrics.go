package main

import "time"

const coordinateSampleEvery = 64

var coordinateStages = [...]string{"wait", "hold", "json", "frame", "write", "apply"}

// Local to a sampled call. No aggregation lock is added to coordinate updates.
type coordinateTiming struct {
	started, locked time.Time
	elapsed         [6]time.Duration
	visited         [6]bool
	ok              [6]bool
	bytes           int
}

func (m *rideMetrics) registerCoordinate() {
	for _, stage := range coordinateStages {
		for _, outcome := range []string{"ok", "fail"} {
			m.registerState("coord_"+stage+"_"+outcome, [4]float64{0.1, 1, 10, 100})
		}
	}
	for _, name := range []string{"coord_calls_ok", "coord_calls_fail", "coord_wal_ok", "coord_wal_fail", "coord_apply_ok", "coord_samples_ok", "coord_samples_fail", "coord_sample_bytes"} {
		m.counters[name] = 0
	}
}

// Caller still owns s.mu. Hold excludes aggregation and mutex release; it
// includes validation, record creation, commit, state publication and notify.
func (s *rideState) finishCoordinate(t *coordinateTiming, success bool) {
	m := s.metrics
	if m == nil {
		return
	}
	outcome := "fail"
	if success {
		outcome = "ok"
	}
	m.counters["coord_calls_"+outcome]++
	if t == nil {
		return
	}
	t.elapsed[1] = time.Since(t.locked)
	t.visited[0], t.visited[1] = true, true
	t.ok[0], t.ok[1] = success, success
	m.counters["coord_samples_"+outcome]++
	m.counters["coord_sample_bytes"] += uint64(t.bytes)
	for i, stage := range coordinateStages {
		if t.visited[i] {
			result := "fail"
			if t.ok[i] {
				result = "ok"
			}
			m.observeState("coord_"+stage+"_"+result, milliseconds(t.elapsed[i]))
		}
	}
}

func milliseconds(d time.Duration) float64 { return float64(max(0, d)) / float64(time.Millisecond) }
