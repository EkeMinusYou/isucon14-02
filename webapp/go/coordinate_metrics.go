package main

import "time"

const coordinateSampleEvery = 64

var coordinateStages = [...]string{"wait", "hold", "json", "frame", "write", "apply"}

// Local to a sampled call. Wait sums cell/routing/target/stripe/append acquisition;
// hold excludes later lock acquisition and aggregate publication.
type coordinateTiming struct {
	started, locked time.Time
	waitAfter       time.Duration
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

func milliseconds(d time.Duration) float64 { return float64(max(0, d)) / float64(time.Millisecond) }
