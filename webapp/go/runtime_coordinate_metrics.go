package main

import (
	"strings"
	"time"
)

// Same vocabulary, populations and buckets as v3, partitioned across WAL
// stripes. Snapshot merges these fixed aggregates without scanning chairs.
type coordinateAggregates struct {
	calls, samples             [2]uint64
	wal                        [2]uint64
	applied, bytes             uint64
	stages                     [6][2]metricHistogram
	distance                   [2]metricHistogram
	first, noRide, other, zero uint64
}

func (m *coordinateAggregates) finish(t *coordinateTiming, success, attempted, written bool, previous, next distanceTotal, stage int) {
	outcome := 1
	if success {
		outcome = 0
	}
	m.calls[outcome]++
	if attempted {
		if written {
			m.wal[0]++
		} else {
			m.wal[1]++
		}
	}
	if success {
		m.applied++
		if !previous.Latitude.Valid || !previous.Longitude.Valid {
			m.first++
		} else {
			delta := max(0, next.TotalDistance-previous.TotalDistance)
			if delta == 0 {
				m.zero++
			}
			switch stage {
			case flowNone:
				m.noRide++
			case flowEnroute, flowCarrying:
				index := 0
				if stage == flowCarrying {
					index = 1
				}
				h := &m.distance[index]
				h.limits = [4]float64{0, 1, 10, 100}
				h.add(float64(delta))
			default:
				m.other++
			}
		}
	}
	if t == nil {
		return
	}
	m.samples[outcome]++
	m.bytes += uint64(t.bytes)
	t.visited[0], t.visited[1], t.ok[0], t.ok[1] = true, true, success, success
	for i := range coordinateStages {
		if t.visited[i] {
			result := 1
			if t.ok[i] {
				result = 0
			}
			h := &m.stages[i][result]
			h.limits = [4]float64{0.1, 1, 10, 100}
			h.add(milliseconds(t.elapsed[i]))
		}
	}
}

func coordinateOwnedMetric(name string) bool {
	return strings.HasPrefix(name, "coord_") || strings.HasPrefix(name, "flow_dist_") || strings.HasPrefix(name, "flow_coord_")
}

func addHistogram(a *metricHistogram, b metricHistogram) {
	a.count += b.count
	a.sum += b.sum
	for i := range a.buckets {
		a.buckets[i] += b.buckets[i]
	}
}

// Caller owns lifecycle and control state. Writers never hold a stripe while
// waiting for control. Snapshot briefly fences all stripes for one observation
// instant. Failed acquisition releases every stripe before retrying, within
// the snapshot's existing deadline; no writer waits behind a blocked snapshot.
func (s *rideState) copyCoordinateMetrics(histograms *[]histogramSample, counters *[]counterSample, deadline time.Time) (time.Time, bool) {
	for {
		acquired := 0
		for acquired < len(s.coordinates.stripes) && s.coordinates.stripes[acquired].mu.TryLock() {
			acquired++
		}
		if acquired == len(s.coordinates.stripes) {
			break
		}
		for i := acquired - 1; i >= 0; i-- {
			s.coordinates.stripes[i].mu.Unlock()
		}
		if !time.Now().Before(deadline) {
			return time.Time{}, false
		}
		time.Sleep(500 * time.Microsecond)
	}
	defer func() {
		for i := len(s.coordinates.stripes) - 1; i >= 0; i-- {
			s.coordinates.stripes[i].mu.Unlock()
		}
	}()
	observed := time.Now()
	var merged coordinateAggregates
	for i := range s.coordinates.stripes {
		m := s.coordinates.stripes[i].metrics
		for j := range merged.calls {
			merged.calls[j] += m.calls[j]
			merged.samples[j] += m.samples[j]
			merged.wal[j] += m.wal[j]
			addHistogram(&merged.distance[j], m.distance[j])
		}
		merged.applied += m.applied
		merged.bytes += m.bytes
		merged.first += m.first
		merged.noRide += m.noRide
		merged.other += m.other
		merged.zero += m.zero
		for j := range merged.stages {
			for k := range merged.stages[j] {
				addHistogram(&merged.stages[j][k], m.stages[j][k])
			}
		}
	}
	for i, outcome := range []string{"ok", "fail"} {
		*counters = append(*counters, counterSample{"coord_calls_" + outcome, merged.calls[i]}, counterSample{"coord_samples_" + outcome, merged.samples[i]}, counterSample{"coord_wal_" + outcome, merged.wal[i]})
		for j, stage := range coordinateStages {
			*histograms = append(*histograms, histogramSample{"coord_" + stage + "_" + outcome, merged.stages[j][i]})
		}
	}
	*counters = append(*counters, counterSample{"coord_apply_ok", merged.applied}, counterSample{"coord_sample_bytes", merged.bytes},
		counterSample{"flow_coord_first", merged.first}, counterSample{"flow_coord_no_ride", merged.noRide}, counterSample{"flow_coord_other", merged.other}, counterSample{"flow_coord_zero", merged.zero})
	for i, stage := range []string{"enroute", "carrying"} {
		*histograms = append(*histograms, histogramSample{"flow_dist_" + stage, merged.distance[i]})
	}
	return observed, true
}

func (s *rideState) resetCoordinateMetrics() {
	for i := range s.coordinates.stripes {
		stripe := &s.coordinates.stripes[i]
		stripe.mu.Lock()
		stripe.metrics = coordinateAggregates{}
		stripe.mu.Unlock()
	}
}

func finishCoordinateHold(t *coordinateTiming) {
	if t != nil {
		t.elapsed[1] = time.Since(t.locked) - t.waitAfter
	}
}
