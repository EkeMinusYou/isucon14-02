package main

import (
	"sync"
	"sync/atomic"
	"time"

	measurement "github.com/EkeMinusYou/isucon-template/tools/measurement/app"
)

// Metrics belong to one process/initialization epoch, never to WAL replay.
// All exported names and histogram boundaries are fixed and collector-safe.
type rideMetrics struct {
	paymentRetries [2]uint64
	mu             sync.Mutex
	epoch          int64
	started        time.Time
	stateSeries    map[string]*metricHistogram
	counters       map[string]uint64
	flow           flowMetrics
	opportunity    matchingOpportunity
	series         map[string]*metricHistogram
}

type metricHistogram struct {
	count   uint64
	sum     float64
	buckets [5]uint64
	limits  [4]float64
}

var durationLimits = [4]float64{10, 100, 1000, 3000}

var lastRideMetricsEpoch atomic.Int64

func newRideMetrics() *rideMetrics {
	epoch := time.Now().UnixMicro()
	for {
		previous := lastRideMetricsEpoch.Load()
		epoch = max(epoch, previous+1)
		if lastRideMetricsEpoch.CompareAndSwap(previous, epoch) {
			break
		}
	}
	m := &rideMetrics{epoch: epoch, started: time.Now(), series: make(map[string]*metricHistogram), stateSeries: make(map[string]*metricHistogram), counters: make(map[string]uint64)}
	m.registerEvaluation()
	m.registerCoordinate()
	m.registerFlow()
	m.registerMatching()
	return m
}

func (m *rideMetrics) register(name string, limits [4]float64) {
	m.series[name] = &metricHistogram{limits: limits}
}

// Caller holds m.mu. Buckets are cumulative, including the infinity bucket.
func (m *rideMetrics) observe(name string, value float64) {
	m.series[name].add(value)
}

func (h *metricHistogram) add(value float64) {
	h.count++
	h.sum += max(0, value)
	for i, limit := range h.limits {
		if value <= limit {
			h.buckets[i]++
		}
	}
	h.buckets[4]++
}

func (m *rideMetrics) export(values map[string]any) {
	values["ride_metrics_epoch"] = m.epoch
	values["ride_metrics_version"] = 3
	values["eval_payment_ok_retries"] = m.paymentRetries[0]
	values["eval_payment_fail_retries"] = m.paymentRetries[1]
	exportHistograms(values, m.series)
}

func exportHistograms(values map[string]any, series map[string]*metricHistogram) {
	for name, h := range series {
		exportHistogram(values, name, h)
	}
}
func exportHistogram(values map[string]any, name string, h *metricHistogram) {
	values[name+"_n"], values[name+"_sum"] = h.count, h.sum
	for i, suffix := range []string{"_b0", "_b1", "_b2", "_b3", "_inf"} {
		values[name+suffix] = h.buckets[i]
	}
}

// State-owned series never acquire m.mu. Existing evaluation/flush series
// retain their mutex; all snapshots follow lifecycle -> state -> metrics.
func (m *rideMetrics) registerState(name string, limits [4]float64) {
	m.stateSeries[name] = &metricHistogram{limits: limits}
}
func (m *rideMetrics) observeState(name string, value float64) { m.stateSeries[name].add(value) }

type histogramSample struct {
	name  string
	value metricHistogram
}
type counterSample struct {
	name  string
	value uint64
}

func rideMeasurementSnapshot() measurement.Snapshot {
	started := time.Now()
	values := map[string]any{"ride_metrics_ready": 0, "ride_metrics_version": 3, "snapshot_status": 1}
	var copied rideMetrics
	var observed time.Time
	histograms := make([]histogramSample, 0, 64)
	counters := make([]counterSample, 0, 64)
	// Copy only fixed-size aggregates under the locks. Formatting the hundreds
	// of output scalars takes place after releasing the common state lock.
	func() {
		if !stateLifecycle.TryRLock() {
			return
		}
		defer stateLifecycle.RUnlock()
		s := runtimeState
		if s == nil {
			values["snapshot_status"] = 2
			return
		}
		// Bounded acquisition without a goroutine left waiting after HTTP timeout.
		deadline := started.Add(20 * time.Millisecond)
		for !s.mu.TryRLock() {
			if !time.Now().Before(deadline) {
				values["snapshot_status"] = 3
				return
			}
			time.Sleep(500 * time.Microsecond)
		}
		defer s.mu.RUnlock()
		if s.metrics == nil || !s.ready {
			values["snapshot_status"] = 2
			return
		}
		m := s.metrics
		if !m.mu.TryLock() {
			values["snapshot_status"] = 4
			return
		}
		defer m.mu.Unlock()
		at := time.Now()
		observed = at
		values["snapshot_observed_us"] = at.UnixMicro()
		values["ride_state_available"] = 0
		if s.failed == nil {
			values["ride_state_available"] = 1
		}
		values["ride_metrics_ready"], values["snapshot_status"] = 1, 0
		copied.epoch, copied.started = m.epoch, m.started
		copied.flow, copied.opportunity, copied.paymentRetries = m.flow, m.opportunity, m.paymentRetries
		for name, h := range m.series {
			histograms = append(histograms, histogramSample{name, *h})
		}
		for name, h := range m.stateSeries {
			histograms = append(histograms, histogramSample{name, *h})
		}
		for name, count := range m.counters {
			counters = append(counters, counterSample{name, count})
		}
		if s.dispatch != nil {
			s.exportDispatch(values, at)
		}
	}()
	values["snapshot_acquire_ms"] = milliseconds(time.Since(started))
	if values["ride_metrics_ready"] == 1 {
		copied.export(values)
		for _, h := range histograms {
			exportHistogram(values, h.name, &h.value)
		}
		for _, c := range counters {
			values[c.name] = c.value
		}
		values["coord_sample_every"] = coordinateSampleEvery
		copied.exportFlow(values, observed)
		copied.exportOpportunity(values, observed)
	} else {
		values["snapshot_observed_us"] = time.Now().UnixMicro()
	}
	values["snapshot_export_ms"] = milliseconds(time.Since(started))
	return measurement.Snapshot{Metrics: values}
}
