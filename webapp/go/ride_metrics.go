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
	m := &rideMetrics{epoch: epoch, series: make(map[string]*metricHistogram)}
	m.registerEvaluation()
	return m
}

func (m *rideMetrics) register(name string, limits [4]float64) {
	m.series[name] = &metricHistogram{limits: limits}
}

// Caller holds m.mu. Buckets are cumulative, including the infinity bucket.
func (m *rideMetrics) observe(name string, value float64) {
	h := m.series[name]
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
	values["ride_metrics_version"] = 2
	values["eval_payment_ok_retries"] = m.paymentRetries[0]
	values["eval_payment_fail_retries"] = m.paymentRetries[1]
	for name, h := range m.series {
		values[name+"_n"] = h.count
		values[name+"_sum"] = h.sum
		for i, suffix := range []string{"_b0", "_b1", "_b2", "_b3", "_inf"} {
			values[name+suffix] = h.buckets[i]
		}
	}
}

func rideMeasurementSnapshot() measurement.Snapshot {
	stateLifecycle.RLock()
	defer stateLifecycle.RUnlock()
	s := runtimeState
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := map[string]any{"ride_metrics_ready": 0}
	if s.metrics == nil || !s.ready {
		return measurement.Snapshot{Metrics: values}
	}
	m := s.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	values["ride_metrics_ready"] = 1
	m.export(values)
	if s.dispatch != nil {
		s.exportDispatch(values)
	}
	return measurement.Snapshot{Metrics: values}
}
