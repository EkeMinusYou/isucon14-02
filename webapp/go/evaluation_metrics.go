package main

import "time"

const (
	evalTotal = iota
	evalLock
	evalSQL
	evalPayment
	evalComplete
	evalStageCount
)

var evalStageNames = [evalStageCount]string{"eval_total", "eval_lock", "eval_sql", "eval_payment", "eval_complete"}

type evaluationMetrics struct {
	metrics *rideMetrics
	elapsed [evalStageCount]time.Duration
	visited [evalStageCount]bool
	success [evalStageCount]bool
	retries int
}

func (m *rideMetrics) registerEvaluation() {
	for _, name := range evalStageNames {
		for _, outcome := range []string{"ok", "fail"} {
			if name == "eval_lock" && outcome == "fail" {
				continue
			}
			m.register(name+"_"+outcome, durationLimits)
		}
	}
}

// One aggregation lock per evaluation; clocks only bracket the selected stages.
func (e *evaluationMetrics) finish(started time.Time) {
	if e.metrics == nil {
		return
	}
	e.elapsed[evalTotal] = time.Since(started)
	e.visited[evalTotal] = true
	m := e.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, name := range evalStageNames {
		if !e.visited[i] {
			continue
		}
		outcome := "fail"
		if e.success[i] {
			outcome = "ok"
		}
		m.observe(name+"_"+outcome, float64(e.elapsed[i])/float64(time.Millisecond))
	}
	if e.visited[evalPayment] {
		index := 1
		if e.success[evalPayment] {
			index = 0
		}
		m.paymentRetries[index] += uint64(e.retries)
	}
}
