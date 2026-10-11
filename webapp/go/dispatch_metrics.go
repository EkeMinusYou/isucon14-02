package main

import (
	"container/heap"
	"time"
)

// Version 2 names the >8 pickup cohort b9_plus; dispatch_batch still keeps
// its <=64 boundary, with larger plans in the existing infinity bucket.
// These indexes are observational only; matching still uses its existing rules.
type dispatchMetrics struct {
	waiting            waitingHeap
	chair              map[string]*chairMetricState
	available, blocked int
	unknown            [3]uint64
}

type chairMetricState struct {
	flow                chairFlow
	active              bool
	unfinished, pending int
}

func (c *chairMetricState) counts() (int, int) {
	if !c.active || c.unfinished != 0 {
		return 0, 0
	}
	if c.pending != 0 {
		return 0, 1
	}
	return 1, 0
}

// One entry per currently unassigned ride, removed on assignment.
type waitingHeap []*memoryRide

func (h waitingHeap) Len() int           { return len(h) }
func (h waitingHeap) Less(i, j int) bool { return h[i].CreatedAt.Before(h[j].CreatedAt) }
func (h waitingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].waitIndex = i; h[j].waitIndex = j }
func (h *waitingHeap) Push(v any)        { r := v.(*memoryRide); r.waitIndex = len(*h); *h = append(*h, r) }
func (h *waitingHeap) Pop() any {
	a := *h
	r := a[len(a)-1]
	a[len(a)-1] = nil
	*h = a[:len(a)-1]
	r.waitIndex = -1
	return r
}

func (s *rideState) resetDispatchMetrics() {
	d := &dispatchMetrics{chair: map[string]*chairMetricState{}}
	s.dispatch = d
	for id, c := range s.chairs {
		d.chair[id] = &chairMetricState{active: c.IsActive}
	}
	for _, r := range s.rides {
		r.waitIndex = -1
		r.assignedAt = time.Time{}
		r.firstFlushed = [2]bool{}
		if !r.ChairID.Valid {
			heap.Push(&d.waiting, r)
			continue
		}
		c := d.chair[r.ChairID.String]
		if s.latest(r) != "COMPLETED" {
			c.unfinished++
		}
		for _, id := range r.Statuses {
			if s.statuses[id].ChairSentAt == nil {
				c.pending++
			}
		}
	}
	for _, c := range d.chair {
		a, b := c.counts()
		d.available += a
		d.blocked += b
	}
	for _, name := range []string{"dispatch_wait", "assign_flush_chair", "assign_flush_app", "pickup_b1", "pickup_b2_8", "pickup_b9_plus"} {
		s.metrics.register(name, durationLimits)
	}
	s.metrics.register("dispatch_batch", [4]float64{1, 8, 32, 64})
	s.resetCoordinateMetrics()
	s.resetFlow()
	for id := range s.chairs {
		s.refreshCoordinateRoute(id)
	}
	s.observeOpportunity(s.metrics.started)
}

// Called under the state lock. No chair ride history is revisited.
func (d *dispatchMetrics) changeChair(id string, change func(*chairMetricState)) {
	c := d.chair[id]
	if c == nil {
		c = &chairMetricState{}
		d.chair[id] = c
	}
	a, b := c.counts()
	d.available -= a
	d.blocked -= b
	change(c)
	a, b = c.counts()
	d.available += a
	d.blocked += b
}

func (s *rideState) observeAssignments(rows []Ride, candidates []matchingChair, assignments []matchingAssignment, at time.Time) {
	if s.metrics == nil {
		return
	}
	m := s.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe("dispatch_batch", float64(len(rows)))
	group, name := 0, "pickup_b1"
	if len(rows) > 8 {
		group, name = 2, "pickup_b9_plus"
	} else if len(rows) > 1 {
		group, name = 1, "pickup_b2_8"
	}
	for i, row := range rows {
		r := s.rides[row.ID]
		r.assignedAt = at
		m.observe("dispatch_wait", float64(max(0, at.Sub(r.CreatedAt)))/float64(time.Millisecond))
		c := candidates[assignments[i].chairIndex]
		if c.Speed <= 0 {
			s.dispatch.unknown[group]++
			continue
		}
		// Model speed is coordinate units per second; only the pickup leg is measured.
		pickup := float64(matchingDistance(c.Latitude, c.Longitude, r.PickupLatitude, r.PickupLongitude)) / float64(c.Speed)
		m.observe(name, pickup*1000)
	}
}

func observeAssignmentFlush(kind, statusID, generation string, at time.Time) {
	stateLifecycle.RLock()
	defer stateLifecycle.RUnlock()
	s := runtimeState
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics == nil || s.generation != generation {
		return
	}
	status := s.statuses[statusID]
	r := s.rides[status.RideID]
	if r == nil || status.Status != "MATCHING" || r.assignedAt.IsZero() {
		return
	}
	index := 0
	if kind == "app" {
		index = 1
	} else if kind != "chair" {
		return
	}
	if r.firstFlushed[index] {
		return
	}
	r.firstFlushed[index] = true
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	s.metrics.observe("assign_flush_"+kind, float64(max(0, at.Sub(r.assignedAt)))/float64(time.Millisecond))
}

func (s *rideState) exportDispatch(values map[string]any, at time.Time) {
	d := s.dispatch
	values["dispatch_waiting"] = len(d.waiting)
	oldest := 0.0
	if len(d.waiting) > 0 {
		oldest = float64(max(0, at.Sub(d.waiting[0].CreatedAt))) / float64(time.Millisecond)
	}
	values["dispatch_oldest_ms"] = oldest
	values["dispatch_available"] = d.available
	values["dispatch_notify_blocked"] = d.blocked
	for i, name := range []string{"pickup_b1_unknown", "pickup_b2_8_unknown", "pickup_b9_plus_unknown"} {
		values[name] = d.unknown[i]
	}
}
