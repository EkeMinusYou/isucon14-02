package main

import "time"

const (
	flowNone = iota
	flowAssigned
	flowEnroute
	flowPickup
	flowCarrying
	flowArrived
	flowReuse
	flowUnknown
)

var flowNames = [...]string{"none", "assigned", "enroute", "pickup", "carrying", "arrived", "reuse", "unknown"}

type chairFlow struct {
	stage               int
	entered, started    time.Time
	fullStage, fullRide bool
}

type flowOccupancy struct {
	chairs  int
	entered float64 // Sum of epoch-relative entry times, in milliseconds.
	closed  float64 // Completed chair residence, in milliseconds.
}

type flowMetrics struct {
	stage           [8]flowOccupancy
	ongoing         flowOccupancy
	known, restored int
}

func (m *rideMetrics) registerFlow() {
	for _, name := range flowNames[1:] {
		m.registerState("flow_"+name, durationLimits)
		m.counters["flow_"+name+"_partial"] = 0
	}
	m.registerState("flow_cycle", durationLimits)
	for _, name := range []string{"enroute", "carrying"} {
		m.registerState("flow_dist_"+name, [4]float64{0, 1, 10, 100})
	}
	for _, name := range []string{"flow_restored_chairs", "flow_cycle_partial", "flow_coord_first", "flow_coord_no_ride", "flow_coord_other", "flow_coord_zero"} {
		m.counters[name] = 0
	}
}

func flowStage(status string) int {
	switch status {
	case "MATCHING":
		return flowAssigned
	case "ENROUTE":
		return flowEnroute
	case "PICKUP":
		return flowPickup
	case "CARRYING":
		return flowCarrying
	case "ARRIVED":
		return flowArrived
	case "COMPLETED":
		return flowReuse
	default:
		return flowUnknown
	}
}

// These aggregates use observationMu and only touch the affected chair. Snapshot
// derives active ages and integrated chair-ms without scanning ride history.
func (s *rideState) startFlow(r *memoryRide, at time.Time, full bool) {
	if r.ChairID.Valid {
		defer s.refreshCoordinateRoute(r.ChairID.String)
	}
	if s.metrics == nil || s.dispatch == nil || !r.ChairID.Valid {
		return
	}
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	c := s.dispatch.chair[r.ChairID.String]
	if c == nil || c.flow.stage != flowNone {
		return
	}
	m := s.metrics
	c.flow = chairFlow{started: at, fullRide: full}
	m.flow.ongoing.chairs++
	m.flow.ongoing.entered += milliseconds(at.Sub(m.started))
	if full {
		m.flow.known++
	} else {
		m.flow.restored++
		m.counters["flow_restored_chairs"]++
	}
	s.changeFlow(c, flowStage(s.latest(r)), at, full)
}

func (s *rideState) changeFlow(c *chairMetricState, next int, at time.Time, full bool) {
	m, f := s.metrics, &c.flow
	if f.stage == next {
		return
	}
	if f.stage != flowNone {
		o := &m.flow.stage[f.stage]
		elapsed := milliseconds(at.Sub(f.entered))
		o.chairs--
		o.entered -= milliseconds(f.entered.Sub(m.started))
		o.closed += elapsed
		if f.fullStage {
			m.observeState("flow_"+flowNames[f.stage], elapsed)
		} else {
			m.counters["flow_"+flowNames[f.stage]+"_partial"]++
		}
	}
	f.stage, f.entered, f.fullStage = next, at, full
	if next == flowNone {
		m.flow.ongoing.chairs--
		m.flow.ongoing.entered -= milliseconds(f.started.Sub(m.started))
		m.flow.ongoing.closed += milliseconds(at.Sub(f.started))
		if f.fullRide {
			m.flow.known--
			m.observeState("flow_cycle", milliseconds(at.Sub(f.started)))
		} else {
			m.flow.restored--
			m.counters["flow_cycle_partial"]++
		}
		return
	}
	o := &m.flow.stage[next]
	o.chairs++
	o.entered += milliseconds(at.Sub(m.started))
}

func (s *rideState) observeFlowStatus(r *memoryRide, status string) {
	if s.metrics == nil || s.dispatch == nil || !r.ChairID.Valid {
		return
	}
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	c := s.dispatch.chair[r.ChairID.String]
	if c != nil && c.flow.stage != flowNone {
		s.changeFlow(c, flowStage(status), time.Now(), true)
	}
}

func (s *rideState) observeReusable(id string) {
	if s.metrics == nil || s.dispatch == nil {
		return
	}
	s.indexMu.RLock()
	_, available := s.matchingChairs[id]
	s.indexMu.RUnlock()
	s.observationMu.Lock()
	defer s.observationMu.Unlock()
	c := s.dispatch.chair[id]
	if c != nil && c.flow.stage == flowReuse && available {
		s.changeFlow(c, flowNone, time.Now(), true)
	}
}

func (s *rideState) resetFlow() {
	m := s.metrics
	// Restore only current work. Historical completions never become new cycles.
	for id, c := range s.dispatch.chair {
		current := s.chairCurrent.get(id)
		if current != nil && len(current.unfinished) > 0 {
			s.startFlow(current.unfinished[len(current.unfinished)-1], m.started, false)
		} else if c.pending > 0 || (!c.active && len(s.chairRides.get(id)) > 0 && s.latest(s.rides.get(s.chairRides.get(id)[len(s.chairRides.get(id))-1])) == "COMPLETED") {
			// A restored completion blocked by its ACK queue or activity.
			c.flow = chairFlow{started: m.started}
			m.flow.ongoing.chairs++
			m.flow.restored++
			m.counters["flow_restored_chairs"]++
			s.changeFlow(c, flowReuse, m.started, false)
		}
	}
}

func (m *rideMetrics) exportFlow(values map[string]any, at time.Time) {
	now := milliseconds(at.Sub(m.started))
	for i, name := range flowNames[1:] {
		o := m.flow.stage[i+1]
		age := max(0, float64(o.chairs)*now-o.entered)
		values["flow_"+name+"_chairs"] = o.chairs
		values["flow_"+name+"_age_ms"] = age
		values["flow_"+name+"_total_ms"] = o.closed + age
	}
	o := m.flow.ongoing
	values["flow_ongoing_chairs"] = o.chairs
	values["flow_ongoing_age_ms"] = max(0, float64(o.chairs)*now-o.entered)
	values["flow_ongoing_known"] = m.flow.known
	values["flow_ongoing_restored"] = m.flow.restored
}
