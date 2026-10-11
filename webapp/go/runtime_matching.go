package main

import (
	"database/sql"
	"errors"
	"sort"
	"time"
)

type matchingPlan struct {
	observation        *planTiming
	waiting, available int
	generation         string
	rides              []Ride
	chairs             []matchingChair
}

func (s *rideState) matchingChair(id string) matchingChair {
	chair, d := s.chairs[id], s.chairDistance(id)
	speed := s.modelSpeeds[chair.Model]
	if !d.Latitude.Valid || !d.Longitude.Valid {
		speed = 0
	}
	return matchingChair{ID: id, Latitude: int(d.Latitude.Int64), Longitude: int(d.Longitude.Int64), Speed: speed}
}

// Extract ride candidates and copy values under mu. Chair pruning and solving
// use only this snapshot; coordinates can continue changing during the solver.
func (s *rideState) snapshotMatching(generation string, minimum int, timing ...*planTiming) (matchingPlan, error) {
	var t *planTiming
	var started, acquired time.Time
	if len(timing) > 0 {
		t = timing[0]
		started = time.Now()
	}
	s.mu.RLock()
	if t != nil {
		acquired = time.Now()
		t.snapshotLock = acquired.Sub(started)
	}
	defer func() {
		if t != nil {
			t.snapshot = time.Since(acquired)
		}
		s.mu.RUnlock()
	}()
	if !s.ready || s.failed != nil || s.coordinateFailed.Load() != nil {
		return matchingPlan{}, errors.New("state WAL is unavailable")
	}
	n := min(len(s.waitingRides), len(s.matchingChairs), matchingPlanLimit)
	plan := matchingPlan{generation: s.generation, waiting: len(s.waitingRides), available: len(s.matchingChairs)}
	if n == 0 || n < minimum || (generation != "" && generation != s.generation) {
		return plan, nil
	}
	plan.chairs = make([]matchingChair, 0, len(s.matchingChairs))
	for id := range s.matchingChairs {
		plan.chairs = append(plan.chairs, s.matchingChair(id))
	}
	plan.rides = s.selectWaitingRides(n, plan.chairs, time.Now())
	return plan, nil
}

// A conflict invalidates the entire plan: accepting only its surviving pairs
// could drop a rescued ride or retain choices that depended on a lost chair.
// Return retry only for conflicts within the same initialization generation.
func (s *rideState) commitMatching(plan matchingPlan, assignments []matchingAssignment) (retry bool, err error) {
	started := time.Now()
	s.mu.Lock()
	acquired := time.Now()
	reason := "ok"
	defer func() {
		s.finishPlan(plan.observation, reason, acquired.Sub(started), time.Since(acquired), true)
		s.mu.Unlock()
	}()
	if !s.ready || s.failed != nil || s.coordinateFailed.Load() != nil {
		reason = "unavailable"
		return false, errors.New("state WAL is unavailable")
	}
	if s.generation != plan.generation {
		reason = "generation"
		return false, nil
	}
	if len(s.waitingRides) < len(plan.rides) {
		reason = "conflict_rides"
		return true, nil
	}
	seenRides := make(map[string]bool, len(plan.rides))
	for _, row := range plan.rides {
		current := s.rides[row.ID]
		if current == nil || current.ChairID.Valid || s.latest(current) != "MATCHING" || seenRides[row.ID] {
			reason = "conflict_rides"
			return true, nil
		}
		seenRides[row.ID] = true
	}
	if len(assignments) != len(plan.rides) {
		reason = "conflict_rides"
		return true, nil
	}
	seenRows := make(map[int]bool, len(assignments))
	seenChairs := make(map[string]bool, len(assignments))
	for _, assignment := range assignments {
		if assignment.rideIndex < 0 || assignment.rideIndex >= len(plan.rides) || seenRows[assignment.rideIndex] {
			reason = "conflict_rides"
			return true, nil
		}
		seenRows[assignment.rideIndex] = true
		if assignment.chairIndex < 0 || assignment.chairIndex >= len(plan.chairs) {
			reason = "conflict_chairs"
			return true, nil
		}
		id := plan.chairs[assignment.chairIndex].ID
		chair, exists := s.chairs[id]
		current, queue := s.chairCurrent[id], s.chairEvents[id]
		if !exists || !chair.IsActive || seenChairs[id] || (current != nil && len(current.unfinished) != 0) ||
			(queue != nil && queue.pending.Len() != 0) {
			reason = "conflict_chairs"
			return true, nil
		}
		seenChairs[id] = true
	}
	record := stateRecord{Rides: make([]Ride, 0, len(assignments))}
	for _, assignment := range assignments {
		row := s.rides[plan.rides[assignment.rideIndex].ID].Ride
		chairID := plan.chairs[assignment.chairIndex].ID
		at := time.Now().UTC().Truncate(time.Microsecond)
		previous := s.chairRides[chairID]
		if len(previous) > 0 {
			last := s.rides[previous[len(previous)-1]].UpdatedAt
			if !at.After(last) {
				at = last.Add(time.Microsecond)
			}
		}
		row.ChairID, row.UpdatedAt = sql.NullString{String: chairID, Valid: true}, at
		record.Rides = append(record.Rides, row)
		// Report the estimated pickup at commit, not the older solver position.
		plan.chairs[assignment.chairIndex] = s.matchingChair(chairID)
	}
	if err := s.commit(record); err != nil {
		reason = "wal_fail"
		return false, err
	}
	if plan.observation != nil {
		plan.observation.assigned = len(record.Rides)
	}
	s.observeAssignments(record.Rides, plan.chairs, assignments, time.Now())
	for _, row := range record.Rides {
		notifyRide(&row)
	}
	return false, nil
}

func (s *rideState) match() error {
	// Serialize planners, not state updates. Avoid computing overlapping plans
	// for concurrent matcher requests while coordinate/ACK handlers keep going.
	started := time.Now()
	s.matchingMu.Lock()
	defer s.matchingMu.Unlock()
	s.observeMatcherCall(time.Since(started))
	generation, err := s.matchBatch("", 0)
	if err != nil || generation == "" {
		return err
	}
	// Preserve main's saturation rule: a second complete plan only when both
	// waiting demand and ACK-gated supply can fill another planning horizon.
	_, err = s.matchBatch(generation, matchingPlanLimit)
	return err
}

// Caller serializes planners with matchingMu. Each plan has its own snapshot,
// WAL record and metrics; state updates can progress between and during plans.
func (s *rideState) matchBatch(generation string, minimum int) (string, error) {
	var lastTiming *planTiming
	for attempt := 0; attempt < 3; attempt++ {
		timing := s.beginPlan()
		lastTiming = timing
		plan, err := s.snapshotMatching(generation, minimum, timing)
		plan.observation = timing
		timing.rides, timing.candidates = len(plan.rides), len(plan.chairs)
		if err != nil || len(plan.rides) == 0 {
			reason := "below_min"
			switch {
			case err != nil:
				reason = "unavailable"
			case generation != "" && generation != plan.generation:
				reason = "generation"
			case plan.waiting == 0:
				reason = "empty_demand"
			case plan.available == 0:
				reason = "empty_supply"
			}
			s.finishEmptyPlan(timing, reason)
			return "", err
		}
		solving := time.Now()
		sort.Slice(plan.chairs, func(i, j int) bool { return plan.chairs[i].ID < plan.chairs[j].ID })
		assignments, err := s.matchingSolver.solve(plan.rides, plan.chairs)
		timing.solver = time.Since(solving)
		if err != nil {
			s.finishEmptyPlan(timing, "unavailable")
			return "", err
		}
		retry, err := s.commitMatching(plan, assignments)
		if err != nil || !retry {
			return plan.generation, err
		}
	}
	s.mu.Lock()
	if lastTiming.metrics != nil && lastTiming.metrics == s.metrics && lastTiming.generation == s.generation {
		s.metrics.counters["match_abandon_limit"]++
	}
	s.mu.Unlock()
	// Continued churn defers work to the next external matcher invocation.
	return "", nil
}
