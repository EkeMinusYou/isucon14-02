package main

import (
	"database/sql"
	"errors"
	"sort"
	"time"
)

type matchingPlan struct {
	generation string
	rides      []Ride
	chairs     []matchingChair
}

func (s *rideState) matchingChair(id string) matchingChair {
	chair, d := s.chairs[id], s.distances[id]
	speed := s.modelSpeeds[chair.Model]
	if !d.Latitude.Valid || !d.Longitude.Valid {
		speed = 0
	}
	return matchingChair{ID: id, Latitude: int(d.Latitude.Int64), Longitude: int(d.Longitude.Int64), Speed: speed}
}

// Copy values under mu; sorting, candidate extraction and solving use only
// this snapshot. Coordinates can continue changing while the solver runs.
func (s *rideState) snapshotMatching(generation string, minimum int) (matchingPlan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.ready || s.failed != nil {
		return matchingPlan{}, errors.New("state WAL is unavailable")
	}
	n := min(len(s.waitingRides), len(s.matchingChairs), matchingPlanLimit)
	plan := matchingPlan{generation: s.generation}
	if n == 0 || n < minimum || (generation != "" && generation != s.generation) {
		return plan, nil
	}
	plan.rides = make([]Ride, n)
	for i, ride := range s.waitingRides[:n] {
		plan.rides[i] = ride.Ride
	}
	plan.chairs = make([]matchingChair, 0, len(s.matchingChairs))
	for id := range s.matchingChairs {
		plan.chairs = append(plan.chairs, s.matchingChair(id))
	}
	return plan, nil
}

// A conflict invalidates the entire plan: accepting only its surviving pairs
// could skip an older ride or retain choices that depended on a lost chair.
// Return retry only for conflicts within the same initialization generation.
func (s *rideState) commitMatching(plan matchingPlan, assignments []matchingAssignment) (retry bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready || s.failed != nil {
		return false, errors.New("state WAL is unavailable")
	}
	if s.generation != plan.generation {
		return false, nil
	}
	if len(s.waitingRides) < len(plan.rides) {
		return true, nil
	}
	for i, row := range plan.rides {
		current := s.rides[row.ID]
		if current == nil || current.ChairID.Valid || s.waitingRides[i].ID != row.ID {
			return true, nil
		}
	}
	for _, assignment := range assignments {
		id := plan.chairs[assignment.chairIndex].ID
		chair, exists := s.chairs[id]
		current, queue := s.chairCurrent[id], s.chairEvents[id]
		if !exists || !chair.IsActive || (current != nil && len(current.unfinished) != 0) ||
			(queue != nil && queue.pending.Len() != 0) {
			return true, nil
		}
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
		return false, err
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
	s.matchingMu.Lock()
	defer s.matchingMu.Unlock()
	generation, err := s.matchBatch("", 0)
	if err != nil || generation == "" {
		return err
	}
	// Preserve main's saturation rule: a second complete plan only when both
	// FIFO demand and ACK-gated supply can fill another planning horizon.
	_, err = s.matchBatch(generation, matchingPlanLimit)
	return err
}

// Caller serializes planners with matchingMu. Each plan has its own snapshot,
// WAL record and metrics; state updates can progress between and during plans.
func (s *rideState) matchBatch(generation string, minimum int) (string, error) {
	for attempt := 0; attempt < 3; attempt++ {
		plan, err := s.snapshotMatching(generation, minimum)
		if err != nil || len(plan.rides) == 0 {
			return "", err
		}
		sort.Slice(plan.chairs, func(i, j int) bool { return plan.chairs[i].ID < plan.chairs[j].ID })
		assignments := assignMatchingChairs(plan.rides, plan.chairs)
		retry, err := s.commitMatching(plan, assignments)
		if err != nil || !retry {
			return plan.generation, err
		}
	}
	// Continued churn defers work to the next external matcher invocation.
	return "", nil
}
