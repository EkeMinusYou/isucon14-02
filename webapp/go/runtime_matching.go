package main

import (
	"container/heap"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

type matchingPlan struct {
	observation        *planTiming
	waiting, available int
	generation         string
	count, required    int
	rides              []Ride
	chairs             []matchingChair
}

func (s *rideState) matchingChair(id string) matchingChair {
	chair, d := s.chairs.get(id), s.chairDistance(id)
	speed := s.modelSpeeds[chair.Model]
	if speed <= 0 || !d.Latitude.Valid || !d.Longitude.Valid {
		speed = 0
	}
	return matchingChair{ID: id, Latitude: int(d.Latitude.Int64), Longitude: int(d.Longitude.Int64), Speed: speed}
}

// Copy bounded immutable candidate values under the index lock. Spatial
// candidate construction and joint selection run on the worker without locks.
func (s *rideState) snapshotMatching(generation string, minimum int, timing ...*planTiming) (matchingPlan, error) {
	var t *planTiming
	if len(timing) > 0 {
		t = timing[0]
	}
	started := time.Now()
	s.indexMu.RLock()
	acquired := time.Now()
	if t != nil {
		t.snapshotLock = acquired.Sub(started)
		defer func() { t.snapshot = time.Since(acquired) }()
	}
	if err := s.coordinateAvailable(); err != nil {
		s.indexMu.RUnlock()
		return matchingPlan{}, err
	}
	n := min(len(s.waitingRides), len(s.matchingChairs), matchingPlanLimit)
	plan := matchingPlan{generation: s.generation, waiting: len(s.waitingRides), available: len(s.matchingChairs)}
	if n == 0 || n < minimum || (generation != "" && generation != s.generation) {
		s.indexMu.RUnlock()
		return plan, nil
	}
	plan.count = n
	// Only copy bounded immutable values under the index lock. All spatial
	// candidate construction and pair scoring run on the matching worker.
	plan.rides = make([]Ride, min(len(s.waitingRides), matching.MaxRides))
	for i := range plan.rides {
		plan.rides[i] = s.waitingRides[i].Ride
	}
	plan.required = requiredMatchingRides(plan.rides, n, time.Now())
	// Keep the smallest IDs if supply exceeds the RPC cap. Normally this is
	// just a linear copy; the bounded heap is needed only above MaxChairs.
	ids := make(chairIDHeap, 0, min(len(s.matchingChairs), matching.MaxChairs))
	for id := range s.matchingChairs {
		if len(ids) < matching.MaxChairs {
			ids = append(ids, id)
			if len(ids) == matching.MaxChairs {
				heap.Init(&ids)
			}
		} else if id < ids[0] {
			ids[0] = id
			heap.Fix(&ids, 0)
		}
	}
	plan.chairs = make([]matchingChair, len(ids))
	for i, id := range ids {
		plan.chairs[i] = s.matchingChair(id)
	}
	s.indexMu.RUnlock()
	return plan, nil
}

// A conflict invalidates the entire plan: accepting only its surviving pairs
// could drop a rescued ride or retain choices that depended on a lost chair.
// Return retry only for conflicts within the same initialization generation.
func (s *rideState) commitMatching(plan matchingPlan, assignments []matchingAssignment) (retry bool, err error) {
	started := time.Now()
	users := make([]string, 0, len(assignments))
	selected := make([]Ride, 0, len(assignments))
	for _, a := range assignments {
		if a.rideIndex >= 0 && a.rideIndex < len(plan.rides) {
			row := plan.rides[a.rideIndex]
			users = append(users, row.UserID)
			selected = append(selected, row)
		}
	}
	unlockUsers := lockStateKeys(&s.userStateLocks, users, false)
	defer unlockUsers()
	owners := make(map[string]string, len(selected))
	ids := make([]string, 0, len(selected))
	for _, row := range selected {
		ids = append(ids, row.ID)
		owners[row.ID] = row.UserID
	}
	sort.Strings(ids)
	lockedRides := make([]*memoryRide, 0, len(ids))
	for i, id := range ids {
		if i > 0 && ids[i-1] == id {
			continue
		}
		if r := s.rides.get(id); r != nil && r.ownerID == owners[id] {
			r.mu.Lock()
			lockedRides = append(lockedRides, r)
		}
	}
	defer func() {
		for i := len(lockedRides) - 1; i >= 0; i-- {
			lockedRides[i].mu.Unlock()
		}
	}()
	chairs := make([]string, 0, len(assignments))
	for _, a := range assignments {
		if a.chairIndex >= 0 && a.chairIndex < len(plan.chairs) {
			chairs = append(chairs, plan.chairs[a.chairIndex].ID)
		}
	}
	unlockChairs := lockStateKeys(&s.chairLocks, chairs, false)
	defer unlockChairs()
	acquired := time.Now()
	reason := "ok"
	defer func() {
		s.finishPlan(plan.observation, reason, acquired.Sub(started), time.Since(acquired), true)
	}()
	if s.coordinateAvailable() != nil {
		reason = "unavailable"
		return false, errors.New("state WAL is unavailable")
	}
	if s.generation != plan.generation {
		reason = "generation"
		return false, nil
	}
	if len(assignments) != plan.count || plan.required < 0 || plan.required > plan.count {
		reason = "conflict_rides"
		return true, nil
	}
	seenRows := make(map[int]bool, len(assignments))
	seenRides := make(map[string]bool, len(assignments))
	seenChairs := make(map[string]bool, len(assignments))
	for _, assignment := range assignments {
		if assignment.rideIndex < 0 || assignment.rideIndex >= len(plan.rides) || seenRows[assignment.rideIndex] {
			reason = "conflict_rides"
			return true, nil
		}
		row := plan.rides[assignment.rideIndex]
		current := s.rides.get(row.ID)
		if current == nil || current.ownerID != row.UserID || current.ChairID.Valid || s.latest(current) != "MATCHING" || seenRides[row.ID] {
			reason = "conflict_rides"
			return true, nil
		}
		seenRows[assignment.rideIndex], seenRides[row.ID] = true, true
		if assignment.chairIndex < 0 || assignment.chairIndex >= len(plan.chairs) {
			reason = "conflict_chairs"
			return true, nil
		}
		id := plan.chairs[assignment.chairIndex].ID
		chair, exists := s.chairs.lookup(id)
		currentChair, queue := s.chairCurrent.get(id), s.chairEvents.get(id)
		if !exists || !chair.IsActive || seenChairs[id] || (currentChair != nil && len(currentChair.unfinished) != 0) ||
			(queue != nil && queue.pending.Len() != 0) {
			reason = "conflict_chairs"
			return true, nil
		}
		seenChairs[id] = true
	}
	for i := 0; i < plan.required; i++ {
		if !seenRows[i] {
			reason = "conflict_rides"
			return true, nil
		}
	}
	record := stateRecord{Rides: make([]Ride, 0, len(assignments))}
	for _, assignment := range assignments {
		row := s.rides.get(plan.rides[assignment.rideIndex].ID).Ride
		chairID := plan.chairs[assignment.chairIndex].ID
		at := time.Now().UTC().Truncate(time.Microsecond)
		previous := s.chairRides.get(chairID)
		if len(previous) > 0 {
			last := s.rides.get(previous[len(previous)-1]).UpdatedAt
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
		timing.rides, timing.candidates = plan.count, len(plan.chairs)
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
		assignments, err := s.matchingSolver.solve(plan)
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
	s.observationMu.Lock()
	if lastTiming.metrics != nil && lastTiming.metrics == s.metrics && lastTiming.generation == s.generation {
		s.metrics.counters["match_abandon_limit"]++
	}
	s.observationMu.Unlock()
	// Continued churn defers work to the next external matcher invocation.
	return "", nil
}

// A max-heap bounds snapshot supply while preserving deterministic IDs.
type chairIDHeap []string

func (h chairIDHeap) Len() int           { return len(h) }
func (h chairIDHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h chairIDHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *chairIDHeap) Push(x any)        { *h = append(*h, x.(string)) }
func (h *chairIDHeap) Pop() any          { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }
