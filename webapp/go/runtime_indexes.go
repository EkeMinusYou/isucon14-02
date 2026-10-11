package main

import (
	"sort"
	"time"
)

// Chair current state is chair-owned. Candidate indexes use indexMu and contain
// immutable ride copies, separate from the business state.
// Completed rides stay in the historical maps but leave the working sets.
type chairCurrentState struct {
	unfinished        []*memoryRide
	completionPending int
	nearbyAvailableAt time.Time
}

func (s *rideState) resetCurrentIndexes() {
	s.waitingRides = nil
	s.userActiveRides = newStateMap(map[string]int{})
	s.chairCurrent = newStateMap(map[string]*chairCurrentState{})
	s.matchingChairs = map[string]struct{}{}
	s.nearbyChairs = map[string]*coordinateCell{}
	s.nearbyMissingStatus = map[string]struct{}{}
}

func ridePrecedes(a, b *memoryRide) bool {
	if a.CreatedAt.Equal(b.CreatedAt) {
		return a.ID < b.ID
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

// SQL creation can commit out of order. FIFO follows created_at/id, not arrival.
func insertCurrentRide(rides []*memoryRide, ride *memoryRide) []*memoryRide {
	i := sort.Search(len(rides), func(i int) bool { return !ridePrecedes(rides[i], ride) })
	rides = append(rides, nil)
	copy(rides[i+1:], rides[i:])
	rides[i] = ride
	return rides
}

func removeCurrentRide(rides []*memoryRide, ride *memoryRide) []*memoryRide {
	i := sort.Search(len(rides), func(i int) bool { return !ridePrecedes(rides[i], ride) })
	if i < len(rides) && rides[i].ID == ride.ID {
		copy(rides[i:], rides[i+1:])
		rides[len(rides)-1] = nil
		rides = rides[:len(rides)-1]
	}
	return rides
}

func (s *rideState) currentChair(id string) *chairCurrentState {
	current := s.chairCurrent.get(id)
	if current == nil {
		current = &chairCurrentState{}
		s.chairCurrent.set(id, current)
	}
	return current
}

func (s *rideState) indexRide(ride *memoryRide) {
	if !ride.ChairID.Valid {
		entry := &memoryRide{Ride: ride.Ride, ownerID: ride.UserID}
		s.indexMu.Lock()
		s.waitingRides = insertCurrentRide(s.waitingRides, entry)
		s.indexMu.Unlock()
	}
	if s.latest(ride) != "COMPLETED" {
		s.userActiveRides.set(ride.UserID, s.userActiveRides.get(ride.UserID)+1)
	}
	if ride.ChairID.Valid {
		s.indexChairRide(ride)
	}
}

func (s *rideState) indexChairRide(ride *memoryRide) {
	current := s.currentChair(ride.ChairID.String)
	if s.latest(ride) != "COMPLETED" {
		current.unfinished = insertCurrentRide(current.unfinished, ride)
	} else {
		s.indexCompletion(ride)
	}
	s.refreshChairIndexes(ride.ChairID.String)
}

func (s *rideState) indexCompletion(ride *memoryRide) {
	current := s.currentChair(ride.ChairID.String)
	status := s.statuses.get(ride.Statuses[len(ride.Statuses)-1])
	current.nearbyAvailableAt = maxTime(current.nearbyAvailableAt, status.CreatedAt.Add(nearbyCompletionGrace))
	current.nearbyAvailableAt = maxTime(current.nearbyAvailableAt, ride.NearbyAvailableAt)
	if ride.CompletionResponsePending {
		current.completionPending++
	}
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (s *rideState) refreshChairIndexes(id string) {
	defer func() { s.observeReusable(id); s.refreshCoordinateRoute(id) }()
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	delete(s.matchingChairs, id)
	delete(s.nearbyChairs, id)
	delete(s.nearbyMissingStatus, id)
	if !s.chairs.get(id).IsActive {
		return
	}
	current := s.currentChair(id)
	if len(current.unfinished) != 0 {
		if s.latest(current.unfinished[len(current.unfinished)-1]) == "" {
			s.nearbyMissingStatus[id] = struct{}{}
		}
		return
	}
	// Nearby publication does not require notification ACKs. Its time boundary
	// is checked at read time, so no timer or mutation during reads is needed.
	// Cells stay stable within a generation; reads load their latest snapshot.
	s.nearbyChairs[id] = s.coordinateCell(id)
	queue := s.chairEvents.get(id)
	if queue == nil || queue.pending.Len() == 0 {
		s.matchingChairs[id] = struct{}{}
	}
}
