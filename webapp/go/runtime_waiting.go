package main

import "time"

// Rescue is a FIFO trigger, not a latency guarantee above available supply.
const matchingRideRescueAge = 10 * time.Second

// The queue is ordered by created_at/id. Mark a mandatory prefix of the actual
// output horizon, extending the quarter quota through every aged request.
func requiredMatchingRides(rides []Ride, n int, now time.Time) int {
	required := (n + 3) / 4
	for required < n && now.Sub(rides[required].CreatedAt) >= matchingRideRescueAge {
		required++
	}
	return required
}
