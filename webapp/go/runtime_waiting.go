package main

import "time"

// Rescue is a FIFO trigger, not a latency guarantee above available supply.
const matchingRideRescueAge = 10 * time.Second

// The queue is ordered by created_at/id. Mark the aged prefix of the actual
// output horizon as mandatory. Younger rides compete only on pickup and trip,
// so frequent small plans are not forced to send distant chairs to them.
func requiredMatchingRides(rides []Ride, n int, now time.Time) int {
	required := 0
	for required < n && now.Sub(rides[required].CreatedAt) >= matchingRideRescueAge {
		required++
	}
	return required
}
