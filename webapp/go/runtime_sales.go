package main

import (
	"sort"
	"time"
)

// One prefix total per distinct ride UpdatedAt, using the undiscounted fare.
type salesPoint struct {
	At    time.Time
	Total int
}

type rideSale struct {
	ChairID string
	At      time.Time
	Amount  int
}

func (s *rideState) saleForRide(ride *memoryRide) rideSale {
	if s.sales == nil || !ride.ChairID.Valid || s.latest(ride) != "COMPLETED" {
		return rideSale{}
	}
	return rideSale{ChairID: ride.ChairID.String, At: ride.UpdatedAt, Amount: calculateSale(ride.Ride)}
}

// Called under mu after a successful WAL append, or during reconstruction.
// Comparing contributions makes repeated completion records and
// evaluation-only changes idempotent.
func (s *rideState) updateRideSale(before rideSale, ride *memoryRide) {
	after := s.saleForRide(ride)
	if before.ChairID == after.ChairID && before.At.Equal(after.At) && before.Amount == after.Amount {
		return
	}
	if before.Amount != 0 {
		s.addSale(before.ChairID, before.At, -before.Amount)
	}
	if after.Amount != 0 {
		s.addSale(after.ChairID, after.At, after.Amount)
	}
}

func (s *rideState) addSale(chairID string, at time.Time, amount int) {
	points := s.sales[chairID]
	n := len(points)
	// Normal completions append in constant amortized time.
	if n == 0 || at.After(points[n-1].At) {
		total := amount
		if n != 0 {
			total += points[n-1].Total
		}
		s.sales[chairID] = append(points, salesPoint{At: at, Total: total})
		return
	}
	i := n - 1
	if !at.Equal(points[i].At) {
		i = sort.Search(n, func(i int) bool { return !points[i].At.Before(at) })
	}
	if !points[i].At.Equal(at) {
		points = append(points, salesPoint{})
		copy(points[i+1:], points[i:])
		points[i] = salesPoint{At: at}
		if i != 0 {
			points[i].Total = points[i-1].Total
		}
	}
	// Late timestamps and corrections change only this suffix, never API reads.
	for j := i; j < len(points); j++ {
		points[j].Total += amount
	}
	s.sales[chairID] = points
}

// Both bounds are inclusive; the handler supplies the existing 999us extension.
func (s *rideState) salesBetween(chairID string, since, until time.Time) int {
	points := s.sales[chairID]
	if len(points) == 0 || until.Before(since) {
		return 0
	}
	lo, hi := 0, len(points)
	if since.After(points[0].At) {
		lo = sort.Search(len(points), func(i int) bool { return !points[i].At.Before(since) })
	}
	if until.Before(points[len(points)-1].At) {
		hi = sort.Search(len(points), func(i int) bool { return points[i].At.After(until) })
	}
	total := 0
	if hi != 0 {
		total = points[hi-1].Total
	}
	if lo != 0 {
		total -= points[lo-1].Total
	}
	return total
}
