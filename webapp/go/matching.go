package main

import "github.com/isucon/isucon14/webapp/go/internal/matching"

type matchingChair struct {
	ID        string `db:"id"`
	Latitude  int    `db:"latitude"`
	Longitude int    `db:"longitude"`
	Speed     int    `db:"speed"`
}
type matchingAssignment struct{ rideIndex, chairIndex int }

func matchingTravelTime(ride Ride, chair matchingChair) float64 {
	return matching.TravelTime(matching.Ride{PickupLatitude: ride.PickupLatitude, PickupLongitude: ride.PickupLongitude,
		DestinationLatitude: ride.DestinationLatitude, DestinationLongitude: ride.DestinationLongitude},
		matching.Chair{Latitude: chair.Latitude, Longitude: chair.Longitude, Speed: chair.Speed})
}
func matchingDistance(aLat, aLon, bLat, bLon int) int64 {
	return matching.Distance(aLat, aLon, bLat, bLon)
}
