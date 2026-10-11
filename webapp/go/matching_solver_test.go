package main

import "github.com/isucon/isucon14/webapp/go/internal/matching"

func assignMatchingChairs(rides []Ride, chairs []matchingChair) []matchingAssignment {
	request := matchingRequest(rides, chairs)
	result := matching.Assign(request.Rides, request.Chairs)
	if result == nil {
		return nil
	}
	assignments := make([]matchingAssignment, len(result))
	for i, a := range result {
		assignments[i] = matchingAssignment{a.RideIndex, a.ChairIndex}
	}
	return assignments
}

type matchingCandidate struct {
	chairIndex int
	cost       float64
}

// Keep the least promising candidate at the root, including stable ties.
type matchingCandidates []matchingCandidate

func (h matchingCandidates) Len() int { return len(h) }
func (h matchingCandidates) Less(i, j int) bool {
	if h[i].cost == h[j].cost {
		return h[i].chairIndex > h[j].chairIndex
	}
	return h[i].cost > h[j].cost
}
func (h matchingCandidates) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *matchingCandidates) Push(x any)   { *h = append(*h, x.(matchingCandidate)) }
func (h *matchingCandidates) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
