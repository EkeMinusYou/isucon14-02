package main

import (
	"context"
	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

func assignMatchingPlan(plan matchingPlan) []matchingAssignment {
	result, err := matching.Solve(context.Background(), matchingRequest(plan))
	if err != nil {
		panic(err)
	}
	assignments := make([]matchingAssignment, len(result))
	for i, a := range result {
		assignments[i] = matchingAssignment{a.RideIndex, a.ChairIndex}
	}
	return assignments
}
