package main

import (
	"context"
	"net/http"

	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

// Bound both the FIFO planning horizon and one atomic WAL assignment record.
const matchingPlanLimit = matching.PlanLimit

// While this many rides still wait after a plan, demand no longer depends on
// free chairs staying visible in nearby-chairs, so the matcher plans again
// sooner. Below it, the slower cadence keeps free chairs visible to users.
const matchingSaturatedWaiting = 1000

func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	if err := matchWaitingRides(r.Context()); err != nil {
		writeStateError(w, err)
		return
	}
	// The matcher service shortens its sleep on 200.
	if runtimeState.waitingRideCount() >= matchingSaturatedWaiting {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func matchWaitingRides(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return runtimeState.match()
}

func (s *rideState) waitingRideCount() int {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	return len(s.waitingRides)
}
