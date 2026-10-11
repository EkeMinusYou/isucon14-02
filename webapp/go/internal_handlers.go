package main

import (
	"context"
	"net/http"

	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

// Bound both the FIFO planning horizon and one atomic WAL assignment record.
const matchingPlanLimit = matching.PlanLimit

func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	if err := matchWaitingRides(r.Context()); err != nil {
		writeStateError(w, err)
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
