package main

import (
	"context"
	"net/http"
)

const matchingBatchSize = 64

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
