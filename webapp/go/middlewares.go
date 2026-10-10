package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
)

func appAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("app_session")
		if err != nil || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("app_session cookie is required"))
			return
		}
		accessToken := c.Value
		runtimeState.mu.RLock()
		userID, ok := runtimeState.userTokens[accessToken]
		user := runtimeState.users[userID]
		ready := runtimeState.ready
		runtimeState.mu.RUnlock()
		if !ready {
			writeError(w, http.StatusServiceUnavailable, errors.New("runtime state is unavailable"))
			return
		}
		if !ok {
			writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
			return
		}

		ctx = context.WithValue(ctx, "user", &user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func ownerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("owner_session")
		if err != nil || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("owner_session cookie is required"))
			return
		}
		accessToken := c.Value
		owner := &Owner{}
		if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE access_token = ?", accessToken); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		ctx = context.WithValue(ctx, "owner", owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func chairAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("chair_session")
		if err != nil || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("chair_session cookie is required"))
			return
		}
		accessToken := c.Value
		runtimeState.mu.RLock()
		chairID, ok := runtimeState.chairTokens[accessToken]
		chair := runtimeState.chairs[chairID]
		ready := runtimeState.ready
		runtimeState.mu.RUnlock()
		if !ready {
			writeError(w, http.StatusServiceUnavailable, errors.New("runtime state is unavailable"))
			return
		}
		if !ok {
			writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
			return
		}

		ctx = context.WithValue(ctx, "chair", &chair)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Streams fence individual reads and acknowledgements so initialize can close
// them without waiting for a long-lived request to release a lifecycle lock.
func runtimeStateMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/initialize" || r.URL.Path == "/api/app/notification" || r.URL.Path == "/api/chair/notification" {
			next.ServeHTTP(w, r)
			return
		}
		stateLifecycle.RLock()
		defer stateLifecycle.RUnlock()
		if err := runtimeState.checkReady(); err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}
