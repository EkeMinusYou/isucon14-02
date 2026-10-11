package main

import (
	"context"
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
		stream := r.URL.Path == "/api/app/notification" || r.URL.Path == "/api/chair/notification"
		if stream {
			stateLifecycle.RLock()
		}
		accessToken := runtimeState.accountKey(c.Value)
		userID, ok := runtimeState.userTokens.lookup(accessToken)
		user := runtimeState.users.get(userID)
		ready := runtimeState.ready
		if stream {
			stateLifecycle.RUnlock()
		}
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
		stream := r.URL.Path == "/api/app/notification" || r.URL.Path == "/api/chair/notification"
		if stream {
			stateLifecycle.RLock()
		}
		accessToken := runtimeState.accountKey(c.Value)
		ownerID, ok := runtimeState.ownerTokens.lookup(accessToken)
		owner := runtimeState.owners.get(ownerID)
		ready := runtimeState.ready
		if stream {
			stateLifecycle.RUnlock()
		}
		if !ready {
			writeError(w, http.StatusServiceUnavailable, errors.New("runtime state is unavailable"))
			return
		}
		if !ok {
			writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
			return
		}

		ctx = context.WithValue(ctx, "owner", &owner)
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
		stream := r.URL.Path == "/api/app/notification" || r.URL.Path == "/api/chair/notification"
		if stream {
			stateLifecycle.RLock()
		}
		accessToken := runtimeState.accountKey(c.Value)
		var chair Chair
		var ok, ready bool
		if r.URL.Path == "/api/chair/coordinate" {
			ready = runtimeState.coordinateAvailable() == nil
			if id, found := runtimeState.coordinates.tokens.Load(accessToken); found {
				if cell := runtimeState.coordinateCell(id.(string)); cell != nil {
					if snapshot := cell.chair.Load(); snapshot != nil {
						chair, ok = *snapshot, true
					}
				}
			}
		} else {
			chairID, found := runtimeState.chairTokens.lookup(accessToken)
			chair, ok, ready = runtimeState.chairs.get(chairID), found, runtimeState.ready
		}
		if stream {
			stateLifecycle.RUnlock()
		}
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
		var err error
		if r.URL.Path == "/api/chair/coordinate" {
			err = runtimeState.coordinateAvailable()
		} else {
			err = runtimeState.checkReady()
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}
