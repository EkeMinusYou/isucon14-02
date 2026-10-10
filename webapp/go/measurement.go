package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
)

// Emit an irreversible session pseudonym without persisting access tokens.
func measurementSessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var name string
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/app/"):
			name = "app_session"
		case strings.HasPrefix(r.URL.Path, "/api/owner/"):
			name = "owner_session"
		case strings.HasPrefix(r.URL.Path, "/api/chair/"):
			name = "chair_session"
		}
		if name != "" {
			if cookie, err := r.Cookie(name); err == nil && cookie.Value != "" {
				digest := sha256.Sum256([]byte(name + ":" + cookie.Value))
				w.Header().Set("X-Measurement-Session-ID", hex.EncodeToString(digest[:]))
			}
		}
		next.ServeHTTP(w, r)
	})
}
