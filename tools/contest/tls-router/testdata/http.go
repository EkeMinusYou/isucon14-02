// HTTP fixture for testing the real nginx/TCP routing configuration locally.
package main

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/api/internal/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	http.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Measurement-Session-ID", "fixture-session")
		if r.URL.Path == "/api/app/notification" || r.URL.Path == "/api/chair/notification" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("X-Accel-Buffering", "no")
			for i := 0; i < 3; i++ {
				fmt.Fprintf(w, "data: %d\n\n", i)
				w.(http.Flusher).Flush()
				time.Sleep(time.Second)
			}
			return
		}
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%s|%s|%s|%s", r.Method, r.Host, r.Header.Get("Cookie"), body)
	})
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
