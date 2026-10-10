package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// Use the actual smoke artifacts through the production API and parsers. The
// temporary routing fixture is never stored as a benchmark RUN or manifest.
func TestISURIDESetupSmokeAPI(t *testing.T) {
	dir := os.Getenv("ISUCON_SETUP_SMOKE_DIR")
	if dir == "" {
		t.Skip("set ISUCON_SETUP_SMOKE_DIR to the collected setup smoke artifacts")
	}
	fixtures := t.TempDir()
	const fixtureID = "20000101-000000"
	fixture := filepath.Join(fixtures, fixtureID)
	if err := os.Mkdir(fixture, 0755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.Symlink(filepath.Join(dir, entry.Name()), filepath.Join(fixture, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	a := &app{runsDir: fixtures}
	for _, tc := range []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		array   string
		minimum int
	}{
		{"alp", a.handleAlp, "rows", 5},
		{"slowquery", a.handleSlowQuery, "classes", 1},
		{"metrics", a.handleMetrics, "hosts", 3},
		{"fgprof", a.handleFgprof, "profiles", 1},
		{"pprof", a.handleGoPprof, "profiles", 4},
		{"timeline", a.handleTimeline, "buckets", 1},
		{"mysql", a.handleMysql, "hosts", 1},
		{"upstream", a.handleUpstream, "rows", 1},
		{"user-transitions", a.handleUserTransitions, "edges", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/runs/"+fixtureID+"/"+tc.name, nil)
			req.SetPathValue("run_id", fixtureID)
			w := httptest.NewRecorder()
			tc.handler(w, req)
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			var response map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if available, ok := response["available"]; ok && string(available) != "true" {
				t.Fatal("API did not expose the smoke measurement")
			}
			var rows []json.RawMessage
			if err := json.Unmarshal(response[tc.array], &rows); err != nil || len(rows) < tc.minimum {
				t.Fatalf("%s: rows=%d error=%v response=%s", tc.array, len(rows), err, w.Body.String())
			}
			t.Logf("%s: %d %s", tc.name, len(rows), tc.array)
		})
	}
}
