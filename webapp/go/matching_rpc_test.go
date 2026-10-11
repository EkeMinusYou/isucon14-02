package main

import (
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

// Exercise RPC parity for joint choices, ties, fallback and full horizon.
func matchingRPCFixtures() []matchingPlan {
	rng := rand.New(rand.NewPCG(17, 31))
	var plans []matchingPlan
	for _, n := range []int{0, 1, 2, 7, 32, 127, 256} {
		for _, mode := range []string{"random", "ties", "unknown"} {
			plan := matchingPlan{count: n, required: (n + 3) / 4, rides: make([]Ride, n*2), chairs: make([]matchingChair, n*3+1)}
			for i := range plan.rides {
				plan.rides[i] = Ride{PickupLatitude: rng.IntN(100) - 50, PickupLongitude: rng.IntN(100) - 50,
					DestinationLatitude: rng.IntN(100) - 50, DestinationLongitude: rng.IntN(100) - 50}
				if mode == "ties" {
					plan.rides[i] = Ride{DestinationLatitude: 10}
				}
			}
			for i := range plan.chairs {
				plan.chairs[i] = matchingChair{Latitude: rng.IntN(100) - 50, Longitude: rng.IntN(100) - 50, Speed: rng.IntN(8)}
				if mode == "ties" {
					plan.chairs[i] = matchingChair{Speed: 2}
				}
				if mode == "unknown" {
					plan.chairs[i].Speed = 0
				}
			}
			plans = append(plans, plan)
		}
	}
	plans = append(plans, matchingPlan{count: 1, required: 1, rides: []Ride{{PickupLatitude: -2147483648, DestinationLongitude: 2147483647}}, chairs: []matchingChair{{Latitude: 2147483647, Longitude: -2147483648, Speed: 3}}})
	return plans
}

func TestMatchingRPCJointAssignments(t *testing.T) {
	client := testMatchingRPCClient(t, nil)
	for _, plan := range matchingRPCFixtures() {
		var expected []matchingAssignment
		if plan.count != 0 {
			expected = assignMatchingPlan(plan)
		}
		var actual []matchingAssignment
		if len(plan.rides) != 0 {
			var err error
			actual, err = client.solve(plan)
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("RPC changed assignments: %v != %v", actual, expected)
		}
	}

}

func testMatchingRPCClient(t *testing.T, handler http.Handler) *matchingRPCClient {
	t.Helper()
	if handler == nil {
		handler = matching.NewHandler(netip.MustParseAddr("127.0.0.1"))
	}
	server := httptest.NewServer(handler)
	client, err := newMatchingRPCClient(server.URL + "/solve")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.client.CloseIdleConnections(); server.Close() })
	return client
}

func TestMatchingRPCFailuresDoNotPublish(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{
		{"unavailable", ``, http.StatusServiceUnavailable},
		{"malformed", `{`, http.StatusOK},
		{"truncated", `{"r":[0,1],"c":[0,1]`, http.StatusOK},
		{"trailing", `{"r":[0,1],"c":[0,1]}{}`, http.StatusOK},
		{"missing-row", `{"r":[0,1],"c":[0]}`, http.StatusOK},
		{"extra-row", `{"r":[0,1],"c":[0,1,2]}`, http.StatusOK},
		{"ride-duplicate", `{"r":[0,0],"c":[0,1]}`, http.StatusOK},
		{"rescue-omitted", `{"r":[1,2],"c":[0,1]}`, http.StatusOK},
		{"ride-negative", `{"r":[-1,0],"c":[0,1]}`, http.StatusOK},
		{"ride-range", `{"r":[0,3],"c":[0,1]}`, http.StatusOK},
		{"ride-null", `{"r":[null,1],"c":[0,1]}`, http.StatusOK},
		{"duplicate", `{"r":[0,1],"c":[0,0]}`, http.StatusOK},
		{"negative", `{"r":[0,1],"c":[-1,0]}`, http.StatusOK},
		{"out-of-range", `{"r":[0,1],"c":[0,4]}`, http.StatusOK},
		{"null-index", `{"r":[0,1],"c":[null,1]}`, http.StatusOK},
		{"null-response", `null`, http.StatusOK},
		{"unknown-field", `{"r":[0,1],"c":[0,1],"unexpected":1}`, http.StatusOK},
		{"oversized", `{"r":[0,1],"c":[0,1]}` + strings.Repeat(" ", matching.MaxResponseBytes), http.StatusOK},
		{"disconnect", ``, 0},
		{"timeout", ``, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			rides, chairs := 2, 4
			if test.name == "rescue-omitted" {
				rides, chairs = 3, 2
			}
			s := matchingBatchTestState(t, rides, chairs)
			good := s.matchingSolver
			s.matchingSolver = testMatchingRPCClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch test.status {
				case 0:
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
				case -1:
					// Release a late response only after the client has timed out.
					io.Copy(io.Discard, r.Body)
					<-r.Context().Done()
					io.WriteString(w, `{"r":[0,1],"c":[0,1]}`)
				default:
					w.WriteHeader(test.status)
					io.WriteString(w, test.body)
				}
			}))
			if test.status == -1 {
				s.matchingSolver.client.Timeout = 50 * time.Millisecond
			}
			err := s.match()
			if err == nil {
				t.Fatal("invalid RPC was accepted")
			}
			if test.name == "rescue-omitted" && !strings.Contains(err.Error(), "required ride") {
				t.Fatalf("rescue failed for wrong reason: %v", err)
			}
			if test.status == -1 {
				var timeout interface{ Timeout() bool }
				if !errors.As(err, &timeout) || !timeout.Timeout() {
					t.Fatalf("want timeout, got %v", err)
				}
			}
			if s.sequence != 0 || len(readTestWAL(t, s)) != 0 || len(s.waitingRides) != rides || len(s.matchingChairs) != chairs {
				t.Fatal("failed RPC wrote WAL or changed assignment indexes")
			}
			for _, ride := range s.rides.all() {
				if ride.ChairID.Valid || !ride.assignedAt.IsZero() {
					t.Fatal("failed RPC published ride assignment")
				}
			}
			for _, queue := range s.chairEvents.all() {
				if queue.pending.Len() != 0 {
					t.Fatal("failed RPC published chair notification")
				}
			}
			if s.metrics.series["dispatch_batch"].count != 0 || s.metrics.opportunity.planning != 0 {
				t.Fatal("failed RPC left an assignment or unfinished timing observation")
			}
			// The next invocation uses a fresh snapshot and can make progress.
			s.matchingSolver = good
			if err := s.match(); err != nil {
				t.Fatal(err)
			}
			if s.sequence != 1 || len(readTestWAL(t, s)) != 1 || len(s.waitingRides) != rides-min(rides, chairs) {
				t.Fatal("next invocation did not recover")
			}
		})
	}
}

func TestMatchingRPCSnapshotRevalidation(t *testing.T) {
	for _, change := range []string{"inactive", "generation", "coordinate", "wal-failure"} {
		t.Run(change, func(t *testing.T) {
			s := matchingBatchTestState(t, 2, 3)
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			worker := matching.NewHandler(netip.MustParseAddr("127.0.0.1"))
			s.matchingSolver = testMatchingRPCClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					close(started)
					<-release
				}
				worker.ServeHTTP(w, r)
			}))
			finished := make(chan error, 1)
			go func() { finished <- s.match() }()
			<-started
			// These updates must progress while the owner awaits the remote solver.
			switch change {
			case "inactive":
				if err := s.activity("chair-000", false); err != nil {
					t.Fatal(err)
				}
			case "generation":
				s.walMu.Lock()
				s.generation = "initialized-again"
				s.walMu.Unlock()
			case "coordinate":
				if _, err := s.coordinate("chair-000", Coordinate{Latitude: 10, Longitude: 20}); err != nil {
					t.Fatal(err)
				}
			case "wal-failure":
				if err := s.wal.Close(); err != nil {
					t.Fatal(err)
				}
			}
			sequence := s.sequence
			close(release)
			err := <-finished
			if (err != nil) != (change == "wal-failure") {
				t.Fatalf("unexpected error: %v", err)
			}
			switch change {
			case "inactive":
				if calls.Load() != 2 || s.sequence != sequence+1 || len(s.waitingRides) != 0 {
					t.Fatal("conflict did not retry a fresh snapshot")
				}
				for _, ride := range s.rides.all() {
					if ride.ChairID.String == "chair-000" {
						t.Fatal("stale chair committed")
					}
				}
			case "coordinate":
				if calls.Load() != 1 || s.sequence != sequence+1 || len(s.waitingRides) != 0 {
					t.Fatal("coordinate update changed snapshot acceptance")
				}
				if s.rides.get("ride-000").ChairID.String != "chair-000" {
					t.Fatal("RPC did not retain snapshot assignment")
				}
			case "generation", "wal-failure":
				if calls.Load() != 1 || s.sequence != sequence || len(s.waitingRides) != 2 {
					t.Fatal("uncommitted result was published")
				}
				for _, ride := range s.rides.all() {
					if ride.ChairID.Valid {
						t.Fatal("stale or failed WAL result published")
					}
				}
			}
		})
	}
}
