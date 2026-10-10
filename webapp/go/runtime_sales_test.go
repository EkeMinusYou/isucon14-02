package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"
)

func requestOwnerSales(t *testing.T, ownerID, query string) ownerGetSalesResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/owner/sales"+query, nil)
	req = req.WithContext(context.WithValue(req.Context(), "owner", &Owner{ID: ownerID}))
	res := httptest.NewRecorder()
	ownerGetSales(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("sales: %d %s", res.Code, res.Body.String())
	}
	var body ownerGetSalesResponse
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	sort.Slice(body.Chairs, func(i, j int) bool { return body.Chairs[i].ID < body.Chairs[j].ID })
	sort.Slice(body.Models, func(i, j int) bool { return body.Models[i].Model < body.Models[j].Model })
	return body
}

// Keep the previous history scan as an independent result oracle.
func scanChairSales(s *rideState, chairID string, since, until time.Time) int {
	total := 0
	for _, id := range s.chairRides[chairID] {
		ride := s.rides[id]
		if s.latest(ride) == "COMPLETED" && !ride.UpdatedAt.Before(since) && !ride.UpdatedAt.After(until) {
			total += calculateSale(ride.Ride)
		}
	}
	return total
}

func TestSalesPeriodsAndOwnerResponse(t *testing.T) {
	s := memoryTestState(t)
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState = previous })
	for _, chair := range []Chair{
		{ID: "a", OwnerID: "o", Model: "shared", Name: "A"},
		{ID: "b", OwnerID: "o", Model: "shared", Name: "B"},
		{ID: "empty", OwnerID: "o", Model: "empty"},
		{ID: "other", OwnerID: "other", Model: "shared"},
	} {
		s.addChair(chair)
	}
	base := time.UnixMilli(1_000)
	// Equal and out-of-order completions include the exact microsecond limits.
	for i, offset := range []time.Duration{2 * time.Millisecond, 999 * time.Microsecond, 0, time.Millisecond, 0, -time.Microsecond, 999*time.Microsecond + time.Nanosecond} {
		id := fmt.Sprintf("r%d", i)
		seedMemoryRide(s, id, "a", base.Add(-time.Hour))
		row := s.rides[id].Ride
		row.UpdatedAt = base.Add(offset)
		s.rides[id].Fare = 1 // Discounted payment must not reduce owner sales.
		if err := s.commit(stateRecord{Rides: []Ride{row}, Statuses: []RideStatus{{ID: id + "-complete", RideID: id, Status: "COMPLETED", CreatedAt: base.Add(time.Hour)}}}); err != nil {
			t.Fatal(err)
		}
	}
	seedMemoryRide(s, "b", "b", base)
	s.addStatus(RideStatus{ID: "b-complete", RideID: "b", Status: "COMPLETED", CreatedAt: base})
	seedMemoryRide(s, "other", "other", base)
	s.addStatus(RideStatus{ID: "other-complete", RideID: "other", Status: "COMPLETED", CreatedAt: base})
	seedMemoryRide(s, "arrived", "a", base)
	s.addStatus(RideStatus{ID: "arrived-status", RideID: "arrived", Status: "ARRIVED", CreatedAt: base})
	seedMemoryRide(s, "unassigned", "", base)
	s.addStatus(RideStatus{ID: "unassigned-complete", RideID: "unassigned", Status: "COMPLETED", CreatedAt: base})
	evaluation := 5
	seedMemoryRide(s, "rated", "a", base)
	s.rides["rated"].Evaluation = &evaluation // Evaluation alone is not completion.
	for lo := -2; lo <= 4; lo++ {
		for hi := -2; hi <= 4; hi++ {
			since := base.Add(time.Duration(lo) * time.Millisecond)
			until := base.Add(time.Duration(hi)*time.Millisecond + 999*time.Microsecond)
			for _, chairID := range []string{"a", "b", "empty", "other"} {
				if got, want := s.salesBetween(chairID, since, until), scanChairSales(s, chairID, since, until); got != want {
					t.Fatalf("%s [%v, %v]: %d != %d", chairID, since, until, got, want)
				}
			}
		}
	}
	fare := calculateSale(s.rides["r0"].Ride)
	for _, tc := range []struct {
		query string
		a, b  int
	}{
		{"", 7 * fare, fare},
		{"?since=1000&until=1000", 3 * fare, fare},
		{"?since=1001", 2 * fare, 0},
		{"?until=1000", 4 * fare, fare},
		{"?since=1001&until=1000", 0, 0},
	} {
		got := requestOwnerSales(t, "o", tc.query)
		want := ownerGetSalesResponse{TotalSales: tc.a + tc.b,
			Chairs: []chairSales{{ID: "a", Name: "A", Sales: tc.a}, {ID: "b", Name: "B", Sales: tc.b}, {ID: "empty"}},
			Models: []modelSales{{Model: "empty"}, {Model: "shared", Sales: tc.a + tc.b}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got %+v, want %+v", tc.query, got, want)
		}
	}
	if got := requestOwnerSales(t, "absent", ""); got.TotalSales != 0 || got.Chairs != nil || got.Models == nil || len(got.Models) != 0 {
		t.Fatalf("empty owner response changed: %+v", got)
	}
	before := requestOwnerSales(t, "o", "?since=1000&until=1000")
	s.rebuildIndexes()
	if got := requestOwnerSales(t, "o", "?since=1000&until=1000"); !reflect.DeepEqual(got, before) {
		t.Fatalf("rebuild differs: %+v != %+v", got, before)
	}
}

func TestSalesDuplicateCorrectionAndFailedWAL(t *testing.T) {
	s := memoryTestState(t)
	base := time.UnixMilli(1_000)
	seedMemoryRide(s, "r", "c", base)
	row := s.rides["r"].Ride
	completed := RideStatus{ID: "completed", RideID: "r", Status: "COMPLETED", CreatedAt: base}
	record := stateRecord{Rides: []Ride{row}, Statuses: []RideStatus{completed}}
	for i := 0; i < 2; i++ {
		if err := s.commit(record); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.complete("r", 5); err == nil {
		t.Fatal("duplicate evaluation accepted")
	}
	if got := s.salesBetween("c", base, base); got != calculateSale(row) {
		t.Fatal("duplicate record counted twice")
	}
	evaluation := 1
	row.Evaluation = &evaluation
	if err := s.commit(stateRecord{Rides: []Ride{row}}); err != nil {
		t.Fatal(err)
	}
	if s.salesBetween("c", base, base) != calculateSale(row) {
		t.Fatal("reevaluation counted twice")
	}
	// A changed UpdatedAt replaces the old contribution, including backward time.
	for _, offset := range []time.Duration{time.Millisecond, -time.Millisecond, 0} {
		row.UpdatedAt = base.Add(offset)
		row.DestinationLatitude++
		if err := s.commit(stateRecord{Rides: []Ride{row}}); err != nil {
			t.Fatal(err)
		}
		for _, at := range []time.Time{base.Add(-time.Millisecond), base, base.Add(time.Millisecond)} {
			if got, want := s.salesBetween("c", at, at), scanChairSales(s, "c", at, at); got != want {
				t.Fatalf("correction at %v: %d != %d", at, got, want)
			}
		}
	}
	seedMemoryRide(s, "failed", "c", base)
	s.addStatus(RideStatus{ID: "arrived", RideID: "failed", Status: "ARRIVED", CreatedAt: base})
	before := s.salesBetween("c", time.Time{}, base.Add(time.Hour))
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("failed", 5); err == nil {
		t.Fatal("completion accepted without WAL")
	}
	if s.latest(s.rides["failed"]) != "ARRIVED" || s.salesBetween("c", time.Time{}, base.Add(time.Hour)) != before {
		t.Fatal("failed WAL published sales")
	}
}

func TestSalesSQLAndWALRecovery(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "seed", "2026-01-01", true)
	insertTestStatus("seed", "seed-complete", "COMPLETED")
	db.MustExec(`INSERT INTO coupons (user_id, code, discount, used_by) VALUES ('u', 'discount', 1500, 'seed')`)
	insertTestRide(t, "pending", "2026-01-02", true)
	insertTestStatus("pending", "pending-arrived", "ARRIVED")
	directory := t.TempDir()
	s, err := newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState.close(); runtimeState = previous })
	fare := calculateSale(s.rides["seed"].Ride)
	if s.rides["seed"].Fare >= fare || requestOwnerSales(t, "o", "").TotalSales != fare {
		t.Fatal("SQL seed must use undiscounted sales")
	}
	completedAt, err := s.complete("pending", 4)
	if err != nil {
		t.Fatal(err)
	}
	query := fmt.Sprintf("?since=%d&until=%d", completedAt.UnixMilli(), completedAt.UnixMilli())
	all, period := requestOwnerSales(t, "o", ""), requestOwnerSales(t, "o", query)
	if all.TotalSales != 2*fare || period.TotalSales != fare {
		t.Fatal("completion totals differ")
	}
	for restart := 1; restart <= 2; restart++ {
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		s, err = newRideState(context.Background(), db, directory)
		if err != nil {
			t.Fatal(err)
		}
		runtimeState = s
		if got := requestOwnerSales(t, "o", ""); !reflect.DeepEqual(got, all) {
			t.Fatalf("restart=%d: restored total %+v != %+v", restart, got, all)
		}
		if got := requestOwnerSales(t, "o", query); !reflect.DeepEqual(got, period) {
			t.Fatalf("restart=%d: restored period %+v != %+v", restart, got, period)
		}
	}
	// Initialize replaces SQL data and generation; old WAL sales must disappear.
	db.MustExec(`DELETE FROM ride_statuses; DELETE FROM rides; DELETE FROM coupons;
		UPDATE state_projection SET generation=UUID()`)
	insertTestRide(t, "fresh", "2026-01-03", true)
	insertTestStatus("fresh", "fresh-complete", "COMPLETED")
	if err := s.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requestOwnerSales(t, "o", "").TotalSales != fare || requestOwnerSales(t, "o", query).TotalSales != 0 {
		t.Fatal("initialize retained previous sales")
	}
}
