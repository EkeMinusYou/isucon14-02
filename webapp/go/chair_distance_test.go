package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
)

// Run through task check-chair-distance, which supplies a disposable local DB.
func TestChairDistance(t *testing.T) {
	dsn := os.Getenv("CHAIR_DISTANCE_TEST_DSN")
	if dsn == "" {
		t.Skip("run task check-chair-distance to test with MySQL")
	}
	var err error
	db, err = sqlx.Connect("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	execSQL := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	startTestRuntime(t)
	checkTotals := func() {
		flushTestRuntime(t)
		t.Helper()
		var mismatches int
		// Independently recompute the distance and final coordinate from history.
		err := db.Get(&mismatches, `SELECT COUNT(*) FROM chairs c
LEFT JOIN chair_distance_totals d ON d.chair_id = c.id
LEFT JOIN (
    SELECT chair_id, SUM(IFNULL(distance, 0)) AS distance, MAX(created_at) AS updated_at
    FROM (
        SELECT chair_id, created_at,
               ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at, id)) +
               ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at, id)) AS distance
        FROM chair_locations
    ) steps GROUP BY chair_id
) expected ON expected.chair_id = c.id
WHERE d.chair_id IS NULL
   OR d.total_distance <> IFNULL(expected.distance, 0)
   OR NOT (d.updated_at <=> expected.updated_at)
   OR NOT (d.last_latitude <=> (SELECT latitude FROM chair_locations WHERE chair_id = c.id ORDER BY created_at DESC, id DESC LIMIT 1))
   OR NOT (d.last_longitude <=> (SELECT longitude FROM chair_locations WHERE chair_id = c.id ORDER BY created_at DESC, id DESC LIMIT 1))`)
		if err != nil || mismatches != 0 {
			t.Fatalf("distance mismatches = %d, error = %v", mismatches, err)
		}
	}
	ownerChairs := func(ownerID string) ownerGetChairResponse {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/owner/chairs", nil)
		req = req.WithContext(context.WithValue(req.Context(), "owner", &Owner{ID: ownerID}))
		res := httptest.NewRecorder()
		ownerGetChairs(res, req)
		var body ownerGetChairResponse
		if res.Code != http.StatusOK {
			t.Fatalf("owner chairs: %d %s", res.Code, res.Body.String())
		}
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		for _, chair := range body.Chairs {
			var expected struct {
				TotalDistance int          `db:"total_distance"`
				UpdatedAt     sql.NullTime `db:"updated_at"`
			}
			if err := db.Get(&expected, "SELECT total_distance, updated_at FROM chair_distance_totals WHERE chair_id = ?", chair.ID); err != nil {
				t.Fatal(err)
			}
			if chair.TotalDistance != expected.TotalDistance || (chair.TotalDistanceUpdatedAt != nil) != expected.UpdatedAt.Valid {
				t.Fatalf("owner response differs for %s: %+v", chair.ID, chair)
			}
			if expected.UpdatedAt.Valid && *chair.TotalDistanceUpdatedAt != expected.UpdatedAt.Time.UnixMilli() {
				t.Fatalf("owner timestamp differs for %s", chair.ID)
			}
		}
		return body
	}
	coordinate := func(chairID string, latitude, longitude int) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/chair/coordinate", strings.NewReader(fmt.Sprintf(`{"latitude":%d,"longitude":%d}`, latitude, longitude)))
		req = req.WithContext(context.WithValue(req.Context(), "chair", &Chair{ID: chairID}))
		res := httptest.NewRecorder()
		chairPostCoordinate(res, req)
		return res
	}
	register := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/chair/chairs", strings.NewReader(`{"name":"distance test","model":"test","chair_register_token":"distance-token"}`))
		res := httptest.NewRecorder()
		chairPostChairs(res, req)
		return res
	}

	t.Run("initial history", func(t *testing.T) {
		checkTotals()
		var ownerID string
		if err := db.Get(&ownerID, "SELECT owner_id FROM chairs LIMIT 1"); err != nil {
			t.Fatal(err)
		}
		if len(ownerChairs(ownerID).Chairs) == 0 {
			t.Fatal("initial chairs are missing")
		}
	})

	execSQL("INSERT INTO owners (id, name, access_token, chair_register_token) VALUES ('distance-owner', 'distance-owner', 'distance-owner-token', 'distance-token')")
	execSQL("INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES ('distance-history', 'distance-owner', 'history', 'test', 0, 'history-token'), ('distance-empty', 'distance-owner', 'empty', 'test', 0, 'empty-token'), ('distance-single', 'distance-owner', 'single', 'test', 0, 'single-token')")
	execSQL(`INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES
('distance-1', 'distance-history', -3, 4, '2024-01-01 00:00:00'),
('distance-2', 'distance-history', 5, -6, '2024-01-01 00:00:00'),
('distance-3', 'distance-history', 5, -6, '2024-01-01 00:00:01'),
('distance-4', 'distance-single', 0, 0, '2024-01-01 00:00:00')`)
	backfill, err := os.ReadFile("../sql/4-chair-distance.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL("TRUNCATE chair_distance_totals")
	execSQL(string(backfill))
	reloadTestRuntime(t)
	t.Run("empty single repeated and tied history", func(t *testing.T) {
		checkTotals()
		chairs := ownerChairs("distance-owner").Chairs
		if len(chairs) != 3 {
			t.Fatalf("chairs = %d", len(chairs))
		}
	})

	res := register()
	if res.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", res.Code, res.Body.String())
	}
	var registered chairPostChairsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &registered); err != nil {
		t.Fatal(err)
	}
	t.Run("registration and incremental updates", func(t *testing.T) {
		checkTotals()
		ownerChairs("distance-owner")
		for _, point := range []Coordinate{{0, 0}, {-3, 4}, {-3, 4}, {5, -6}, {-2147483648, -2147483648}, {2147483647, 2147483647}} {
			res := coordinate(registered.ID, point.Latitude, point.Longitude)
			if res.Code != http.StatusOK {
				t.Fatalf("coordinate: %d %s", res.Code, res.Body.String())
			}
			var body chairPostCoordinateResponse
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil || body.RecordedAt == 0 {
				t.Fatalf("recorded_at: %+v, %v", body, err)
			}
			checkTotals()
			ownerChairs("distance-owner")
		}
		if res := coordinate("distance-history", 2, -8); res.Code != http.StatusOK {
			t.Fatalf("append to initial history: %d %s", res.Code, res.Body.String())
		}
		checkTotals()
	})

	t.Run("concurrent coordinates", func(t *testing.T) {
		const count = 16
		var wg sync.WaitGroup
		results := make(chan *httptest.ResponseRecorder, count)
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results <- coordinate(registered.ID, i*3, -i*7)
			}(i)
		}
		wg.Wait()
		close(results)
		for res := range results {
			if res.Code != http.StatusOK {
				t.Fatalf("concurrent coordinate: %d %s", res.Code, res.Body.String())
			}
		}
		flushTestRuntime(t)
		var locations int
		if err := db.Get(&locations, "SELECT COUNT(*) FROM chair_locations WHERE chair_id = ?", registered.ID); err != nil || locations != count+6 {
			t.Fatalf("location count = %d, error = %v", locations, err)
		}
		checkTotals()
	})

	t.Run("projection failure preserves accepted coordinate and ride status", func(t *testing.T) {
		execSQL("INSERT INTO rides (id, user_id, chair_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude) VALUES ('distance-ride', 'test', ?, 100, 100, 200, 200)", registered.ID)
		execSQL("INSERT INTO ride_statuses (id, ride_id, status) VALUES ('distance-status', 'distance-ride', 'ENROUTE')")
		reloadTestRuntime(t)
		execSQL("CREATE TRIGGER reject_distance_status BEFORE INSERT ON ride_statuses FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test rollback'")
		res := coordinate(registered.ID, 100, 100)
		if err := runtimeState.flush(context.Background()); err == nil {
			t.Fatal("projection should fail without discarding the accepted update")
		}
		execSQL("DROP TRIGGER reject_distance_status")
		if res.Code != http.StatusOK {
			t.Fatalf("rollback response: %d %s", res.Code, res.Body.String())
		}
		var locations int
		if err := db.Get(&locations, "SELECT COUNT(*) FROM chair_locations WHERE chair_id = ?", registered.ID); err != nil || locations != 22 {
			t.Fatalf("rollback location count = %d, error = %v", locations, err)
		}
		checkTotals()
		if res := coordinate(registered.ID, 100, 100); res.Code != http.StatusOK {
			t.Fatalf("pickup: %d %s", res.Code, res.Body.String())
		}
		flushTestRuntime(t)
		var pickup int
		if err := db.Get(&pickup, "SELECT COUNT(*) FROM ride_statuses WHERE ride_id = 'distance-ride' AND status = 'PICKUP'"); err != nil || pickup != 1 {
			t.Fatalf("pickup statuses = %d, error = %v", pickup, err)
		}
		checkTotals()
	})

	t.Run("registration rollback", func(t *testing.T) {
		execSQL("CREATE TRIGGER reject_distance_chair BEFORE INSERT ON chair_distance_totals FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test rollback'")
		res := register()
		execSQL("DROP TRIGGER reject_distance_chair")
		if res.Code != http.StatusInternalServerError {
			t.Fatalf("registration rollback: %d %s", res.Code, res.Body.String())
		}
		checkTotals()
	})

	t.Run("reconnect reads persisted totals", func(t *testing.T) {
		db.Close()
		db, err = sqlx.Connect("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		runtimeState.database = db
		checkTotals()
		ownerChairs("distance-owner")
	})

	t.Run("initialize resets totals", func(t *testing.T) {
		if res := coordinate("distance-history", 123, 456); res.Code != http.StatusOK {
			t.Fatal("coordinate before initialize failed")
		}
		if len(runtimeState.pending) == 0 {
			t.Fatal("test needs an unprojected update")
		}
		sub, unsubscribe := notifications.subscribe("chair", "distance-history")
		defer unsubscribe()
		req := httptest.NewRequest(http.MethodPost, "/api/initialize", strings.NewReader(`{"payment_server":"http://localhost:12345"}`))
		res := httptest.NewRecorder()
		postInitialize(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("initialize: %d %s", res.Code, res.Body.String())
		}
		checkTotals()
		select {
		case <-sub.done:
		default:
			t.Fatal("initialize did not close the old stream")
		}
		if runtimeState.sequence != 0 {
			t.Fatal("initialize replayed an old WAL generation")
		}
		if len(ownerChairs("distance-owner").Chairs) != 0 {
			t.Fatal("test chairs remain after initialization")
		}
	})
}
