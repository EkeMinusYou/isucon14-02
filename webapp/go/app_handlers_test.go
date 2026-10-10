package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func TestAppGetNearbyChairs(t *testing.T) {
	address := os.Getenv("ISURIDE_TEST_MYSQL_ADDR")
	if address == "" {
		t.Skip("run task test-nearby-chairs with a disposable local MySQL")
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Net, cfg.Addr, cfg.ParseTime = "root", "tcp", address, true
	cfg.DBName = "isuride_nearby_test"
	testDB, err := sqlx.Connect("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer testDB.Close()
	// Session counters belong to this one connection.
	testDB.SetMaxOpenConns(1)
	testDB.SetMaxIdleConns(1)
	previousDB := db
	db = testDB
	t.Cleanup(func() { db = previousDB })
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := testDB.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, ddl := range []string{
		`CREATE TABLE chairs (id VARCHAR(26) PRIMARY KEY, name VARCHAR(30), model TEXT, is_active TINYINT(1))`,
		`CREATE TABLE rides (id VARCHAR(26) PRIMARY KEY, chair_id VARCHAR(26), created_at DATETIME(6), updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))`,
		`CREATE TABLE ride_statuses (id VARCHAR(26) PRIMARY KEY, ride_id VARCHAR(26), status ENUM('MATCHING', 'ENROUTE', 'PICKUP', 'CARRYING', 'ARRIVED', 'COMPLETED') NOT NULL, created_at DATETIME(6), INDEX (ride_id, created_at))`,
		`CREATE TABLE chair_locations (id VARCHAR(26) PRIMARY KEY, chair_id VARCHAR(26), latitude INTEGER, longitude INTEGER, created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), INDEX (chair_id, created_at))`,
		`CREATE TABLE chair_distance_totals (chair_id VARCHAR(26) PRIMARY KEY, total_distance BIGINT NOT NULL DEFAULT 0, last_latitude INTEGER NULL, last_longitude INTEGER NULL, updated_at DATETIME(6) NULL)`,
	} {
		exec(ddl)
	}
	chair := func(id string, active bool, latitude, longitude int) {
		exec(`INSERT INTO chairs VALUES (?, ?, 'model', ?)`, id, "name-"+id, active)
		exec(`INSERT INTO chair_locations VALUES (?, ?, ?, ?, '2026-01-01 00:00:01')`, "loc-"+id, id, latitude, longitude)
		exec(`INSERT INTO chair_distance_totals (chair_id, last_latitude, last_longitude, updated_at) VALUES (?, ?, ?, '2026-01-01 00:00:01')`, id, latitude, longitude)
	}
	coordinate := func(id string, latitude, longitude int) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/api/chair/coordinate",
			strings.NewReader(fmt.Sprintf(`{"latitude":%d,"longitude":%d}`, latitude, longitude)))
		request = request.WithContext(context.WithValue(request.Context(), "chair", &Chair{ID: id}))
		response := httptest.NewRecorder()
		chairPostCoordinate(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("coordinate: %d %s", response.Code, response.Body.String())
		}
	}
	ride := func(id, chairID, status string, second int) {
		createdAt := fmt.Sprintf("2026-01-01 00:00:%02d", second)
		exec(`INSERT INTO rides (id, chair_id, created_at) VALUES (?, ?, ?)`, id, chairID, createdAt)
		if status != "" {
			exec(`INSERT INTO ride_statuses VALUES (?, ?, ?, ?)`, "status-"+id, id, status, createdAt)
		}
	}
	selectCount := func() int {
		t.Helper()
		var name, value string
		if err := testDB.QueryRow(`SHOW SESSION STATUS LIKE 'Com_select'`).Scan(&name, &value); err != nil {
			t.Fatal(err)
		}
		count, err := strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}
	check := func(query string, expectedStatus int, expected map[string]Coordinate) {
		t.Helper()
		before := selectCount()
		response := httptest.NewRecorder()
		appGetNearbyChairs(response, httptest.NewRequest(http.MethodGet, "/api/app/nearby-chairs?"+query, nil))
		if response.Code != expectedStatus {
			t.Fatalf("status = %d, want %d: %s", response.Code, expectedStatus, response.Body.String())
		}
		if expectedStatus != http.StatusOK {
			return
		}
		if got := selectCount() - before; got != 3 {
			t.Fatalf("SELECT count = %d, want 3", got)
		}
		var result appGetNearbyChairsResponse
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Chairs == nil || result.RetrievedAt <= 0 || len(result.Chairs) != len(expected) {
			t.Fatalf("unexpected response: %+v", result)
		}
		seen := make(map[string]bool)
		for _, got := range result.Chairs {
			want, ok := expected[got.ID]
			if !ok || seen[got.ID] || got.CurrentCoordinate != want || got.Name != "name-"+got.ID || got.Model != "model" {
				t.Fatalf("unexpected chair: %+v", got)
			}
			seen[got.ID] = true
		}
	}

	t.Run("availability and latest coordinates", func(t *testing.T) {
		chair("idle", true, 0, 0)
		chair("boundary", true, -20, 30)
		chair("outside", true, 20, 31)
		chair("inactive", false, 0, 0)
		chair("finished", true, 0, 0)
		ride("finished-old", "finished", "ARRIVED", 1)
		exec(`INSERT INTO ride_statuses VALUES ('finished-new', 'finished-old', 'COMPLETED', '2026-01-01 00:00:02')`)
		ride("finished-next", "finished", "COMPLETED", 3)
		// A completed newer ride must not hide an older unfinished ride.
		chair("busy", true, 0, 0)
		ride("busy-old", "busy", "CARRYING", 1)
		ride("busy-new", "busy", "COMPLETED", 3)
		for i, status := range []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
			id := fmt.Sprintf("busy-%d", i)
			chair(id, true, 0, 0)
			ride("ride-"+id, id, status, 1)
		}
		exec(`INSERT INTO chairs VALUES ('no-location', 'name-no-location', 'model', TRUE)`)
		exec(`INSERT INTO chair_distance_totals (chair_id) VALUES ('no-location')`)
		chair("moved", true, 100, 100)
		coordinate("moved", -1, -1)
		chair("moved-away", true, 0, 0)
		coordinate("moved-away", 100, 100)
		check("latitude=0&longitude=0", http.StatusOK, map[string]Coordinate{
			"idle": {}, "boundary": {-20, 30}, "finished": {}, "moved": {-1, -1},
		})
		check("latitude=0&longitude=0&distance=0", http.StatusOK, map[string]Coordinate{"idle": {}, "finished": {}})
		check("latitude=0&longitude=0&distance=-1", http.StatusOK, map[string]Coordinate{})
	})
	t.Run("fresh state and constant query count", func(t *testing.T) {
		coordinate("idle", 200, 200)
		ride("moved-busy", "moved", "ENROUTE", 4)
		for i := 0; i < 100; i++ {
			chair(fmt.Sprintf("extra-%d", i), true, 500, 500)
		}
		check("latitude=0&longitude=0", http.StatusOK, map[string]Coordinate{"boundary": {-20, 30}, "finished": {}})
	})
	t.Run("missing status keeps existing error behavior", func(t *testing.T) {
		// The old loop skips older missing states after finding a busy ride.
		ride("busy-missing", "busy", "", 0)
		check("latitude=0&longitude=0", http.StatusOK, map[string]Coordinate{"boundary": {-20, 30}, "finished": {}})
		chair("missing-status", true, 0, 0)
		ride("missing-status-ride", "missing-status", "", 1)
		check("latitude=0&longitude=0", http.StatusInternalServerError, nil)
	})
	for _, query := range []string{"", "latitude=x&longitude=0", "latitude=0&longitude=x", "latitude=0&longitude=0&distance=x"} {
		check(query, http.StatusBadRequest, nil)
	}
}
