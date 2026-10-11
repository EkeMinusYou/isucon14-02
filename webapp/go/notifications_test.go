package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func TestNotificationHub(t *testing.T) {
	h := &notificationHub{}
	sub, stop := h.subscribe("app", "user")
	other, stopOther := h.subscribe("chair", "user")
	defer stopOther()
	for i := 0; i < 100; i++ {
		h.publish("app", "user")
	}
	if len(sub.wake) != 1 || len(other.wake) != 0 {
		t.Fatal("wakeups must coalesce and stay isolated by recipient")
	}
	h.reset()
	select {
	case <-sub.done:
	default:
		t.Fatal("initialize must terminate existing streams")
	}
	fresh, stopFresh := h.subscribe("app", "user")
	defer stopFresh()
	stop()
	h.publish("app", "user")
	if len(fresh.wake) != 1 {
		t.Fatal("cleanup of old streams removed a new subscription")
	}
}

func notificationTestDB(t *testing.T) {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is required for local MySQL integration tests")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.DBName = ""
	config.MultiStatements = true
	config.ParseTime = true
	admin, err := sqlx.Connect("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("notification_test_%d", time.Now().UnixNano())
	admin.MustExec("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
	config.DBName = name
	testDB, err := sqlx.Connect("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	previous := db
	db = testDB
	notifications.reset()
	t.Cleanup(func() {
		notifications.reset()
		db = previous
		testDB.Close()
		admin.MustExec("DROP DATABASE " + name)
		admin.Close()
	})
	schema, err := os.ReadFile("../sql/1-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	db.MustExec(strings.ReplaceAll(string(schema), "USE isuride;", "USE "+name+";"))
	db.MustExec(`INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code)
		VALUES ('u', 'user', 'Test', 'User', '', 'token', 'invite')`)
	db.MustExec(`INSERT INTO chairs (id, owner_id, name, model, is_active, access_token)
		VALUES ('c', 'o', 'chair', 'model', 1, 'chair-token')`)
}

func insertTestRide(t *testing.T, id, createdAt string, assigned bool) {
	t.Helper()
	var chair any
	if assigned {
		chair = "c"
	}
	db.MustExec(`INSERT INTO rides (id, user_id, chair_id, pickup_latitude, pickup_longitude,
		destination_latitude, destination_longitude, created_at, updated_at)
		VALUES (?, 'u', ?, 0, 0, 10, 10, ?, ?)`, id, chair, createdAt, createdAt)
}

func insertTestStatus(ride, id, status string) {
	db.MustExec(`INSERT INTO ride_statuses (id, ride_id, status, created_at)
		VALUES (?, ?, ?, '2026-01-01 00:00:00.000000')`, id, ride, status)
}

func TestNotificationsHistoryAndStats(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "a", "2026-01-01", true)
	insertTestRide(t, "b", "2026-01-02", true)
	// Deliberately reverse IDs within the same timestamp.
	for i, status := range []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"} {
		insertTestStatus("a", fmt.Sprintf("z%d", 9-i), status)
	}
	insertTestStatus("b", "b0", "MATCHING")
	db.MustExec(`UPDATE rides SET evaluation = 4, updated_at = '2026-01-01 00:00:00' WHERE id = 'a'`)
	backfill, err := os.ReadFile("../sql/4-chair-stats.sql")
	if err != nil {
		t.Fatal(err)
	}
	db.MustExec(string(backfill))
	startTestRuntime(t)
	for _, kind := range []string{"app", "chair"} {
		id := "u"
		if kind == "chair" {
			id = "c"
		}
		events, err := loadNotifications(context.Background(), kind, id, "")
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, event := range events {
			payload, _ := json.Marshal(event.data)
			var data struct {
				RideID string `json:"ride_id"`
				Status string `json:"status"`
			}
			json.Unmarshal(payload, &data)
			got = append(got, data.RideID+":"+data.Status)
			if kind == "app" {
				stats := event.data.(appGetNotificationResponseData).Chair.Stats
				if stats.TotalRidesCount != 1 || stats.TotalEvaluationAvg != 4 {
					t.Fatalf("invalid backfilled stats: %+v", stats)
				}
			}
			if err := acknowledgeNotification(context.Background(), kind, event.statusID); err != nil {
				t.Fatal(err)
			}
		}
		want := []string{"a:MATCHING", "a:ENROUTE", "a:PICKUP", "a:CARRYING", "a:ARRIVED", "a:COMPLETED", "b:MATCHING"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("history order: got %v, want %v", got, want)
		}
		replay, err := loadNotifications(context.Background(), kind, id, "z5")
		if err != nil || len(replay) != 2 || replay[0].eventID != "z4" || replay[1].eventID != "b0" {
			t.Fatalf("client cursor must replay flushed but unreceived states across rides: %v, %v", replay, err)
		}
		latest, err := loadNotifications(context.Background(), kind, id, "")
		if err != nil || len(latest) != 1 || latest[0].statusID != "" {
			t.Fatalf("reconnect must immediately return current state: %v, %v", latest, err)
		}
	}
	status, err := getLatestRideStatus(context.Background(), db, "a")
	if err != nil || status != "COMPLETED" {
		t.Fatalf("latest status with timestamp ties: %s, %v", status, err)
	}
}

type failingNotificationWriter struct{ header http.Header }

func (w *failingNotificationWriter) Header() http.Header       { return w.header }
func (w *failingNotificationWriter) WriteHeader(int)           {}
func (w *failingNotificationWriter) Write([]byte) (int, error) { return 0, errors.New("disconnected") }
func (w *failingNotificationWriter) Flush()                    {}

func TestNotificationsFailedWrite(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "a", "2026-01-01", true)
	insertTestStatus("a", "a0", "MATCHING")
	startTestRuntime(t)
	serveNotifications(&failingNotificationWriter{header: make(http.Header)}, httptest.NewRequest("GET", "/", nil), "chair", "c")
	var pending int
	db.Get(&pending, `SELECT COUNT(*) FROM ride_statuses WHERE chair_sent_at IS NULL`)
	if pending != 1 {
		t.Fatal("failed writes must not consume pending status")
	}
}

func TestNotificationStreamWakeup(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "a", "2026-01-01", false)
	insertTestStatus("a", "a0", "MATCHING")
	startTestRuntime(t)
	// Match the production logger wrapper to verify ResponseController forwarding.
	server := httptest.NewServer(middleware.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveNotifications(w, r, "app", "u")
	})))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.Header.Get("Content-Type") != "text/event-stream" || res.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("invalid SSE headers: %v", res.Header)
	}
	scanner := bufio.NewScanner(res.Body)
	read := func() appGetNotificationResponseData {
		t.Helper()
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var data appGetNotificationResponseData
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
					t.Fatal(err)
				}
				return data
			}
		}
		t.Fatalf("stream ended before notification: %v", scanner.Err())
		return appGetNotificationResponseData{}
	}
	if data := read(); data.Status != "MATCHING" || data.Chair != nil {
		t.Fatalf("initial state: %+v", data)
	}
	if err := runtimeState.match(); err != nil {
		t.Fatal(err)
	}
	if data := read(); data.Chair == nil || data.Status != "MATCHING" {
		t.Fatalf("assignment must notify even without a status transition: %+v", data)
	}
	if err := runtimeState.chairStatus("c", "a", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeState.coordinate("c", Coordinate{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := runtimeState.chairStatus("c", "a", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeState.coordinate("c", Coordinate{10, 10}); err != nil {
		t.Fatal(err)
	}

	notifications.publish("app", "u")
	for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
		if data := read(); data.Status != status {
			t.Fatalf("coalesced wakeup lost transition: got %s, want %s", data.Status, status)
		}
	}
	// Close the stream and let the handler finish before test DB cleanup.
	cancel()
	res.Body.Close()
	server.Close()
}

func TestChairStatsCompletionAndWALRecovery(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "a", "2026-01-01", true)
	for i, status := range []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED"} {
		insertTestStatus("a", fmt.Sprintf("s%d", i), status)
	}
	payment := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer payment.Close()
	db.MustExec(`INSERT INTO settings (name, value) VALUES ('payment_gateway_url', ?)`, payment.URL)
	startTestRuntime(t)
	appSub, stopApp := notifications.subscribe("app", "u")
	defer stopApp()
	chairSub, stopChair := notifications.subscribe("chair", "c")
	defer stopChair()
	evaluate := func() int {
		req := httptest.NewRequest("POST", "/", strings.NewReader(`{"evaluation":5}`))
		req.SetPathValue("ride_id", "a")
		req = req.WithContext(context.WithValue(req.Context(), "user", &User{ID: "u"}))
		res := httptest.NewRecorder()
		appPostRideEvaluatation(res, req)
		return res.Code
	}
	if code := evaluate(); code != http.StatusBadRequest {
		t.Fatalf("missing payment token must abort completion: %d", code)
	}
	var statsRows int
	if err := db.Get(&statsRows, `SELECT COUNT(*) FROM chair_stats`); err != nil || statsRows != 0 || len(appSub.wake) != 0 || len(chairSub.wake) != 0 {
		t.Fatalf("aborted completion changed statistics or woke subscribers: %d, %v", statsRows, err)
	}
	if err := runtimeState.registerPaymentToken("u", "payment"); err != nil {
		t.Fatal(err)
	}
	results := make(chan int, 2)
	go func() { results <- evaluate() }()
	go func() { results <- evaluate() }()
	counts := map[int]int{}
	counts[<-results]++
	counts[<-results]++
	if counts[http.StatusOK] != 1 || counts[http.StatusBadRequest] != 1 {
		t.Fatalf("concurrent evaluations must complete exactly once: %v", counts)
	}
	if len(appSub.wake) != 1 || len(chairSub.wake) != 1 {
		t.Fatal("committed completion must wake both recipients")
	}
	// Reopen the WAL and rebuild from SQL to check completion independently of memory.
	directory := filepath.Dir(runtimeState.wal.Name())
	if err := runtimeState.close(); err != nil {
		t.Fatal(err)
	}
	s, err := newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	t.Cleanup(func() { s.close() })
	stats := s.stats.get("c")
	if stats.Count != 1 || stats.Evaluation != 5 || s.latest(s.rides.get("a")) != "COMPLETED" {
		t.Fatalf("invalid recovered completion: %+v", stats)
	}
}

type executableGet interface {
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func getLatestRideStatus(ctx context.Context, tx executableGet, rideID string) (string, error) {
	status := ""
	if err := tx.GetContext(ctx, &status, `SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC, status DESC, id DESC LIMIT 1`, rideID); err != nil {
		return "", err
	}
	return status, nil
}
