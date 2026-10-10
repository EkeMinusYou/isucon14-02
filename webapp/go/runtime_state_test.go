package main

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestRuntime(t *testing.T) {
	t.Helper()
	previous := runtimeState
	s, err := newRideState(context.Background(), db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	t.Cleanup(func() { s.close(); runtimeState = previous })
}

func reloadTestRuntime(t *testing.T) {
	t.Helper()
	flushTestRuntime(t)
	if err := runtimeState.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func flushTestRuntime(t *testing.T) {
	t.Helper()
	for len(runtimeState.pending) != 0 {
		if err := runtimeState.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func memoryTestState(t *testing.T) *rideState {
	t.Helper()
	f, err := openStateWAL(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &rideState{wal: f, generation: "test", ready: true,
		owners: map[string]Owner{}, users: map[string]User{}, userTokens: map[string]string{}, invitationUsers: map[string]string{},
		chairs: map[string]Chair{}, modelSpeeds: map[string]int{}, chairTokens: map[string]string{}, distances: map[string]distanceTotal{}, stats: map[string]chairStatsTotal{},
		rides: map[string]*memoryRide{}, statuses: map[string]RideStatus{}, userRides: map[string][]string{}, chairRides: map[string][]string{},
		appEvents: map[string][]string{}, chairEvents: map[string][]string{},
	}
	if err := writeWALRecord(f, stateRecord{Version: 1, Generation: s.generation}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return s
}

func seedMemoryRide(s *rideState, id, chairID string, at time.Time) {
	r := Ride{ID: id, UserID: "u", CreatedAt: at, UpdatedAt: at, PickupLatitude: 10, PickupLongitude: 20, DestinationLatitude: 30, DestinationLongitude: 40}
	if chairID != "" {
		r.ChairID = sql.NullString{String: chairID, Valid: true}
	}
	s.addRide(r, calculateSale(r))
	s.addStatus(RideStatus{ID: id + "-matching", RideID: id, Status: "MATCHING", CreatedAt: at})
}

func TestCoordinatesAndTransitionsWithoutDB(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "r", "c", time.Now().Add(-time.Hour))
	if err := s.chairStatus("c", "r", "CARRYING"); err == nil {
		t.Fatal("carrying before pickup accepted")
	}
	if err := s.chairStatus("other", "r", "ENROUTE"); err == nil {
		t.Fatal("wrong chair accepted")
	}
	if err := s.chairStatus("c", "r", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []Coordinate{{0, 0}, {10, 20}, {10, 20}} {
		if _, err := s.coordinate("c", p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.chairStatus("c", "r", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{30, 40}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("r", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("r", 5); err == nil {
		t.Fatal("duplicate completion accepted")
	}
	var statuses []string
	for _, id := range s.rides["r"].Statuses {
		statuses = append(statuses, s.statuses[id].Status)
	}
	if !reflect.DeepEqual(statuses, []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"}) {
		t.Fatal(statuses)
	}
	if s.distances["c"].TotalDistance != 70 || s.stats["c"].Count != 1 || s.stats["c"].Evaluation != 5 {
		t.Fatal("incorrect distance or stats")
	}
	var locations int
	for _, record := range s.pending {
		if record.Location != nil {
			locations++
		}
	}
	if locations != 4 {
		t.Fatal("repeated coordinate was not retained")
	}
}

func TestConcurrentCoordinatesRetainOrderedHistory(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c"})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.coordinate("c", Coordinate{i * 3, -i * 7}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if len(s.pending) != 64 {
		t.Fatal("accepted history missing")
	}
	distance := 0
	for i, record := range s.pending {
		if record.Sequence != uint64(i+1) {
			t.Fatal("unordered WAL")
		}
		if i > 0 {
			prev := s.pending[i-1].Location
			if !record.Location.CreatedAt.After(prev.CreatedAt) {
				t.Fatal("coordinates need strictly increasing timestamps")
			}
			distance += calculateDistance(prev.Latitude, prev.Longitude, record.Location.Latitude, record.Location.Longitude)
		}
	}
	if s.distances["c"].TotalDistance != distance {
		t.Fatal("distance disagrees with accepted history")
	}
}

func TestFailedWALDoesNotPublish(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c"})
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{1, 2}); err == nil {
		t.Fatal("closed WAL accepted a coordinate")
	}
	if s.sequence != 0 || len(s.pending) != 0 || s.distances["c"].UpdatedAt.Valid {
		t.Fatal("failed append changed memory")
	}
}

func TestWALFramingAndRecovery(t *testing.T) {
	for _, damaged := range []string{"partial-header", "partial-payload", "checksum", "sequence"} {
		t.Run(damaged, func(t *testing.T) {
			s := memoryTestState(t)
			s.addChair(Chair{ID: "c"})
			if _, err := s.coordinate("c", Coordinate{1, 2}); err != nil {
				t.Fatal(err)
			}
			end, _ := s.wal.Seek(0, io.SeekCurrent)
			switch damaged {
			case "partial-header":
				s.wal.Write([]byte{1, 2, 3})
			case "partial-payload":
				s.wal.Write([]byte{10, 0, 0, 0, 0, 0, 0, 0, 1, 2})
			case "checksum":
				s.wal.WriteAt([]byte{0xff}, end-1)
			case "sequence":
				writeWALRecord(s.wal, stateRecord{Version: 1, Generation: "test", Sequence: 3})
			}
			s.pending, s.sequence = nil, 0
			s.distances = map[string]distanceTotal{}
			err := s.recoverWAL()
			if damaged == "checksum" || damaged == "sequence" {
				if err == nil {
					t.Fatal("corrupt WAL accepted")
				}
			} else {
				if err != nil || s.sequence != 1 || len(s.pending) != 1 || !s.distances["c"].UpdatedAt.Valid {
					t.Fatalf("recovery: %v", err)
				}
				info, _ := s.wal.Stat()
				if info.Size() != end {
					t.Fatal("partial tail not truncated")
				}
			}
		})
	}
}

func TestWALExclusiveOwner(t *testing.T) {
	directory := t.TempDir()
	f, err := openStateWAL(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if other, err := openStateWAL(directory); err == nil {
		other.Close()
		t.Fatal("two owners accepted")
	}
}

func TestRuntimeRecoveryAndProjection(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "a", "2026-01-01", true)
	insertTestStatus("a", "a0", "MATCHING")
	db.MustExec(`INSERT INTO chair_distance_totals (chair_id) VALUES ('c')`)
	directory := t.TempDir()
	s, err := newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState.close(); runtimeState = previous })
	if err := s.chairStatus("c", "a", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("c", "a", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{10, 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("a", 4); err != nil {
		t.Fatal(err)
	}
	events, err := loadNotifications(context.Background(), "chair", "c", "")
	if err != nil || len(events) != 6 {
		t.Fatalf("history: %v %v", events, err)
	}
	for _, event := range events {
		if err := acknowledgeNotification(context.Background(), "chair", event.statusID); err != nil {
			t.Fatal(err)
		}
	}
	sequence := s.sequence
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// Nothing has been projected: recovery must use only the accepted WAL.
	s, err = newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	if s.sequence != sequence || s.latest(s.rides["a"]) != "COMPLETED" || s.distances["c"].TotalDistance != 20 || s.stats["c"].Count != 1 {
		t.Fatal("unprojected state not recovered")
	}
	replay, err := loadNotifications(context.Background(), "chair", "c", events[3].eventID)
	if err != nil || len(replay) != 2 {
		t.Fatalf("cursor replay after restart: %v %v", replay, err)
	}
	// A SQL failure must leave the projection marker and all rows unchanged.
	db.MustExec(`CREATE TRIGGER reject_projection BEFORE INSERT ON ride_statuses FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test failure'`)
	if err := s.flush(context.Background()); err == nil {
		t.Fatal("projection should fail")
	}
	var count int
	db.Get(&count, `SELECT COUNT(*) FROM chair_locations`)
	if count != 0 || s.applied != 0 || len(s.pending) == 0 {
		t.Fatal("failed projection committed partial rows")
	}
	db.MustExec(`DROP TRIGGER reject_projection`)
	flushTestRuntime(t)
	db.Get(&count, `SELECT COUNT(*) FROM ride_statuses WHERE ride_id='a'`)
	if count != 6 {
		t.Fatal("projection lost transitions")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	if len(s.pending) != 0 || s.stats["c"].Count != 1 || s.distances["c"].TotalDistance != 20 {
		t.Fatal("projected WAL applied twice")
	}
	// Simulate an ambiguous commit by replaying its immutable batch again.
	if _, err := s.coordinate("c", Coordinate{20, 20}); err != nil {
		t.Fatal(err)
	}
	batch := append([]stateRecord(nil), s.pending...)
	flushTestRuntime(t)
	s.pending = batch
	flushTestRuntime(t)
	db.Get(&count, `SELECT COUNT(*) FROM chair_locations`)
	if count != 3 {
		t.Fatal("projection retry duplicated history")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// A deliberate DB initialization invalidates the old generation's WAL.
	db.MustExec(`UPDATE state_projection SET generation=UUID(),applied_seq=0`)
	s, err = newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	if s.sequence != 0 || len(s.pending) != 0 {
		t.Fatal("old generation replayed")
	}
	if _, err := os.Stat(filepath.Join(directory, "state.wal")); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionBacklogRejectsBeforeAppend(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c"})
	s.pending = make([]stateRecord, 32768)
	if _, err := s.coordinate("c", Coordinate{1, 1}); err == nil {
		t.Fatal("full backlog accepted a coordinate")
	}
	if s.sequence != 0 || s.distances["c"].UpdatedAt.Valid {
		t.Fatal("backlog rejection changed state")
	}
}

func TestNewRideAndCouponBeforeOldCompletionProjection(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "old", "2026-01-01", true)
	insertTestStatus("old", "old0", "MATCHING")
	db.MustExec(`INSERT INTO coupons (user_id,code,discount) VALUES ('u','test',1500)`)
	directory := t.TempDir()
	s, err := newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState.close(); runtimeState = previous })
	if err := s.chairStatus("c", "old", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("c", "old", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{10, 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete("old", 5); err != nil {
		t.Fatal(err)
	}
	create := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/", strings.NewReader(`{"pickup_coordinate":{"latitude":0,"longitude":0},"destination_coordinate":{"latitude":10,"longitude":10}}`))
		req = req.WithContext(context.WithValue(req.Context(), "user", &User{ID: "u"}))
		res := httptest.NewRecorder()
		appPostRides(res, req)
		return res
	}
	if res := create(); res.Code != http.StatusAccepted {
		t.Fatalf("new ride after memory completion: %d %s", res.Code, res.Body.String())
	}
	if res := create(); res.Code != http.StatusConflict {
		t.Fatal("concurrent active ride accepted")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	events, err := loadNotifications(context.Background(), "app", "u", "")
	if err != nil || len(events) != 7 {
		t.Fatalf("restart history: %v %v", events, err)
	}
	var statuses []string
	for _, event := range events {
		statuses = append(statuses, event.data.(appGetNotificationResponseData).Status)
	}
	if !reflect.DeepEqual(statuses, []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED", "MATCHING"}) {
		t.Fatalf("cross-ride replay out of order: %v", statuses)
	}
	if fare := events[6].data.(appGetNotificationResponseData).Fare; fare != 1000 {
		t.Fatalf("coupon fare not restored: %d", fare)
	}
}

func TestRecoveredChairOrderFollowsAssignment(t *testing.T) {
	s := memoryTestState(t)
	previous := runtimeState
	runtimeState = s
	t.Cleanup(func() { runtimeState = previous })
	s.addChair(Chair{ID: "c", IsActive: true})
	seedMemoryRide(s, "younger", "c", time.Now().Add(-time.Hour))
	for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"} {
		s.addStatus(s.newStatus(s.rides["younger"], status, time.Now().UTC().Add(-time.Minute)))
	}
	s.rides["younger"].UpdatedAt = time.Now().Add(-time.Minute)
	for _, id := range s.rides["younger"].Statuses {
		status := s.statuses[id]
		at := time.Now().UTC()
		status.ChairSentAt = &at
		s.statuses[id] = status
	}
	// An older request was still in its DB transaction while the younger ride
	// used this chair; only now does the older request become assignable.
	seedMemoryRide(s, "late", "c", time.Now().Add(-2*time.Hour))
	s.rides["late"].UpdatedAt = time.Now().UTC()
	if err := s.chairStatus("c", "late", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	s.rebuildIndexes()
	if ids := s.chairRides["c"]; len(ids) != 2 || ids[1] != "late" {
		t.Fatalf("wrong current ride: %v", ids)
	}
	if _, err := s.coordinate("c", Coordinate{10, 20}); err != nil {
		t.Fatal(err)
	}
	if s.latest(s.rides["late"]) != "PICKUP" {
		t.Fatal("arrival lost after recovery of a delayed request")
	}
	events, err := loadNotifications(context.Background(), "chair", "c", "late-matching")
	if err != nil || len(events) != 3 {
		t.Fatalf("chair replay: %v %v", events, err)
	}
	for _, event := range events {
		if event.data.(chairGetNotificationResponseData).RideID != "late" {
			t.Fatal("old completion replayed after current ride")
		}
	}
}

func TestBackgroundProjection(t *testing.T) {
	notificationTestDB(t)
	s, err := newRideState(context.Background(), db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	previous := runtimeState
	runtimeState = s
	s.start()
	t.Cleanup(func() { s.close(); runtimeState = previous })
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.coordinate("c", Coordinate{i, -i}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.RLock()
		applied, sequence, pending := s.applied, s.sequence, len(s.pending)
		s.mu.RUnlock()
		if applied == sequence && pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background worker did not project accepted records")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var count int
	if err := db.Get(&count, `SELECT COUNT(*) FROM chair_locations WHERE chair_id='c'`); err != nil || count != 128 {
		t.Fatalf("projected history: %d %v", count, err)
	}
	var expected, actual int
	if err := db.Get(&expected, `SELECT SUM(IFNULL(distance,0)) FROM (
		SELECT ABS(latitude-LAG(latitude) OVER (ORDER BY created_at,id)) + ABS(longitude-LAG(longitude) OVER (ORDER BY created_at,id)) distance
		FROM chair_locations WHERE chair_id='c') steps`); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&actual, `SELECT total_distance FROM chair_distance_totals WHERE chair_id='c'`); err != nil || expected != actual {
		t.Fatalf("background distance: %d != %d %v", actual, expected, err)
	}
}

func TestAuthenticatedCoordinateAndNearbyWithoutDB(t *testing.T) {
	s := memoryTestState(t)
	previous, previousDB := runtimeState, db
	runtimeState, db = s, nil
	t.Cleanup(func() { runtimeState, db = previous, previousDB })
	s.addChair(Chair{ID: "c", OwnerID: "o", IsActive: true, AccessToken: "chair-token"})
	s.addUser(User{ID: "u", AccessToken: "app-token"})
	req := httptest.NewRequest(http.MethodPost, "/api/chair/coordinate", strings.NewReader(`{"latitude":1,"longitude":2}`))
	req.AddCookie(&http.Cookie{Name: "chair_session", Value: "chair-token"})
	res := httptest.NewRecorder()
	runtimeStateMiddleware(chairAuthMiddleware(http.HandlerFunc(chairPostCoordinate))).ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("authenticated coordinate: %d %s", res.Code, res.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/api/app/nearby-chairs?latitude=1&longitude=2", nil)
	req.AddCookie(&http.Cookie{Name: "app_session", Value: "app-token"})
	res = httptest.NewRecorder()
	runtimeStateMiddleware(appAuthMiddleware(http.HandlerFunc(appGetNearbyChairs))).ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"id":"c"`) {
		t.Fatalf("nearby must use the unprojected location: %d %s", res.Code, res.Body.String())
	}
}
