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
	"unicode"
	"unicode/utf8"
)

func startTestRuntime(t *testing.T) {
	t.Helper()
	previous := runtimeState
	s, err := newRideState(context.Background(), db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	s.matchingSolver = testMatchingRPCClient(t, nil)
	t.Cleanup(func() { s.close(); runtimeState = previous })
}

func reloadTestRuntime(t *testing.T) {
	t.Helper()
	if err := runtimeState.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Read immutable history from disk without retaining a projection backlog.
func readTestWAL(t *testing.T, s *rideState) []stateRecord {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, err := os.Open(s.wal.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := readWALRecord(f); err != nil {
		t.Fatal(err)
	}
	var records []stateRecord
	for {
		record, err := readWALRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

var testAccountWeights = func() []uint16 {
	weights := make([]uint16, utf8.MaxRune+1)
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if r > 0xffff {
			weights[r] = 0xfffd
		} else {
			weights[r] = uint16(unicode.ToUpper(r))
		}
	}
	return weights
}()

func memoryTestState(t *testing.T) *rideState {
	return memoryTestStateIn(t, t.TempDir(), "test", nil)
}

func memoryTestStateIn(t *testing.T, directory, generation string, seed func(*rideState)) *rideState {
	t.Helper()
	f, err := openStateWAL(directory)
	if err != nil {
		t.Fatal(err)
	}
	coordinates, err := openCoordinateState(directory)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	s := &rideState{wal: f, coordinates: coordinates, generation: generation,
		chairs: map[string]Chair{}, modelSpeeds: map[string]int{}, chairTokens: map[string]string{}, stats: map[string]chairStatsTotal{},
		rides: map[string]*memoryRide{}, statuses: map[string]RideStatus{}, userRides: map[string][]string{}, chairRides: map[string][]string{},
		appEvents: map[string]*notificationQueue{}, chairEvents: map[string]*notificationQueue{}, sales: map[string][]salesPoint{},
	}
	s.matchingSolver = testMatchingRPCClient(t, nil)
	t.Cleanup(func() { s.close() })
	s.collationWeights = testAccountWeights
	s.resetAccounts()
	s.coordinates.reset()
	s.resetCurrentIndexes()
	if seed != nil {
		seed(s)
	}
	if err := s.recoverWAL(); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverCoordinates(); err != nil {
		t.Fatal(err)
	}
	s.rebuildIndexes()
	s.ready = true
	s.coordinateReady.Store(true)
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
	if s.chairDistance("c").TotalDistance != 70 || s.stats["c"].Count != 1 || s.stats["c"].Evaluation != 5 {
		t.Fatal("incorrect distance or stats")
	}
	var locations int
	for _, record := range readTestCoordinates(t, s) {
		if record.Location.ID != "" {
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
	records := readTestCoordinates(t, s)
	if len(records) != 64 {
		t.Fatal("accepted history missing")
	}
	distance := 0
	for i, record := range records {
		if record.Sequence != uint64(i+1) {
			t.Fatal("unordered WAL")
		}
		if i > 0 {
			prev := records[i-1].Location
			if !record.Location.CreatedAt.After(prev.CreatedAt) {
				t.Fatal("coordinates need strictly increasing timestamps")
			}
			distance += calculateDistance(prev.Latitude, prev.Longitude, record.Location.Latitude, record.Location.Longitude)
		}
	}
	if s.chairDistance("c").TotalDistance != distance {
		t.Fatal("distance disagrees with accepted history")
	}
}

func TestFailedWALDoesNotPublish(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c"})
	if err := s.coordinates.stripes[coordinateStripeIndex("c")].wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("c", Coordinate{1, 2}); err == nil {
		t.Fatal("closed WAL accepted a coordinate")
	}
	if s.sequence != 0 || s.chairDistance("c").UpdatedAt.Valid {
		t.Fatal("failed append changed memory")
	}
}

func TestWALFramingAndRecovery(t *testing.T) {
	for _, damaged := range []string{"partial-header", "partial-payload", "checksum", "sequence"} {
		t.Run(damaged, func(t *testing.T) {
			s := memoryTestState(t)
			s.addChair(Chair{ID: "c"})
			if err := s.commit(stateRecord{Chair: &Chair{ID: "c", IsActive: true}}); err != nil {
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
			s.sequence = 0
			s.seedCoordinateDistance("c", distanceTotal{})
			err := s.recoverWAL()
			if damaged == "checksum" || damaged == "sequence" {
				if err == nil {
					t.Fatal("corrupt WAL accepted")
				}
			} else {
				if err != nil || s.sequence != 1 || !s.chairs["c"].IsActive {
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

func TestRuntimeRecoveryFromWAL(t *testing.T) {
	notificationTestDB(t)
	insertTestRide(t, "a", "2026-01-01", false)
	db.MustExec(`UPDATE chairs SET is_active=0 WHERE id='c'`)
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
	if err := s.activity("c", true); err != nil {
		t.Fatal(err)
	}
	if err := s.match(); err != nil {
		t.Fatal(err)
	}
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
	for _, event := range events[:len(events)-1] {
		if err := acknowledgeNotification(context.Background(), "chair", event.statusID); err != nil {
			t.Fatal(err)
		}
	}
	if err := acknowledgeNotification(context.Background(), "app", events[0].statusID); err != nil {
		t.Fatal(err)
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
	if s.sequence != sequence || s.latest(s.rides["a"]) != "COMPLETED" || s.chairDistance("c").TotalDistance != 20 || s.stats["c"].Count != 1 || !s.chairs["c"].IsActive || s.rides["a"].ChairID.String != "c" {
		t.Fatal("unprojected state not recovered")
	}
	replay, err := loadNotifications(context.Background(), "chair", "c", events[3].eventID)
	if err != nil || len(replay) != 2 {
		t.Fatalf("cursor replay after restart: %v %v", replay, err)
	}
	// The DB stays at its seed state even after all accepted mutations.
	var count int
	if err := db.Get(&count, `SELECT COUNT(*) FROM chair_locations`); err != nil || count != 0 {
		t.Fatalf("coordinate SQL projection: %d %v", count, err)
	}
	if err := db.Get(&count, `SELECT COUNT(*) FROM ride_statuses WHERE ride_id='a'`); err != nil || count != 1 {
		t.Fatalf("status SQL projection: %d %v", count, err)
	}
	if _, reusable := s.matchingChairs["c"]; reusable {
		t.Fatal("chair reused before recovered completion ACK")
	}
	if s.statuses[events[0].statusID].AppSentAt == nil {
		t.Fatal("app ACK not recovered")
	}
	if err := acknowledgeNotification(context.Background(), "chair", events[len(events)-1].statusID); err != nil {
		t.Fatal(err)
	}
	if _, reusable := s.matchingChairs["c"]; !reusable {
		t.Fatal("completion ACK did not release chair")
	}
	// A second ordinary restart must neither lose nor double-count history.
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	if _, reusable := s.matchingChairs["c"]; s.stats["c"].Count != 1 || s.stats["c"].Evaluation != 4 || s.chairDistance("c").TotalDistance != 20 || !reusable {
		t.Fatal("full WAL replay duplicated totals")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// A deliberate DB initialization invalidates the old generation's WAL.
	db.MustExec(`UPDATE state_projection SET generation=UUID()`)
	s, err = newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState = s
	if s.sequence != 0 || s.latest(s.rides["a"]) != "MATCHING" || s.chairDistance("c").UpdatedAt.Valid {
		t.Fatal("old generation replayed")
	}
	if err := acknowledgeNotificationGeneration(context.Background(), "chair", events[0].statusID, events[0].generation); err == nil {
		t.Fatal("old-generation ACK accepted")
	}
	if _, err := os.Stat(filepath.Join(directory, "state.wal")); err != nil {
		t.Fatal(err)
	}
}

func TestWALAcceptsBeyondFormerProjectionLimit(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c"})
	for i := 0; i < 32769; i++ {
		if _, err := s.coordinate("c", Coordinate{i, i}); err != nil {
			t.Fatal(err)
		}
	}
	if s.sequence != 0 || s.coordinateCell("c").snapshot.Load().sequence != 32769 || s.chairDistance("c").TotalDistance != 65536 {
		t.Fatal("accepted history missing")
	}
	s.seedCoordinateDistance("c", distanceTotal{})
	for i := range s.coordinates.stripes {
		s.coordinates.stripes[i].sequence = 0
	}
	if err := s.recoverCoordinates(); err != nil {
		t.Fatal(err)
	}
	if s.sequence != 0 || s.coordinateCell("c").snapshot.Load().sequence != 32769 || s.chairDistance("c").TotalDistance != 65536 {
		t.Fatal("long WAL not recovered")
	}
}

func TestNewRideAndCouponWithWALCompletion(t *testing.T) {
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

func TestBackgroundWALSyncWithoutSQLProjection(t *testing.T) {
	s := memoryTestState(t)
	s.addChair(Chair{ID: "c"})
	s.start()
	for i := 0; i < 128; i++ {
		if _, err := s.coordinate("c", Coordinate{i, -i}); err != nil {
			t.Fatal(err)
		}
	}
	// Let the unchanged sync interval fire; no SQL connection is available.
	time.Sleep(550 * time.Millisecond)
	if err := s.checkReady(); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
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
