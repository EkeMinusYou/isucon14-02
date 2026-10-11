package main

import (
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

var runtimeState *rideState
var stateLifecycle sync.RWMutex

type distanceTotal struct {
	ChairID       string        `db:"chair_id"`
	TotalDistance int           `db:"total_distance"`
	Latitude      sql.NullInt64 `db:"last_latitude"`
	Longitude     sql.NullInt64 `db:"last_longitude"`
	UpdatedAt     sql.NullTime  `db:"updated_at"`
}

type chairStatsTotal struct {
	ChairID    string `db:"chair_id"`
	Count      int    `db:"total_rides_count"`
	Evaluation int    `db:"total_evaluation"`
}

type memoryRide struct {
	waitIndex    int
	assignedAt   time.Time
	firstFlushed [2]bool
	Ride
	Fare                      int
	Statuses                  []string
	CompletionResponsePending bool
	NearbyAvailableAt         time.Time
}

// Nearby responses can overtake the evaluation response and completion SSE
// on separate client connections. Keep the observed completion boundary out
// of nearby listings; coordinate writes and matching do not wait here.
const nearbyCompletionGrace = 100 * time.Millisecond

// The single application owner serializes validation, WAL append and publication.
// SQL and external payment calls never hold mu.
type rideState struct {
	accountState
	// Immutable after initial startup; request normalization does not hold mu.
	collationWeights []uint16
	coordinateCalls  atomic.Uint64
	metrics          *rideMetrics
	dispatch         *dispatchMetrics
	mu               sync.RWMutex
	matchingMu       sync.Mutex
	userLocks        sync.Map
	database         *sqlx.DB
	wal              *os.File
	generation       string
	sequence         uint64
	ready            bool
	failed           error
	chairs           map[string]Chair
	modelSpeeds      map[string]int
	chairTokens      map[string]string
	distances        map[string]distanceTotal
	stats            map[string]chairStatsTotal
	rides            map[string]*memoryRide
	statuses         map[string]RideStatus
	userRides        map[string][]string
	chairRides       map[string][]string
	sales            map[string][]salesPoint
	appEvents        map[string]*notificationQueue
	chairEvents      map[string]*notificationQueue
	stop             chan struct{}
	done             chan struct{}

	waitingRides        []*memoryRide
	waitingPickups      *waitingPickupNode
	userActiveRides     map[string]int
	chairCurrent        map[string]*chairCurrentState
	matchingChairs      map[string]struct{}
	nearbyChairs        map[string]struct{}
	nearbyMissingStatus map[string]struct{}
}

// Only the initialization generation lives in SQL; runtime mutations live in WAL.
const generationSchema = `CREATE TABLE IF NOT EXISTS state_projection (
 id TINYINT NOT NULL PRIMARY KEY,
 generation VARCHAR(36) NOT NULL
)`

func newRideState(ctx context.Context, database *sqlx.DB, directory string) (*rideState, error) {
	f, err := openStateWAL(directory)
	if err != nil {
		return nil, err
	}
	s := &rideState{database: database, wal: f}
	if err := s.restore(ctx); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *rideState) restore(ctx context.Context) error {
	s.ready = false
	s.metrics = nil
	s.dispatch = nil
	if err := s.loadCollation(ctx); err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, generationSchema); err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, `INSERT IGNORE INTO state_projection (id, generation) VALUES (1, UUID())`); err != nil {
		return err
	}
	tx, err := s.database.BeginTxx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var generation string
	if err := tx.GetContext(ctx, &generation, "SELECT generation FROM state_projection WHERE id = 1"); err != nil {
		return err
	}
	var chairs []Chair
	var models []ChairModel
	var distances []distanceTotal
	var stats []chairStatsTotal
	var rides []Ride
	var statuses []RideStatus
	for _, query := range []struct {
		dest any
		sql  string
	}{
		{&models, "SELECT * FROM chair_models"}, {&chairs, "SELECT * FROM chairs"},
		{&distances, "SELECT * FROM chair_distance_totals"}, {&stats, "SELECT * FROM chair_stats"},
		{&rides, "SELECT * FROM rides ORDER BY created_at, id"},
		{&statuses, `SELECT s.* FROM ride_statuses s JOIN rides r ON r.id = s.ride_id ORDER BY r.created_at, r.id, s.created_at, s.status, s.id`},
	} {
		if err := tx.SelectContext(ctx, query.dest, query.sql); err != nil {
			return err
		}
	}
	accounts, err := loadAccountSeed(ctx, tx)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.chairs, s.chairTokens = map[string]Chair{}, map[string]string{}
	s.modelSpeeds = map[string]int{}
	for _, model := range models {
		s.modelSpeeds[model.Name] = model.Speed
	}
	s.distances, s.stats = map[string]distanceTotal{}, map[string]chairStatsTotal{}
	s.rides, s.statuses = map[string]*memoryRide{}, map[string]RideStatus{}
	s.userRides, s.chairRides = map[string][]string{}, map[string][]string{}
	// Build sales once from the final SQL and WAL state, in timestamp order.
	s.sales = nil
	s.appEvents, s.chairEvents = map[string]*notificationQueue{}, map[string]*notificationQueue{}
	s.resetCurrentIndexes()
	s.generation, s.sequence, s.failed = generation, 0, nil
	discounts := accounts.install(s)
	for _, c := range chairs {
		s.addChair(c)
	}
	for _, d := range distances {
		s.distances[d.ChairID] = d
	}
	for _, stats := range stats {
		s.stats[stats.ChairID] = stats
	}
	for _, r := range rides {
		s.addRide(r, initialFare+max(0, calculateSale(r)-initialFare-discounts[r.ID]))
	}
	for _, status := range statuses {
		s.addStatus(status)
	}
	if err := s.recoverWAL(); err != nil {
		return err
	}
	// Rebuild ordered recipient queues and current indexes after replay.
	s.rebuildIndexes()
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	s.ready = true
	s.observeOpportunity(time.Now())
	return nil
}

func (s *rideState) rebuildIndexes() {
	s.userRides, s.chairRides = map[string][]string{}, map[string][]string{}
	s.appEvents, s.chairEvents = map[string]*notificationQueue{}, map[string]*notificationQueue{}
	s.resetCurrentIndexes()
	rides := make([]*memoryRide, 0, len(s.rides))
	for _, ride := range s.rides {
		rides = append(rides, ride)
	}
	sort.Slice(rides, func(i, j int) bool {
		if rides[i].CreatedAt.Equal(rides[j].CreatedAt) {
			return rides[i].ID < rides[j].ID
		}
		return rides[i].CreatedAt.Before(rides[j].CreatedAt)
	})
	order := map[string]int{"MATCHING": 1, "ENROUTE": 2, "PICKUP": 3, "CARRYING": 4, "ARRIVED": 5, "COMPLETED": 6}
	for _, ride := range rides {
		sort.Slice(ride.Statuses, func(i, j int) bool {
			a, b := s.statuses[ride.Statuses[i]], s.statuses[ride.Statuses[j]]
			if !a.CreatedAt.Equal(b.CreatedAt) {
				return a.CreatedAt.Before(b.CreatedAt)
			}
			if a.Status != b.Status {
				return order[a.Status] < order[b.Status]
			}
			return a.ID < b.ID
		})
		s.userRides[ride.UserID] = append(s.userRides[ride.UserID], ride.ID)
		s.indexRide(ride)
		for _, id := range ride.Statuses {
			s.appendNotification("app", ride.UserID, s.statuses[id])
		}
	}
	// Chair order follows assignment/completion, not request creation.
	sort.Slice(rides, func(i, j int) bool {
		if rides[i].UpdatedAt.Equal(rides[j].UpdatedAt) {
			return rides[i].ID < rides[j].ID
		}
		return rides[i].UpdatedAt.Before(rides[j].UpdatedAt)
	})
	for _, ride := range rides {
		if ride.ChairID.Valid {
			s.chairRides[ride.ChairID.String] = append(s.chairRides[ride.ChairID.String], ride.ID)
			for _, id := range ride.Statuses {
				s.appendNotification("chair", ride.ChairID.String, s.statuses[id])
			}
		}
	}
	for id := range s.chairs {
		s.refreshChairIndexes(id)
	}
	s.sales = map[string][]salesPoint{}
	for _, ride := range rides {
		s.updateRideSale(rideSale{}, ride)
	}
}

func (s *rideState) addChair(c Chair) {
	s.chairs[c.ID], s.chairTokens[s.accountKey(c.AccessToken)] = c, c.ID
	if s.dispatch != nil {
		s.dispatch.changeChair(c.ID, func(state *chairMetricState) { state.active = c.IsActive })
	}
	s.refreshChairIndexes(c.ID)
}

func (s *rideState) addRide(r Ride, fare int) {
	s.rides[r.ID] = &memoryRide{Ride: r, Fare: fare, waitIndex: -1}
	if s.dispatch != nil {
		if !r.ChairID.Valid {
			heap.Push(&s.dispatch.waiting, s.rides[r.ID])
		} else {
			s.dispatch.changeChair(r.ChairID.String, func(c *chairMetricState) { c.unfinished++ })
		}
	}
	s.indexRide(s.rides[r.ID])
	s.userRides[r.UserID] = append(s.userRides[r.UserID], r.ID)
	if r.ChairID.Valid {
		s.chairRides[r.ChairID.String] = append(s.chairRides[r.ChairID.String], r.ID)
		s.startFlow(s.rides[r.ID], time.Now(), true)
	}
}

func (s *rideState) addStatus(status RideStatus) {
	ride := s.rides[status.RideID]
	before := s.saleForRide(ride)
	wasCompleted := s.latest(ride) == "COMPLETED"
	if s.dispatch != nil && ride.ChairID.Valid {
		s.dispatch.changeChair(ride.ChairID.String, func(c *chairMetricState) {
			if status.ChairSentAt == nil {
				c.pending++
			}
			if wasCompleted != (status.Status == "COMPLETED") {
				if status.Status == "COMPLETED" {
					c.unfinished--
				} else {
					c.unfinished++
				}
			}
		})
	}
	s.statuses[status.ID] = status
	ride.Statuses = append(ride.Statuses, status.ID)
	s.observeFlowStatus(ride, status.Status)
	s.appendNotification("app", ride.UserID, status)
	if wasCompleted != (status.Status == "COMPLETED") {
		if status.Status == "COMPLETED" {
			s.userActiveRides[ride.UserID]--
		} else {
			s.userActiveRides[ride.UserID]++
		}
	}
	if ride.ChairID.Valid {
		id := ride.ChairID.String
		current := s.currentChair(id)
		if !wasCompleted && status.Status == "COMPLETED" {
			current.unfinished = removeCurrentRide(current.unfinished, ride)
			s.indexCompletion(ride)
		} else if wasCompleted && status.Status != "COMPLETED" {
			current.unfinished = insertCurrentRide(current.unfinished, ride)
		}
		s.appendNotification("chair", id, status)
		s.refreshChairIndexes(id)
	}
	s.updateRideSale(before, ride)
}

func (s *rideState) apply(record stateRecord) {
	s.applyAccounts(record.Accounts)
	if seed := record.CreatedRide; seed != nil {
		// Initial SQL data may already contain an older WAL creation record.
		if s.rides[seed.Ride.ID] == nil {
			s.addRide(seed.Ride, seed.Fare)
		}
	}
	if record.Distance != nil {
		s.observeAcceptedDistance(*record.Distance)
		s.distances[record.Distance.ChairID] = *record.Distance
	}
	if record.Chair != nil {
		s.addChair(*record.Chair)
	}
	for _, row := range record.Rides {
		ride := s.rides[row.ID]
		before := s.saleForRide(ride)
		assigned := !ride.ChairID.Valid && row.ChairID.Valid
		if assigned {
			if s.dispatch != nil {
				heap.Remove(&s.dispatch.waiting, ride.waitIndex)
				s.dispatch.changeChair(row.ChairID.String, func(c *chairMetricState) {
					if s.latest(ride) != "COMPLETED" {
						c.unfinished++
					}
					for _, id := range ride.Statuses {
						if s.statuses[id].ChairSentAt == nil {
							c.pending++
						}
					}
				})
			}
			s.chairRides[row.ChairID.String] = append(s.chairRides[row.ChairID.String], row.ID)
			s.waitingRides = removeCurrentRide(s.waitingRides, ride)
			s.waitingPickups = removeWaitingPickup(s.waitingPickups, ride)
			for _, id := range ride.Statuses {
				s.appendNotification("chair", row.ChairID.String, s.statuses[id])
			}
		}
		ride.Ride = row
		if assigned {
			s.indexChairRide(ride)
			s.startFlow(ride, time.Now(), true)
		}
		s.updateRideSale(before, ride)
	}
	for _, status := range record.Statuses {
		if _, exists := s.statuses[status.ID]; !exists {
			s.addStatus(status)
		}
	}
	if record.Stats != nil {
		s.stats[record.Stats.ChairID] = *record.Stats
	}
	if ack := record.Ack; ack != nil {
		status := s.statuses[ack.StatusID]
		at := ack.At
		if ack.Kind == "chair" {
			if s.dispatch != nil && status.ChairSentAt == nil {
				r := s.rides[status.RideID]
				if r.ChairID.Valid {
					s.dispatch.changeChair(r.ChairID.String, func(c *chairMetricState) { c.pending-- })
				}
			}
			status.ChairSentAt = &at
		} else {
			status.AppSentAt = &at
		}
		s.statuses[status.ID] = status
		ride := s.rides[status.RideID]
		id := ride.UserID
		if ack.Kind == "chair" {
			id = ride.ChairID.String
		}
		if queue := s.notificationQueue(ack.Kind, id); queue != nil {
			queue.ack(status.ID)
		}
		if ack.Kind == "chair" && ride.ChairID.Valid {
			s.refreshChairIndexes(id)
		}
	}
}

func (s *rideState) userMutex(id string) *sync.Mutex {
	lock, _ := s.userLocks.LoadOrStore(id, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (s *rideState) latest(ride *memoryRide) string {
	if len(ride.Statuses) == 0 {
		return ""
	}
	return s.statuses[ride.Statuses[len(ride.Statuses)-1]].Status
}

func (s *rideState) newStatus(ride *memoryRide, status string, at time.Time) RideStatus {
	if !at.After(ride.UpdatedAt) {
		at = ride.UpdatedAt.Add(time.Microsecond)
	}
	if len(ride.Statuses) != 0 {
		last := s.statuses[ride.Statuses[len(ride.Statuses)-1]].CreatedAt
		if !at.After(last) {
			at = last.Add(time.Microsecond)
		}
	}
	return RideStatus{ID: ulid.Make().String(), RideID: ride.ID, Status: status, CreatedAt: at}
}

func (s *rideState) continuingRide(userID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userActiveRides[userID] != 0
}

func (s *rideState) coordinate(chairID string, coordinate Coordinate) (time.Time, error) {
	var timing *coordinateTiming
	if s.coordinateCalls.Add(1)%coordinateSampleEvery == 1 {
		timing = &coordinateTiming{started: time.Now()}
	}
	s.mu.Lock()
	success := false
	if timing != nil {
		timing.locked = time.Now()
		timing.elapsed[0] = timing.locked.Sub(timing.started)
	}
	defer func() { s.finishCoordinate(timing, success); s.mu.Unlock() }()
	if _, ok := s.chairs[chairID]; !ok {
		return time.Time{}, sql.ErrNoRows
	}
	d := s.distances[chairID]
	d.ChairID = chairID
	at := time.Now().UTC().Truncate(time.Microsecond)
	if d.UpdatedAt.Valid && !at.After(d.UpdatedAt.Time) {
		at = d.UpdatedAt.Time.Add(time.Microsecond)
	}
	if d.Latitude.Valid {
		d.TotalDistance += calculateDistance(int(d.Latitude.Int64), int(d.Longitude.Int64), coordinate.Latitude, coordinate.Longitude)
	}
	d.Latitude, d.Longitude = sql.NullInt64{Int64: int64(coordinate.Latitude), Valid: true}, sql.NullInt64{Int64: int64(coordinate.Longitude), Valid: true}
	d.UpdatedAt = sql.NullTime{Time: at, Valid: true}
	record := stateRecord{Location: &ChairLocation{ID: ulid.Make().String(), ChairID: chairID, Latitude: coordinate.Latitude, Longitude: coordinate.Longitude, CreatedAt: at}, Distance: &d}
	ids := s.chairRides[chairID]
	var changed *memoryRide
	if len(ids) != 0 {
		ride := s.rides[ids[len(ids)-1]]
		status := s.latest(ride)
		if status == "ENROUTE" && coordinate.Latitude == ride.PickupLatitude && coordinate.Longitude == ride.PickupLongitude {
			record.Statuses = []RideStatus{s.newStatus(ride, "PICKUP", at)}
			changed = ride
		} else if status == "CARRYING" && coordinate.Latitude == ride.DestinationLatitude && coordinate.Longitude == ride.DestinationLongitude {
			record.Statuses = []RideStatus{s.newStatus(ride, "ARRIVED", at)}
			changed = ride
		}
	}
	if err := s.commitCoordinate(record, timing); err != nil {
		return time.Time{}, err
	}
	if changed != nil {
		notifyRide(&changed.Ride)
	}
	success = true
	return at, nil
}

type stateHTTPError struct {
	code    int
	message string
}

func (e *stateHTTPError) Error() string         { return e.message }
func stateError(code int, message string) error { return &stateHTTPError{code, message} }
func writeStateError(w http.ResponseWriter, err error) {
	var response *stateHTTPError
	if errors.As(err, &response) {
		writeError(w, response.code, err)
	} else {
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (s *rideState) chairStatus(chairID, rideID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ride := s.rides[rideID]
	if ride == nil {
		return stateError(http.StatusNotFound, "ride not found")
	}
	if ride.ChairID.String != chairID {
		return stateError(http.StatusBadRequest, "not assigned to this ride")
	}
	switch status {
	case "ENROUTE":
		if s.latest(ride) != "MATCHING" {
			return stateError(http.StatusBadRequest, "ride already acknowledged")
		}
	case "CARRYING":
		if s.latest(ride) != "PICKUP" {
			return stateError(http.StatusBadRequest, "chair has not arrived yet")
		}
	default:
		return stateError(http.StatusBadRequest, "invalid status")
	}
	if err := s.commit(stateRecord{Statuses: []RideStatus{s.newStatus(ride, status, time.Now().UTC().Truncate(time.Microsecond))}}); err != nil {
		return err
	}
	notifyRide(&ride.Ride)
	return nil
}

func (s *rideState) activity(chairID string, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.chairs[chairID]
	if !ok {
		return sql.ErrNoRows
	}
	c.IsActive, c.UpdatedAt = active, time.Now().UTC().Truncate(time.Microsecond)
	return s.commit(stateRecord{Chair: &c})
}

func (s *rideState) nearbyAvailable(chairID string, at time.Time) (bool, error) {
	if _, missing := s.nearbyMissingStatus[chairID]; missing {
		return false, sql.ErrNoRows
	}
	if _, idle := s.nearbyChairs[chairID]; !idle {
		return false, nil
	}
	current := s.chairCurrent[chairID]
	return current.completionPending == 0 && !at.Before(current.nearbyAvailableAt), nil
}

func (s *rideState) completionCandidate(rideID, userID string) (Ride, int, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ride := s.rides[rideID]
	if ride == nil {
		return Ride{}, 0, "", stateError(http.StatusNotFound, "ride not found")
	}
	if ride.UserID != userID {
		return Ride{}, 0, "", stateError(http.StatusNotFound, "ride not found")
	}
	if s.latest(ride) != "ARRIVED" {
		return Ride{}, 0, "", stateError(http.StatusBadRequest, "not arrived yet")
	}
	// The persisted generation survives restarts and changes on initialize.
	return ride.Ride, ride.Fare, s.generation + ":" + ride.ID, nil
}

func (s *rideState) complete(rideID string, evaluation int) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ride := s.rides[rideID]
	if ride == nil || s.latest(ride) != "ARRIVED" {
		return time.Time{}, stateError(http.StatusBadRequest, "not arrived yet")
	}
	status := s.newStatus(ride, "COMPLETED", time.Now().UTC().Truncate(time.Microsecond))
	row := ride.Ride
	row.Evaluation, row.UpdatedAt = &evaluation, status.CreatedAt
	stats := s.stats[row.ChairID.String]
	stats.ChairID, stats.Count, stats.Evaluation = row.ChairID.String, stats.Count+1, stats.Evaluation+evaluation
	if err := s.commit(stateRecord{Rides: []Ride{row}, Statuses: []RideStatus{status}, Stats: &stats}); err != nil {
		return time.Time{}, err
	}
	ride.CompletionResponsePending = true
	s.currentChair(ride.ChairID.String).completionPending++
	notifyRide(&row)
	return row.UpdatedAt, nil
}

func (s *rideState) finishCompletionResponse(rideID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ride := s.rides[rideID]; ride != nil && ride.CompletionResponsePending {
		ride.CompletionResponsePending = false
		ride.NearbyAvailableAt = time.Now().UTC().Add(nearbyCompletionGrace)
		current := s.currentChair(ride.ChairID.String)
		current.completionPending--
		current.nearbyAvailableAt = maxTime(current.nearbyAvailableAt, ride.NearbyAvailableAt)
	}
}

func (s *rideState) checkReady() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.ready || s.failed != nil {
		return fmt.Errorf("runtime state is unavailable: %v", s.failed)
	}
	return nil
}
