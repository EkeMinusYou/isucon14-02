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
	mu           sync.RWMutex
	ownerID      string // Immutable, including while replacing the Ride after-image.
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

// Business state uses user -> ride -> chair locks. Index and metric locks cover
// only derived publication; WAL append is the sole shared write ordering point.
type rideState struct {
	accountState
	// Immutable after initial startup; request normalization needs no state lock.
	collationWeights  []uint16
	coordinateCalls   atomic.Uint64
	coordinateReady   atomic.Bool
	coordinateFailed  atomic.Pointer[coordinateFailure]
	coordinates       *coordinateState
	metrics           *rideMetrics
	dispatch          *dispatchMetrics
	walMu             sync.RWMutex // Append order and failure state only.
	indexMu           sync.RWMutex // Candidate indexes only; calculation uses immutable copies.
	observationMu     sync.RWMutex // Existing fixed-size metric aggregates only.
	registrationLocks sync.Map
	userStateLocks    sync.Map
	chairLocks        sync.Map
	ownerStateLocks   sync.Map
	ownerChairs       *stateMap[[]string]
	matchingMu        sync.Mutex
	matchingSolver    *matchingRPCClient
	userLocks         sync.Map // Per-user payment/request gate, separate from state locks.
	database          *sqlx.DB
	wal               *os.File
	generation        string
	sequence          uint64
	ready             bool
	failed            error
	chairs            *stateMap[Chair]
	modelSpeeds       map[string]int
	chairTokens       *stateMap[string]
	stats             *stateMap[chairStatsTotal]
	rides             *stateMap[*memoryRide]
	statuses          *stateMap[RideStatus]
	userRides         *stateMap[[]string]
	chairRides        *stateMap[[]string]
	sales             *stateMap[[]salesPoint]
	appEvents         *stateMap[*notificationQueue]
	chairEvents       *stateMap[*notificationQueue]
	stop              chan struct{}
	done              chan struct{}

	waitingRides        []*memoryRide
	userActiveRides     *stateMap[int]
	chairCurrent        *stateMap[*chairCurrentState]
	matchingChairs      map[string]struct{}
	nearbyChairs        map[string]*coordinateCell
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
	coordinates, err := openCoordinateState(directory)
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &rideState{database: database, wal: f, coordinates: coordinates}
	if err := s.restore(ctx); err != nil {
		f.Close()
		coordinates.close()
		return nil, err
	}
	return s, nil
}

func (s *rideState) restore(ctx context.Context) error {
	s.ready = false
	s.coordinateReady.Store(false)
	s.coordinates.reset()
	// Restore runs under exclusive lifecycle ownership; discard old recipient locks.
	s.userLocks, s.userStateLocks, s.chairLocks, s.ownerStateLocks, s.registrationLocks = sync.Map{}, sync.Map{}, sync.Map{}, sync.Map{}, sync.Map{}
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
	s.ownerChairs = newStateMap(map[string][]string{})
	s.chairs, s.chairTokens = newStateMap(map[string]Chair{}), newStateMap(map[string]string{})
	s.modelSpeeds = map[string]int{}
	for _, model := range models {
		s.modelSpeeds[model.Name] = model.Speed
	}
	s.stats = newStateMap(map[string]chairStatsTotal{})
	s.rides, s.statuses = newStateMap(map[string]*memoryRide{}), newStateMap(map[string]RideStatus{})
	s.userRides, s.chairRides = newStateMap(map[string][]string{}), newStateMap(map[string][]string{})
	// Build sales once from the final SQL and WAL state, in timestamp order.
	s.sales = newStateMap(map[string][]salesPoint{})

	s.appEvents, s.chairEvents = newStateMap(map[string]*notificationQueue{}), newStateMap(map[string]*notificationQueue{})
	s.resetCurrentIndexes()
	s.generation, s.sequence, s.failed = generation, 0, nil
	discounts := accounts.install(s)
	for _, c := range chairs {
		s.addChair(c)
	}
	for _, d := range distances {
		s.seedCoordinateDistance(d.ChairID, d)
	}
	for _, stats := range stats {
		s.stats.set(stats.ChairID, stats)
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
	if err := s.recoverCoordinates(); err != nil {
		return err
	}
	// Rebuild ordered recipient queues and current indexes after replay.
	s.rebuildIndexes()
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	s.ready = true
	s.coordinateFailed.Store(nil)
	s.coordinateReady.Store(true)
	s.observeOpportunity()
	return nil
}

func (s *rideState) rebuildIndexes() {
	s.userRides, s.chairRides = newStateMap(map[string][]string{}), newStateMap(map[string][]string{})
	s.appEvents, s.chairEvents = newStateMap(map[string]*notificationQueue{}), newStateMap(map[string]*notificationQueue{})
	s.resetCurrentIndexes()
	rides := make([]*memoryRide, 0, s.rides.len())
	for _, ride := range s.rides.all() {
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
			a, b := s.statuses.get(ride.Statuses[i]), s.statuses.get(ride.Statuses[j])
			if !a.CreatedAt.Equal(b.CreatedAt) {
				return a.CreatedAt.Before(b.CreatedAt)
			}
			if a.Status != b.Status {
				return order[a.Status] < order[b.Status]
			}
			return a.ID < b.ID
		})
		s.userRides.set(ride.UserID, append(s.userRides.get(ride.UserID), ride.ID))
		s.indexRide(ride)
		for _, id := range ride.Statuses {
			s.appendNotification("app", ride.UserID, s.statuses.get(id))
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
			s.chairRides.set(ride.ChairID.String, append(s.chairRides.get(ride.ChairID.String), ride.ID))
			for _, id := range ride.Statuses {
				s.appendNotification("chair", ride.ChairID.String, s.statuses.get(id))
			}
		}
	}
	for id := range s.chairs.all() {
		s.refreshChairIndexes(id)
	}
	s.sales = newStateMap(map[string][]salesPoint{})
	for _, ride := range rides {
		s.updateRideSale(rideSale{}, ride)
	}
}

func (s *rideState) addChair(c Chair) {
	if _, exists := s.chairs.lookup(c.ID); !exists {
		s.ownerChairs.set(c.OwnerID, append(s.ownerChairs.get(c.OwnerID), c.ID))
	}
	s.chairs.set(c.ID, c)
	s.chairTokens.set(s.accountKey(c.AccessToken), c.ID)
	s.publishCoordinateChair(c)
	if s.dispatch != nil {
		s.dispatch.changeChair(c.ID, func(state *chairMetricState) { state.active = c.IsActive })
	}
	s.refreshChairIndexes(c.ID)
}

func (s *rideState) addRide(r Ride, fare int) {
	s.rides.set(r.ID, &memoryRide{Ride: r, ownerID: r.UserID, Fare: fare, waitIndex: -1})
	if s.dispatch != nil {
		if !r.ChairID.Valid {
			s.observationMu.Lock()
			heap.Push(&s.dispatch.waiting, s.rides.get(r.ID))
			s.observationMu.Unlock()
		} else {
			s.dispatch.changeChair(r.ChairID.String, func(c *chairMetricState) { c.unfinished++ })
		}
	}
	s.indexRide(s.rides.get(r.ID))
	s.userRides.set(r.UserID, append(s.userRides.get(r.UserID), r.ID))
	if r.ChairID.Valid {
		s.chairRides.set(r.ChairID.String, append(s.chairRides.get(r.ChairID.String), r.ID))
		s.startFlow(s.rides.get(r.ID), time.Now(), true)
	}
}

func (s *rideState) addStatus(status RideStatus) {
	ride := s.rides.get(status.RideID)
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
	s.statuses.set(status.ID, status)
	ride.Statuses = append(ride.Statuses, status.ID)
	s.observeFlowStatus(ride, status.Status)
	s.appendNotification("app", ride.UserID, status)
	if wasCompleted != (status.Status == "COMPLETED") {
		if status.Status == "COMPLETED" {
			s.userActiveRides.set(ride.UserID, s.userActiveRides.get(ride.UserID)-1)
		} else {
			s.userActiveRides.set(ride.UserID, s.userActiveRides.get(ride.UserID)+1)
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
		if s.rides.get(seed.Ride.ID) == nil {
			s.addRide(seed.Ride, seed.Fare)
		}
	}
	if record.Coordinate != nil && !s.ready {
		s.coordinates.arrivals[*record.Coordinate] = false
	}
	if record.Chair != nil {
		s.addChair(*record.Chair)
	}
	for _, row := range record.Rides {
		ride := s.rides.get(row.ID)
		before := s.saleForRide(ride)
		assigned := !ride.ChairID.Valid && row.ChairID.Valid
		if assigned {
			if s.dispatch != nil {
				s.observationMu.Lock()
				heap.Remove(&s.dispatch.waiting, ride.waitIndex)
				s.observationMu.Unlock()
				s.dispatch.changeChair(row.ChairID.String, func(c *chairMetricState) {
					if s.latest(ride) != "COMPLETED" {
						c.unfinished++
					}
					for _, id := range ride.Statuses {
						if s.statuses.get(id).ChairSentAt == nil {
							c.pending++
						}
					}
				})
			}
			s.chairRides.set(row.ChairID.String, append(s.chairRides.get(row.ChairID.String), row.ID))
			s.indexMu.Lock()
			s.waitingRides = removeCurrentRide(s.waitingRides, ride)
			s.indexMu.Unlock()
			for _, id := range ride.Statuses {
				s.appendNotification("chair", row.ChairID.String, s.statuses.get(id))
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
		if _, exists := s.statuses.lookup(status.ID); !exists {
			s.addStatus(status)
		}
	}
	if record.Stats != nil {
		s.stats.set(record.Stats.ChairID, *record.Stats)
	}
	if ack := record.Ack; ack != nil {
		status := s.statuses.get(ack.StatusID)
		at := ack.At
		if ack.Kind == "chair" {
			if s.dispatch != nil && status.ChairSentAt == nil {
				r := s.rides.get(status.RideID)
				if r.ChairID.Valid {
					s.dispatch.changeChair(r.ChairID.String, func(c *chairMetricState) { c.pending-- })
				}
			}
			status.ChairSentAt = &at
		} else {
			status.AppSentAt = &at
		}
		s.statuses.set(status.ID, status)
		ride := s.rides.get(status.RideID)
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
	if lock, ok := s.userLocks.Load(id); ok {
		return lock.(*sync.Mutex)
	}
	lock, _ := s.userLocks.LoadOrStore(id, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (s *rideState) latest(ride *memoryRide) string {
	if len(ride.Statuses) == 0 {
		return ""
	}
	return s.statuses.get(ride.Statuses[len(ride.Statuses)-1]).Status
}

func (s *rideState) newStatus(ride *memoryRide, status string, at time.Time) RideStatus {
	if !at.After(ride.UpdatedAt) {
		at = ride.UpdatedAt.Add(time.Microsecond)
	}
	if len(ride.Statuses) != 0 {
		last := s.statuses.get(ride.Statuses[len(ride.Statuses)-1]).CreatedAt
		if !at.After(last) {
			at = last.Add(time.Microsecond)
		}
	}
	return RideStatus{ID: ulid.Make().String(), RideID: ride.ID, Status: status, CreatedAt: at}
}

func (s *rideState) continuingRide(userID string) bool {
	lock := s.userStateMutex(userID)
	lock.RLock()
	defer lock.RUnlock()
	return s.userActiveRides.get(userID) != 0
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
	ride, unlock := s.lockRide(rideID, false)
	defer unlock()
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
	lock := s.chairMutex(chairID)
	lock.Lock()
	defer lock.Unlock()
	c, ok := s.chairs.lookup(chairID)
	if !ok {
		return sql.ErrNoRows
	}
	c.IsActive, c.UpdatedAt = active, time.Now().UTC().Truncate(time.Microsecond)
	return s.commit(stateRecord{Chair: &c})
}

func (s *rideState) nearbyAvailable(chairID string, at time.Time) (bool, error) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	if _, missing := s.nearbyMissingStatus[chairID]; missing {
		return false, sql.ErrNoRows
	}
	if _, idle := s.nearbyChairs[chairID]; !idle {
		return false, nil
	}
	current := s.chairCurrent.get(chairID)
	return current.completionPending == 0 && !at.Before(current.nearbyAvailableAt), nil
}

func (s *rideState) completionCandidate(rideID, userID string) (Ride, int, string, error) {
	ride, unlock := s.lockRide(rideID, true)
	defer unlock()
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
	ride, unlock := s.lockRide(rideID, false)
	defer unlock()
	if ride == nil || s.latest(ride) != "ARRIVED" {
		return time.Time{}, stateError(http.StatusBadRequest, "not arrived yet")
	}
	status := s.newStatus(ride, "COMPLETED", time.Now().UTC().Truncate(time.Microsecond))
	row := ride.Ride
	row.Evaluation, row.UpdatedAt = &evaluation, status.CreatedAt
	stats := s.stats.get(row.ChairID.String)
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
	ride, unlock := s.lockRide(rideID, false)
	defer unlock()
	if ride != nil && ride.CompletionResponsePending {
		ride.CompletionResponsePending = false
		ride.NearbyAvailableAt = time.Now().UTC().Add(nearbyCompletionGrace)
		current := s.currentChair(ride.ChairID.String)
		current.completionPending--
		current.nearbyAvailableAt = maxTime(current.nearbyAvailableAt, ride.NearbyAvailableAt)
	}
}

func (s *rideState) checkReady() error {
	if err := s.coordinateAvailable(); err != nil {
		return fmt.Errorf("runtime state is unavailable: %w", err)
	}
	return nil
}
