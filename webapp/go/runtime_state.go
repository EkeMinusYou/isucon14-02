package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
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
	mu              sync.RWMutex
	flushMu         sync.Mutex
	userLocks       sync.Map
	database        *sqlx.DB
	wal             *os.File
	generation      string
	sequence        uint64
	applied         uint64
	pending         []stateRecord
	ready           bool
	failed          error
	owners          map[string]Owner
	users           map[string]User
	userTokens      map[string]string
	invitationUsers map[string]string
	chairs          map[string]Chair
	modelSpeeds     map[string]int
	chairTokens     map[string]string
	distances       map[string]distanceTotal
	stats           map[string]chairStatsTotal
	rides           map[string]*memoryRide
	statuses        map[string]RideStatus
	userRides       map[string][]string
	chairRides      map[string][]string
	sales           map[string][]salesPoint
	appEvents       map[string][]string
	chairEvents     map[string][]string
	stop            chan struct{}
	done            chan struct{}
}

const projectionSchema = `CREATE TABLE IF NOT EXISTS state_projection (
 id TINYINT NOT NULL PRIMARY KEY,
 generation VARCHAR(36) NOT NULL,
 applied_seq BIGINT UNSIGNED NOT NULL DEFAULT 0
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
	if err := s.ensureCouponIndex(ctx); err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, projectionSchema); err != nil {
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
	var marker struct {
		Generation string `db:"generation"`
		Applied    uint64 `db:"applied_seq"`
	}
	if err := tx.GetContext(ctx, &marker, "SELECT generation, applied_seq FROM state_projection WHERE id = 1"); err != nil {
		return err
	}
	var owners []Owner
	var users []User
	var chairs []Chair
	var models []ChairModel
	var distances []distanceTotal
	var stats []chairStatsTotal
	var rides []Ride
	var statuses []RideStatus
	var coupons []Coupon
	for _, query := range []struct {
		dest any
		sql  string
	}{
		{&models, "SELECT * FROM chair_models"}, {&owners, "SELECT * FROM owners"}, {&users, "SELECT * FROM users"}, {&chairs, "SELECT * FROM chairs"},
		{&distances, "SELECT * FROM chair_distance_totals"}, {&stats, "SELECT * FROM chair_stats"},
		{&rides, "SELECT * FROM rides ORDER BY created_at, id"},
		{&statuses, `SELECT s.* FROM ride_statuses s JOIN rides r ON r.id = s.ride_id ORDER BY r.created_at, r.id, s.created_at, s.status, s.id`},
		{&coupons, "SELECT * FROM coupons WHERE used_by IS NOT NULL"},
	} {
		if err := tx.SelectContext(ctx, query.dest, query.sql); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.owners = map[string]Owner{}
	for _, o := range owners {
		s.owners[o.ID] = o
	}
	s.users, s.userTokens = map[string]User{}, map[string]string{}
	s.invitationUsers = map[string]string{}
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
	s.appEvents, s.chairEvents = map[string][]string{}, map[string][]string{}
	s.generation, s.applied, s.sequence = marker.Generation, marker.Applied, 0
	s.pending, s.failed = nil, nil
	for _, u := range users {
		s.addUser(u)
	}
	for _, c := range chairs {
		s.addChair(c)
	}
	for _, d := range distances {
		s.distances[d.ChairID] = d
	}
	for _, stats := range stats {
		s.stats[stats.ChairID] = stats
	}
	discounts := map[string]int{}
	for _, c := range coupons {
		discounts[*c.UsedBy] = c.Discount
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
	// New DB-created rides can exist beyond an older ride's unprojected WAL.
	// Rebuild recipient queues after replay, rather than appending old events
	// behind the new ride's already-persistent MATCHING status.
	s.rebuildIndexes()
	s.ready = true
	return nil
}

func (s *rideState) rebuildIndexes() {
	s.userRides, s.chairRides = map[string][]string{}, map[string][]string{}
	s.appEvents, s.chairEvents = map[string][]string{}, map[string][]string{}
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
		s.appEvents[ride.UserID] = append(s.appEvents[ride.UserID], ride.Statuses...)
	}
	// SQL-created requests may commit late and be assigned after a younger
	// request. Chair order follows assignment/completion, not request creation.
	sort.Slice(rides, func(i, j int) bool {
		if rides[i].UpdatedAt.Equal(rides[j].UpdatedAt) {
			return rides[i].ID < rides[j].ID
		}
		return rides[i].UpdatedAt.Before(rides[j].UpdatedAt)
	})
	for _, ride := range rides {
		if ride.ChairID.Valid {
			s.chairRides[ride.ChairID.String] = append(s.chairRides[ride.ChairID.String], ride.ID)
			s.chairEvents[ride.ChairID.String] = append(s.chairEvents[ride.ChairID.String], ride.Statuses...)
		}
	}
	s.sales = map[string][]salesPoint{}
	for _, ride := range rides {
		s.updateRideSale(rideSale{}, ride)
	}
}

func (s *rideState) addUser(u User) {
	s.users[u.ID], s.userTokens[u.AccessToken] = u, u.ID
	s.invitationUsers[u.InvitationCode] = u.ID
}
func (s *rideState) addChair(c Chair) { s.chairs[c.ID], s.chairTokens[c.AccessToken] = c, c.ID }

func (s *rideState) addRide(r Ride, fare int) {
	s.rides[r.ID] = &memoryRide{Ride: r, Fare: fare}
	s.userRides[r.UserID] = append(s.userRides[r.UserID], r.ID)
	if r.ChairID.Valid {
		s.chairRides[r.ChairID.String] = append(s.chairRides[r.ChairID.String], r.ID)
	}
}

func (s *rideState) addStatus(status RideStatus) {
	s.statuses[status.ID] = status
	ride := s.rides[status.RideID]
	before := s.saleForRide(ride)
	ride.Statuses = append(ride.Statuses, status.ID)
	s.appEvents[ride.UserID] = append(s.appEvents[ride.UserID], status.ID)
	if ride.ChairID.Valid {
		s.chairEvents[ride.ChairID.String] = append(s.chairEvents[ride.ChairID.String], status.ID)
	}
	s.updateRideSale(before, ride)
}

func (s *rideState) apply(record stateRecord) {
	if seed := record.CreatedRide; seed != nil {
		// Creation and coupon consumption commit in SQL before WAL publication.
		// Recovery may therefore have already seeded this ride and its MATCHING.
		if s.rides[seed.Ride.ID] == nil {
			s.addRide(seed.Ride, seed.Fare)
		}
	}
	if record.Distance != nil {
		s.distances[record.Distance.ChairID] = *record.Distance
	}
	if record.Chair != nil {
		s.addChair(*record.Chair)
	}
	for _, row := range record.Rides {
		ride := s.rides[row.ID]
		before := s.saleForRide(ride)
		if !ride.ChairID.Valid && row.ChairID.Valid {
			s.chairRides[row.ChairID.String] = append(s.chairRides[row.ChairID.String], row.ID)
			s.chairEvents[row.ChairID.String] = append(s.chairEvents[row.ChairID.String], ride.Statuses...)
		}
		ride.Ride = row
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
			status.ChairSentAt = &at
		} else {
			status.AppSentAt = &at
		}
		s.statuses[status.ID] = status
	}
}

func (s *rideState) publishCreatedRide(ride Ride, fare int, status RideStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.commit(stateRecord{CreatedRide: &createdRide{Ride: ride, Fare: fare}, Statuses: []RideStatus{status}}); err != nil {
		// SQL already contains the new ride. Refuse further mutations until
		// restart reconstructs it, rather than allowing a second active ride.
		s.failed = err
		return err
	}
	return nil
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
	for _, id := range s.userRides[userID] {
		if s.latest(s.rides[id]) != "COMPLETED" {
			return true
		}
	}
	return false
}

func (s *rideState) coordinate(chairID string, coordinate Coordinate) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if err := s.commit(record); err != nil {
		return time.Time{}, err
	}
	if changed != nil {
		notifyRide(&changed.Ride)
	}
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

func (s *rideState) available(chairID string, requireSent bool) bool {
	if !s.chairs[chairID].IsActive {
		return false
	}
	for _, id := range s.chairRides[chairID] {
		ride := s.rides[id]
		if s.latest(ride) != "COMPLETED" {
			return false
		}
		if requireSent {
			for _, statusID := range ride.Statuses {
				if s.statuses[statusID].ChairSentAt == nil {
					return false
				}
			}
		}
	}
	return true
}

// All unfinished rides affect nearby availability, even when a newer ride completed.
func (s *rideState) nearbyAvailable(chairID string, at time.Time) (bool, error) {
	if !s.chairs[chairID].IsActive {
		return false, nil
	}
	var unfinished *memoryRide
	publicationPending := false
	for _, id := range s.chairRides[chairID] {
		ride := s.rides[id]
		if s.latest(ride) == "COMPLETED" {
			status := s.statuses[ride.Statuses[len(ride.Statuses)-1]]
			if ride.CompletionResponsePending || at.Before(status.CreatedAt.Add(nearbyCompletionGrace)) || at.Before(ride.NearbyAvailableAt) {
				publicationPending = true
			}
			continue
		}
		if unfinished == nil || ride.CreatedAt.After(unfinished.CreatedAt) ||
			(ride.CreatedAt.Equal(unfinished.CreatedAt) && ride.ID > unfinished.ID) {
			unfinished = ride
		}
	}
	if unfinished == nil {
		return !publicationPending, nil
	}
	if s.latest(unfinished) == "" {
		return false, sql.ErrNoRows
	}
	return false, nil
}

func (s *rideState) match() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var waiting []*memoryRide
	for _, ride := range s.rides {
		if !ride.ChairID.Valid {
			waiting = append(waiting, ride)
		}
	}
	sort.Slice(waiting, func(i, j int) bool {
		if waiting[i].CreatedAt.Equal(waiting[j].CreatedAt) {
			return waiting[i].ID < waiting[j].ID
		}
		return waiting[i].CreatedAt.Before(waiting[j].CreatedAt)
	})
	if len(waiting) > matchingBatchSize {
		waiting = waiting[:matchingBatchSize]
	}
	var candidates []matchingChair
	for id, chair := range s.chairs {
		if s.available(id, true) {
			d := s.distances[id]
			speed := s.modelSpeeds[chair.Model]
			if !d.Latitude.Valid || !d.Longitude.Valid {
				speed = 0
			}
			candidates = append(candidates, matchingChair{ID: id, Latitude: int(d.Latitude.Int64), Longitude: int(d.Longitude.Int64), Speed: speed})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	rows := make([]Ride, len(waiting))
	for i, ride := range waiting {
		rows[i] = ride.Ride
	}
	assignments := assignMatchingChairs(rows, candidates)
	if len(assignments) == 0 {
		return nil
	}
	record := stateRecord{}
	for _, assignment := range assignments {
		row := waiting[assignment.rideIndex].Ride
		chairID := candidates[assignment.chairIndex].ID
		at := time.Now().UTC().Truncate(time.Microsecond)
		previous := s.chairRides[chairID]
		if len(previous) > 0 {
			last := s.rides[previous[len(previous)-1]].UpdatedAt
			if !at.After(last) {
				at = last.Add(time.Microsecond)
			}
		}
		row.ChairID, row.UpdatedAt = sql.NullString{String: chairID, Valid: true}, at
		record.Rides = append(record.Rides, row)
	}
	if err := s.commit(record); err != nil {
		return err
	}
	for _, row := range record.Rides {
		notifyRide(&row)
	}
	return nil
}

func (s *rideState) completionCandidate(rideID, userID string) (Ride, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ride := s.rides[rideID]
	if ride == nil {
		return Ride{}, 0, stateError(http.StatusNotFound, "ride not found")
	}
	if ride.UserID != userID {
		return Ride{}, 0, stateError(http.StatusNotFound, "ride not found")
	}
	if s.latest(ride) != "ARRIVED" {
		return Ride{}, 0, stateError(http.StatusBadRequest, "not arrived yet")
	}
	return ride.Ride, ride.Fare, nil
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
	notifyRide(&row)
	return row.UpdatedAt, nil
}

func (s *rideState) finishCompletionResponse(rideID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ride := s.rides[rideID]; ride != nil && ride.CompletionResponsePending {
		ride.CompletionResponsePending = false
		ride.NearbyAvailableAt = time.Now().UTC().Add(nearbyCompletionGrace)
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
