package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
)

const coordinateStripeCount = 8

type coordinateFailure struct{ err error }

// Operation order is cell.mu -> control state, only for arrivals. Control
// writers may fence routing with routeMu, but never take cell.mu. Coordinates
// release routeMu before acquiring s.mu. Position readers only load snapshots.
type coordinateCell struct {
	mu       sync.Mutex
	routeMu  sync.RWMutex
	route    coordinateRoute
	chair    atomic.Pointer[Chair]
	snapshot atomic.Pointer[coordinateSnapshot]
}

type coordinateSnapshot struct {
	distance distanceTotal
	sequence uint64
}

type coordinateRoute struct {
	rideID, status      string
	latitude, longitude int
	stage               int
}

type coordinateReference struct {
	ChairID        string `json:"c"`
	ChairSequence  uint64 `json:"s"`
	StripeSequence uint64 `json:"w"`
}

type coordinateRecord struct {
	Version       int           `json:"v"`
	Generation    string        `json:"g"`
	Sequence      uint64        `json:"s"`
	ChairSequence uint64        `json:"cs"`
	Control       bool          `json:"control,omitempty"`
	Location      ChairLocation `json:"l"`
	Distance      distanceTotal `json:"d"`
}

type coordinateStripe struct {
	mu       sync.Mutex
	wal      *os.File
	sequence uint64
	frame    []byte
	metrics  coordinateAggregates
}

type coordinateState struct {
	cells    sync.Map
	tokens   sync.Map
	stripes  [coordinateStripeCount]coordinateStripe
	arrivals map[coordinateReference]bool // Only used during exclusive recovery.
}

func openCoordinateState(directory string) (*coordinateState, error) {
	c := &coordinateState{}
	for i := range c.stripes {
		f, err := os.OpenFile(filepath.Join(directory, fmt.Sprintf("coordinates-%d.wal", i)), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			c.close()
			return nil, err
		}
		c.stripes[i].wal = f
	}
	return c, nil
}

func coordinateStripeIndex(id string) int {
	// FNV-1a is stable across process restarts and does not need allocations.
	h := uint32(2166136261)
	for i := 0; i < len(id); i++ {
		h = (h ^ uint32(id[i])) * 16777619
	}
	return int(h % coordinateStripeCount)
}

func (s *rideState) coordinateCell(id string) *coordinateCell {
	v, ok := s.coordinates.cells.Load(id)
	if !ok {
		return nil
	}
	return v.(*coordinateCell)
}

// Called by control-state writers under s.mu, or during exclusive restore.
func (s *rideState) publishCoordinateChair(chair Chair) {
	cell := s.coordinateCell(chair.ID)
	if cell == nil {
		cell = &coordinateCell{}
		cell.snapshot.Store(&coordinateSnapshot{distance: distanceTotal{ChairID: chair.ID}})
		s.coordinates.cells.Store(chair.ID, cell)
	}
	cell.chair.Store(&chair)
	s.coordinates.tokens.Store(s.accountKey(chair.AccessToken), chair.ID)
}

func (s *rideState) seedCoordinateDistance(id string, d distanceTotal) {
	d.ChairID = id
	cell := s.coordinateCell(id)
	if cell == nil {
		cell = &coordinateCell{}
		s.coordinates.cells.Store(id, cell)
	}
	cell.snapshot.Store(&coordinateSnapshot{distance: d})
}

func (s *rideState) chairDistance(id string) distanceTotal {
	if cell := s.coordinateCell(id); cell != nil {
		if snapshot := cell.snapshot.Load(); snapshot != nil {
			return snapshot.distance
		}
	}
	return distanceTotal{ChairID: id}
}

func (s *rideState) refreshCoordinateRoute(id string) {
	cell := s.coordinateCell(id)
	if cell == nil {
		return
	}
	route := coordinateRoute{}
	if ids := s.chairRides[id]; len(ids) != 0 {
		ride := s.rides[ids[len(ids)-1]]
		route.rideID, route.status = ride.ID, s.latest(ride)
		if route.status == "ENROUTE" {
			route.latitude, route.longitude = ride.PickupLatitude, ride.PickupLongitude
		}
		if route.status == "CARRYING" {
			route.latitude, route.longitude = ride.DestinationLatitude, ride.DestinationLongitude
		}
	}
	if s.dispatch != nil {
		if c := s.dispatch.chair[id]; c != nil {
			route.stage = c.flow.stage
		}
	}
	cell.routeMu.Lock()
	cell.route = route
	cell.routeMu.Unlock()
}

func (r coordinateRoute) arrival(p Coordinate) bool {
	return (r.status == "ENROUTE" || r.status == "CARRYING") && r.latitude == p.Latitude && r.longitude == p.Longitude
}

func (s *rideState) coordinateAvailable() error {
	if !s.coordinateReady.Load() {
		return errors.New("runtime state is unavailable")
	}
	if failure := s.coordinateFailed.Load(); failure != nil {
		return failure.err
	}
	return nil
}

func (s *rideState) failCoordinates(err error) {
	s.coordinateFailed.CompareAndSwap(nil, &coordinateFailure{err: err})
}

func (s *rideState) coordinate(chairID string, p Coordinate) (time.Time, error) {
	var timing *coordinateTiming
	if s.coordinateCalls.Add(1)%coordinateSampleEvery == 1 {
		timing = &coordinateTiming{started: time.Now()}
	}
	stripe := &s.coordinates.stripes[coordinateStripeIndex(chairID)]
	cell := s.coordinateCell(chairID)
	success, attempted, written, aggregated := false, false, false, false
	var writeFailure error
	var previous, next distanceTotal
	stage := flowNone
	// Aggregates are owned by the existing WAL stripes, never by a common
	// coordinate aggregation mutex or the control-state lock.
	defer func() {
		if timing != nil && timing.locked.IsZero() {
			timing.locked = time.Now()
			timing.elapsed[0] = timing.locked.Sub(timing.started)
		}
		if !aggregated {
			stripe.mu.Lock()
			stripe.metrics.finish(timing, success, attempted, written, previous, next, stage)
			stripe.mu.Unlock()
		}
		if writeFailure != nil {
			s.mu.Lock()
			if s.failed == nil {
				s.failed = writeFailure
				s.observeOpportunity(time.Now())
			}
			s.mu.Unlock()
		}
	}()
	if cell == nil || cell.chair.Load() == nil {
		return time.Time{}, sql.ErrNoRows
	}
	cell.mu.Lock()
	defer cell.mu.Unlock()
	cell.routeMu.RLock()
	route := cell.route
	if route.arrival(p) {
		// Drop routing before waiting for control. No control->operation inversion.
		cell.routeMu.RUnlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		cell.routeMu.RLock()
		route = cell.route
	}
	routeLocked := true
	defer func() {
		if routeLocked {
			cell.routeMu.RUnlock()
		}
	}()
	if timing != nil {
		timing.locked = time.Now()
		timing.elapsed[0] = timing.locked.Sub(timing.started)
	}
	defer func() {
		if !aggregated {
			finishCoordinateHold(timing)
		}
	}()
	if err := s.coordinateAvailable(); err != nil {
		return time.Time{}, err
	}
	snapshot := cell.snapshot.Load()
	previous, next = snapshot.distance, snapshot.distance
	stage = route.stage
	at := time.Now().UTC().Truncate(time.Microsecond)
	if next.UpdatedAt.Valid && !at.After(next.UpdatedAt.Time) {
		at = next.UpdatedAt.Time.Add(time.Microsecond)
	}
	if next.Latitude.Valid {
		next.TotalDistance += calculateDistance(int(next.Latitude.Int64), int(next.Longitude.Int64), p.Latitude, p.Longitude)
	}
	next.Latitude, next.Longitude = sql.NullInt64{Int64: int64(p.Latitude), Valid: true}, sql.NullInt64{Int64: int64(p.Longitude), Valid: true}
	next.UpdatedAt = sql.NullTime{Time: at, Valid: true}
	arrival := route.arrival(p)
	record := coordinateRecord{Version: 1, Generation: s.generation, ChairSequence: snapshot.sequence + 1, Control: arrival,
		Location: ChairLocation{ID: ulid.Make().String(), ChairID: chairID, Latitude: p.Latitude, Longitude: p.Longitude, CreatedAt: at}, Distance: next}
	var waitStarted time.Time
	if timing != nil {
		waitStarted = time.Now()
	}
	stripe.mu.Lock()
	if timing != nil {
		timing.waitAfter = time.Since(waitStarted)
		timing.elapsed[0] += timing.waitAfter
	}
	if err := s.coordinateAvailable(); err != nil {
		stripe.mu.Unlock()
		return time.Time{}, err
	}
	record.Sequence = stripe.sequence + 1
	attempted = true
	err := stripe.write(record, timing)
	if err == nil {
		stripe.sequence = record.Sequence
		written = true
	} else {
		writeFailure = err
		s.failCoordinates(err)
	}
	if err == nil && !arrival {
		publishCoordinateSnapshot(cell, next, record.ChairSequence, timing)
		success = true
		finishCoordinateHold(timing)
		stripe.metrics.finish(timing, success, attempted, written, previous, next, stage)
		aggregated = true
		stripe.mu.Unlock()
		return at, nil
	}
	stripe.mu.Unlock()
	if err != nil {
		return time.Time{}, err
	}
	if arrival {
		// The coordinate frame is prepared durably, but publication requires the
		// control WAL reference. Recovery ignores an uncommitted prepared arrival.
		cell.routeMu.RUnlock()
		routeLocked = false
		ride := s.rides[route.rideID]
		status := "PICKUP"
		if route.status == "CARRYING" {
			status = "ARRIVED"
		}
		ref := coordinateReference{ChairID: chairID, ChairSequence: record.ChairSequence, StripeSequence: record.Sequence}
		var controlTiming *coordinateTiming
		if timing != nil {
			controlTiming = &coordinateTiming{}
		}
		err := s.commitMeasured(stateRecord{Coordinate: &ref, Statuses: []RideStatus{s.newStatus(ride, status, at)}}, controlTiming)
		if timing != nil {
			for i := 2; i <= 4; i++ {
				if controlTiming.visited[i] {
					timing.elapsed[i] += controlTiming.elapsed[i]
					timing.ok[i] = timing.ok[i] && controlTiming.ok[i]
				}
			}
			timing.bytes += controlTiming.bytes
			timing.elapsed[5] = controlTiming.elapsed[5]
		}
		if err != nil {
			written = false
			return time.Time{}, err
		}
		publishCoordinateSnapshot(cell, next, record.ChairSequence, timing)
		notifyRide(&ride.Ride)
	}
	success = true
	return at, nil
}

func publishCoordinateSnapshot(cell *coordinateCell, distance distanceTotal, sequence uint64, timing *coordinateTiming) {
	var started time.Time
	if timing != nil {
		started = time.Now()
	}
	cell.snapshot.Store(&coordinateSnapshot{distance: distance, sequence: sequence})
	if timing != nil {
		timing.elapsed[5] += time.Since(started)
		timing.visited[5], timing.ok[5] = true, true
	}
}

func (c *coordinateState) reset() {
	c.cells, c.tokens = sync.Map{}, sync.Map{}
	c.arrivals = map[coordinateReference]bool{}
	for i := range c.stripes {
		c.stripes[i].sequence = 0
		c.stripes[i].metrics = coordinateAggregates{}
	}
}

func (s *rideState) recoverCoordinates() error {
	for i := range s.coordinates.stripes {
		stripe := &s.coordinates.stripes[i]
		if err := s.recoverCoordinateStripe(i, stripe); err != nil {
			return err
		}
	}
	for ref, found := range s.coordinates.arrivals {
		if !found {
			return fmt.Errorf("control WAL references missing coordinate: %+v", ref)
		}
	}
	s.coordinates.arrivals = nil
	return nil
}

func (s *rideState) recoverCoordinateStripe(index int, stripe *coordinateStripe) error {
	f := stripe.wal
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header, err := readCoordinateRecord(f)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (err == nil && header.Generation != s.generation) {
		if err := f.Truncate(0); err != nil {
			return err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := stripe.write(coordinateRecord{Version: 1, Generation: s.generation}, nil); err != nil {
			return err
		}
		return f.Sync()
	}
	if err != nil {
		return err
	}
	if header.Sequence != 0 || header.ChairSequence != 0 {
		return errors.New("invalid coordinate WAL generation header")
	}
	for {
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		record, err := readCoordinateRecord(f)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if err := f.Truncate(offset); err != nil {
				return err
			}
			_, err = f.Seek(offset, io.SeekStart)
			return err
		}
		if err != nil {
			return fmt.Errorf("coordinate WAL %d at %d: %w", index, offset, err)
		}
		if record.Generation != s.generation || record.Sequence != stripe.sequence+1 || coordinateStripeIndex(record.Location.ChairID) != index {
			return errors.New("coordinate WAL generation, stripe or sequence mismatch")
		}
		stripe.sequence = record.Sequence
		if record.Control {
			ref := coordinateReference{ChairID: record.Location.ChairID, ChairSequence: record.ChairSequence, StripeSequence: record.Sequence}
			if _, committed := s.coordinates.arrivals[ref]; !committed {
				continue
			}
			s.coordinates.arrivals[ref] = true
		}
		cell := s.coordinateCell(record.Location.ChairID)
		if cell == nil || cell.chair.Load() == nil {
			return errors.New("coordinate WAL references missing chair")
		}
		old := cell.snapshot.Load()
		expected := old.distance
		if expected.Latitude.Valid {
			expected.TotalDistance += calculateDistance(int(expected.Latitude.Int64), int(expected.Longitude.Int64), record.Location.Latitude, record.Location.Longitude)
		}
		if record.ChairSequence != old.sequence+1 || record.Distance.ChairID != record.Location.ChairID || record.Distance.TotalDistance != expected.TotalDistance ||
			!record.Distance.Latitude.Valid || record.Distance.Latitude.Int64 != int64(record.Location.Latitude) || !record.Distance.Longitude.Valid || record.Distance.Longitude.Int64 != int64(record.Location.Longitude) ||
			!record.Distance.UpdatedAt.Valid || !record.Distance.UpdatedAt.Time.Equal(record.Location.CreatedAt) || (old.distance.UpdatedAt.Valid && !record.Location.CreatedAt.After(old.distance.UpdatedAt.Time)) {
			return errors.New("coordinate WAL chair order or distance mismatch")
		}
		cell.snapshot.Store(&coordinateSnapshot{distance: record.Distance, sequence: record.ChairSequence})
	}
}

func (c *coordinateState) sync() error {
	for i := range c.stripes {
		if err := c.stripes[i].wal.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (c *coordinateState) close() error {
	var result error
	for i := range c.stripes {
		f := c.stripes[i].wal
		if f == nil {
			continue
		}
		result = errors.Join(result, f.Sync(), f.Close())
	}
	return result
}
