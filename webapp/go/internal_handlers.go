package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

const matchingBatchSize = 64

var matchingMu sync.Mutex

type matchingChair struct {
	ID        string        `db:"id"`
	Latitude  sql.NullInt64 `db:"last_latitude"`
	Longitude sql.NullInt64 `db:"last_longitude"`
	Speed     sql.NullInt64 `db:"speed"`
}

// Rides arrive oldest first. Reserve each chair once, minimizing estimated
// pickup time over all remaining candidates without truncating integer ratios.
func selectMatchingChairs(rides []Ride, chairs []matchingChair) []string {
	chairIDs := make([]string, 0, min(len(rides), len(chairs)))
	for _, ride := range rides {
		if len(chairs) == 0 {
			break
		}
		best, bestTime := 0, math.Inf(1)
		for i, chair := range chairs {
			pickupTime := math.Inf(1)
			if chair.Latitude.Valid && chair.Longitude.Valid && chair.Speed.Valid && chair.Speed.Int64 > 0 {
				distance := math.Abs(float64(ride.PickupLatitude)-float64(chair.Latitude.Int64)) +
					math.Abs(float64(ride.PickupLongitude)-float64(chair.Longitude.Int64))
				pickupTime = distance / float64(chair.Speed.Int64)
			}
			// Unknown positions or models are fallbacks, never zero-distance chairs.
			if pickupTime < bestTime || (pickupTime == bestTime && chair.ID < chairs[best].ID) {
				best, bestTime = i, pickupTime
			}
		}
		chairIDs = append(chairIDs, chairs[best].ID)
		chairs[best] = chairs[len(chairs)-1]
		chairs = chairs[:len(chairs)-1]
	}
	return chairIDs
}

// A chair remains occupied until its previous ride has completed and every
// chair notification has been consumed.
const availableChairsQuery = `
SELECT c.id, d.last_latitude, d.last_longitude, m.speed
FROM chairs c
LEFT JOIN chair_distance_totals d ON d.chair_id = c.id
LEFT JOIN chair_models m ON m.name = c.model
LEFT JOIN rides previous ON previous.id = (
	SELECT id FROM rides WHERE chair_id = c.id
	ORDER BY updated_at DESC, id DESC LIMIT 1
)
WHERE c.is_active = TRUE
AND (
	previous.id IS NULL OR (
		EXISTS (
			SELECT 1 FROM ride_statuses
			WHERE ride_id = previous.id AND status = 'COMPLETED'
		)
		AND NOT EXISTS (
			SELECT 1 FROM ride_statuses
			WHERE ride_id = previous.id AND chair_sent_at IS NULL
		)
	)
)`

func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	if err := matchWaitingRides(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func matchWaitingRides(ctx context.Context) error {
	matchingMu.Lock()
	defer matchingMu.Unlock()

	// READ COMMITTED allows availability to be refreshed after waiting for a
	// chair lock and avoids range locks blocking newly requested rides.
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var rides []Ride
	if err := tx.SelectContext(ctx, &rides, `
		SELECT id, user_id, pickup_latitude, pickup_longitude FROM rides WHERE chair_id IS NULL
		ORDER BY created_at, id LIMIT ? FOR UPDATE`, matchingBatchSize); err != nil {
		return err
	}
	if len(rides) == 0 {
		return nil
	}

	var chairs []matchingChair
	if err := tx.SelectContext(ctx, &chairs, availableChairsQuery); err != nil {
		return err
	}
	if len(chairs) == 0 {
		return nil
	}

	chairIDs := selectMatchingChairs(rides, chairs)

	// Lock only selected chairs, in a consistent order. Activity updates use
	// the same rows, and another matcher must recheck occupancy after locking.
	lockQuery, lockArgs, err := sqlx.In(`SELECT id FROM chairs WHERE id IN (?) ORDER BY id FOR UPDATE`, chairIDs)
	if err != nil {
		return err
	}
	var lockedChairIDs []string
	if err := tx.SelectContext(ctx, &lockedChairIDs, lockQuery, lockArgs...); err != nil {
		return err
	}

	checkQuery, checkArgs, err := sqlx.In(availableChairsQuery+` AND c.id IN (?)`, chairIDs)
	if err != nil {
		return err
	}
	if err := tx.SelectContext(ctx, &chairs, checkQuery, checkArgs...); err != nil {
		return err
	}
	chairIDs = selectMatchingChairs(rides, chairs)
	if len(chairIDs) == 0 {
		return nil
	}

	// One statement persists the batch without one SQL round trip per ride.
	var query strings.Builder
	query.WriteString("UPDATE rides SET chair_id = CASE id")
	args := make([]any, 0, len(chairIDs)*3)
	for i, chairID := range chairIDs {
		query.WriteString(" WHEN ? THEN ?")
		args = append(args, rides[i].ID, chairID)
	}
	query.WriteString(" END WHERE chair_id IS NULL AND id IN (")
	query.WriteString(strings.TrimSuffix(strings.Repeat("?,", len(chairIDs)), ","))
	query.WriteString(")")
	for _, ride := range rides[:len(chairIDs)] {
		args = append(args, ride.ID)
	}
	result, err := tx.ExecContext(ctx, query.String(), args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != int64(len(chairIDs)) {
		return fmt.Errorf("matching updated %d rides, expected %d", count, len(chairIDs))
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for i, chairID := range chairIDs {
		notifications.publish("app", rides[i].UserID)
		notifications.publish("chair", chairID)
	}
	return nil
}
