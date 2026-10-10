package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

const matchingBatchSize = 64

var matchingMu sync.Mutex

// A chair remains occupied until its previous ride has completed and every
// chair notification has been consumed.
const availableChairsQuery = `
SELECT c.id, COALESCE(position.last_latitude, 0) AS latitude,
       COALESCE(position.last_longitude, 0) AS longitude,
       CASE WHEN position.last_latitude IS NOT NULL AND position.last_longitude IS NOT NULL
                 AND model.speed > 0 THEN model.speed ELSE 0 END AS speed
FROM chairs c
LEFT JOIN chair_distance_totals position ON position.chair_id = c.id
LEFT JOIN chair_models model ON model.name = c.model
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
		SELECT id, user_id, pickup_latitude, pickup_longitude,
		       destination_latitude, destination_longitude
		FROM rides WHERE chair_id IS NULL
		ORDER BY created_at, id LIMIT ? FOR UPDATE`, matchingBatchSize); err != nil {
		return err
	}
	if len(rides) == 0 {
		return nil
	}

	var chairs []matchingChair
	if err := tx.SelectContext(ctx, &chairs, availableChairsQuery+` ORDER BY c.id`); err != nil {
		return err
	}
	if len(chairs) == 0 {
		return nil
	}

	assignments := assignMatchingChairs(rides, chairs)
	chairIDs := make([]string, len(assignments))
	for i, assignment := range assignments {
		chairIDs[i] = chairs[assignment.chairIndex].ID
	}

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
	if err := tx.SelectContext(ctx, &chairs, checkQuery+` ORDER BY c.id`, checkArgs...); err != nil {
		return err
	}
	if len(chairs) == 0 {
		return nil
	}
	// Recompute using fresh positions and only chairs still available under
	// lock. If some became unavailable, keep the oldest rides in the batch.
	assignments = assignMatchingChairs(rides, chairs)

	// One statement persists the batch without one SQL round trip per ride.
	var query strings.Builder
	query.WriteString("UPDATE rides SET chair_id = CASE id")
	args := make([]any, 0, len(assignments)*3)
	for _, assignment := range assignments {
		query.WriteString(" WHEN ? THEN ?")
		args = append(args, rides[assignment.rideIndex].ID, chairs[assignment.chairIndex].ID)
	}
	query.WriteString(" END WHERE chair_id IS NULL AND id IN (")
	query.WriteString(strings.TrimSuffix(strings.Repeat("?,", len(assignments)), ","))
	query.WriteString(")")
	for _, assignment := range assignments {
		args = append(args, rides[assignment.rideIndex].ID)
	}
	result, err := tx.ExecContext(ctx, query.String(), args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != int64(len(assignments)) {
		return fmt.Errorf("matching updated %d rides, expected %d", count, len(assignments))
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, assignment := range assignments {
		notifications.publish("app", rides[assignment.rideIndex].UserID)
		notifications.publish("chair", chairs[assignment.chairIndex].ID)
	}
	return nil
}
