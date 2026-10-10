package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

func bulkStateRows(ctx context.Context, tx *sqlx.Tx, table, columns string, rows [][]any, suffix string) error {
	if len(rows) == 0 {
		return nil
	}
	values := "(" + strings.TrimSuffix(strings.Repeat("?,", len(rows[0])), ",") + ")"
	args := make([]any, 0, len(rows)*len(rows[0]))
	for _, row := range rows {
		args = append(args, row...)
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+columns+") VALUES "+strings.TrimSuffix(strings.Repeat(values+",", len(rows)), ",")+suffix, args...)
	return err
}

// Projection markers and business rows commit together. Retrying a failed or
// ambiguously committed batch only writes the same after-images and history IDs.
func (s *rideState) flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.RLock()
	count := min(len(s.pending), 1024)
	batch := append([]stateRecord(nil), s.pending[:count]...)
	generation := s.generation
	s.mu.RUnlock()
	if count == 0 {
		return nil
	}
	var locations, statusRows [][]any
	distances := map[string]distanceTotal{}
	rides := map[string]Ride{}
	chairs := map[string]Chair{}
	stats := map[string]chairStatsTotal{}
	acks := map[string]map[string]notificationAck{"app": {}, "chair": {}}
	for _, record := range batch {
		if l := record.Location; l != nil {
			locations = append(locations, []any{l.ID, l.ChairID, l.Latitude, l.Longitude, l.CreatedAt})
		}
		if d := record.Distance; d != nil {
			distances[d.ChairID] = *d
		}
		for _, r := range record.Rides {
			rides[r.ID] = r
		}
		for _, status := range record.Statuses {
			statusRows = append(statusRows, []any{status.ID, status.RideID, status.Status, status.CreatedAt, status.AppSentAt, status.ChairSentAt})
		}
		if c := record.Chair; c != nil {
			chairs[c.ID] = *c
		}
		if total := record.Stats; total != nil {
			stats[total.ChairID] = *total
		}
		if ack := record.Ack; ack != nil {
			acks[ack.Kind][ack.StatusID] = *ack
		}
	}
	tx, err := s.database.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := bulkStateRows(ctx, tx, "chair_locations", "id,chair_id,latitude,longitude,created_at", locations, " ON DUPLICATE KEY UPDATE id=VALUES(id)"); err != nil {
		return err
	}
	if err := bulkStateRows(ctx, tx, "ride_statuses", "id,ride_id,status,created_at,app_sent_at,chair_sent_at", statusRows, " ON DUPLICATE KEY UPDATE id=VALUES(id)"); err != nil {
		return err
	}
	var rows [][]any
	for _, d := range distances {
		rows = append(rows, []any{d.ChairID, d.TotalDistance, d.Latitude, d.Longitude, d.UpdatedAt})
	}
	if err := bulkStateRows(ctx, tx, "chair_distance_totals", "chair_id,total_distance,last_latitude,last_longitude,updated_at", rows, " ON DUPLICATE KEY UPDATE total_distance=VALUES(total_distance),last_latitude=VALUES(last_latitude),last_longitude=VALUES(last_longitude),updated_at=VALUES(updated_at)"); err != nil {
		return err
	}
	rows = nil
	for _, r := range rides {
		rows = append(rows, []any{r.ID, r.UserID, r.ChairID, r.PickupLatitude, r.PickupLongitude, r.DestinationLatitude, r.DestinationLongitude, r.Evaluation, r.CreatedAt, r.UpdatedAt})
	}
	if err := bulkStateRows(ctx, tx, "rides", "id,user_id,chair_id,pickup_latitude,pickup_longitude,destination_latitude,destination_longitude,evaluation,created_at,updated_at", rows, " ON DUPLICATE KEY UPDATE chair_id=VALUES(chair_id),evaluation=VALUES(evaluation),updated_at=VALUES(updated_at)"); err != nil {
		return err
	}
	rows = nil
	for _, c := range chairs {
		rows = append(rows, []any{c.ID, c.OwnerID, c.Name, c.Model, c.IsActive, c.AccessToken, c.CreatedAt, c.UpdatedAt})
	}
	if err := bulkStateRows(ctx, tx, "chairs", "id,owner_id,name,model,is_active,access_token,created_at,updated_at", rows, " ON DUPLICATE KEY UPDATE is_active=VALUES(is_active),updated_at=VALUES(updated_at)"); err != nil {
		return err
	}
	rows = nil
	for _, total := range stats {
		rows = append(rows, []any{total.ChairID, total.Count, total.Evaluation})
	}
	if err := bulkStateRows(ctx, tx, "chair_stats", "chair_id,total_rides_count,total_evaluation", rows, " ON DUPLICATE KEY UPDATE total_rides_count=VALUES(total_rides_count),total_evaluation=VALUES(total_evaluation)"); err != nil {
		return err
	}
	for kind, statuses := range acks {
		if len(statuses) == 0 {
			continue
		}
		column := "app_sent_at"
		if kind == "chair" {
			column = "chair_sent_at"
		}
		var query strings.Builder
		query.WriteString("UPDATE ride_statuses SET " + column + " = COALESCE(" + column + ", CASE id")
		var args, ids []any
		for id, ack := range statuses {
			query.WriteString(" WHEN ? THEN ?")
			args = append(args, id, ack.At)
			ids = append(ids, id)
		}
		query.WriteString(" END) WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")")
		if _, err := tx.ExecContext(ctx, query.String(), append(args, ids...)...); err != nil {
			return err
		}
	}
	end := batch[len(batch)-1].Sequence
	result, err := tx.ExecContext(ctx, `UPDATE state_projection SET applied_seq = ? WHERE id = 1 AND generation = ?`, end, generation)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		// A retry after an ambiguous commit may already have this sequence.
		var applied uint64
		if err := tx.GetContext(ctx, &applied, `SELECT applied_seq FROM state_projection WHERE id = 1 AND generation = ?`, generation); err != nil || applied != end {
			return fmt.Errorf("state projection generation changed: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.mu.Lock()
	clear(s.pending[:count])
	s.pending = s.pending[count:]
	s.applied = end
	s.mu.Unlock()
	return nil
}

func (s *rideState) start() {
	s.stop, s.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(s.done)
		projection := time.NewTicker(100 * time.Millisecond)
		syncTicker := time.NewTicker(500 * time.Millisecond)
		defer projection.Stop()
		defer syncTicker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-projection.C:
				stateLifecycle.RLock()
				if s.checkReady() == nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err := s.flush(ctx); err != nil {
						slog.Error("project runtime state", "error", err)
					}
					cancel()
				}
				stateLifecycle.RUnlock()
			case <-syncTicker.C:
				stateLifecycle.RLock()
				// Sync does not hold the mutation mutex or wait in request handlers.
				if err := s.wal.Sync(); err != nil {
					s.mu.Lock()
					s.failed = err
					s.mu.Unlock()
					slog.Error("sync runtime WAL", "error", err)
				}
				stateLifecycle.RUnlock()
			}
		}
	}()
}

func (s *rideState) close() error {
	if s.stop != nil {
		close(s.stop)
		<-s.done
		s.stop = nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = false
	if err := s.wal.Sync(); err != nil {
		s.wal.Close()
		return err
	}
	return s.wal.Close()
}
