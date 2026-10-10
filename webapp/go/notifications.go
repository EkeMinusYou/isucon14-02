package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

type notificationSubscription struct {
	wake chan struct{}
	done chan struct{}
}

// Wakeups may coalesce; the durable status history is the event queue.
type notificationHub struct {
	mu          sync.Mutex
	subscribers map[string]map[*notificationSubscription]struct{}
}

var notifications = &notificationHub{}

func (h *notificationHub) subscribe(kind, id string) (*notificationSubscription, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := kind + ":" + id
	if h.subscribers == nil {
		h.subscribers = make(map[string]map[*notificationSubscription]struct{})
	}
	if h.subscribers[key] == nil {
		h.subscribers[key] = make(map[*notificationSubscription]struct{})
	}
	sub := &notificationSubscription{wake: make(chan struct{}, 1), done: make(chan struct{})}
	h.subscribers[key][sub] = struct{}{}
	return sub, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subscribers[key], sub)
		if len(h.subscribers[key]) == 0 {
			delete(h.subscribers, key)
		}
	}
}

func (h *notificationHub) publish(kind, id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subscribers[kind+":"+id] {
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

func (h *notificationHub) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, subs := range h.subscribers {
		for sub := range subs {
			close(sub.done)
		}
	}
	h.subscribers = nil
}

func notifyRide(ride *Ride) {
	notifications.publish("app", ride.UserID)
	if ride.ChairID.Valid {
		notifications.publish("chair", ride.ChairID.String)
	}
}

type notificationEvent struct {
	statusID string
	eventID  string
	data     any
}

func serveNotifications(w http.ResponseWriter, r *http.Request, kind, id string) {
	ctx := r.Context()
	// Subscribe before reading so a commit during the initial read cannot be lost.
	sub, unsubscribe := notifications.subscribe(kind, id)
	defer unsubscribe()
	events, err := loadNotifications(ctx, kind, id, r.Header.Get("Last-Event-ID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	var last []byte
	for {
		for _, event := range events {
			select {
			case <-ctx.Done():
				return
			case <-sub.done:
				return
			default:
			}
			payload, err := json.Marshal(event.data)
			if err != nil {
				slog.Error("encode notification", "error", err)
				return
			}
			// Pending transitions must be delivered even when their payload repeats.
			if event.statusID == "" && bytes.Equal(last, payload) {
				continue
			}
			message := "data: " + string(payload) + "\n"
			if event.eventID != "" {
				message += "id: " + event.eventID + "\n"
			}
			if err := writeSSE(w, controller, message+"\n"); err != nil {
				return
			}
			last = payload
			// Mark sent only after flushing; a failed write stays pending on reconnect.
			if event.statusID != "" {
				if err := acknowledgeNotification(ctx, kind, event.statusID); err != nil {
					slog.Error("acknowledge notification", "error", err)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-sub.done:
			return
		case <-heartbeat.C:
			if err := writeSSE(w, controller, ": keepalive\n\n"); err != nil {
				return
			}
			continue
		case <-sub.wake:
		}
		events, err = loadNotifications(ctx, kind, id, "")
		if err != nil {
			slog.Error("load notification", "error", err)
			return
		}
	}
}

func writeSSE(w http.ResponseWriter, controller *http.ResponseController, message string) error {
	if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := fmt.Fprint(w, message); err != nil {
		return err
	}
	if err := controller.Flush(); err != nil {
		return err
	}
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func acknowledgeNotification(ctx context.Context, kind, statusID string) error {
	column := "app_sent_at"
	if kind == "chair" {
		column = "chair_sent_at"
	}
	_, err := db.ExecContext(ctx, "UPDATE ride_statuses SET "+column+" = COALESCE("+column+", CURRENT_TIMESTAMP(6)) WHERE id = ?", statusID)
	return err
}

func loadNotifications(ctx context.Context, kind, id, lastEventID string) ([]notificationEvent, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ownerColumn, sentColumn := "user_id", "app_sent_at"
	if kind == "chair" {
		ownerColumn, sentColumn = "chair_id", "chair_sent_at"
	}
	statuses := []RideStatus{}
	condition := "s." + sentColumn + " IS NULL"
	args := []any{id}
	if lastEventID != "" {
		var cursor struct {
			RideCreatedAt time.Time `db:"ride_created_at"`
			RideID        string    `db:"ride_id"`
			CreatedAt     time.Time `db:"created_at"`
			StatusOrder   int       `db:"status_order"`
			ID            string    `db:"id"`
		}
		err := tx.GetContext(ctx, &cursor, `SELECT r.created_at AS ride_created_at, r.id AS ride_id,
			s.created_at, s.status + 0 AS status_order, s.id FROM ride_statuses s JOIN rides r ON r.id = s.ride_id
			WHERE s.id = ? AND r.`+ownerColumn+` = ?`, lastEventID, id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if err == nil {
			// Replay anything beyond the client's cursor, even if a previous flush succeeded.
			condition = "(" + condition + ` OR (r.created_at, r.id, s.created_at, s.status + 0, s.id) > (?, ?, ?, ?, ?))`
			args = append(args, cursor.RideCreatedAt, cursor.RideID, cursor.CreatedAt, cursor.StatusOrder, cursor.ID)
		}
	}
	// Include older rides, and break timestamp ties by lifecycle order then ID.
	err = tx.SelectContext(ctx, &statuses, `SELECT s.* FROM rides r JOIN ride_statuses s ON s.ride_id = r.id
		WHERE r.`+ownerColumn+` = ? AND `+condition+`
		ORDER BY r.created_at, r.id, s.created_at, s.status, s.id`, args...)
	if err != nil {
		return nil, err
	}
	var events []notificationEvent
	if len(statuses) == 0 {
		ride := &Ride{}
		err = tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE `+ownerColumn+` = ? ORDER BY created_at DESC, id DESC LIMIT 1`, id)
		if errors.Is(err, sql.ErrNoRows) {
			events = append(events, notificationEvent{data: nil})
		} else if err != nil {
			return nil, err
		} else {
			var status RideStatus
			if err := tx.GetContext(ctx, &status, `SELECT * FROM ride_statuses WHERE ride_id = ?
				ORDER BY created_at DESC, status DESC, id DESC LIMIT 1`, ride.ID); err != nil {
				return nil, err
			}
			data, err := notificationData(ctx, tx, kind, ride)
			if err != nil {
				return nil, err
			}
			events = append(events, notificationEvent{eventID: status.ID, data: withNotificationStatus(data, status.Status)})
		}
	} else {
		var rideID string
		var data any
		for _, status := range statuses {
			if rideID != status.RideID {
				ride := &Ride{}
				if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, status.RideID); err != nil {
					return nil, err
				}
				data, err = notificationData(ctx, tx, kind, ride)
				if err != nil {
					return nil, err
				}
				rideID = status.RideID
			}
			events = append(events, notificationEvent{statusID: status.ID, eventID: status.ID, data: withNotificationStatus(data, status.Status)})
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return events, nil
}

func withNotificationStatus(data any, status string) any {
	switch data := data.(type) {
	case appGetNotificationResponseData:
		data.Status = status
		return data
	case chairGetNotificationResponseData:
		data.Status = status
		return data
	default:
		panic("invalid notification data")
	}
}

func notificationData(ctx context.Context, tx *sqlx.Tx, kind string, ride *Ride) (any, error) {
	pickup := Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude}
	destination := Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude}
	if kind == "chair" {
		user := &User{}
		if err := tx.GetContext(ctx, user, `SELECT * FROM users WHERE id = ?`, ride.UserID); err != nil {
			return nil, err
		}
		return chairGetNotificationResponseData{
			RideID: ride.ID, User: simpleUser{ID: user.ID, Name: user.Firstname + " " + user.Lastname},
			PickupCoordinate: pickup, DestinationCoordinate: destination,
		}, nil
	}
	fare, err := calculateDiscountedFare(ctx, tx, ride.UserID, ride, pickup.Latitude, pickup.Longitude, destination.Latitude, destination.Longitude)
	if err != nil {
		return nil, err
	}
	data := appGetNotificationResponseData{
		RideID: ride.ID, PickupCoordinate: pickup, DestinationCoordinate: destination,
		Fare: fare, CreatedAt: ride.CreatedAt.UnixMilli(), UpdateAt: ride.UpdatedAt.UnixMilli(),
	}
	if ride.ChairID.Valid {
		chair := &Chair{}
		if err := tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID.String); err != nil {
			return nil, err
		}
		stats, err := getChairStats(ctx, tx, chair.ID)
		if err != nil {
			return nil, err
		}
		data.Chair = &appGetNotificationResponseChair{ID: chair.ID, Name: chair.Name, Model: chair.Model, Stats: stats}
	}
	return data, nil
}
