package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
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
	measureAssignment bool
	statusID          string
	generation        string
	eventID           string
	data              any
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
			var flushed func()
			if event.measureAssignment {
				flushed = func() { observeAssignmentFlush(kind, event.statusID, event.generation, time.Now()) }
			}
			if err := writeSSEWithFlush(w, controller, message+"\n", flushed); err != nil {
				return
			}
			last = payload
			// Mark sent only after flushing; a failed write stays pending on reconnect.
			if event.statusID != "" {
				if err := acknowledgeNotificationGeneration(ctx, kind, event.statusID, event.generation); err != nil {
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
	return writeSSEWithFlush(w, controller, message, nil)
}

func writeSSEWithFlush(w http.ResponseWriter, controller *http.ResponseController, message string, flushed func()) error {
	if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := fmt.Fprint(w, message); err != nil {
		return err
	}
	if err := controller.Flush(); err != nil {
		return err
	}
	if flushed != nil {
		flushed()
	}
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}
