package main

import (
	"container/list"
	"context"
	"errors"
	"time"
)

// Keep the complete ordered history for Last-Event-ID replay. The linked
// pending positions also support ACKs arriving out of order across streams.
type notificationQueue struct {
	ids       []string
	positions map[string]int
	pending   list.List
	unacked   map[string]*list.Element
}

func (s *rideState) notificationQueue(kind, id string) *notificationQueue {
	if kind == "chair" {
		return s.chairEvents.get(id)
	}
	return s.appEvents.get(id)
}

func (s *rideState) appendNotification(kind, id string, status RideStatus) {
	queues, sent := s.appEvents, status.AppSentAt
	if kind == "chair" {
		queues, sent = s.chairEvents, status.ChairSentAt
	}
	queue := queues.get(id)
	if queue == nil {
		queue = &notificationQueue{positions: map[string]int{}, unacked: map[string]*list.Element{}}
		queues.set(id, queue)
	}
	position := len(queue.ids)
	queue.positions[status.ID] = position
	queue.ids = append(queue.ids, status.ID)
	if sent == nil {
		queue.unacked[status.ID] = queue.pending.PushBack(position)
	}
}

func (q *notificationQueue) ack(id string) {
	if element := q.unacked[id]; element != nil {
		q.pending.Remove(element)
		delete(q.unacked, id)
	}
}

func acknowledgeNotification(ctx context.Context, kind, statusID string) error {
	return acknowledgeNotificationGeneration(ctx, kind, statusID, "")
}

func acknowledgeNotificationGeneration(ctx context.Context, kind, statusID, generation string) error {
	stateLifecycle.RLock()
	defer stateLifecycle.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s := runtimeState
	initial := s.statuses.get(statusID)
	_, unlock := s.lockRide(initial.RideID, false)
	defer unlock()
	if generation != "" && generation != s.generation {
		return errors.New("notification belongs to an old generation")
	}
	status, ok := s.statuses.lookup(statusID)
	if !ok {
		return errors.New("notification status not found")
	}
	if (kind == "chair" && status.ChairSentAt != nil) || (kind == "app" && status.AppSentAt != nil) {
		return nil
	}
	return s.commit(stateRecord{Ack: &notificationAck{StatusID: statusID, Kind: kind, At: time.Now().UTC().Truncate(time.Microsecond)}})
}

func loadNotifications(ctx context.Context, kind, id, lastEventID string) ([]notificationEvent, error) {
	stateLifecycle.RLock()
	defer stateLifecycle.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := runtimeState
	lock := s.userStateMutex(id)
	if kind == "chair" {
		lock = s.chairMutex(id)
	}
	lock.RLock()
	defer lock.RUnlock()
	if !s.ready {
		return nil, errors.New("runtime state is unavailable")
	}
	queue, rides := s.notificationQueue(kind, id), s.userRides.get(id)
	if kind == "chair" {
		rides = s.chairRides.get(id)
	}
	cursor := -1
	if lastEventID != "" && queue != nil {
		if position, ok := queue.positions[lastEventID]; ok {
			cursor = position
		}
	}
	var selected []string
	if queue != nil {
		// Merge pending events before the cursor with the complete replay suffix.
		for element := queue.pending.Front(); element != nil; element = element.Next() {
			position := element.Value.(int)
			if cursor >= 0 && position > cursor {
				break
			}
			selected = append(selected, queue.ids[position])
		}
		if cursor >= 0 {
			selected = append(selected, queue.ids[cursor+1:]...)
		}
	}
	fallback := len(selected) == 0
	if fallback {
		if len(rides) == 0 {
			return []notificationEvent{{data: nil, generation: s.generation}}, nil
		}
		ride := s.rides.get(rides[len(rides)-1])
		if len(ride.Statuses) == 0 {
			return nil, errors.New("ride status history is missing")
		}
		selected = append(selected, ride.Statuses[len(ride.Statuses)-1])
	}
	if kind != "chair" {
		// Only chairs appearing in this response need fencing for their statistics.
		// The common one-event path never scans the user's historical rides.
		chairs := make([]string, 0, len(selected))
		for _, statusID := range selected {
			ride := s.rides.get(s.statuses.get(statusID).RideID)
			if ride.ChairID.Valid {
				chairs = append(chairs, ride.ChairID.String)
			}
		}
		unlock := lockStateKeys(&s.chairLocks, chairs, true)
		defer unlock()
	}
	flushIndex := 0
	if kind == "app" {
		flushIndex = 1
	}
	events := make([]notificationEvent, 0, len(selected))
	for i, statusID := range selected {
		status := s.statuses.get(statusID)
		ride := s.rides.get(status.RideID)
		event := notificationEvent{eventID: status.ID, generation: s.generation, data: s.notificationData(kind, ride, status.Status)}
		if !fallback {
			event.statusID = status.ID
			event.measureAssignment = status.Status == "MATCHING" && !ride.assignedAt.IsZero() && !ride.firstFlushed[flushIndex]
			// Only a pending COMPLETED of the user's latest ride with nothing after it ends
			// the stream. A replayed state on connect never does, avoiding reconnect loops.
			event.finishesStream = kind == "app" && status.Status == "COMPLETED" && i == len(selected)-1 &&
				len(rides) > 0 && rides[len(rides)-1] == ride.ID
		}
		events = append(events, event)
	}
	return events, nil
}

func (s *rideState) notificationData(kind string, ride *memoryRide, status string) any {
	pickup := Coordinate{ride.PickupLatitude, ride.PickupLongitude}
	destination := Coordinate{ride.DestinationLatitude, ride.DestinationLongitude}
	if kind == "chair" {
		user := s.users.get(ride.UserID)
		return chairGetNotificationResponseData{RideID: ride.ID, User: simpleUser{ID: user.ID, Name: user.Firstname + " " + user.Lastname}, PickupCoordinate: pickup, DestinationCoordinate: destination, Status: status}
	}
	data := appGetNotificationResponseData{RideID: ride.ID, PickupCoordinate: pickup, DestinationCoordinate: destination, Fare: ride.Fare, Status: status, CreatedAt: ride.CreatedAt.UnixMilli(), UpdateAt: ride.UpdatedAt.UnixMilli()}
	if ride.ChairID.Valid {
		chair := s.chairs.get(ride.ChairID.String)
		totals := s.stats.get(chair.ID)
		stats := appGetNotificationResponseChairStats{TotalRidesCount: totals.Count}
		if totals.Count > 0 {
			stats.TotalEvaluationAvg = float64(totals.Evaluation) / float64(totals.Count)
		}
		data.Chair = &appGetNotificationResponseChair{ID: chair.ID, Name: chair.Name, Model: chair.Model, Stats: stats}
	}
	return data
}
