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
		return s.chairEvents[id]
	}
	return s.appEvents[id]
}

func (s *rideState) appendNotification(kind, id string, status RideStatus) {
	queues, sent := s.appEvents, status.AppSentAt
	if kind == "chair" {
		queues, sent = s.chairEvents, status.ChairSentAt
	}
	queue := queues[id]
	if queue == nil {
		queue = &notificationQueue{positions: map[string]int{}, unacked: map[string]*list.Element{}}
		queues[id] = queue
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != "" && generation != s.generation {
		return errors.New("notification belongs to an old generation")
	}
	status, ok := s.statuses[statusID]
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.ready {
		return nil, errors.New("runtime state is unavailable")
	}
	queue, rides := s.notificationQueue(kind, id), s.userRides[id]
	if kind == "chair" {
		rides = s.chairRides[id]
	}
	cursor := -1
	if lastEventID != "" && queue != nil {
		if position, ok := queue.positions[lastEventID]; ok {
			cursor = position
		}
	}
	flushIndex := 0
	if kind == "app" {
		flushIndex = 1
	}
	var events []notificationEvent
	appendEvent := func(statusID string) {
		status := s.statuses[statusID]
		events = append(events, notificationEvent{measureAssignment: status.Status == "MATCHING" && !s.rides[status.RideID].assignedAt.IsZero() && !s.rides[status.RideID].firstFlushed[flushIndex], statusID: status.ID, eventID: status.ID, generation: s.generation, data: s.notificationData(kind, s.rides[status.RideID], status.Status)})
	}
	if queue != nil {
		// Pending events before the cursor remain required. Merge them with
		// the entire replay suffix in recipient order, without duplicates.
		for element := queue.pending.Front(); element != nil; element = element.Next() {
			position := element.Value.(int)
			if cursor >= 0 && position > cursor {
				break
			}
			appendEvent(queue.ids[position])
		}
		if cursor >= 0 {
			for _, statusID := range queue.ids[cursor+1:] {
				appendEvent(statusID)
			}
		}
	}
	if len(events) == 0 {
		if len(rides) == 0 {
			return []notificationEvent{{data: nil, generation: s.generation}}, nil
		}
		ride := s.rides[rides[len(rides)-1]]
		if len(ride.Statuses) == 0 {
			return nil, errors.New("ride status history is missing")
		}
		status := s.statuses[ride.Statuses[len(ride.Statuses)-1]]
		events = append(events, notificationEvent{eventID: status.ID, generation: s.generation, data: s.notificationData(kind, ride, status.Status)})
	}
	return events, nil
}

func (s *rideState) notificationData(kind string, ride *memoryRide, status string) any {
	pickup := Coordinate{ride.PickupLatitude, ride.PickupLongitude}
	destination := Coordinate{ride.DestinationLatitude, ride.DestinationLongitude}
	if kind == "chair" {
		user := s.users[ride.UserID]
		return chairGetNotificationResponseData{RideID: ride.ID, User: simpleUser{ID: user.ID, Name: user.Firstname + " " + user.Lastname}, PickupCoordinate: pickup, DestinationCoordinate: destination, Status: status}
	}
	data := appGetNotificationResponseData{RideID: ride.ID, PickupCoordinate: pickup, DestinationCoordinate: destination, Fare: ride.Fare, Status: status, CreatedAt: ride.CreatedAt.UnixMilli(), UpdateAt: ride.UpdatedAt.UnixMilli()}
	if ride.ChairID.Valid {
		chair := s.chairs[ride.ChairID.String]
		totals := s.stats[chair.ID]
		stats := appGetNotificationResponseChairStats{TotalRidesCount: totals.Count}
		if totals.Count > 0 {
			stats.TotalEvaluationAvg = float64(totals.Evaluation) / float64(totals.Count)
		}
		data.Chair = &appGetNotificationResponseChair{ID: chair.ID, Name: chair.Name, Model: chair.Model, Stats: stats}
	}
	return data
}
