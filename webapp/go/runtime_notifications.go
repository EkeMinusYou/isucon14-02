package main

import (
	"context"
	"errors"
	"time"
)

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
	ids, rides := s.appEvents[id], s.userRides[id]
	if kind == "chair" {
		ids, rides = s.chairEvents[id], s.chairRides[id]
	}
	cursor := -1
	if lastEventID != "" {
		for i, statusID := range ids {
			if statusID == lastEventID {
				cursor = i
				break
			}
		}
	}
	var events []notificationEvent
	for i, statusID := range ids {
		status := s.statuses[statusID]
		pending := status.AppSentAt == nil
		if kind == "chair" {
			pending = status.ChairSentAt == nil
		}
		if pending || (cursor >= 0 && i > cursor) {
			events = append(events, notificationEvent{statusID: status.ID, eventID: status.ID, generation: s.generation, data: s.notificationData(kind, s.rides[status.RideID], status.Status)})
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
