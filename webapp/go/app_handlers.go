package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	registered, err := runtimeState.registerUser(*req)
	if err != nil {
		writeStateError(w, err)
		return
	}
	userID, accessToken, invitationCode := registered.ID, registered.AccessToken, registered.InvitationCode

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user := ctx.Value("user").(*User)

	if err := runtimeState.registerPaymentToken(user.ID, req.Token); err != nil {
		writeStateError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	s := runtimeState
	s.mu.RLock()
	items := []getAppRidesResponseItem{}
	ids := s.userRides[user.ID]
	for i := len(ids) - 1; i >= 0; i-- {
		ride := s.rides[ids[i]]
		if s.latest(ride) != "COMPLETED" {
			continue
		}
		chair := s.chairs[ride.ChairID.String]
		items = append(items, getAppRidesResponseItem{
			ID: ride.ID, PickupCoordinate: Coordinate{ride.PickupLatitude, ride.PickupLongitude},
			DestinationCoordinate: Coordinate{ride.DestinationLatitude, ride.DestinationLongitude},
			Fare:                  ride.Fare, Evaluation: *ride.Evaluation, RequestedAt: ride.CreatedAt.UnixMilli(), CompletedAt: ride.UpdatedAt.UnixMilli(),
			Chair: getAppRidesResponseItemChair{ID: chair.ID, Name: chair.Name, Model: chair.Model, Owner: s.owners[chair.OwnerID].Name},
		})
	}
	s.mu.RUnlock()

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)
	ride, fare, err := runtimeState.createRide(user.ID, *req.PickupCoordinate, *req.DestinationCoordinate)
	if err != nil {
		writeStateError(w, err)
		return
	}
	rideID := ride.ID

	notifications.publish("app", user.ID)

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   fare,
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	discounted := runtimeState.estimatedFare(user.ID, *req.PickupCoordinate, *req.DestinationCoordinate)

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude) - discounted,
	})
}

// マンハッタン距離を求める
func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	measurement := evaluationMetrics{metrics: runtimeState.metrics}
	defer measurement.finish(started)
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	user := ctx.Value("user").(*User)
	lock := runtimeState.userMutex(user.ID)
	stageStarted := time.Now()
	lock.Lock()
	measurement.elapsed[evalLock] = time.Since(stageStarted)
	measurement.visited[evalLock], measurement.success[evalLock] = true, true
	defer lock.Unlock()
	ride, fare, paymentKey, err := runtimeState.completionCandidate(rideID, user.ID)
	if err != nil {
		writeStateError(w, err)
		return
	}
	measurement.visited[evalSQL] = true
	stageStarted = time.Now()
	// Keep the existing stage series: eval_sql now measures the replacement
	// metadata lookup. No SQL executes during this stage.
	paymentToken, paymentGatewayURL, err := runtimeState.paymentConfiguration(ride.UserID)
	measurement.elapsed[evalSQL] = time.Since(stageStarted)
	if err != nil {
		writeStateError(w, err)
		return
	}
	measurement.success[evalSQL] = true

	measurement.visited[evalPayment] = true
	stageStarted = time.Now()
	retries, paymentErr := requestPaymentGatewayPostPayment(ctx, paymentGatewayURL, paymentToken.Token, paymentKey, &paymentGatewayPostPaymentRequest{Amount: fare})
	measurement.elapsed[evalPayment] = time.Since(stageStarted)
	measurement.retries = retries
	measurement.success[evalPayment] = paymentErr == nil
	if err := paymentErr; err != nil {
		if errors.Is(err, erroredUpstream) {
			writeError(w, http.StatusBadGateway, err)
		} else {
			writeError(w, http.StatusInternalServerError, err)
		}
		return
	}
	measurement.visited[evalComplete] = true
	stageStarted = time.Now()
	at, err := runtimeState.complete(rideID, req.Evaluation)
	measurement.elapsed[evalComplete] = time.Since(stageStarted)
	measurement.success[evalComplete] = err == nil
	if err != nil {
		writeStateError(w, err)
		return
	}
	measurement.success[evalTotal] = true
	defer runtimeState.finishCompletionResponse(rideID)
	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{CompletedAt: at.UnixMilli()})
	if err := http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		slog.Error("flush evaluation response", "error", err)
	}
}

type appGetNotificationResponseData struct {
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

func appGetNotification(w http.ResponseWriter, r *http.Request) {
	serveNotifications(w, r, "app", r.Context().Value("user").(*User).ID)
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	s := runtimeState
	s.mu.RLock()
	snapshotAt := time.Now()
	nearbyChairs := []appGetNearbyChairsResponseChair{}
	if len(s.nearbyMissingStatus) != 0 {
		s.mu.RUnlock()
		writeError(w, http.StatusInternalServerError, sql.ErrNoRows)
		return
	}
	for id, cell := range s.nearbyChairs {
		chair := s.chairs[id]
		available, err := s.nearbyAvailable(id, snapshotAt)
		if err != nil {
			s.mu.RUnlock()
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if !available {
			continue
		}
		if cell == nil {
			continue
		}
		snapshot := cell.snapshot.Load()
		if snapshot == nil {
			continue
		}
		d := snapshot.distance
		if !d.Latitude.Valid || !d.Longitude.Valid {
			continue
		}
		if calculateDistance(lat, lon, int(d.Latitude.Int64), int(d.Longitude.Int64)) <= distance {
			nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{ID: chair.ID, Name: chair.Name, Model: chair.Model, CurrentCoordinate: Coordinate{int(d.Latitude.Int64), int(d.Longitude.Int64)}})
		}
	}
	retrievedAt := snapshotAt.UnixMilli()
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{Chairs: nearbyChairs, RetrievedAt: retrievedAt})
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}
