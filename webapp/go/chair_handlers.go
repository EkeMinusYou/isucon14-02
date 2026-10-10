package main

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	owner := &Owner{}
	if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE chair_register_token = ?", req.ChairRegisterToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO chair_distance_totals (chair_id) VALUES (?)", chairID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	var registered Chair
	if err := tx.GetContext(ctx, &registered, "SELECT * FROM chairs WHERE id = ?", chairID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	runtimeState.mu.Lock()
	runtimeState.addChair(registered)
	runtimeState.distances[chairID] = distanceTotal{ChairID: chairID}
	runtimeState.mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := runtimeState.activity(chair.ID, req.IsActive); err != nil {
		writeStateError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*Chair)

	if req.Latitude < -2147483648 || req.Latitude > 2147483647 || req.Longitude < -2147483648 || req.Longitude > 2147483647 {
		writeError(w, http.StatusBadRequest, errors.New("coordinate is outside integer range"))
		return
	}
	at, err := runtimeState.coordinate(chair.ID, *req)
	if err != nil {
		writeStateError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: at.UnixMilli(),
	})
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	serveNotifications(w, r, "chair", r.Context().Value("chair").(*Chair).ID)
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*Chair)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if err := runtimeState.chairStatus(chair.ID, rideID, req.Status); err != nil {
		writeStateError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
