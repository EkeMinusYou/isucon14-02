package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	measurement "github.com/EkeMinusYou/isucon-template/tools/measurement/app"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

func main() {
	mux := setup()
	server := &http.Server{Addr: ":8080", Handler: mux}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(signals)
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServe() }()
	slog.Info("Listening on :8080")
	select {
	case <-signals:
		notifications.reset()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("shutdown HTTP server", "error", err)
		}
		cancel()
	case err := <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serve HTTP", "error", err)
		}
	}
	if err := runtimeState.close(); err != nil {
		slog.Error("close runtime state", "error", err)
	}
}

func setup() http.Handler {
	solver, err := newMatchingRPCClient(os.Getenv("ISURIDE_SOLVER_URL"))
	if err != nil {
		panic(err)
	}
	host := os.Getenv("ISUCON_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("ISUCON_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err = strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("failed to convert DB port number from ISUCON_DB_PORT environment variable into int: %v", err))
	}
	user := os.Getenv("ISUCON_DB_USER")
	if user == "" {
		user = "isucon"
	}
	password := os.Getenv("ISUCON_DB_PASSWORD")
	if password == "" {
		password = "isucon"
	}
	dbname := os.Getenv("ISUCON_DB_NAME")
	if dbname == "" {
		dbname = "isuride"
	}

	dbConfig := mysql.NewConfig()
	dbConfig.User = user
	dbConfig.Passwd = password
	dbConfig.Addr = net.JoinHostPort(host, port)
	dbConfig.Net = "tcp"
	dbConfig.DBName = dbname
	dbConfig.ParseTime = true
	dbConfig.InterpolateParams = true

	_db, err := sqlx.Connect("mysql", dbConfig.FormatDSN())
	if err != nil {
		panic(err)
	}
	db = _db
	db.SetMaxIdleConns(32)
	directory := os.Getenv("ISURIDE_STATE_DIR")
	if directory == "" {
		directory = "../../state"
	}
	runtimeState, err = newRideState(context.Background(), db, directory)
	if err != nil {
		panic(err)
	}
	runtimeState.matchingSolver = solver
	runtimeState.start()

	measurementServer, err := measurement.Start(measurement.Options{
		MaxProfileSeconds: 180,
		Snapshot:          rideMeasurementSnapshot,
		Pools:             []measurement.Pool{{DB: db, DatabaseHost: host, Name: "main", Role: "primary"}},
	})
	if err != nil {
		panic(err)
	}
	_ = measurementServer // The debug listener lives for the application lifetime.

	mux := chi.NewRouter()
	mux.Use(runtimeStateMiddleware)
	mux.Use(measurementSessionMiddleware)
	mux.Use(middleware.Recoverer)
	mux.HandleFunc("POST /api/initialize", postInitialize)

	// app handlers
	{
		mux.HandleFunc("POST /api/app/users", appPostUsers)

		authedMux := mux.With(appAuthMiddleware)
		authedMux.HandleFunc("POST /api/app/payment-methods", appPostPaymentMethods)
		authedMux.HandleFunc("GET /api/app/rides", appGetRides)
		authedMux.HandleFunc("POST /api/app/rides", appPostRides)
		authedMux.HandleFunc("POST /api/app/rides/estimated-fare", appPostRidesEstimatedFare)
		authedMux.HandleFunc("POST /api/app/rides/{ride_id}/evaluation", appPostRideEvaluatation)
		authedMux.HandleFunc("GET /api/app/notification", appGetNotification)
		authedMux.HandleFunc("GET /api/app/nearby-chairs", appGetNearbyChairs)
	}

	// owner handlers
	{
		mux.HandleFunc("POST /api/owner/owners", ownerPostOwners)

		authedMux := mux.With(ownerAuthMiddleware)
		authedMux.HandleFunc("GET /api/owner/sales", ownerGetSales)
		authedMux.HandleFunc("GET /api/owner/chairs", ownerGetChairs)
	}

	// chair handlers
	{
		mux.HandleFunc("POST /api/chair/chairs", chairPostChairs)

		authedMux := mux.With(chairAuthMiddleware)
		authedMux.HandleFunc("POST /api/chair/activity", chairPostActivity)
		authedMux.HandleFunc("POST /api/chair/coordinate", chairPostCoordinate)
		authedMux.HandleFunc("GET /api/chair/notification", chairGetNotification)
		authedMux.HandleFunc("POST /api/chair/rides/{ride_id}/status", chairPostRideStatus)
	}

	// internal handlers
	{
		mux.HandleFunc("GET /api/internal/matching", internalGetMatching)
	}

	return mux
}

type postInitializeRequest struct {
	PaymentServer string `json:"payment_server"`
}

type postInitializeResponse struct {
	Language string `json:"language"`
}

func postInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &postInitializeRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	stateLifecycle.Lock()
	defer stateLifecycle.Unlock()
	runtimeState.ready = false
	runtimeState.coordinateReady.Store(false)
	notifications.reset()

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize: %s: %w", string(out), err))
		return
	}
	// Initialization discards the previous generation, including accepted
	// records persisted exclusively in the WAL.
	if _, err := db.ExecContext(ctx, generationSchema); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO state_projection (id,generation)
		VALUES (1,UUID()) ON DUPLICATE KEY UPDATE generation=VALUES(generation)`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'", req.PaymentServer); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	err := runtimeState.restore(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, postInitializeResponse{Language: "go"})
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

func bindJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	buf, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	w.Write(buf)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
	buf, marshalError := json.Marshal(map[string]string{"message": err.Error()})
	if marshalError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"marshaling error failed"}`))
		return
	}
	w.Write(buf)

	slog.Error("error response wrote", "error", err)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}
