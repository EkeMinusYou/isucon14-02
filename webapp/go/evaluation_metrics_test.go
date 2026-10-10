package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

type evaluationTestDriver struct{}
type evaluationTestConn struct {
	url  string
	fail bool
}
type evaluationTestRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (evaluationTestDriver) Open(dsn string) (driver.Conn, error) {
	return &evaluationTestConn{url: strings.TrimPrefix(dsn, "fail:"), fail: strings.HasPrefix(dsn, "fail:")}, nil
}
func (*evaluationTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*evaluationTestConn) Close() error { return nil }
func (*evaluationTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c *evaluationTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.fail {
		return nil, errors.New("SQL fixture failure")
	}
	if strings.Contains(query, "payment_tokens") {
		return &evaluationTestRows{columns: []string{"user_id", "token"}, values: []driver.Value{"u", "token"}}, nil
	}
	if strings.Contains(query, "settings") {
		return &evaluationTestRows{columns: []string{"value"}, values: []driver.Value{c.url}}, nil
	}
	return nil, errors.New("unexpected SQL")
}
func (r *evaluationTestRows) Columns() []string { return r.columns }
func (r *evaluationTestRows) Close() error      { return nil }
func (r *evaluationTestRows) Next(v []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(v, r.values)
	return nil
}
func init() { sql.Register("evaluation-metrics-fixture", evaluationTestDriver{}) }

func TestEvaluationMetricsHandlerStages(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failSQL       bool
		paymentStatus int
		retries       int
	}{
		{name: "success", paymentStatus: 204},
		{name: "SQL failure", failSQL: true, paymentStatus: 204},
		{name: "invalid payment", paymentStatus: 400},
		{name: "expired key", paymentStatus: 422},
		{name: "still processing", paymentStatus: 409, retries: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var payments atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payments.Add(1)
				w.WriteHeader(tc.paymentStatus)
			}))
			defer server.Close()
			dsn := server.URL
			if tc.failSQL {
				dsn = "fail:" + dsn
			}
			database, err := sqlx.Open("evaluation-metrics-fixture", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			s := memoryTestState(t)
			s.metrics = newRideMetrics()
			s.resetDispatchMetrics()
			s.addChair(Chair{ID: "c", IsActive: true})
			seedMemoryRide(s, "r", "c", time.Now().Add(-time.Second))
			s.addStatus(s.newStatus(s.rides["r"], "ARRIVED", time.Now()))
			previousState, previousDB := runtimeState, db
			runtimeState, db = s, database
			defer func() { runtimeState, db = previousState, previousDB }()
			request := httptest.NewRequest(http.MethodPost, "/api/app/rides/r/evaluation", strings.NewReader(`{"evaluation":5}`))
			request.SetPathValue("ride_id", "r")
			request = request.WithContext(context.WithValue(request.Context(), "user", &User{ID: "u"}))
			response := httptest.NewRecorder()
			lock := s.userMutex("u")
			lock.Lock()
			done := make(chan struct{})
			go func() { appPostRideEvaluatation(response, request); close(done) }()
			time.Sleep(15 * time.Millisecond)
			lock.Unlock()
			<-done
			if h := s.metrics.series["eval_lock_ok"]; h.count != 1 || h.sum < 10 {
				t.Fatalf("mutex wait: %+v", h)
			}
			if tc.failSQL {
				if response.Code != 500 || payments.Load() != 0 || s.metrics.series["eval_sql_fail"].count != 1 || s.metrics.series["eval_total_fail"].count != 1 || s.metrics.series["eval_payment_ok"].count != 0 {
					t.Fatal("SQL failure metrics")
				}
			} else if tc.paymentStatus != 204 {
				if response.Code != 502 || int(payments.Load()) != tc.retries+1 || s.latest(s.rides["r"]) != "ARRIVED" || s.stats["c"].Count != 0 {
					t.Fatalf("payment failure completed ride: %d %s", response.Code, response.Body)
				}
				if s.metrics.series["eval_payment_fail"].count != 1 || s.metrics.series["eval_total_fail"].count != 1 || s.metrics.series["eval_complete_fail"].count != 0 || s.metrics.series["eval_complete_ok"].count != 0 || s.metrics.paymentRetries[1] != uint64(tc.retries) {
					t.Fatal("payment failure metrics")
				}
			} else {
				if response.Code != 200 || payments.Load() != 1 || s.latest(s.rides["r"]) != "COMPLETED" {
					t.Fatalf("completion: %d %s", response.Code, response.Body)
				}
				for _, name := range []string{"eval_total_ok", "eval_sql_ok", "eval_payment_ok", "eval_complete_ok"} {
					if s.metrics.series[name].count != 1 {
						t.Fatalf("missing %s", name)
					}
				}
			}
		})
	}
}

func TestEvaluationMetricsOutcomesAndRetries(t *testing.T) {
	m := newRideMetrics()
	e := evaluationMetrics{metrics: m, retries: 5}
	e.visited[evalPayment] = true
	e.elapsed[evalPayment] = 600 * time.Millisecond
	e.visited[evalComplete] = true
	e.elapsed[evalComplete] = 2 * time.Millisecond
	e.finish(time.Now())
	if m.paymentRetries[1] != 5 || m.paymentRetries[0] != 0 || m.series["eval_payment_fail"].count != 1 || m.series["eval_payment_fail"].buckets != [5]uint64{0, 0, 1, 1, 1} || m.series["eval_complete_fail"].count != 1 || m.series["eval_sql_fail"].count != 0 {
		t.Fatal("failed stage, bucket or omitted-stage accounting")
	}
	e = evaluationMetrics{metrics: m, retries: 2}
	e.visited[evalPayment], e.success[evalPayment] = true, true
	e.elapsed[evalPayment] = 220 * time.Millisecond
	e.finish(time.Now())
	if m.paymentRetries[0] != 2 || m.series["eval_payment_ok"].sum != 220 {
		t.Fatal("successful retries")
	}
}
