package matching

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestMatchingWorkerRequestBoundaries(t *testing.T) {
	handler := NewHandler(netip.MustParseAddr("172.31.0.126"))
	oversized, _ := json.Marshal(Request{Rides: make([]Ride, MaxRides+1), Chairs: make([]Chair, 1), Count: 1, Required: 1})
	tooManyChairs, _ := json.Marshal(Request{Rides: []Ride{{}}, Chairs: make([]Chair, MaxChairs+1), Count: 1, Required: 1})
	tooManyOutputs, _ := json.Marshal(Request{Rides: make([]Ride, PlanLimit+1), Chairs: make([]Chair, PlanLimit+1), Count: PlanLimit + 1, Required: PlanLimit + 1})
	atCaps, _ := json.Marshal(Request{Rides: make([]Ride, MaxRides), Chairs: make([]Chair, MaxChairs), Count: 1, Required: 1})
	for _, test := range []struct {
		name, peer, method, path, body string
		want                           int
	}{
		{"success", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"f":1,"r":[{"d":1}],"c":[{"s":2}]}`, 200},
		{"other-private-host", "172.31.7.50:1234", "POST", "/solve", `{}`, 403},
		{"public-peer", "203.0.113.4:1234", "POST", "/solve", `{}`, 403},
		{"wrong-method", "172.31.0.126:1234", "GET", "/solve", `{}`, 404},
		{"no-initialize", "172.31.0.126:1234", "POST", "/api/initialize", `{}`, 404},
		{"horizon", "172.31.0.126:1234", "POST", "/solve", string(oversized), 400},
		{"chair-cap", "172.31.0.126:1234", "POST", "/solve", string(tooManyChairs), 400},
		{"output-cap", "172.31.0.126:1234", "POST", "/solve", string(tooManyOutputs), 400},
		{"exact-input-caps", "172.31.0.126:1234", "POST", "/solve", string(atCaps), 200},
		{"missing-count", "172.31.0.126:1234", "POST", "/solve", `{"f":1,"r":[{}],"c":[{}]}`, 400},
		{"no-rescue", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"r":[{}],"c":[{}]}`, 200},
		{"excess-rescue", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"f":2,"r":[{},{}],"c":[{}]}`, 400},
		{"bad-speed", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"f":1,"r":[{}],"c":[{"s":-1}]}`, 400},
		{"bad-coordinate", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"f":1,"r":[{"p":2147483648}],"c":[{}]}`, 400},
		{"empty", "172.31.0.126:1234", "POST", "/solve", `{}`, 400},
		{"insufficient-chairs", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"f":1,"r":[{}],"c":[]}`, 400},
		{"trailing", "172.31.0.126:1234", "POST", "/solve", `{"n":1,"f":1,"r":[{}],"c":[{}]}{}`, 400},
		{"body-limit", "172.31.0.126:1234", "POST", "/solve", strings.Repeat(" ", MaxRequestBytes+1), 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			req.RemoteAddr = test.peer
			req.Header.Set("X-Forwarded-For", "172.31.0.126")
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != test.want {
				t.Fatalf("status=%d want=%d", res.Code, test.want)
			}
			if test.want == 200 {
				var result Response
				if err := Decode(res.Body, &result); err != nil || len(result.Chairs) != 1 || result.Chairs[0] != 0 || len(result.Rides) != 1 || result.Rides[0] != 0 {
					t.Fatalf("response=%v err=%v", result, err)
				}
			}
		})
	}
}

type blockedReader struct {
	started, release chan struct{}
	*bytes.Reader
}

func (r *blockedReader) Read(p []byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	<-r.release
	return r.Reader.Read(p)
}
func (*blockedReader) Close() error { return nil }

func TestMatchingWorkerRejectsConcurrentRequests(t *testing.T) {
	h := NewHandler(netip.MustParseAddr("127.0.0.1"))
	body := &blockedReader{make(chan struct{}), make(chan struct{}), bytes.NewReader([]byte(`{"n":1,"f":1,"r":[{}],"c":[{}]}`))}
	first := httptest.NewRequest("POST", "/solve", nil)
	first.RemoteAddr, first.Body = "127.0.0.1:1234", body
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), first) }()
	<-body.started
	second := httptest.NewRequest("POST", "/solve", strings.NewReader(`{"n":1,"f":1,"r":[{}],"c":[{}]}`))
	second.RemoteAddr = "127.0.0.1:1235"
	res := httptest.NewRecorder()
	h.ServeHTTP(res, second)
	close(body.release)
	<-done
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", res.Code)
	}
}

func TestMatchingWorkerCanceledSolve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := Solve(ctx, Request{Rides: []Ride{{}}, Chairs: []Chair{{}}, Count: 1, Required: 1}); result != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("result=%v err=%v", result, err)
	}
}
