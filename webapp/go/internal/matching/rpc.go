package matching

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

const (
	PlanLimit        = 256
	MaxRides         = 8192
	MaxChairs        = 4096
	SolveTimeout     = 1500 * time.Millisecond
	MaxRequestBytes  = 16 << 20
	MaxResponseBytes = 16 << 10
)

type Request struct {
	Rides    []Ride  `json:"r"`
	Chairs   []Chair `json:"c"`
	Count    int     `json:"n"`
	Required int     `json:"f"`
}

// Rides are FIFO ordered; the first Required indices must be assigned.
func (r Request) Validate() error {
	if r.Count <= 0 || r.Count > PlanLimit || len(r.Rides) < r.Count || len(r.Rides) > MaxRides ||
		len(r.Chairs) < r.Count || len(r.Chairs) > MaxChairs || r.Required < 0 || r.Required > r.Count {
		return errors.New("invalid matching dimensions or rescue quota")
	}
	validCoordinate := func(x int) bool { return x >= -2147483648 && x <= 2147483647 }
	for _, ride := range r.Rides {
		if !validCoordinate(ride.PickupLatitude) || !validCoordinate(ride.PickupLongitude) ||
			!validCoordinate(ride.DestinationLatitude) || !validCoordinate(ride.DestinationLongitude) {
			return errors.New("invalid ride coordinate")
		}
	}
	for _, chair := range r.Chairs {
		if !validCoordinate(chair.Latitude) || !validCoordinate(chair.Longitude) || chair.Speed < 0 || chair.Speed > 2147483647 {
			return errors.New("invalid chair coordinate or speed")
		}
	}
	return nil
}

// Explicit ride indices separate candidate pool size from output cardinality.
// The request and response describe one immutable plan without identities.
type Response struct {
	Rides  []int `json:"r"`
	Chairs []int `json:"c"`
}

func (r *Response) UnmarshalJSON(data []byte) error {
	var wire struct {
		Rides  []*int `json:"r"`
		Chairs []*int `json:"c"`
	}
	if err := Decode(bytes.NewReader(data), &wire); err != nil {
		return err
	}
	r.Rides, r.Chairs = make([]int, len(wire.Rides)), make([]int, len(wire.Chairs))
	for i, index := range wire.Rides {
		if index == nil {
			return errors.New("null ride index")
		}
		r.Rides[i] = *index
	}
	for i, index := range wire.Chairs {
		if index == nil {
			return errors.New("null chair index")
		}
		r.Chairs[i] = *index
	}
	return nil
}

func (r Response) Validate(request Request) error {
	if len(r.Rides) != request.Count || len(r.Chairs) != request.Count {
		return errors.New("matching response has wrong assignment count")
	}
	rides, chairs := make([]bool, len(request.Rides)), make([]bool, len(request.Chairs))
	for i, ride := range r.Rides {
		chair := r.Chairs[i]
		if ride < 0 || ride >= len(rides) || rides[ride] || chair < 0 || chair >= len(chairs) || chairs[chair] {
			return errors.New("matching response has invalid or duplicate index")
		}
		rides[ride], chairs[chair] = true, true
	}
	for _, assigned := range rides[:request.Required] {
		if !assigned {
			return errors.New("matching response omitted a required ride")
		}
	}
	return nil
}

// Decode requires exactly one JSON value, including when a peer sends a valid
// prefix followed by extra data. Limits apply to untrusted response bodies too.
func Decode(r io.Reader, value any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("extra JSON value")
		}
		return err
	}
	return nil
}

func NewHandler(allowed netip.Addr) http.Handler {
	busy := make(chan struct{}, 1)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		peer, parseErr := netip.ParseAddr(host)
		// RemoteAddr is the direct TCP peer; forwarded headers are never trusted.
		if err != nil || parseErr != nil || peer.Unmap() != allowed.Unmap() {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.URL.Path != "/solve" || r.Method != http.MethodPost {
			http.Error(w, "invalid solver request", http.StatusNotFound)
			return
		}
		select {
		case busy <- struct{}{}:
			defer func() { <-busy }()
		default:
			http.Error(w, "solver busy", http.StatusServiceUnavailable)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBytes)
		var request Request
		if err := Decode(r.Body, &request); err != nil {
			http.Error(w, "invalid solver input", http.StatusBadRequest)
			return
		}
		if err := request.Validate(); err != nil {
			http.Error(w, "invalid solver input", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), SolveTimeout)
		defer cancel()
		assignments, err := Solve(ctx, request)
		if err != nil {
			http.Error(w, "solver failed", http.StatusServiceUnavailable)
			return
		}
		response := Response{Rides: make([]int, len(assignments)), Chairs: make([]int, len(assignments))}
		for i, a := range assignments {
			response.Rides[i], response.Chairs[i] = a.RideIndex, a.ChairIndex
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}
