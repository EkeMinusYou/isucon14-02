package matching

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
)

const (
	PlanLimit        = 256
	MaxRequestBytes  = 16 << 20
	MaxResponseBytes = 16 << 10
)

type Request struct {
	Rides  []Ride  `json:"r"`
	Chairs []Chair `json:"c"`
}

// Response row i contains the chair index for request ride i. The HTTP request
// and response identify a single immutable plan; no server state is retained.
type Response struct {
	Chairs []int `json:"c"`
}

func (r *Response) UnmarshalJSON(data []byte) error {
	var wire struct {
		Chairs []*int `json:"c"`
	}
	if err := Decode(bytes.NewReader(data), &wire); err != nil {
		return err
	}
	r.Chairs = make([]int, len(wire.Chairs))
	for i, chair := range wire.Chairs {
		if chair == nil {
			return errors.New("null chair index")
		}
		r.Chairs[i] = *chair
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
		if err := Decode(r.Body, &request); err != nil || len(request.Rides) == 0 ||
			len(request.Rides) > PlanLimit || len(request.Chairs) < len(request.Rides) {
			http.Error(w, "invalid solver input", http.StatusBadRequest)
			return
		}
		assignments, err := assign(r.Context(), request.Rides, request.Chairs)
		if err != nil {
			return
		}
		response := Response{Chairs: make([]int, len(assignments))}
		for _, a := range assignments {
			response.Chairs[a.RideIndex] = a.ChairIndex
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}
