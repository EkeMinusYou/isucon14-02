package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"

	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

type matchingRPCClient struct {
	endpoint string
	client   *http.Client
}

func newMatchingRPCClient(endpoint string) (*matchingRPCClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || (!ip.IsPrivate() && !ip.IsLoopback()) || u.Scheme != "http" ||
		u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/solve" {
		return nil, errors.New("ISURIDE_SOLVER_URL must be a direct private HTTP IP:port/solve URL")
	}
	return &matchingRPCClient{endpoint: endpoint, client: &http.Client{
		Timeout:       2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("solver redirect rejected") },
		Transport: &http.Transport{
			// This private RPC never uses a public proxy or nginx.
			Proxy:           nil,
			DialContext:     (&net.Dialer{Timeout: 300 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
			MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 90 * time.Second,
		},
	}}, nil
}

func matchingRequest(plan matchingPlan) matching.Request {
	rides, chairs := plan.rides, plan.chairs
	request := matching.Request{Count: plan.count, Required: plan.required, Rides: make([]matching.Ride, len(rides)), Chairs: make([]matching.Chair, len(chairs))}
	for i, ride := range rides {
		request.Rides[i] = matching.Ride{PickupLatitude: ride.PickupLatitude, PickupLongitude: ride.PickupLongitude,
			DestinationLatitude: ride.DestinationLatitude, DestinationLongitude: ride.DestinationLongitude}
	}
	for i, chair := range chairs {
		request.Chairs[i] = matching.Chair{Latitude: chair.Latitude, Longitude: chair.Longitude, Speed: chair.Speed}
	}
	return request
}

func (c *matchingRPCClient) solve(plan matchingPlan) ([]matchingAssignment, error) {
	if c == nil {
		return nil, errors.New("matching solver is unavailable")
	}
	// Include conversion, encoding, dialing, decoding and validation in the bound.
	ctx, cancel := context.WithTimeout(context.Background(), c.client.Timeout)
	defer cancel()
	request := matchingRequest(plan)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if len(body) > matching.MaxRequestBytes {
		return nil, errors.New("matching snapshot exceeds RPC limit")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("matching RPC: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("matching RPC status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, matching.MaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > matching.MaxResponseBytes {
		return nil, errors.New("matching response exceeds RPC limit")
	}
	var result matching.Response
	if err := matching.Decode(bytes.NewReader(data), &result); err != nil {
		return nil, fmt.Errorf("matching response: %w", err)
	}
	if err := result.Validate(request); err != nil {
		return nil, err
	}
	assignments := make([]matchingAssignment, len(result.Rides))
	for i, ride := range result.Rides {
		assignments[i] = matchingAssignment{rideIndex: ride, chairIndex: result.Chairs[i]}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return assignments, nil
}
