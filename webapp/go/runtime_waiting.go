package main

import (
	"sort"
	"time"
)

// A ride-only treap groups fixed pickup positions. Subtree bounds prune distant
// positions; queries do not copy the entire waiting queue.
// All updates and queries use rideState.mu; this does not index chair positions.
type pickupPoint struct{ latitude, longitude int }

type waitingPickupNode struct {
	point                          pickupPoint
	priority                       uint64
	rides                          []*memoryRide
	left, right                    *waitingPickupNode
	minLat, maxLat, minLon, maxLon int
	oldest                         *memoryRide
}

func pickupOf(ride *memoryRide) pickupPoint {
	return pickupPoint{ride.PickupLatitude, ride.PickupLongitude}
}

func pickupBefore(a, b pickupPoint) bool {
	return a.latitude < b.latitude || (a.latitude == b.latitude && a.longitude < b.longitude)
}

func pickupPriority(p pickupPoint) uint64 {
	// SplitMix64 keeps sorted coordinates from producing a sorted tree.
	x := uint64(p.latitude)*0x9e3779b97f4a7c15 + uint64(p.longitude)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func pickupAbove(a, b *waitingPickupNode) bool {
	return a.priority < b.priority || (a.priority == b.priority && pickupBefore(a.point, b.point))
}

func (node *waitingPickupNode) refresh() {
	node.minLat, node.maxLat = node.point.latitude, node.point.latitude
	node.minLon, node.maxLon = node.point.longitude, node.point.longitude
	node.oldest = node.rides[0]
	for _, child := range []*waitingPickupNode{node.left, node.right} {
		if child != nil {
			node.minLat, node.maxLat = min(node.minLat, child.minLat), max(node.maxLat, child.maxLat)
			node.minLon, node.maxLon = min(node.minLon, child.minLon), max(node.maxLon, child.maxLon)
			if ridePrecedes(child.oldest, node.oldest) {
				node.oldest = child.oldest
			}
		}
	}
}

func insertWaitingPickup(node *waitingPickupNode, ride *memoryRide) *waitingPickupNode {
	p := pickupOf(ride)
	if node == nil {
		node = &waitingPickupNode{point: p, priority: pickupPriority(p), rides: []*memoryRide{ride}}
	} else if p == node.point {
		node.rides = insertCurrentRide(node.rides, ride)
	} else if pickupBefore(p, node.point) {
		node.left = insertWaitingPickup(node.left, ride)
		if pickupAbove(node.left, node) {
			root := node.left
			node.left, root.right = root.right, node
			node.refresh()
			node = root
		}
	} else {
		node.right = insertWaitingPickup(node.right, ride)
		if pickupAbove(node.right, node) {
			root := node.right
			node.right, root.left = root.left, node
			node.refresh()
			node = root
		}
	}
	node.refresh()
	return node
}

func mergeWaitingPickups(left, right *waitingPickupNode) *waitingPickupNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if pickupAbove(left, right) {
		left.right = mergeWaitingPickups(left.right, right)
		left.refresh()
		return left
	}
	right.left = mergeWaitingPickups(left, right.left)
	right.refresh()
	return right
}

func removeWaitingPickup(node *waitingPickupNode, ride *memoryRide) *waitingPickupNode {
	if node == nil {
		return nil
	}
	p := pickupOf(ride)
	if p == node.point {
		node.rides = removeCurrentRide(node.rides, ride)
		if len(node.rides) == 0 {
			return mergeWaitingPickups(node.left, node.right)
		}
	} else if pickupBefore(p, node.point) {
		node.left = removeWaitingPickup(node.left, ride)
	} else {
		node.right = removeWaitingPickup(node.right, ride)
	}
	node.refresh()
	return node
}

func (node *waitingPickupNode) lowerDistance(chair matchingChair) int64 {
	return matchingDistance(chair.Latitude, chair.Longitude,
		min(max(chair.Latitude, node.minLat), node.maxLat),
		min(max(chair.Longitude, node.minLon), node.maxLon))
}

const (
	matchingPickupCandidateLimit = 8
	matchingPickupExtraTime      = 2
	matchingRideFixedTime        = 5
)

type nearbyWaitingRide struct {
	ride     *memoryRide
	distance int64
}

// Keep the eight nearest unselected rides, including rides sharing a pickup.
// The radius shrinks as a closer pickup is found; extra travel is measured in
// the same distance/model-speed units as the solver, not wall-clock seconds.
func nearbyWaitingPickups(root *waitingPickupNode, chair matchingChair, selected map[string]bool) []nearbyWaitingRide {
	if chair.Speed <= 0 {
		return nil
	}
	best := make([]nearbyWaitingRide, 0, matchingPickupCandidateLimit)
	before := func(a, b nearbyWaitingRide) bool {
		return a.distance < b.distance || (a.distance == b.distance && ridePrecedes(a.ride, b.ride))
	}
	bound := func() int64 {
		limit := best[0].distance + int64(chair.Speed)*matchingPickupExtraTime
		if len(best) == matchingPickupCandidateLimit {
			limit = min(limit, best[len(best)-1].distance)
		}
		return limit
	}
	var visit func(*waitingPickupNode)
	visit = func(node *waitingPickupNode) {
		if node == nil {
			return
		}
		if len(best) > 0 {
			d := node.lowerDistance(chair)
			if d > bound() || (len(best) == matchingPickupCandidateLimit && d == best[len(best)-1].distance &&
				!ridePrecedes(node.oldest, best[len(best)-1].ride)) {
				return
			}
		}
		d := matchingDistance(chair.Latitude, chair.Longitude, node.point.latitude, node.point.longitude)
		for _, ride := range node.rides {
			if len(best) > 0 && d > bound() {
				break
			}
			if selected[ride.ID] {
				continue
			}
			c := nearbyWaitingRide{ride: ride, distance: d}
			if len(best) == matchingPickupCandidateLimit && !before(c, best[len(best)-1]) {
				break // Later rides at this point cannot enter the nearest set.
			}
			index := sort.Search(len(best), func(i int) bool { return before(c, best[i]) })
			if len(best) < matchingPickupCandidateLimit {
				best = append(best, c)
			}
			copy(best[index+1:], best[index:len(best)-1])
			best[index] = c
		}
		first, second := node.left, node.right
		if first == nil || (second != nil && second.lowerDistance(chair) < first.lowerDistance(chair)) {
			first, second = second, first
		}
		visit(first)
		visit(second)
	}
	visit(root)
	if len(best) > 0 {
		// Candidates collected before finding the nearest point may be outside
		// the final radius. Never let their trip length override that radius.
		limit := best[0].distance + int64(chair.Speed)*matchingPickupExtraTime
		for len(best) > 0 && best[len(best)-1].distance > limit {
			best = best[:len(best)-1]
		}
	}
	return best
}

type waitingPickupCandidate struct {
	ride       *memoryRide
	chair      matchingChair
	efficiency float64
	pickupTime float64
}

type waitingPickupCandidates []waitingPickupCandidate

func (h waitingPickupCandidates) Less(i, j int) bool {
	if h[i].efficiency != h[j].efficiency {
		return h[i].efficiency > h[j].efficiency
	}
	if h[i].pickupTime != h[j].pickupTime {
		return h[i].pickupTime < h[j].pickupTime
	}
	if h[i].ride.ID != h[j].ride.ID {
		return ridePrecedes(h[i].ride, h[j].ride)
	}
	return h[i].chair.ID < h[j].chair.ID
}

const matchingRideRescueAge = 10 * time.Second

func efficientWaitingPickup(root *waitingPickupNode, chair matchingChair, selected map[string]bool) (waitingPickupCandidate, bool) {
	var best waitingPickupCandidate
	for _, nearby := range nearbyWaitingPickups(root, chair, selected) {
		ride := nearby.ride
		trip := float64(matchingDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude))
		pickupTime := float64(nearby.distance) / float64(chair.Speed)
		// Fixed overhead represents notifications, boarding, evaluation and
		// payment in model-time units. Five is a tuning prior, not a measured
		// wall-clock cycle; payment milliseconds must not be added here.
		c := waitingPickupCandidate{ride: ride, chair: chair, pickupTime: pickupTime,
			efficiency: trip / (pickupTime + trip/float64(chair.Speed) + matchingRideFixedTime)}
		if best.ride == nil || (waitingPickupCandidates{c, best}).Less(0, 1) {
			best = c
		}
	}
	return best, best.ride != nil
}

// Reserve at least a quarter of every plan for FIFO progress. Requests waiting
// ten seconds take further FIFO slots, up to the entire plan. This is a rescue
// trigger, not a latency guarantee when demand exceeds chair supply.
func (s *rideState) selectWaitingRides(n int, chairs []matchingChair, now time.Time) []Ride {
	selected := make(map[string]bool, n)
	rides := make([]*memoryRide, 0, n)
	add := func(ride *memoryRide) {
		selected[ride.ID] = true
		rides = append(rides, ride)
	}
	quota := (n + 3) / 4
	for _, ride := range s.waitingRides {
		if len(rides) == n || (len(rides) >= quota && now.Sub(ride.CreatedAt) < matchingRideRescueAge) {
			break
		}
		add(ride)
	}
	if len(rides) < n && n < len(s.waitingRides) {
		candidates := make(waitingPickupCandidates, 0, len(chairs))
		candidate := func(chair matchingChair) (waitingPickupCandidate, bool) {
			return efficientWaitingPickup(s.waitingPickups, chair, selected)
		}
		for _, chair := range chairs {
			if chair.Speed > 0 {
				if c, ok := candidate(chair); ok {
					candidates = append(candidates, c)
				}
			}
		}
		sort.Slice(candidates, candidates.Less)
		for _, c := range candidates {
			if len(rides) == n {
				break
			}
			if selected[c.ride.ID] {
				next, ok := candidate(c.chair)
				if !ok {
					continue
				}
				c = next
			}
			add(c.ride)
			// One candidate per chair avoids filling a plan from one local cluster.
			// Rank chairs by initial efficiency, refreshing collisions once.
			// Extraction makes at most C+n bounded nearby queries, not n*C.
		}
	}
	// Missing coordinates/speeds are still usable through the existing solver.
	for _, ride := range s.waitingRides {
		if len(rides) == n {
			break
		}
		if !selected[ride.ID] {
			add(ride)
		}
	}
	sort.Slice(rides, func(i, j int) bool { return ridePrecedes(rides[i], rides[j]) })
	rows := make([]Ride, len(rides))
	for i, ride := range rides {
		rows[i] = ride.Ride
	}
	return rows
}
