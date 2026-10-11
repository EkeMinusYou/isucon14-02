package matching

import (
	"container/heap"
	"context"
	"sort"
)

const (
	NearbyLimit      = 8
	PickupVisitLimit = 512
	// A model-time tuning prior for notification, boarding and completion.
	// This is not a measured wall-clock cycle or payment latency.
	FixedCycleTime = 5.0
)

func Efficiency(ride Ride, chair Chair) float64 {
	if chair.Speed <= 0 {
		return 0
	}
	trip := float64(Distance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude))
	return trip / (TravelTime(ride, chair) + FixedCycleTime)
}

// Solve jointly chooses rides and chairs from a bounded sparse graph. The
// first Count FIFO rides each retain Count best chairs. At most Count-1 can be
// occupied, so this backbone guarantees full cardinality and all Required
// rides even when nearby candidates collide or metadata is entirely unknown.
// Other rides compete through up to NearbyLimit pickup neighbors per chair.
// E <= Count*Count + NearbyLimit*C; R <= min(input R, Count+NearbyLimit*C).
func Solve(ctx context.Context, request Request) ([]Assignment, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	rows, err := candidateRows(ctx, request)
	if err != nil {
		return nil, err
	}
	maxSpeed := 1
	for _, chair := range request.Chairs {
		maxSpeed = max(maxSpeed, chair.Speed)
	}
	// Known pair costs are in [0,1]. One fallback costs more than all known
	// pairs; omitting a required ride costs more than the entire plan. Thus
	// priorities are cardinality, rescue, fallback count, then summed distance
	// per cycle. Normalizing rates avoids large coordinate-dependent penalties.
	n := request.Count
	fallbackCost := float64(n + 1)
	optionalCost := float64((n + 1) * (n + 2))
	rideIndices := make([]int, 0, len(rows))
	for i, row := range rows {
		if len(row) != 0 {
			rideIndices = append(rideIndices, i)
		}
	}
	r, c := len(rideIndices), len(request.Chairs)
	source, sink := r+c, r+c+1
	g := make(matchingGraph, sink+1)
	for v, i := range rideIndices {
		penalty := 0.0
		if i >= request.Required {
			penalty = optionalCost
		}
		g.add(source, v, penalty)
		for _, candidate := range rows[i] {
			chair := request.Chairs[candidate.chairIndex]
			cost := 1 - Efficiency(request.Rides[i], chair)/float64(maxSpeed)
			if chair.Speed <= 0 {
				cost = fallbackCost
			}
			g.add(v, r+candidate.chairIndex, cost)
		}
	}
	for j := range request.Chairs {
		g.add(r+j, sink, 0)
	}
	if err := g.assign(ctx, source, sink, n); err != nil {
		return nil, err
	}
	assignments := make([]Assignment, 0, n)
	for v, i := range rideIndices {
		for _, edge := range g[v] {
			if edge.to >= r && edge.to < source && !edge.available {
				assignments = append(assignments, Assignment{RideIndex: i, ChairIndex: edge.to - r})
				break
			}
		}
	}
	return assignments, nil
}

func candidateRows(ctx context.Context, request Request) ([]matchingCandidates, error) {
	n := request.Count
	rows := make([]matchingCandidates, len(request.Rides))
	for i, ride := range request.Rides[:n] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		best := make(matchingCandidates, 0, n)
		for j, chair := range request.Chairs {
			// Prefer known metadata, then efficiency, with stable index ties.
			cost := -Efficiency(ride, chair)
			if chair.Speed <= 0 {
				cost = 1
			}
			candidate := matchingCandidate{chairIndex: j, cost: cost}
			if len(best) < n {
				best = append(best, candidate)
				if len(best) == n {
					heap.Init(&best)
				}
			} else if cost < best[0].cost || (cost == best[0].cost && j < best[0].chairIndex) {
				best[0] = candidate
				heap.Fix(&best, 0)
			}
		}
		rows[i] = best
	}
	if request.Required < n && n < len(request.Rides) {
		indices := make([]int, len(request.Rides)-n)
		for i := range indices {
			indices[i] = i + n
		}
		root := buildPickups(request.Rides, indices, 0)
		for j, chair := range request.Chairs {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if chair.Speed <= 0 {
				continue // Unknown positions never create fake nearby choices.
			}
			for _, i := range nearbyPickups(root, request.Rides, chair, n+(j*NearbyLimit)%(len(request.Rides)-n)) {
				rows[i] = append(rows[i], matchingCandidate{chairIndex: j})
			}
		}
	}
	for _, row := range rows {
		sort.Slice(row, func(i, j int) bool { return row[i].chairIndex < row[j].chairIndex })
	}
	return rows, ctx.Err()
}

// A balanced, immutable pickup tree is built on the worker, outside the state
// lock. O(R log^2 R) construction, O(R) memory. A fixed visit budget also bounds
// pathological spatial distributions to C*PickupVisitLimit visited nodes.
// Budget exhaustion may reduce quality, but cannot reduce cardinality/rescue.
type pickupNode struct {
	index                          int
	minIndex, maxIndex             int
	minLat, maxLat, minLon, maxLon int
	left, right                    *pickupNode
}

func buildPickups(rides []Ride, indices []int, depth int) *pickupNode {
	if len(indices) == 0 {
		return nil
	}
	sort.Slice(indices, func(a, b int) bool {
		x, y := rides[indices[a]], rides[indices[b]]
		p, q := x.PickupLatitude, y.PickupLatitude
		if depth%2 != 0 {
			p, q = x.PickupLongitude, y.PickupLongitude
		}
		return p < q || (p == q && indices[a] < indices[b])
	})
	mid := len(indices) / 2
	i := indices[mid]
	ride := rides[i]
	node := &pickupNode{index: i, minIndex: i, maxIndex: i, minLat: ride.PickupLatitude, maxLat: ride.PickupLatitude,
		minLon: ride.PickupLongitude, maxLon: ride.PickupLongitude,
		left: buildPickups(rides, indices[:mid], depth+1), right: buildPickups(rides, indices[mid+1:], depth+1)}
	for _, child := range []*pickupNode{node.left, node.right} {
		if child != nil {
			node.minIndex, node.maxIndex = min(node.minIndex, child.minIndex), max(node.maxIndex, child.maxIndex)
			node.minLat, node.maxLat = min(node.minLat, child.minLat), max(node.maxLat, child.maxLat)
			node.minLon, node.maxLon = min(node.minLon, child.minLon), max(node.maxLon, child.maxLon)
		}
	}
	return node
}

func (node *pickupNode) bound(chair Chair) int64 {
	return Distance(chair.Latitude, chair.Longitude,
		min(max(chair.Latitude, node.minLat), node.maxLat), min(max(chair.Longitude, node.minLon), node.maxLon))
}

// Rotate equal-distance FIFO ties by chair, so identical pickup clusters
// provide distinct optional choices without adding edges or a greedy plan.
func nearbyPickups(root *pickupNode, rides []Ride, chair Chair, offset int) []int {
	rank := func(index int) int {
		if index >= offset {
			return index - offset
		}
		return index + len(rides) - offset
	}
	lowerRank := func(node *pickupNode) int {
		if node.minIndex <= offset && offset <= node.maxIndex {
			return 0
		}
		return rank(node.minIndex)
	}
	type neighbor struct {
		index    int
		distance int64
	}
	best := make([]neighbor, 0, NearbyLimit)
	visited := 0
	var visit func(*pickupNode)
	visit = func(node *pickupNode) {
		if node == nil || visited >= PickupVisitLimit {
			return
		}
		visited++
		if len(best) == NearbyLimit {
			worst, bound := best[len(best)-1], node.bound(chair)
			if bound > worst.distance || (bound == worst.distance && lowerRank(node) >= rank(worst.index)) {
				return
			}
		}
		ride := rides[node.index]
		candidate := neighbor{node.index, Distance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)}
		pos := sort.Search(len(best), func(i int) bool {
			return best[i].distance > candidate.distance || (best[i].distance == candidate.distance && rank(best[i].index) > rank(candidate.index))
		})
		if pos < NearbyLimit {
			best = append(best, neighbor{})
			copy(best[pos+1:], best[pos:])
			best[pos] = candidate
			best = best[:min(len(best), NearbyLimit)]
		}
		first, second := node.left, node.right
		if first == nil || (second != nil && (second.bound(chair) < first.bound(chair) ||
			(second.bound(chair) == first.bound(chair) && lowerRank(second) < lowerRank(first)))) {
			first, second = second, first
		}
		visit(first)
		visit(second)
	}
	visit(root)
	indices := make([]int, len(best))
	for i := range best {
		indices[i] = best[i].index
	}
	return indices
}
