package main

import "sort"

// Coupons remain in the canonical map after use; the count includes consumed
// invitation benefits, while the small ordered index only contains unused ones.
func (s *rideState) addCoupon(c Coupon) {
	key := s.accountKey(c.Code)
	byCode := s.coupons.get(c.UserID)
	if byCode == nil {
		byCode = map[string]Coupon{}
		s.coupons.set(c.UserID, byCode)
	}
	old, exists := byCode[key]
	if !exists {
		s.couponCounts.update(key, func(n int) int { return n + 1 })
	}
	byCode[key] = c
	queue := s.unusedCoupons.get(c.UserID)
	if exists && old.UsedBy == nil {
		for i, candidate := range queue {
			if candidate == key {
				queue = append(queue[:i], queue[i+1:]...)
				break
			}
		}
	}
	if c.UsedBy == nil {
		i := sort.Search(len(queue), func(i int) bool {
			other := byCode[queue[i]]
			return other.CreatedAt.After(c.CreatedAt) || (other.CreatedAt.Equal(c.CreatedAt) && queue[i] >= key)
		})
		queue = append(queue, "")
		copy(queue[i+1:], queue[i:])
		queue[i] = key
	}
	s.unusedCoupons.set(c.UserID, queue)
}

// Estimates preserve the original campaign priority; ride creation requests it
// only for the first ride. Neither selection nor estimates consume a coupon.
func (s *rideState) nextCoupon(userID string, campaignPriority bool) (Coupon, bool) {
	if campaignPriority {
		if c, ok := s.coupons.get(userID)[s.accountKey("CP_NEW2024")]; ok && c.UsedBy == nil {
			return c, true
		}
	}
	queue := s.unusedCoupons.get(userID)
	if len(queue) == 0 {
		return Coupon{}, false
	}
	return s.coupons.get(userID)[queue[0]], true
}

func discountedRideFare(ride Ride, discount int) int {
	return initialFare + max(0, calculateSale(ride)-initialFare-discount)
}
