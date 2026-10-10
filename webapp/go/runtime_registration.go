package main

import (
	"database/sql"
	"errors"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/oklog/ulid/v2"
)

func (s *rideState) registerUser(req appPostUsersRequest) (User, error) {
	fieldError := normalizeAccountStrings(varchar(&req.Username, 30), varchar(&req.FirstName, 30), varchar(&req.LastName, 30), varchar(&req.DateOfBirth, 30))
	u := User{ID: ulid.Make().String(), Username: req.Username, Firstname: req.FirstName, Lastname: req.LastName,
		DateOfBirth: req.DateOfBirth, AccessToken: secureRandomStr(32), InvitationCode: secureRandomStr(15)}
	rewardID := ulid.Make().String()
	nameKey, tokenKey, invitationKey := s.accountKey(u.Username), s.accountKey(u.AccessToken), s.accountKey(u.InvitationCode)
	invitation := ""
	if req.InvitationCode != nil {
		invitation = *req.InvitationCode
	}
	benefitCode, rewardCode := "INV_"+invitation, "RWD_"+invitation+"_"+rewardID
	couponError := normalizeAccountStrings(varchar(&benefitCode, 255), varchar(&rewardCode, 255))
	inviterKey, benefitKey := s.accountKey(invitation), s.accountKey(benefitCode)
	s.mu.Lock()
	defer s.mu.Unlock()
	inviterID := ""
	if invitation != "" {
		var ok bool
		inviterID, ok = s.invitationUsers[inviterKey]
		if !ok || s.couponCounts[benefitKey] >= 3 {
			return User{}, stateError(http.StatusBadRequest, "この招待コードは使用できません。")
		}
	}
	if fieldError != nil {
		return User{}, fieldError
	}
	if s.userNames[nameKey] != "" || s.userTokens[tokenKey] != "" || s.invitationUsers[invitationKey] != "" || s.users[u.ID].ID != "" {
		return User{}, errors.New("duplicate user key")
	}
	if invitation != "" && couponError != nil {
		return User{}, couponError
	}
	if invitation != "" {
		if _, exists := s.coupons[inviterID][s.accountKey(rewardCode)]; exists {
			return User{}, errors.New("duplicate reward coupon key")
		}
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	u.CreatedAt, u.UpdatedAt = at, at
	coupons := []Coupon{{UserID: u.ID, Code: "CP_NEW2024", Discount: 3000, CreatedAt: at}}
	if invitation != "" {
		coupons = append(coupons, Coupon{UserID: u.ID, Code: benefitCode, Discount: 1500, CreatedAt: at},
			Coupon{UserID: inviterID, Code: rewardCode, Discount: 1000, CreatedAt: at})
	}
	if err := s.commit(stateRecord{Accounts: &accountRecord{User: &u, Coupons: coupons}}); err != nil {
		return User{}, err
	}
	return u, nil
}

func (s *rideState) registerOwner(name string) (Owner, error) {
	if err := normalizeAccountStrings(varchar(&name, 30)); err != nil {
		return Owner{}, err
	}
	o := Owner{ID: ulid.Make().String(), Name: name, AccessToken: secureRandomStr(32), ChairRegisterToken: secureRandomStr(32)}
	nameKey, tokenKey, chairKey := s.accountKey(name), s.accountKey(o.AccessToken), s.accountKey(o.ChairRegisterToken)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ownerNames[nameKey] != "" || s.ownerTokens[tokenKey] != "" || s.chairRegisterTokens[chairKey] != "" || s.owners[o.ID].ID != "" {
		return Owner{}, errors.New("duplicate owner key")
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	o.CreatedAt, o.UpdatedAt = at, at
	if err := s.commit(stateRecord{Accounts: &accountRecord{Owner: &o}}); err != nil {
		return Owner{}, err
	}
	return o, nil
}

func (s *rideState) registerChair(req chairPostChairsRequest) (Chair, error) {
	fieldError := normalizeAccountStrings(varchar(&req.Name, 30))
	// TEXT capacity is in bytes, unlike VARCHAR capacity.
	if !utf8.ValidString(req.Model) || len(req.Model) > 65535 {
		fieldError = errors.New("invalid or oversized chair model")
	}
	c := Chair{ID: ulid.Make().String(), Name: req.Name, Model: req.Model, AccessToken: secureRandomStr(32)}
	ownerKey := s.accountKey(req.ChairRegisterToken)
	s.mu.Lock()
	defer s.mu.Unlock()
	var ok bool
	c.OwnerID, ok = s.chairRegisterTokens[ownerKey]
	if !ok {
		return Chair{}, stateError(http.StatusUnauthorized, "invalid chair_register_token")
	}
	if fieldError != nil {
		return Chair{}, fieldError
	}
	if s.chairs[c.ID].ID != "" {
		return Chair{}, errors.New("duplicate chair key")
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	c.CreatedAt, c.UpdatedAt = at, at
	if err := s.commit(stateRecord{Chair: &c}); err != nil {
		return Chair{}, err
	}
	return c, nil
}

func (s *rideState) registerPaymentToken(userID, token string) error {
	if err := normalizeAccountStrings(varchar(&token, 255)); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.paymentTokens[userID]; exists {
		return errors.New("duplicate payment token user")
	}
	p := PaymentToken{UserID: userID, Token: token, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	return s.commit(stateRecord{Accounts: &accountRecord{PaymentToken: &p}})
}

func (s *rideState) paymentConfiguration(userID string) (PaymentToken, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	token, ok := s.paymentTokens[userID]
	if !ok {
		return PaymentToken{}, "", stateError(http.StatusBadRequest, "payment token not registered")
	}
	if !s.paymentGatewayConfigured {
		return PaymentToken{}, "", sql.ErrNoRows
	}
	return token, s.paymentGatewayURL, nil
}

func (s *rideState) createRide(userID string, pickup, destination Coordinate) (Ride, int, error) {
	var coordinateError error
	for _, v := range []int{pickup.Latitude, pickup.Longitude, destination.Latitude, destination.Longitude} {
		if v < -2147483648 || v > 2147483647 {
			coordinateError = errors.New("ride coordinate is outside integer range")
			break
		}
	}
	ride := Ride{ID: ulid.Make().String(), UserID: userID, PickupLatitude: pickup.Latitude, PickupLongitude: pickup.Longitude,
		DestinationLatitude: destination.Latitude, DestinationLongitude: destination.Longitude}
	status := RideStatus{ID: ulid.Make().String(), RideID: ride.ID, Status: "MATCHING"}
	lock := s.userMutex(userID)
	lock.Lock()
	defer lock.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.userActiveRides[userID] > 0 {
		return Ride{}, 0, stateError(http.StatusConflict, "ride already exists")
	}
	if coordinateError != nil {
		return Ride{}, 0, coordinateError
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	ride.CreatedAt, ride.UpdatedAt, status.CreatedAt = at, at, at
	coupon, hasCoupon := s.nextCoupon(userID, len(s.userRides[userID]) == 0)
	fare := discountedRideFare(ride, coupon.Discount)
	record := stateRecord{CreatedRide: &createdRide{Ride: ride, Fare: fare}, Statuses: []RideStatus{status}}
	if hasCoupon {
		coupon.UsedBy = &ride.ID
		record.Accounts = &accountRecord{Coupons: []Coupon{coupon}}
	}
	if err := s.commit(record); err != nil {
		return Ride{}, 0, err
	}
	return ride, fare, nil
}

func (s *rideState) estimatedFare(userID string, pickup, destination Coordinate) int {
	s.mu.RLock()
	coupon, _ := s.nextCoupon(userID, true)
	s.mu.RUnlock()
	return discountedRideFare(Ride{PickupLatitude: pickup.Latitude, PickupLongitude: pickup.Longitude,
		DestinationLatitude: destination.Latitude, DestinationLongitude: destination.Longitude}, coupon.Discount)
}
