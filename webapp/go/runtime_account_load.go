package main

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type accountSeed struct {
	owners        []Owner
	users         []User
	coupons       []Coupon
	paymentTokens []PaymentToken
	settings      []struct {
		Name  string `db:"name"`
		Value string `db:"value"`
	}
}

// Only initialization and ordinary startup read the SQL account baseline.
func loadAccountSeed(ctx context.Context, tx *sqlx.Tx) (*accountSeed, error) {
	seed := &accountSeed{}
	for _, query := range []struct {
		dest any
		sql  string
	}{
		{&seed.owners, "SELECT * FROM owners"}, {&seed.users, "SELECT * FROM users"},
		{&seed.coupons, "SELECT * FROM coupons ORDER BY created_at, code"},
		{&seed.paymentTokens, "SELECT * FROM payment_tokens"}, {&seed.settings, "SELECT * FROM settings"},
	} {
		if err := tx.SelectContext(ctx, query.dest, query.sql); err != nil {
			return nil, err
		}
	}
	return seed, nil
}

func (seed *accountSeed) install(s *rideState) map[string]int {
	s.resetAccounts()
	for _, owner := range seed.owners {
		s.addOwner(owner)
	}
	for _, user := range seed.users {
		s.addUser(user)
	}
	discounts := map[string]int{}
	for _, coupon := range seed.coupons {
		s.addCoupon(coupon)
		if coupon.UsedBy != nil {
			discounts[*coupon.UsedBy] = coupon.Discount
		}
	}
	for _, token := range seed.paymentTokens {
		s.paymentTokens[token.UserID] = token
	}
	for _, setting := range seed.settings {
		if s.accountKey(setting.Name) == s.accountKey("payment_gateway_url") {
			s.paymentGatewayURL, s.paymentGatewayConfigured = setting.Value, true
		}
	}
	return discounts
}
