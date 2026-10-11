package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Runtime account after-images share the control WAL commit with rides/coupons.
// This type has no coordinate or arrival state, keeping that integration separate.
type accountRecord struct {
	User         *User         `json:"user,omitempty"`
	Owner        *Owner        `json:"owner,omitempty"`
	Coupons      []Coupon      `json:"coupons,omitempty"`
	PaymentToken *PaymentToken `json:"payment_token,omitempty"`
}

type accountState struct {
	owners                   *stateMap[Owner]
	users                    *stateMap[User]
	userTokens               *stateMap[string]
	invitationUsers          *stateMap[string]
	userNames                *stateMap[string]
	ownerNames               *stateMap[string]
	ownerTokens              *stateMap[string]
	chairRegisterTokens      *stateMap[string]
	paymentTokens            *stateMap[PaymentToken]
	paymentGatewayURL        string
	paymentGatewayConfigured bool
	coupons                  *stateMap[map[string]Coupon]
	unusedCoupons            *stateMap[[]string]
	couponCounts             *stateMap[int]
}

func (s *rideState) resetAccounts() {
	s.accountState = accountState{
		owners: newStateMap(map[string]Owner{}), users: newStateMap(map[string]User{}), userTokens: newStateMap(map[string]string{}), invitationUsers: newStateMap(map[string]string{}),
		userNames: newStateMap(map[string]string{}), ownerNames: newStateMap(map[string]string{}),
		ownerTokens: newStateMap(map[string]string{}), chairRegisterTokens: newStateMap(map[string]string{}),
		paymentTokens: newStateMap(map[string]PaymentToken{}), coupons: newStateMap(map[string]map[string]Coupon{}),
		unusedCoupons: newStateMap(map[string][]string{}), couponCounts: newStateMap(map[string]int{}),
	}
}

// general_ci has one 16-bit weight per Unicode character, including supplementary
// characters. Load MySQL's actual weights once rather than approximating folding.
// PAD SPACE equality ignores trailing U+0020 in VARCHAR comparisons.
func (s *rideState) loadCollation(ctx context.Context) error {
	var incompatible int
	if err := s.database.GetContext(ctx, &incompatible, `SELECT COUNT(*) FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name IN ('users','owners','chairs','coupons','payment_tokens','settings')
  AND collation_name IS NOT NULL AND collation_name <> 'utf8mb4_general_ci'`); err != nil {
		return err
	}
	if incompatible != 0 {
		return errors.New("runtime accounts require utf8mb4_general_ci")
	}
	if s.collationWeights != nil {
		return nil
	}
	var chars strings.Builder
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if r >= 0xd800 && r <= 0xdfff {
			continue
		}
		chars.WriteRune(r)
	}
	var weights []byte
	if err := s.database.GetContext(ctx, &weights,
		"SELECT WEIGHT_STRING(CONVERT(? USING utf8mb4) COLLATE utf8mb4_general_ci)", chars.String()); err != nil {
		return err
	}
	const validRunes = utf8.MaxRune + 1 - 0x800
	if len(weights) != validRunes*2 {
		return fmt.Errorf("unexpected general_ci weights: %d", len(weights))
	}
	s.collationWeights = make([]uint16, utf8.MaxRune+1)
	offset := 0
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if r >= 0xd800 && r <= 0xdfff {
			continue
		}
		s.collationWeights[r] = binary.BigEndian.Uint16(weights[offset:])
		offset += 2
	}
	return nil
}

func (s *rideState) accountKey(value string) string {
	value = strings.TrimRight(value, " ")
	key := make([]byte, 0, len(value)*2)
	for _, r := range value {
		weight := s.collationWeights[r]
		key = append(key, byte(weight>>8), byte(weight))
	}
	return string(key)
}

// Strict SQL mode rejects non-space overflow, but VARCHAR permits truncation
// when only trailing U+0020 exceeds the column capacity.
type varcharField struct {
	value *string
	limit int
}

func normalizeAccountStrings(values ...varcharField) error {
	for _, field := range values {
		value := *field.value
		if !utf8.ValidString(value) {
			return errors.New("invalid account encoding")
		}
		if utf8.RuneCountInString(value) > field.limit {
			if utf8.RuneCountInString(strings.TrimRight(value, " ")) > field.limit {
				return errors.New("oversized account field")
			}
			*field.value = string([]rune(value)[:field.limit])
		}
	}
	return nil
}

func varchar(value *string, limit int) varcharField { return varcharField{value, limit} }

func (s *rideState) addOwner(o Owner) {
	s.owners.set(o.ID, o)
	s.ownerNames.set(s.accountKey(o.Name), o.ID)
	s.ownerTokens.set(s.accountKey(o.AccessToken), o.ID)
	s.chairRegisterTokens.set(s.accountKey(o.ChairRegisterToken), o.ID)
}

func (s *rideState) applyAccounts(record *accountRecord) {
	if record == nil {
		return
	}
	if record.User != nil {
		s.addUser(*record.User)
	}
	if record.Owner != nil {
		s.addOwner(*record.Owner)
	}
	for _, coupon := range record.Coupons {
		s.addCoupon(coupon)
	}
	if record.PaymentToken != nil {
		s.paymentTokens.set(record.PaymentToken.UserID, *record.PaymentToken)
	}
}

func (s *rideState) addUser(u User) {
	s.users.set(u.ID, u)
	s.userTokens.set(s.accountKey(u.AccessToken), u.ID)
	s.userNames.set(s.accountKey(u.Username), u.ID)
	s.invitationUsers.set(s.accountKey(u.InvitationCode), u.ID)
}
