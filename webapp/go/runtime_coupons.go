package main

import "context"

// Fresh initialization installs this index through the schema. Existing
// databases acquire it before accepting traffic after an application restart.
func (s *rideState) ensureCouponIndex(ctx context.Context) error {
	var count int
	if err := s.database.GetContext(ctx, &count, `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'coupons'
		AND column_name = 'code' AND seq_in_index = 1`); err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	_, err := s.database.ExecContext(ctx, "ALTER TABLE coupons ADD INDEX idx_coupons_code (code)")
	return err
}
