package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentInvitationsAndCouponConsumption(t *testing.T) {
	notificationTestDB(t)
	const inviters, attempts = 12, 8
	for i := 0; i < inviters; i++ {
		id := fmt.Sprintf("inviter-%d", i)
		db.MustExec(`INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code)
			VALUES (?, ?, 'Test', 'Inviter', '', ?, ?)`, id, id, id, id)
		// Exercise the oldest-coupon path while invitation rewards are inserted.
		db.MustExec(`INSERT INTO coupons (user_id, code, discount) VALUES (?, 'existing', 1000)`, id)
	}
	// Upgrade the schema of an existing database through the startup path.
	db.MustExec("ALTER TABLE coupons DROP INDEX idx_coupons_code")
	startTestRuntime(t)
	var indexCount int
	if err := db.Get(&indexCount, `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema=DATABASE() AND table_name='coupons' AND index_name='idx_coupons_code'`); err != nil || indexCount != 1 {
		t.Fatalf("coupon code index not restored: %d %v", indexCount, err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan int, inviters*attempts)
	for i := 0; i < inviters; i++ {
		id := fmt.Sprintf("inviter-%d", i)
		for j := 0; j < attempts; j++ {
			wg.Add(1)
			go func(i, j int, id string) {
				defer wg.Done()
				<-start
				body, _ := json.Marshal(appPostUsersRequest{Username: fmt.Sprintf("new-%d-%d", i, j), FirstName: "New", LastName: "User", DateOfBirth: "2000-01-01", InvitationCode: &id})
				res := httptest.NewRecorder()
				appPostUsers(res, httptest.NewRequest(http.MethodPost, "/api/app/users", strings.NewReader(string(body))))
				results <- res.Code
			}(i, j, id)
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			res := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/app/rides", strings.NewReader(`{"pickup_coordinate":{"latitude":0,"longitude":0},"destination_coordinate":{"latitude":1,"longitude":1}}`))
			req = req.WithContext(context.WithValue(req.Context(), "user", &User{ID: id}))
			appPostRides(res, req)
			if res.Code != http.StatusAccepted {
				t.Errorf("coupon consumption: %d %s", res.Code, res.Body.String())
			}
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	counts := map[int]int{}
	for code := range results {
		counts[code]++
	}
	if counts[http.StatusCreated] != inviters*3 || counts[http.StatusBadRequest] != inviters*(attempts-3) || len(counts) != 2 {
		t.Fatalf("concurrent invitation outcomes: %v", counts)
	}
	for i := 0; i < inviters; i++ {
		id := fmt.Sprintf("inviter-%d", i)
		var issued, rewards, consumed int
		if err := db.Get(&issued, "SELECT COUNT(*) FROM coupons WHERE code = ?", "INV_"+id); err != nil {
			t.Fatal(err)
		}
		if err := db.Get(&rewards, "SELECT COUNT(*) FROM coupons WHERE user_id = ? AND code LIKE 'RWD_%'", id); err != nil {
			t.Fatal(err)
		}
		if err := db.Get(&consumed, "SELECT COUNT(*) FROM coupons WHERE user_id = ? AND used_by IS NOT NULL", id); err != nil {
			t.Fatal(err)
		}
		if issued != 3 || rewards != 3 || consumed != 1 {
			t.Fatalf("%s: invitations=%d rewards=%d consumed=%d", id, issued, rewards, consumed)
		}
	}
	// Publication of invitation lookup survives a restart from the DB.
	if err := runtimeState.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtimeState.invitationUsers["inviter-0"] != "inviter-0" {
		t.Fatal("invitation lookup was not restored")
	}
	res := httptest.NewRecorder()
	appPostUsers(res, httptest.NewRequest(http.MethodPost, "/api/app/users", strings.NewReader(`{"username":"invalid","firstname":"Test","lastname":"User","date_of_birth":"2000-01-01","invitation_code":"missing"}`)))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("invalid invitation: %d %s", res.Code, res.Body.String())
	}
}
