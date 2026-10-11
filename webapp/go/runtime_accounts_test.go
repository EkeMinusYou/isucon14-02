package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func accountRequest(handler http.HandlerFunc, body string, userID string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if userID != "" {
		r = r.WithContext(context.WithValue(r.Context(), "user", &User{ID: userID}))
	}
	w := httptest.NewRecorder()
	handler(w, r)
	return w
}

func TestRuntimeAccountsAtomicConcurrencyAndReplay(t *testing.T) {
	s := memoryTestState(t)
	previousState, previousDB := runtimeState, db
	runtimeState, db = s, nil
	defer func() { runtimeState, db = previousState, previousDB }()
	owner := accountRequest(ownerPostOwners, `{"name":"Example"}`, "")
	if owner.Code != 201 {
		t.Fatalf("owner: %d %s", owner.Code, owner.Body)
	}
	if duplicate := accountRequest(ownerPostOwners, `{"name":"EXAMPLE "}`, ""); duplicate.Code != 500 {
		t.Fatalf("duplicate owner: %d", duplicate.Code)
	}
	var o ownerPostOwnersResponse
	if err := json.Unmarshal(owner.Body.Bytes(), &o); err != nil {
		t.Fatal(err)
	}
	chair := accountRequest(chairPostChairs, fmt.Sprintf(`{"name":"chair","model":"model","chair_register_token":%q}`, strings.ToUpper(o.ChairRegisterToken)+" "), "")
	if chair.Code != 201 {
		t.Fatalf("chair: %d %s", chair.Code, chair.Body)
	}
	var c chairPostChairsResponse
	if err := json.Unmarshal(chair.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	registered := accountRequest(appPostUsers, `{"username":"Inviter","firstname":"First","lastname":"Last","date_of_birth":"2000-01-01"}`, "")
	if registered.Code != 201 {
		t.Fatalf("user: %d %s", registered.Code, registered.Body)
	}
	var u appPostUsersResponse
	if err := json.Unmarshal(registered.Body.Bytes(), &u); err != nil {
		t.Fatal(err)
	}
	if duplicate := accountRequest(appPostUsers, `{"username":"INVITER ","firstname":"F","lastname":"L","date_of_birth":"x"}`, ""); duplicate.Code != 500 {
		t.Fatalf("duplicate user: %d", duplicate.Code)
	}
	if got := s.estimatedFare(u.ID, Coordinate{}, Coordinate{20, 20}); got != 1500 {
		t.Fatalf("estimate=%d", got)
	}
	const attempts = 12
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan int, attempts)
	tokens := make(chan int, attempts)
	rides := make(chan int, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			<-start
			body := fmt.Sprintf(`{"username":"guest-%d","firstname":"F","lastname":"L","date_of_birth":"x","invitation_code":%q}`, i, strings.ToUpper(u.InvitationCode))
			results <- accountRequest(appPostUsers, body, "").Code
		}(i)
		go func() {
			defer wg.Done()
			<-start
			tokens <- accountRequest(appPostPaymentMethods, `{"token":"payment"}`, u.ID).Code
		}()
		go func() {
			defer wg.Done()
			<-start
			rides <- accountRequest(appPostRides, `{"pickup_coordinate":{"latitude":0,"longitude":0},"destination_coordinate":{"latitude":20,"longitude":20}}`, u.ID).Code
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(tokens)
	close(rides)
	count := func(ch chan int, want int) int {
		n := 0
		for code := range ch {
			if code == want {
				n++
			} else {
				expected := 400
				if want == 204 {
					expected = 500
				}
				if want == 202 {
					expected = 409
				}
				if code != expected {
					t.Fatalf("unexpected status=%d want=%d", code, want)
				}
			}
		}
		return n
	}
	if n := count(results, 201); n != 3 {
		t.Fatalf("invitation successes=%d", n)
	}
	if n := count(tokens, 204); n != 1 {
		t.Fatalf("payment successes=%d", n)
	}
	if n := count(rides, 202); n != 1 {
		t.Fatalf("ride successes=%d", n)
	}
	rideID := s.userRides.get(u.ID)[0]
	if s.rides.get(rideID).Fare != 1500 || s.couponCounts.get(s.accountKey("INV_"+u.InvitationCode)) != 3 || len(s.unusedCoupons.get(u.ID)) != 3 {
		t.Fatal("coupon consumption or limit changed")
	}
	for _, guest := range s.users.all() {
		if guest.ID != u.ID {
			campaign, ok := s.nextCoupon(guest.ID, true)
			if !ok || campaign.Code != "CP_NEW2024" || len(s.coupons.get(guest.ID)) != 2 {
				t.Fatal("invited campaign priority or atomic benefits")
			}
		}
	}
	records := readTestWAL(t, s)
	for _, record := range records {
		if record.Accounts != nil && record.Accounts.User != nil {
			expected := 1
			if record.Accounts.User.ID != u.ID {
				expected = 3
			}
			if len(record.Accounts.Coupons) != expected {
				t.Fatal("registration benefits were split")
			}
		}
		if record.CreatedRide != nil && (len(record.Statuses) != 1 || record.Accounts == nil || len(record.Accounts.Coupons) != 1) {
			t.Fatal("ride and coupon were split")
		}
	}
	// Exercise completed history with a WAL-only owner, chair, and user.
	row := s.rides.get(rideID).Ride
	row.ChairID = sql.NullString{String: c.ID, Valid: true}
	row.UpdatedAt = time.Now().UTC()
	if err := s.commit(stateRecord{Rides: []Ride{row}, Statuses: []RideStatus{{ID: "arrived", RideID: rideID, Status: "ARRIVED", CreatedAt: row.UpdatedAt}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.complete(rideID, 5); err != nil {
		t.Fatal(err)
	}
	s.finishCompletionResponse(rideID)
	// Normal close/reopen uses the same disk file and generation, with no account
	// snapshots retained. SQL provides only initialization seeds and settings.
	directory := filepath.Dir(s.wal.Name())
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	recovered := memoryTestStateIn(t, directory, s.generation, nil)
	runtimeState = recovered
	if recovered.users.len() != 4 || recovered.owners.len() != 1 || recovered.chairs.len() != 1 || recovered.rides.get(rideID).Fare != 1500 || len(recovered.unusedCoupons.get(u.ID)) != 3 || recovered.paymentTokens.get(u.ID).Token != "payment" {
		t.Fatal("account/coupon/ride after-images lost after restart")
	}
	historyResponse := accountRequest(appGetRides, "", u.ID)
	var history getAppRidesResponse
	if historyResponse.Code != 200 || json.Unmarshal(historyResponse.Body.Bytes(), &history) != nil || len(history.Rides) != 1 || history.Rides[0].Fare != 1500 || history.Rides[0].Chair.Owner != "Example" || history.Rides[0].Evaluation != 5 {
		t.Fatalf("restored history: %d %s", historyResponse.Code, historyResponse.Body)
	}
	for _, auth := range []struct {
		handler func(http.Handler) http.Handler
		cookie  *http.Cookie
		kind    string
	}{
		{appAuthMiddleware, registered.Result().Cookies()[0], "user"},
		{ownerAuthMiddleware, owner.Result().Cookies()[0], "owner"},
		{chairAuthMiddleware, chair.Result().Cookies()[0], "chair"},
	} {
		cookie := *auth.cookie
		cookie.Value = strings.ToUpper(cookie.Value) + " "
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.AddCookie(&cookie)
		w := httptest.NewRecorder()
		auth.handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Context().Value(auth.kind) == nil {
				t.Error("missing identity")
			}
			w.WriteHeader(204)
		})).ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatalf("restored auth=%d %s", w.Code, w.Body)
		}
	}
	if got := recovered.estimatedFare(u.ID, Coordinate{}, Coordinate{20, 20}); got != 3500 {
		t.Fatalf("restored reward estimate=%d", got)
	}
	rejected := accountRequest(appPostUsers, fmt.Sprintf(`{"username":"fourth","firstname":"F","lastname":"L","date_of_birth":"x","invitation_code":%q}`, u.InvitationCode), "")
	if rejected.Code != 400 {
		t.Fatalf("restored invitation limit=%d", rejected.Code)
	}
}

func TestRuntimeAccountsWALFailureDoesNotPublish(t *testing.T) {
	for _, kind := range []string{"user", "owner", "chair", "token", "ride"} {
		t.Run(kind, func(t *testing.T) {
			s := memoryTestState(t)
			u, err := s.registerUser(appPostUsersRequest{Username: "inviter", FirstName: "F", LastName: "L", DateOfBirth: "x"})
			if err != nil {
				t.Fatal(err)
			}
			o, err := s.registerOwner("owner")
			if err != nil {
				t.Fatal(err)
			}
			before := s.sequence
			s.wal.Close()
			switch kind {
			case "user":
				_, err = s.registerUser(appPostUsersRequest{Username: "guest", FirstName: "F", LastName: "L", DateOfBirth: "x", InvitationCode: &u.InvitationCode})
			case "owner":
				_, err = s.registerOwner("new")
			case "chair":
				_, err = s.registerChair(chairPostChairsRequest{Name: "chair", Model: "model", ChairRegisterToken: o.ChairRegisterToken})
			case "token":
				err = s.registerPaymentToken(u.ID, "payment")
			case "ride":
				_, _, err = s.createRide(u.ID, Coordinate{}, Coordinate{1, 1})
			}
			if err == nil || s.failed == nil || s.sequence != before || s.users.len() != 1 || s.owners.len() != 1 || s.chairs.len() != 0 || s.paymentTokens.len() != 0 || s.rides.len() != 0 || len(s.coupons.get(u.ID)) != 1 || len(s.unusedCoupons.get(u.ID)) != 1 {
				t.Fatal("failed WAL published state")
			}
		})
	}
}

func TestRuntimeAccountsCouponOrdering(t *testing.T) {
	s := memoryTestState(t)
	at := time.Now()
	for _, c := range []Coupon{
		{UserID: "u", Code: "older", Discount: 1000, CreatedAt: at},
		{UserID: "u", Code: "CP_NEW2024", Discount: 3000, CreatedAt: at.Add(time.Second)},
		{UserID: "u", Code: "newer", Discount: 1500, CreatedAt: at.Add(2 * time.Second)},
	} {
		s.addCoupon(c)
	}
	r, fare, err := s.createRide("u", Coordinate{}, Coordinate{20, 20})
	if err != nil || fare != 1500 {
		t.Fatalf("campaign first: %d %v", fare, err)
	}
	// Completion is a fixture here; state transition/payment contracts are tested
	// by the existing evaluation and ordinary-restart tests.
	if err := s.commit(stateRecord{Statuses: []RideStatus{{ID: "completed", RideID: r.ID, Status: "COMPLETED", CreatedAt: at.Add(time.Minute)}}}); err != nil {
		t.Fatal(err)
	}
	_, fare, err = s.createRide("u", Coordinate{}, Coordinate{20, 20})
	if err != nil || fare != 3500 {
		t.Fatalf("oldest reward: %d %v", fare, err)
	}
	if len(s.unusedCoupons.get("u")) != 1 {
		t.Fatal("coupon used more than once")
	}
}

func TestRuntimeAccountsSQLCollationAndSeedReload(t *testing.T) {
	notificationTestDB(t)
	db.MustExec(`INSERT INTO owners (id,name,access_token,chair_register_token) VALUES ('o','Café','owner-token','register-token')`)
	db.MustExec(`INSERT INTO coupons(user_id,code,discount) VALUES ('u','CP_NEW2024',3000),('u','older',1000)`)
	db.MustExec(`INSERT INTO payment_tokens(user_id,token) VALUES ('u','seed-payment')`)
	db.MustExec(`INSERT INTO settings(name,value) VALUES ('payment_gateway_url','http://payment.local')`)
	startTestRuntime(t)
	s := runtimeState
	for _, pair := range [][2]string{{"Café", "CAFE "}, {"ß", "s"}, {"😀", "😃"}, {"ς", "Σ"}, {"É", "e"}, {"Å", "a"}, {"a\u0301", "á"}, {"İ", "i"}, {"Ａ", "a"}, {"æ", "a"}, {"user", "user "}} {
		var equal bool
		if err := db.Get(&equal, `SELECT CONVERT(? USING utf8mb4) COLLATE utf8mb4_general_ci = CONVERT(? USING utf8mb4) COLLATE utf8mb4_general_ci`, pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
		if got := s.accountKey(pair[0]) == s.accountKey(pair[1]); got != equal {
			t.Fatalf("SQL equality %q/%q=%v memory=%v", pair[0], pair[1], equal, got)
		}
	}
	if _, err := s.registerOwner("CAFE "); err == nil {
		t.Fatal("SQL-equivalent owner accepted")
	}
	if p, url, err := s.paymentConfiguration("u"); err != nil || p.Token != "seed-payment" || url != "http://payment.local" {
		t.Fatalf("loaded payment config: %v %s %v", p, url, err)
	}
	previousDB := db
	db = nil
	u, err := s.registerUser(appPostUsersRequest{Username: "new", FirstName: "F", LastName: "L", DateOfBirth: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registerPaymentToken(u.ID, "new-payment"); err != nil {
		t.Fatal(err)
	}
	ride, fare, err := s.createRide(u.ID, Coordinate{}, Coordinate{20, 20})
	if err != nil || fare != 1500 {
		t.Fatalf("SQL-free create: %d %v", fare, err)
	}
	db = previousDB
	// A fresh state object must reconstruct accounts absent from SQL.
	directory := filepath.Dir(s.wal.Name())
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	restored, err := newRideState(context.Background(), db, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.close()
	if restored.users.get(u.ID).Username != "new" || restored.paymentTokens.get(u.ID).Token != "new-payment" || restored.rides.get(ride.ID).Fare != fare || len(restored.unusedCoupons.get(u.ID)) != 0 || len(restored.unusedCoupons.get("u")) != 2 {
		t.Fatal("SQL seeds and runtime WAL did not restore")
	}
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM users WHERE id = ?", u.ID); err != nil || count != 0 {
		t.Fatalf("runtime unexpectedly projected to SQL: %d %v", count, err)
	}

	db.MustExec("UPDATE state_projection SET generation = UUID()")
	if err := restored.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restored.users.len() != 1 || restored.users.get(u.ID).ID != "" || restored.rides.len() != 0 || restored.paymentTokens.len() != 1 || len(restored.unusedCoupons.get("u")) != 2 || restored.sequence != 0 {
		t.Fatal("generation reset replayed runtime accounts")
	}
}

func TestRuntimeAccountsSQLFieldLimitsAndErrors(t *testing.T) {
	s := memoryTestState(t)
	previousState, previousDB := runtimeState, db
	runtimeState, db = s, nil
	defer func() { runtimeState, db = previousState, previousDB }()
	invalid := accountRequest(chairPostChairs, fmt.Sprintf(`{"name":%q,"model":"m","chair_register_token":"invalid"}`, strings.Repeat("n", 31)), "")
	if invalid.Code != 401 {
		t.Fatalf("chair token precedence=%d", invalid.Code)
	}
	invalid = accountRequest(appPostUsers, fmt.Sprintf(`{"username":%q,"firstname":"F","lastname":"L","date_of_birth":"x","invitation_code":"missing"}`, strings.Repeat("n", 31)), "")
	if invalid.Code != 400 {
		t.Fatalf("invitation precedence=%d", invalid.Code)
	}
	for _, name := range []string{strings.Repeat("n", 31), strings.Repeat("名", 31)} {
		if _, err := s.registerOwner(name); err == nil {
			t.Fatal("VARCHAR overflow accepted")
		}
	}
	o, err := s.registerOwner("valid" + strings.Repeat(" ", 40))
	if err != nil || len(o.Name) != 30 {
		t.Fatalf("space overflow=%q %v", o.Name, err)
	}
	p := accountRequest(appPostPaymentMethods, `{"token":""}`, "u")
	if p.Code != 400 {
		t.Fatalf("empty token=%d", p.Code)
	}
	if _, _, err := s.paymentConfiguration("u"); err == nil {
		t.Fatal("missing token accepted")
	}
	if err := s.registerPaymentToken("u", "token"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.paymentConfiguration("u"); err == nil {
		t.Fatal("missing URL accepted")
	}
	if _, _, err := s.createRide("u", Coordinate{}, Coordinate{1, 1}); err != nil {
		t.Fatal(err)
	}
	badRide := accountRequest(appPostRides, `{"pickup_coordinate":{"latitude":2147483648,"longitude":0},"destination_coordinate":{"latitude":1,"longitude":1}}`, "u")
	if badRide.Code != 409 {
		t.Fatalf("active ride precedence=%d", badRide.Code)
	}
}
