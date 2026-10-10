package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// The test creates its own local MySQL container. It never accepts a DSN for
// an existing server, so schema resets cannot affect a contest database.
func TestMatching(t *testing.T) {
	image := os.Getenv("ISURIDE_TEST_MYSQL_IMAGE")
	if image == "" {
		t.Skip("run task test-matching to use a temporary local MySQL container")
	}
	start := exec.Command("docker", "run", "--pull=never", "--detach", "--rm",
		"--env", "MYSQL_ALLOW_EMPTY_PASSWORD=yes", "--publish", "127.0.0.1::3306", image)
	out, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("start local MySQL container: %v\n%s", err, out)
	}
	containerID := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if out, err := exec.Command("docker", "stop", containerID).CombinedOutput(); err != nil {
			t.Errorf("stop local MySQL container: %v\n%s", err, out)
		}
	})
	out, err = exec.Command("docker", "inspect", "--format",
		`{{(index (index .NetworkSettings.Ports "3306/tcp") 0).HostPort}}`, containerID).CombinedOutput()
	if err != nil {
		t.Fatalf("find local MySQL port: %v\n%s", err, out)
	}
	config := mysql.NewConfig()
	config.User, config.Net, config.Addr = "root", "tcp", "127.0.0.1:"+strings.TrimSpace(string(out))
	config.MultiStatements = true
	config.ParseTime = true
	config.InterpolateParams = true
	// Docker exposes the port before MySQL is ready; report readiness failures
	// with container logs instead of repeated handshake errors.
	config.Logger = log.New(io.Discard, "", 0)
	openPool := func() *sqlx.DB {
		connector, err := mysql.NewConnector(config)
		if err != nil {
			t.Fatal(err)
		}
		return sqlx.NewDb(sql.OpenDB(connector), "mysql")
	}
	pool := openPool()
	t.Cleanup(func() { pool.Close() })
	deadline := time.Now().Add(30 * time.Second)
	for pool.Ping() != nil {
		if time.Now().After(deadline) {
			log, _ := exec.Command("docker", "logs", containerID).CombinedOutput()
			t.Fatalf("local MySQL did not start:\n%s", log)
		}
		time.Sleep(250 * time.Millisecond)
	}
	pool.MustExec("CREATE DATABASE isuride")
	pool.Close()
	config.DBName = "isuride"
	pool = openPool()
	originalDB := db
	db = pool
	t.Cleanup(func() { notifications.reset(); db = originalDB })
	schema, err := os.ReadFile("../sql/1-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	reset := func(t *testing.T) {
		t.Helper()
		notifications.reset()
		pool.MustExec(string(schema))
		pool.MustExec("INSERT INTO chair_models (name, speed) VALUES ('model', 2), ('fast', 10)")
		pool.MustExec(`INSERT INTO users
			(id, username, firstname, lastname, date_of_birth, access_token, invitation_code)
			VALUES ('user', 'user', 'Test', 'User', '2000-01-01', 'token', 'invite')`)
	}
	chair := func(id string, active bool) {
		pool.MustExec(`INSERT INTO chairs (id, owner_id, name, model, is_active, access_token)
			VALUES (?, 'owner', ?, 'model', ?, ?)`, id, id, active, id)
		pool.MustExec(`INSERT INTO chair_distance_totals (chair_id, last_latitude, last_longitude)
			VALUES (?, 0, 0)`, id)
	}
	location := func(id string, latitude, longitude int) {
		pool.MustExec(`UPDATE chair_distance_totals SET last_latitude = ?, last_longitude = ? WHERE chair_id = ?`, latitude, longitude, id)
	}
	ride := func(id string, chairID any, createdAt time.Time) {
		pool.MustExec(`INSERT INTO rides (id, user_id, chair_id, pickup_latitude,
			pickup_longitude, destination_latitude, destination_longitude, created_at)
			VALUES (?, 'user', ?, 0, 0, 1, 1, ?)`, id, chairID, createdAt)
		pool.MustExec(`INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, 'MATCHING')`, id+"-matching", id)
	}
	request := func() int {
		response := httptest.NewRecorder()
		internalGetMatching(response, httptest.NewRequest(http.MethodGet, "/api/internal/matching", nil))
		return response.Code
	}
	assertCount := func(t *testing.T, query string, want int) {
		t.Helper()
		var got int
		if err := pool.Get(&got, query); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s: got %d, want %d", query, got, want)
		}
	}
	waitForLock := func(t *testing.T) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var waits int
			if err := pool.Get(&waits, "SELECT COUNT(*) FROM performance_schema.data_lock_waits"); err != nil {
				t.Fatal(err)
			}
			if waits > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("matcher did not wait for the chair transaction")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	t.Run("BatchTravelTimeUsesAllKnownCandidates", func(t *testing.T) {
		reset(t)
		chair("a-near", true)
		location("a-near", 0, 3)
		chair("z-fast", true)
		location("z-fast", 4, 4)
		pool.MustExec(`UPDATE chairs SET model = 'fast' WHERE id = 'z-fast'`)
		chair("unknown-position", true)
		pool.MustExec("UPDATE chair_distance_totals SET last_latitude = NULL, last_longitude = NULL WHERE chair_id = 'unknown-position'")
		chair("unknown-model", true)
		location("unknown-model", 0, 0)
		pool.MustExec(`UPDATE chairs SET model = 'missing' WHERE id = 'unknown-model'`)
		ride("oldest", nil, time.Unix(1_700_000_000, 0))
		ride("newer", nil, time.Unix(1_700_000_001, 0))
		pool.MustExec(`UPDATE rides SET pickup_latitude = 4, pickup_longitude = 4 WHERE id = 'newer'`)
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		// Reserve the fast chair for the ride at its current position, minimizing
		// the total time of the batch instead of greedily serving the first ride.
		assertCount(t, `SELECT COUNT(*) FROM rides WHERE id = 'oldest' AND chair_id = 'a-near'`, 1)
		assertCount(t, `SELECT COUNT(*) FROM rides WHERE id = 'newer' AND chair_id = 'z-fast'`, 1)
	})

	t.Run("ManhattanDistanceAndFractionalPickupTime", func(t *testing.T) {
		reset(t)
		pool.MustExec(`UPDATE chair_models SET speed = 4 WHERE name = 'model'`)
		chair("a-diagonal", true)
		location("a-diagonal", -13, -23)
		chair("z-nearest", true)
		location("z-nearest", -10, -25)
		ride("waiting", nil, time.Unix(1_700_000_000, 0))
		pool.MustExec(`UPDATE rides SET pickup_latitude = -10, pickup_longitude = -20 WHERE id = 'waiting'`)
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		// Manhattan ETA is 6/4 versus 5/4; Euclidean distance would favor
		// the diagonal chair, and integer division would incorrectly tie them.
		assertCount(t, `SELECT COUNT(*) FROM rides WHERE chair_id = 'z-nearest'`, 1)
	})

	t.Run("UnknownMetadataRemainsAvailableAsFallback", func(t *testing.T) {
		reset(t)
		chair("no-position", true)
		pool.MustExec("UPDATE chair_distance_totals SET last_latitude = NULL, last_longitude = NULL WHERE chair_id = 'no-position'")
		chair("no-model", true)
		location("no-model", 0, 0)
		pool.MustExec(`UPDATE chairs SET model = 'missing' WHERE id = 'no-model'`)
		chair("zero-speed", true)
		location("zero-speed", 0, 0)
		pool.MustExec(`INSERT INTO chair_models (name, speed) VALUES ('stopped', 0)`)
		pool.MustExec(`UPDATE chairs SET model = 'stopped' WHERE id = 'zero-speed'`)
		for i := 0; i < 3; i++ {
			ride(fmt.Sprintf("waiting-%d", i), nil, time.Unix(1_700_000_000+int64(i), 0))
		}
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, `SELECT COUNT(DISTINCT chair_id) FROM rides`, 3)
	})

	t.Run("OldestFirstBoundedBatchAndConcurrentRequests", func(t *testing.T) {
		reset(t)
		for i := 0; i < 130; i++ {
			chair(fmt.Sprintf("chair-%03d", i), true)
			ride(fmt.Sprintf("ride-%03d", i), nil, time.Unix(1_700_000_000+int64(i), 0))
		}
		appSub, unsubscribeApp := notifications.subscribe("app", "user")
		defer unsubscribeApp()
		chairSubs := make(map[string]*notificationSubscription)
		for i := 0; i < 130; i++ {
			id := fmt.Sprintf("chair-%03d", i)
			sub, unsubscribe := notifications.subscribe("chair", id)
			defer unsubscribe()
			chairSubs[id] = sub
		}
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL", 64)
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id < 'ride-064' AND chair_id IS NULL", 0)
		if len(appSub.wake) != 1 {
			t.Fatal("assignment must wake the user's SSE connection")
		}
		var assigned []string
		if err := pool.Select(&assigned, "SELECT chair_id FROM rides WHERE chair_id IS NOT NULL"); err != nil {
			t.Fatal(err)
		}
		for _, id := range assigned {
			if len(chairSubs[id].wake) != 1 {
				t.Fatalf("assignment did not wake chair %s", id)
			}
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if status := request(); status != http.StatusNoContent {
					t.Errorf("concurrent matching status: %d", status)
				}
			}()
		}
		wg.Wait()
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL", 130)
		assertCount(t, "SELECT COUNT(DISTINCT chair_id) FROM rides", 130)
		assertCount(t, "SELECT COUNT(*) FROM ride_statuses WHERE status <> 'MATCHING'", 0)
		// A fresh application connection must see the committed assignments.
		fresh := openPool()
		defer fresh.Close()
		var count int
		if err := fresh.Get(&count, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL"); err != nil || count != 130 {
			t.Fatalf("persisted assignments: count=%d err=%v", count, err)
		}
	})

	t.Run("TravelTimeAssignmentPreservesRideCoordinates", func(t *testing.T) {
		reset(t)
		chair("fast", true)
		chair("slow", true)
		pool.MustExec("UPDATE chairs SET model = 'fast' WHERE id = 'fast'")
		ride("short", nil, time.Unix(1_700_000_000, 0))
		ride("long", nil, time.Unix(1_700_000_001, 0))
		pool.MustExec("UPDATE rides SET destination_latitude = 100, destination_longitude = 0 WHERE id = 'long'")
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'long' AND chair_id = 'fast' AND pickup_latitude = 0 AND pickup_longitude = 0 AND destination_latitude = 100 AND destination_longitude = 0", 1)
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'short' AND chair_id = 'slow' AND pickup_latitude = 0 AND pickup_longitude = 0 AND destination_latitude = 1 AND destination_longitude = 1", 1)
	})

	t.Run("UnknownMetadataDoesNotDisplaceKnownChair", func(t *testing.T) {
		reset(t)
		chair("no-position", true)
		pool.MustExec("UPDATE chair_distance_totals SET last_latitude = NULL, last_longitude = NULL WHERE chair_id = 'no-position'")
		chair("no-model", true)
		chair("zero-speed", true)
		pool.MustExec("UPDATE chair_distance_totals SET last_latitude = NULL, last_longitude = NULL WHERE chair_id = 'no-position'")
		pool.MustExec("UPDATE chairs SET model = 'unknown' WHERE id = 'no-model'")
		pool.MustExec("INSERT INTO chair_models (name, speed) VALUES ('stationary', 0)")
		pool.MustExec("UPDATE chairs SET model = 'stationary' WHERE id = 'zero-speed'")
		chair("known", true)
		location("known", 1000000, 1000000)
		ride("waiting", nil, time.Unix(1_700_000_000, 0))
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'waiting' AND chair_id = 'known'", 1)
	})

	t.Run("CompletionNotificationGatesReuse", func(t *testing.T) {
		reset(t)
		for _, id := range []string{"new", "completed", "unsent", "busy"} {
			chair(id, true)
		}
		chair("inactive", false)
		for _, id := range []string{"completed", "unsent", "busy"} {
			ride("old-"+id, id, time.Unix(1_700_000_000, 0))
			pool.MustExec("UPDATE ride_statuses SET chair_sent_at = NOW(6) WHERE ride_id = ?", "old-"+id)
			for _, status := range []string{"ENROUTE", "PICKUP", "CARRYING", "ARRIVED", "COMPLETED"} {
				if id == "busy" && status != "ENROUTE" {
					continue
				}
				pool.MustExec(`INSERT INTO ride_statuses (id, ride_id, status, chair_sent_at)
					VALUES (?, ?, ?, IF(? = 'unsent' AND ? = 'COMPLETED', NULL, NOW(6)))`,
					id+"-"+status, "old-"+id, status, id, status)
			}
		}
		for _, id := range []string{"waiting-1", "waiting-2", "waiting-3"} {
			ride(id, nil, time.Unix(1_700_000_100, 0))
		}
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id LIKE 'waiting-%' AND chair_id IN ('new', 'completed')", 2)
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'waiting-3' AND chair_id IS NULL", 1)
		events, err := loadNotifications(context.Background(), "chair", "unsent", "")
		if err != nil || len(events) != 1 {
			t.Fatalf("completion notification: events=%v err=%v", events, err)
		}
		if data := events[0].data.(chairGetNotificationResponseData); data.Status != "COMPLETED" {
			t.Fatalf("completion status: %s", data.Status)
		}
		if err := acknowledgeNotification(context.Background(), "chair", events[0].statusID); err != nil {
			t.Fatal(err)
		}
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching after notification status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'waiting-3' AND chair_id = 'unsent'", 1)
	})

	t.Run("ActivityRecheckedAfterLockWait", func(t *testing.T) {
		reset(t)
		chair("chair", true)
		ride("waiting", nil, time.Unix(1_700_000_000, 0))
		activity := pool.MustBegin()
		defer activity.Rollback()
		activity.MustExec("UPDATE chairs SET is_active = FALSE WHERE id = 'chair'")
		result := make(chan int, 1)
		go func() { result <- request() }()
		waitForLock(t)
		if err := activity.Commit(); err != nil {
			t.Fatal(err)
		}
		if status := <-result; status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL", 0)
	})

	t.Run("PositionsRefreshedAfterLockWait", func(t *testing.T) {
		reset(t)
		chair("chair-a", true)
		chair("chair-b", true)
		pool.MustExec("UPDATE chair_distance_totals SET last_latitude = 10 WHERE chair_id = 'chair-b'")
		ride("ride-a", nil, time.Unix(1_700_000_000, 0))
		ride("ride-b", nil, time.Unix(1_700_000_001, 0))
		pool.MustExec("UPDATE rides SET pickup_latitude = 10, destination_latitude = 11 WHERE id = 'ride-b'")
		positions := pool.MustBegin()
		defer positions.Rollback()
		positions.MustExec("SELECT id FROM chairs WHERE id = 'chair-a' FOR UPDATE")
		result := make(chan int, 1)
		go func() { result <- request() }()
		waitForLock(t)
		positions.MustExec("UPDATE chair_distance_totals SET last_latitude = IF(chair_id = 'chair-a', 10, 0)")
		if err := positions.Commit(); err != nil {
			t.Fatal(err)
		}
		if status := <-result; status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'ride-a' AND chair_id = 'chair-b'", 1)
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'ride-b' AND chair_id = 'chair-a'", 1)
	})

	t.Run("OccupancyRecheckedAndOldestRideKeptAfterLockWait", func(t *testing.T) {
		reset(t)
		chair("chair-a", true)
		chair("chair-b", true)
		ride("old-long", nil, time.Unix(1_700_000_000, 0))
		ride("new-short", nil, time.Unix(1_700_000_001, 0))
		pool.MustExec("UPDATE rides SET destination_latitude = 100 WHERE id = 'old-long'")
		otherMatcher := pool.MustBegin()
		defer otherMatcher.Rollback()
		otherMatcher.MustExec("SELECT id FROM chairs WHERE id = 'chair-a' FOR UPDATE")
		result := make(chan int, 1)
		go func() { result <- request() }()
		waitForLock(t)
		otherMatcher.MustExec(`INSERT INTO rides (id, user_id, chair_id, pickup_latitude,
			pickup_longitude, destination_latitude, destination_longitude)
			VALUES ('previous', 'user', 'chair-a', 0, 0, 1, 1)`)
		otherMatcher.MustExec("INSERT INTO ride_statuses (id, ride_id, status) VALUES ('previous-matching', 'previous', 'MATCHING')")
		if err := otherMatcher.Commit(); err != nil {
			t.Fatal(err)
		}
		if status := <-result; status != http.StatusNoContent {
			t.Fatalf("matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'old-long' AND chair_id = 'chair-b'", 1)
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE id = 'new-short' AND chair_id IS NULL", 1)
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id = 'chair-a'", 1)
	})

	t.Run("FailedBatchRollsBack", func(t *testing.T) {
		reset(t)
		for i := 0; i < 3; i++ {
			chair(fmt.Sprintf("chair-%d", i), true)
			ride(fmt.Sprintf("ride-%d", i), nil, time.Unix(1_700_000_000, 0))
		}
		pool.MustExec(`CREATE TRIGGER reject_assignment BEFORE UPDATE ON rides FOR EACH ROW
			BEGIN IF NEW.id = 'ride-2' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test failure'; END IF; END`)
		sub, unsubscribe := notifications.subscribe("app", "user")
		defer unsubscribe()
		if status := request(); status != http.StatusInternalServerError {
			t.Fatalf("failed matching status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL", 0)
		if len(sub.wake) != 0 {
			t.Fatal("failed assignments must not publish a notification")
		}
		pool.MustExec("DROP TRIGGER reject_assignment")
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching after failure status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL", 3)
	})

	t.Run("EmptyQueueAndNoAvailableChairs", func(t *testing.T) {
		reset(t)
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("empty matching status: %d", status)
		}
		ride("waiting", nil, time.Unix(1_700_000_000, 0))
		if status := request(); status != http.StatusNoContent {
			t.Fatalf("matching without chairs status: %d", status)
		}
		assertCount(t, "SELECT COUNT(*) FROM rides WHERE chair_id IS NOT NULL", 0)
	})
}
