package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func readTestCoordinates(t *testing.T, s *rideState) []coordinateRecord {
	t.Helper()
	committed := map[coordinateReference]bool{}
	for _, record := range readTestWAL(t, s) {
		if record.Coordinate != nil {
			committed[*record.Coordinate] = true
		}
	}
	var records []coordinateRecord
	for i := range s.coordinates.stripes {
		f, err := os.Open(s.coordinates.stripes[i].wal.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readCoordinateRecord(f); err != nil {
			f.Close()
			t.Fatal(err)
		}
		for {
			record, err := readCoordinateRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				t.Fatal(err)
			}
			if !record.Control || committed[coordinateReference{ChairID: record.Location.ChairID, ChairSequence: record.ChairSequence, StripeSequence: record.Sequence}] {
				records = append(records, record)
			}
		}
		f.Close()
	}
	return records
}

func coordinateTestMetrics(t *testing.T, s *rideState) (map[string]*metricHistogram, map[string]uint64) {
	t.Helper()
	var histograms []histogramSample
	var counters []counterSample
	if _, ok := s.copyCoordinateMetrics(&histograms, &counters, time.Now().Add(20*time.Millisecond)); !ok {
		t.Fatal("coordinate metrics busy")
	}
	series := map[string]*metricHistogram{}
	values := map[string]uint64{}
	for _, h := range histograms {
		h := h
		series[h.name] = &h.value
	}
	for _, c := range counters {
		values[c.name] = c.value
	}
	return series, values
}

func coordinateCounter(t *testing.T, s *rideState, name string) uint64 {
	t.Helper()
	_, values := coordinateTestMetrics(t, s)
	return values[name]
}

func coordinateHistogram(t *testing.T, s *rideState, name string) *metricHistogram {
	t.Helper()
	series, _ := coordinateTestMetrics(t, s)
	return series[name]
}

func TestCoordinatePathIndependentOfControlAndOtherStripe(t *testing.T) {
	s := observedState(t)
	s.addChair(Chair{ID: "a", AccessToken: "token-a", IsActive: true})
	s.addChair(Chair{ID: "b", AccessToken: "token-b", IsActive: true})
	if coordinateStripeIndex("a") == coordinateStripeIndex("b") {
		t.Fatal("test requires different stripes")
	}
	// Fence both common state and an unrelated chair's WAL/operation mutex.
	s.mu.Lock()
	stripe := &s.coordinates.stripes[coordinateStripeIndex("b")]
	stripe.mu.Lock()
	cell := s.coordinateCell("b")
	cell.mu.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			cell.mu.Unlock()
			stripe.mu.Unlock()
			s.mu.Unlock()
		}
	}
	defer unlock()
	result := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/api/chair/coordinate", strings.NewReader(`{"latitude":3,"longitude":4}`))
		req.AddCookie(&http.Cookie{Name: "chair_session", Value: "TOKEN-A "})
		res := httptest.NewRecorder()
		runtimeStateMiddleware(chairAuthMiddleware(http.HandlerFunc(chairPostCoordinate))).ServeHTTP(res, req)
		result <- res.Code
	}()
	select {
	case code := <-result:
		if code != http.StatusOK {
			t.Fatalf("HTTP %d", code)
		}
	case <-time.After(time.Second):
		unlock()
		<-result
		t.Fatal("ordinary coordinate waited for control or unrelated stripe")
	}
	if s.sequence != 0 || !s.chairDistance("a").Latitude.Valid {
		t.Fatal("coordinate used control sequence or failed to publish")
	}
	unlock()
	// Readers under control must not take the chair operation mutex in reverse.
	readerCell := s.coordinateCell("a")
	readerCell.mu.Lock()
	read := make(chan distanceTotal, 1)
	go func() { s.mu.RLock(); d := s.chairDistance("a"); s.mu.RUnlock(); read <- d }()
	select {
	case d := <-read:
		if d.TotalDistance != 0 || d.Latitude.Int64 != 3 {
			t.Fatal("reader missed latest snapshot")
		}
	case <-time.After(time.Second):
		readerCell.mu.Unlock()
		<-read
		t.Fatal("position reader took operation mutex")
	}
	readerCell.mu.Unlock()
}

func TestCoordinateRecoveryArrivalsAndGeneration(t *testing.T) {
	directory := t.TempDir()
	at := time.Now().Add(-time.Hour)
	seed := func(s *rideState) {
		s.addChair(Chair{ID: "a", AccessToken: "token", IsActive: true})
		s.addChair(Chair{ID: "b", IsActive: true})
		seedMemoryRide(s, "r", "a", at)
	}
	s := memoryTestStateIn(t, directory, "generation-one", seed)
	for _, p := range []Coordinate{{0, 0}, {1, 2}, {1, 2}} {
		if _, err := s.coordinate("a", p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.chairStatus("a", "r", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("a", Coordinate{10, 20}); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("a", "r", "CARRYING"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("b", Coordinate{8, 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("a", Coordinate{30, 40}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("a", Coordinate{30, 40}); err != nil {
		t.Fatal(err)
	}
	before := s.chairDistance("a")
	controlSequence := s.sequence
	for restart := 0; restart < 2; restart++ {
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		s = memoryTestStateIn(t, directory, "generation-one", seed)
		d := s.chairDistance("a")
		if d.TotalDistance != before.TotalDistance || d.Latitude != before.Latitude || !d.UpdatedAt.Time.Equal(before.UpdatedAt.Time) || s.latest(s.rides["r"]) != "ARRIVED" || s.sequence != controlSequence {
			t.Fatal("restart lost distance, position or arrival")
		}
		history := readTestCoordinates(t, s)
		if len(history) != 7 {
			t.Fatalf("history length %d", len(history))
		}
		var statuses []string
		for _, id := range s.chairEvents["a"].ids {
			statuses = append(statuses, s.statuses[id].Status)
		}
		if !reflect.DeepEqual(statuses, []string{"MATCHING", "ENROUTE", "PICKUP", "CARRYING", "ARRIVED"}) {
			t.Fatalf("notification order: %v", statuses)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s = memoryTestStateIn(t, directory, "generation-two", seed)
	if len(readTestCoordinates(t, s)) != 0 || s.chairDistance("a").UpdatedAt.Valid || s.latest(s.rides["r"]) != "MATCHING" {
		t.Fatal("initialize retained old coordinate generation")
	}
	if _, ok := s.coordinates.tokens.Load(s.accountKey("token")); !ok {
		t.Fatal("auth index not rebuilt")
	}
}

func TestCoordinateControlWriteFailureDoesNotPublishOrReplay(t *testing.T) {
	directory := t.TempDir()
	at := time.Now().Add(-time.Hour)
	seed := func(s *rideState) { s.addChair(Chair{ID: "a", IsActive: true}); seedMemoryRide(s, "r", "a", at) }
	s := memoryTestStateIn(t, directory, "test", seed)
	if _, err := s.coordinate("a", Coordinate{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("a", "r", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	before := s.chairDistance("a")
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.coordinate("a", Coordinate{10, 20}); err == nil {
		t.Fatal("failed control WAL accepted arrival")
	}
	if s.chairDistance("a") != before || s.latest(s.rides["r"]) != "ENROUTE" {
		t.Fatal("failed control WAL published arrival")
	}
	s.close()
	s = memoryTestStateIn(t, directory, "test", seed)
	d := s.chairDistance("a")
	if d.TotalDistance != before.TotalDistance || d.Latitude != before.Latitude || s.latest(s.rides["r"]) != "ENROUTE" || len(readTestCoordinates(t, s)) != 1 {
		t.Fatal("prepared arrival replayed without control commit")
	}
	// Reuse the unpublished chair sequence after restart; stripe sequence still
	// advances past its prepared record so the two references cannot collide.
	if _, err := s.coordinate("a", Coordinate{10, 20}); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s = memoryTestStateIn(t, directory, "test", seed)
	if s.latest(s.rides["r"]) != "PICKUP" || s.chairDistance("a").TotalDistance != 27 || len(readTestCoordinates(t, s)) != 2 {
		t.Fatal("retry arrival did not survive restart")
	}
}

func TestCoordinateWALTailAndCorruption(t *testing.T) {
	for _, damage := range []string{"partial-header", "partial-payload", "checksum", "sequence", "missing-arrival"} {
		t.Run(damage, func(t *testing.T) {
			s := memoryTestState(t)
			s.addChair(Chair{ID: "a"})
			if _, err := s.coordinate("a", Coordinate{1, 2}); err != nil {
				t.Fatal(err)
			}
			stripe := &s.coordinates.stripes[coordinateStripeIndex("a")]
			end, _ := stripe.wal.Seek(0, io.SeekCurrent)
			switch damage {
			case "partial-header":
				stripe.wal.Write([]byte{1, 2, 3})
			case "partial-payload":
				stripe.wal.Write([]byte{10, 0, 0, 0, 0, 0, 0, 0, 1, 2})
			case "checksum":
				stripe.wal.WriteAt([]byte{0xff}, end-1)
			case "sequence":
				stripe.write(coordinateRecord{Version: 1, Generation: "test", Sequence: 3, Location: ChairLocation{ChairID: "a"}}, nil)
			case "missing-arrival":
				s.coordinates.arrivals = map[coordinateReference]bool{{ChairID: "a", ChairSequence: 2, StripeSequence: 2}: false}
			}
			s.seedCoordinateDistance("a", distanceTotal{})
			for i := range s.coordinates.stripes {
				s.coordinates.stripes[i].sequence = 0
			}
			err := s.recoverCoordinates()
			if damage == "checksum" || damage == "sequence" || damage == "missing-arrival" {
				if err == nil {
					t.Fatal("corrupt coordinate WAL accepted")
				}
				return
			}
			if err != nil || !s.chairDistance("a").UpdatedAt.Valid {
				t.Fatalf("tail recovery: %v", err)
			}
			info, _ := stripe.wal.Stat()
			if info.Size() != end {
				t.Fatal("incomplete tail not truncated")
			}
		})
	}
}

func TestCoordinateMetricsArrivalControlFailure(t *testing.T) {
	s := observedState(t)
	s.addChair(Chair{ID: "a", IsActive: true})
	seedMemoryRide(s, "r", "a", time.Now().Add(-time.Hour))
	if _, err := s.coordinate("a", Coordinate{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.chairStatus("a", "r", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	s.coordinateCalls.Store(64)
	if _, err := s.coordinate("a", Coordinate{10, 20}); err == nil {
		t.Fatal("failed control WAL accepted")
	}
	if coordinateCounter(t, s, "coord_wal_ok") != 1 || coordinateCounter(t, s, "coord_wal_fail") != 1 || coordinateCounter(t, s, "coord_apply_ok") != 1 || coordinateHistogram(t, s, "coord_write_fail").count != 1 || coordinateHistogram(t, s, "coord_apply_ok").count != 1 || coordinateHistogram(t, s, "flow_dist_enroute").count != 0 {
		t.Fatal("arrival failure violated coordinate metric populations")
	}
}

func TestCoordinateConcurrentStripesTransitionsAndReaders(t *testing.T) {
	s := observedState(t)
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("chair-%d", i)
		s.addChair(Chair{ID: id, IsActive: true})
		seedMemoryRide(s, id, id, time.Now().Add(-time.Hour))
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			rideMeasurementSnapshot()
			s.mu.RLock()
			for id := range s.chairs {
				s.matchingChair(id)
				s.chairDistance(id)
			}
			s.mu.RUnlock()
		}
	}()
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func(id string) {
			defer workers.Done()
			if _, err := s.coordinate(id, Coordinate{}); err != nil {
				t.Error(err)
				return
			}
			if err := s.chairStatus(id, id, "ENROUTE"); err != nil {
				t.Error(err)
				return
			}
			for j := 1; j < 10; j++ {
				if _, err := s.coordinate(id, Coordinate{j, j}); err != nil {
					t.Error(err)
					return
				}
			}
			if _, err := s.coordinate(id, Coordinate{10, 20}); err != nil {
				t.Error(err)
				return
			}
			if err := s.chairStatus(id, id, "CARRYING"); err != nil {
				t.Error(err)
				return
			}
			if _, err := s.coordinate(id, Coordinate{30, 40}); err != nil {
				t.Error(err)
				return
			}
		}(fmt.Sprintf("chair-%d", i))
	}
	workers.Wait()
	close(done)
	wg.Wait()
	for id := range s.chairs {
		if s.latest(s.rides[id]) != "ARRIVED" || s.chairDistance(id).TotalDistance != 70 {
			t.Fatalf("chair %s lost transition or distance", id)
		}
	}
	if len(readTestCoordinates(t, s)) != 16*12 || coordinateCounter(t, s, "coord_calls_ok") != 16*12 || coordinateHistogram(t, s, "flow_dist_enroute").count != 16*10 || coordinateHistogram(t, s, "flow_dist_carrying").sum != 16*40 {
		t.Fatal("striped history or metrics lost accepted coordinates")
	}
}
