package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	measurement "github.com/EkeMinusYou/isucon-template/tools/measurement/app"
)

type rideMetricPool struct{}

func (rideMetricPool) Stats() sql.DBStats { return sql.DBStats{MaxOpenConnections: 8} }

// Exercise the real exporter, bounded collector, TSV importer and contest SQL.
// Files are isolated temporary fixtures, never saved RUNs or server artifacts.
func TestRideMetricsCollectorDuckDB(t *testing.T) {
	s := memoryTestState(t)
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	previous := runtimeState
	runtimeState = s
	defer func() { runtimeState = previous }()
	snapshots := []measurement.Snapshot{rideMeasurementSnapshot()}
	observe := func(values ...float64) {
		for _, v := range values {
			s.metrics.observe("dispatch_wait", v)
		}
	}
	observe(100, 200)
	snapshots = append(snapshots, rideMeasurementSnapshot())
	observe(300)
	missing := rideMeasurementSnapshot()
	delete(missing.Metrics, "dispatch_wait_sum")
	for _, suffix := range []string{"_n", "_sum", "_b0", "_b1", "_b2", "_b3", "_inf"} {
		delete(missing.Metrics, "flow_dist_enroute"+suffix)
	}
	snapshots = append(snapshots, missing)
	observe(400)
	snapshots = append(snapshots, rideMeasurementSnapshot())
	s.metrics = newRideMetrics()
	s.resetDispatchMetrics()
	snapshots = append(snapshots, rideMeasurementSnapshot())
	observe(50)
	snapshots = append(snapshots, rideMeasurementSnapshot())
	for name := range snapshots[0].Metrics {
		if len(name) > 32 {
			t.Fatalf("metric name too long: %s", name)
		}
	}
	if len(snapshots[0].Metrics) != 452 {
		t.Fatalf("metric count %d (collector maximum 1024)", len(snapshots[0].Metrics))
	}
	var polls atomic.Int32
	complete := make(chan struct{})
	handler, err := measurement.NewHandler(measurement.Options{Pools: []measurement.Pool{{DB: rideMetricPool{}, DatabaseHost: "db.local", Name: "main", Role: "primary"}}, Snapshot: func() measurement.Snapshot {
		i := int(polls.Add(1)) - 1
		if i == len(snapshots)-1 {
			close(complete)
		}
		return snapshots[min(i, len(snapshots)-1)]
	}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	temporary := t.TempDir()
	binary := filepath.Join(temporary, "sql-pool-metrics")
	build := exec.Command("go", "build", "-o", binary, "./cmd/sql-pool-metrics")
	build.Dir = "../../tools/measurement"
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("collector build: %v\n%s", err, output)
	}
	fixture := filepath.Join(temporary, "runs", "fixture")
	if err := os.MkdirAll(fixture, 0700); err != nil {
		t.Fatal(err)
	}
	tsv := filepath.Join(fixture, "isucon-1-sql-pool-metrics.tsv")
	command := exec.Command(binary, "-endpoint", server.URL+"/debug/sql-pools", "-interval", "50ms", "-output", tsv)
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-complete:
	case <-time.After(5 * time.Second):
		command.Process.Kill()
		command.Wait()
		t.Fatalf("collector did not accept metrics: %s", stderr.String())
	}
	// The last response may still be writing; let the collector finish that poll.
	time.Sleep(20 * time.Millisecond)
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("collector: %v %s", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatal(stderr.String())
	}
	importer, err := os.ReadFile("../../tools/analysis/schema/sql-pool.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile("../../tools/contest/analysis-schema/ride-metrics.sql")
	if err != nil {
		t.Fatal(err)
	}
	query := fmt.Sprintf("set variable run_glob='%s';\n", strings.ReplaceAll(fixture, "'", "''")) + string(importer) + "\ncreate table metrics as select * from metrics_sql_pool;\n" + string(schema) + `
 select metric,count(*) as rows,count(delta) as valid,sum(delta) as total
 from ride_metric_intervals where metric='dispatch_wait_sum' group by metric;
 `
	duck := exec.Command("duckdb", "-json", "-batch")
	duck.Stdin = strings.NewReader(query)
	output, err := duck.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB: %v\n%s", err, output)
	}
	var rows []struct {
		Metric string
		Rows   int
		Valid  int
		Total  float64
	}
	if err := json.Unmarshal(output, &rows); err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if len(rows) != 1 || rows[0].Rows < 5 || rows[0].Valid < 2 || rows[0].Total != 350 {
		t.Fatalf("missing metric/reset deltas: %s", output)
	}
	// Also bind the histogram/gauge views and verify the reset interval is absent.
	duck = exec.Command("duckdb", "-json", "-batch")
	duck.Stdin = strings.NewReader(query[:strings.Index(query, "\n select metric,count(*)")] + `
 select sum(events) filter(where total is not null) as events,
        sum(total) as total,
        count(*) filter(where events is null) as missing,
        (select count(*) from ride_metric_gauges where value is null) as missing_gauges
 from ride_metric_histograms where series='dispatch_wait';`)
	output, err = duck.CombinedOutput()
	if err != nil {
		t.Fatalf("histogram SQL: %v\n%s", err, output)
	}
	var hist []struct {
		Events        float64
		Total         float64
		Missing       int
		MissingGauges int `json:"missing_gauges"`
	}
	if err := json.Unmarshal(output, &hist); err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Events != 3 || hist[0].Total != 350 || hist[0].Missing != 2 || hist[0].MissingGauges != 0 {
		t.Fatalf("histograms: %s", output)
	}
	// A completely absent series remains visible as missing. The definition
	// grid must exactly match each exported version, without phantom fields.
	duck = exec.Command("duckdb", "-json", "-batch")
	duck.Stdin = strings.NewReader(query[:strings.Index(query, "\n select metric,count(*)")] + `
 select v.version,
  (select count(*) from ride_metric_definitions d where v.version between d.min_version and d.max_version) as fields,
  (select count(*) from ride_metric_histograms where series='flow_dist_enroute' and events is null) as missing_series
 from (values (1),(2),(3)) v(version) order by version;`)
	output, err = duck.CombinedOutput()
	if err != nil {
		t.Fatalf("vocabulary SQL: %v\n%s", err, output)
	}
	var vocab []struct {
		Version       int
		Fields        int
		MissingSeries int `json:"missing_series"`
	}
	if err := json.Unmarshal(output, &vocab); err != nil {
		t.Fatal(err)
	}
	if len(vocab) != 3 || vocab[0].Fields != 124 || vocab[1].Fields != 124 || vocab[2].Fields != 452 || vocab[2].MissingSeries != 4 {
		t.Fatalf("version/absent-series grid: %s", output)
	}

	duck = exec.Command("duckdb", "-json", "-batch")
	duck.Stdin = strings.NewReader("SET TimeZone='Asia/Tokyo';\n" + query[:strings.Index(query, "\n select metric,count(*)")] + `
 select max(abs(epoch_us(observed_at)-observed_us)) as timestamp_error_us,
        count(*) filter(where valid) as valid_samples
 from ride_metric_samples;`)
	output, err = duck.CombinedOutput()
	if err != nil {
		t.Fatalf("timezone SQL: %v\n%s", err, output)
	}
	var instants []struct {
		Error float64 `json:"timestamp_error_us"`
		Valid int     `json:"valid_samples"`
	}
	if err := json.Unmarshal(output, &instants); err != nil {
		t.Fatal(err)
	}
	if len(instants) != 1 || instants[0].Error != 0 || instants[0].Valid < 6 {
		t.Fatalf("UTC observation shifted by local timezone: %s", output)
	}

	duck = exec.Command("duckdb", "-json", "-batch")
	duck.Stdin = strings.NewReader(query[:strings.Index(query, "\n select metric,count(*)")] + `
 update metrics set value=0 where metric='ride_metrics_ready'
  and ts=(select distinct ts from metrics order by ts limit 1 offset 1);
 select sum(delta) as total,count(delta) as valid_intervals
 from ride_metric_intervals where metric='dispatch_wait_sum';`)
	output, err = duck.CombinedOutput()
	if err != nil {
		t.Fatalf("busy boundary SQL: %v\n%s", err, output)
	}
	var busy []struct {
		Total float64
		Valid int `json:"valid_intervals"`
	}
	if err := json.Unmarshal(output, &busy); err != nil {
		t.Fatal(err)
	}
	if len(busy) != 1 || busy[0].Total != 50 || busy[0].Valid != 1 {
		t.Fatalf("counter delta crossed unready boundary: %s", output)
	}

}
