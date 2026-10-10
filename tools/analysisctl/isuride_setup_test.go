package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestISURIDESetupSmokeDuckDB(t *testing.T) {
	dir := os.Getenv("ISUCON_SETUP_SMOKE_DIR")
	if dir == "" {
		t.Skip("set ISUCON_SETUP_SMOKE_DIR to collected setup smoke artifacts")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(filepath.Join(root, "tools/analysis/sources.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	// The profile protobuf adapter is checked separately; no RUN is fabricated.
	cfg.Profiles.Schema = ""
	r := runner{config: cfg}
	active, err := r.activeSources([]string{dir})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := r.buildSchema(active)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	for _, source := range active {
		queries = append(queries, fmt.Sprintf("select '%s' as measurement, count(*) as rows from %s", source.View, source.View))
	}
	cmd := exec.Command("duckdb", "-json", "-batch")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "ISUCON_RUN_GLOB="+dir)
	cmd.Stdin = strings.NewReader(string(schema) + "\n" + strings.Join(queries, " union all ") + ";\n")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("DuckDB schema/analysis failed: %v\n%s", err, output)
	}
	var rows []struct {
		Measurement string `json:"measurement"`
		Rows        int64  `json:"rows"`
	}
	if err := json.Unmarshal(output, &rows); err != nil {
		t.Fatalf("decode DuckDB output: %v\n%s", err, output)
	}
	for _, row := range rows {
		if row.Rows == 0 && row.Measurement != "bench_scenarios_raw" {
			t.Fatalf("measurement %s has no analyzed smoke rows", row.Measurement)
		}
		t.Logf("%s: %d rows", row.Measurement, row.Rows)
	}
	if len(rows) < 12 {
		t.Fatalf("only %d measurement views were checked", len(rows))
	}
	if err := os.WriteFile(filepath.Join(dir, "duckdb-smoke.json"), output, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestISURIDEPortalResultMarkers(t *testing.T) {
	schema, err := os.ReadFile("../contest/analysis-schema/bench-errors-semantic.sql")
	if err != nil {
		t.Fatal(err)
	}
	fixtures := "insert into bench_error_logs values ('pass', 'SCORE: 9\nSCORE: 123\nBENCHMARK_PASS', 'bench.log'), ('fail', 'SCORE: 0\nBENCHMARK_FAIL', 'bench.log'), ('unknown', '', 'bench.log');\n"
	cmd := exec.Command("duckdb", "-json", "-batch")
	cmd.Stdin = strings.NewReader(string(schema) + fixtures + "select run_id,score,passed from bench_results order by run_id;\n")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	var rows []struct {
		ID     string `json:"run_id"`
		Score  *int64 `json:"score"`
		Passed *bool  `json:"passed"`
	}
	if err := json.Unmarshal(output, &rows); err != nil || len(rows) != 3 {
		t.Fatalf("result rows: %v\n%s", err, output)
	}
	if rows[0].Score == nil || *rows[0].Score != 0 || rows[0].Passed == nil || *rows[0].Passed {
		t.Fatalf("failed result was lost: %s", output)
	}
	if rows[1].Score == nil || *rows[1].Score != 123 || rows[1].Passed == nil || !*rows[1].Passed {
		t.Fatalf("latest manual result was lost: %s", output)
	}
	if rows[2].Score != nil || rows[2].Passed != nil {
		t.Fatalf("unknown result was replaced: %s", output)
	}
}
