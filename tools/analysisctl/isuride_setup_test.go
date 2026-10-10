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

func TestISURIDEParticipantResultsAndProgress(t *testing.T) {
	resultSchema, err := os.ReadFile("../contest/analysis-schema/bench-errors-semantic.sql")
	if err != nil {
		t.Fatal(err)
	}
	progressSchema, err := os.ReadFile("../contest/analysis-schema/bench-progress.sql")
	if err != nil {
		t.Fatal(err)
	}
	fixtures := `create table load_windows(run_id varchar, started_at timestamptz, ended_at timestamptz, status varchar);
insert into load_windows values ('pass', '2026-10-10T23:59:58+09:00', '2026-10-11T00:00:10+09:00', 'ok');
insert into bench_error_logs values ('pass', 'time=00:00:01.000 level=INFO msg=これまでに地域内の評判によって2人、既存ユーザーの招待経由で3人が新規登録しました
time=00:00:02.000 level=INFO msg=100.0%のライドは椅子がマッチされるまでの時間、50.0%のライドはマッチされた椅子が乗車地点までに掛かる時間、25.0%のライドは椅子の実移動時間に不満がありました
time=00:00:10.000 level=INFO msg=結果 pass=true スコア=671 種別エラー数=map[]', 'bench.log'),
('fail', 'time=03:56:51.330 level=INFO msg=結果 pass=false スコア=0 種別エラー数=map[]', 'bench.log'),
('unknown', 'time=03:56:51.330 level=INFO msg=負荷走行が終了しました', 'bench.log');`
	query := `select
    (select score = 671 and passed from bench_results where run_id = 'pass') as success,
    (select score = 0 and passed = false from bench_results where run_id = 'fail') as failure,
    (select score is null and passed is null from bench_results where run_id = 'unknown') as unknown,
    (select count(*) = 5 and min(elapsed_s) = 3 and max(elapsed_s) = 4
        and count(*) filter (where value_semantics = 'cumulative' and unit = 'people') = 2
        and count(*) filter (where value_semantics = 'ratio' and unit = 'percent') = 3
        and count(denominator) = 0 from bench_progress) as progress;`
	cmd := exec.Command("duckdb", "-json", "-batch")
	cmd.Stdin = strings.NewReader(string(resultSchema) + fixtures + string(progressSchema) + query)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	var rows []map[string]bool
	if err := json.Unmarshal(output, &rows); err != nil || len(rows) != 1 {
		t.Fatalf("%v\n%s", err, output)
	}
	for name, ok := range rows[0] {
		if !ok {
			t.Fatalf("%s contract failed: %s", name, output)
		}
	}
}
