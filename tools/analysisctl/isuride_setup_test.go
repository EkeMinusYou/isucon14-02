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
		if row.Rows == 0 {
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

func checkISURIDESummarySQL(t *testing.T, fixtures, query string) {
	t.Helper()
	var sql strings.Builder
	result, err := os.ReadFile("../contest/analysis-schema/bench-errors-semantic.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql.Write(result)
	sql.WriteString("create table load_windows(run_id varchar, started_at timestamptz, ended_at timestamptz, status varchar);\n")
	sql.WriteString(fixtures)
	for _, path := range []string{"bench-progress.sql", "bench-summary.sql"} {
		data, err := os.ReadFile("../contest/analysis-schema/" + path)
		if err != nil {
			t.Fatal(err)
		}
		sql.Write(data)
	}
	sql.WriteString(query)
	cmd := exec.Command("duckdb", "-json", "-batch")
	cmd.Stdin = strings.NewReader(sql.String())
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
			t.Fatalf("%s failed: %s", name, output)
		}
	}
}

func TestISURIDETypedSummaries(t *testing.T) {
	fixtures := `insert into load_windows values ('night', '2026-10-10T23:59:58+09:00', '2026-10-11T00:00:01+09:00', 'ok');
insert into bench_error_logs values ('night', 'time=00:00:02.000 level=INFO msg=最終地域情報 名前="Region East" ユーザー登録数=0 アクティブユーザー数=0
time=00:00:03.000 level=INFO msg=最終オーナー情報 名前="椅子 \"研究\" 社" 売上=3000000000 椅子数=4
time=00:00:04.000 level=INFO msg=100.0%のライドは椅子がマッチされるまでの時間、66.7%のライドはマッチされた椅子が乗車地点までに掛かる時間、0.0%のライドは椅子の実移動時間に不満がありました
time=00:00:05.000 level=INFO msg=結果 pass=false スコア=0 種別エラー数=map[correctness:1 network timeout:2]', 'bench.log'),
('20261011-050000', 'time=05:00:01.000 level=INFO msg=最終地域情報 名前=地域 ユーザー登録数=5 アクティブユーザー数=3
time=05:00:02.000 level=INFO msg=最終オーナー情報 名前=椅子工藝舎 売上=0 椅子数=4
time=05:00:03.000 level=INFO msg=結果 pass=true スコア=12 種別エラー数=map[]', 'bench.log'),
('bad', 'time=00:00:02.000 level=INFO msg=最終地域情報 名前=地域 ユーザー登録数=unknown アクティブユーザー数=3
time=00:00:03.000 level=INFO msg=結果 pass=true スコア=12 種別エラー数=map[broken]
time=00:00:04.000 level=INFO msg=unknown format', 'bench.log');`
	query := `select
(select count(*) = 1 and bool_and(parsed and region_name = 'Region East' and registered_users = 0 and active_users = 0 and elapsed_s = 4 and reported_at = '2026-10-11T00:00:02+09:00'::timestamptz) from bench_final_regions where run_id='night') as midnight_region,
(select count(*) = 1 and bool_and(parsed and owner_name = '椅子 "研究" 社' and sales = 3000000000 and chairs = 4) from bench_final_owners where run_id='night') as quoted_owner,
(select count(*) = 1 and bool_and(parsed and matching_pct=100 and pickup_pct=66.7 and travel_pct=0 and not in_load_window and denominator is null) from bench_latest_dissatisfaction where run_id='night') as outside_ratios,
(select parsed and not passed and score=0 and reported_error_count=3 and error_types['network timeout']=2 from bench_completion where run_id='night') as failure_counts,
(select count(*)=2 and sum(reported_count)=3 from bench_error_types where run_id='night') as error_types,
(select count(*)=1 and bool_and(parsed and reported_at is not null and elapsed_s is null) from bench_final_regions where run_id='20261011-050000') as missing_window,
(select parsed and reported_error_count=0 and cardinality(error_types)=0 from bench_completion where run_id='20261011-050000') as empty_errors,
(select count(*)=1 and bool_and(not parsed and registered_users is null) from bench_final_regions where run_id='bad') as malformed_region,
(select not parsed and reported_error_count is null and error_types is null from bench_completion where run_id='bad') as malformed_errors,
(select count(*)=3 and count(*) filter (where not parsed)=3 from bench_info_coverage where run_id='bad') as unknown_visible;`
	checkISURIDESummarySQL(t, fixtures, query)
}

func TestISURIDESavedRunTypedSummaries(t *testing.T) {
	log, err := os.ReadFile("../../runs/20261011-035537/bench.log")
	if os.IsNotExist(err) {
		t.Skip("saved participant RUN is absent")
	}
	if err != nil {
		t.Fatal(err)
	}
	fixtures := "insert into bench_error_logs values ('20261011-035537', '" + strings.ReplaceAll(string(log), "'", "''") + "', 'bench.log');\n"
	query := `select
(select count(*)=2 and sum(registered_users)=10 and sum(active_users)=10 and bool_and(parsed) from bench_final_regions) as regions,
(select count(*)=5 and sum(sales)=25700 and sum(chairs)=20 and bool_and(parsed) from bench_final_owners) as owners,
(select count(*)=1 and bool_and(owner_name='Skyline Taxis' and sales=0 and chairs=4) from bench_final_owners where owner_name='Skyline Taxis') as spaced_name,
(select matching_pct=100 and pickup_pct=66.7 and travel_pct=100 and parsed from bench_latest_dissatisfaction) as latest_ratios,
(select count(*)=11 and bool_and(parsed) from bench_registrations) as registrations,
(select score=671 and passed and reported_error_count=0 and parsed from bench_completion) as completion,
(select count(*)=31 and bool_and(parsed) from bench_info_coverage) as all_info_decoded;`
	checkISURIDESummarySQL(t, fixtures, query)
}

func TestISURIDEChairGrowth(t *testing.T) {
	fixtures := `insert into bench_error_logs values ('20261011-050000', 'time=05:00:01.000 level=INFO msg=一定の売上が立ったためオーナーの椅子が増加します 名前="Owner East" 増加数=0
time=05:00:02.000 level=INFO msg=一定の売上が立ったためオーナーの椅子が増加します 名前=Owner 増加数=unknown', 'bench.log');`
	query := `select
(select count(*)=1 and bool_and(parsed and owner_name='Owner East' and added_chairs=0 and value_semantics='event' and reason='sales_threshold') from bench_chair_growth where line_number=1) as quoted_zero,
(select count(*)=1 and bool_and(not parsed and added_chairs is null) from bench_chair_growth where line_number=2) as unknown_count,
(select count(*)=2 and count(*) filter (where not parsed)=1 from bench_info_coverage) as growth_coverage;`
	checkISURIDESummarySQL(t, fixtures, query)
	t.Run("saved RUN", func(t *testing.T) {
		log, err := os.ReadFile("../../runs/20261011-041159/bench.log")
		if os.IsNotExist(err) {
			t.Skip("saved participant RUN is absent")
		}
		if err != nil {
			t.Fatal(err)
		}
		fixtures := "insert into bench_error_logs values ('20261011-041159', '" + strings.ReplaceAll(string(log), "'", "''") + "', 'bench.log');\n"
		query := `select
(select count(*)=1 and bool_and(parsed and owner_name='晴れ道タクシー' and added_chairs=1) from bench_chair_growth) as growth,
(select count(*)=32 and bool_and(parsed) from bench_info_coverage) as complete_info;`
		checkISURIDESummarySQL(t, fixtures, query)
	})
}
