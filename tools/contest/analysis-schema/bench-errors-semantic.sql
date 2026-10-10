-- bench.log のエラー行を意味ビューにする、当日のベンチマーカー書式に依存するschema。
-- 生テキストの取り込みは汎用側（tools/analysis/schema/bench-errors.sql が
-- bench_error_logs へ入れる）が担当し、ここは行の解釈だけを持つ。
--
-- Portal results use the explicit markers recorded by task bench-manual.
-- Participant warning/progress formats are adapted after the first user RUN.
-- ビュー名と列はdashboard（tools/dashboard/server/bench_errors.go、scores.go）とREADMEの契約なので
-- 変えない。書き換えるまでは、いずれも0行を返す。
-- 書式に依存するテストは tools/dashboard/server/<contest>_test.go へ置き、
-- tools/template/paths.txt の [contest] へ加える。
create table if not exists bench_error_logs (
    run_id varchar, content varchar, source_artifact varchar
);

-- 1RUNにつき1行のベンチ結果。manifestにscore/passedが無いRUNをdashboardが補う。
create or replace view bench_results as
select
    run_id,
    try_cast(list_last(regexp_extract_all(content, 'SCORE:\s*([0-9]+)', 1)) as bigint) as score,
    case
        when regexp_matches(content, 'BENCHMARK_FAIL') then false
        when regexp_matches(content, 'BENCHMARK_PASS') then true
        else cast(null as boolean)
    end as passed,
    cast(null as bigint) as addition,
    cast(null as bigint) as deduction,
    source_artifact
from bench_error_logs;

-- ベンチマーカーが操作別に出す得点。
create or replace view bench_score_routes as
select
    cast(null as varchar) as run_id,
    cast(null as varchar) as method,
    cast(null as varchar) as route,
    cast(null as bigint) as points,
    cast(null as varchar) as source_artifact
where false;

-- 報告された1エラーにつき1行。発生した全エラーではない場合があるので、
-- time_semantics にその行の時刻の意味を入れる（例: 完了時のまとめなら 'completion_summary'）。
create or replace view bench_errors as
select
    cast(null as varchar) as run_id,
    cast(null as bigint) as line_number,
    cast(null as time) as reported_time,
    cast(null as bigint) as error_index,
    cast(null as varchar) as message,
    cast(null as varchar) as time_semantics,
    cast(null as varchar) as source_artifact
where false;

-- 発生時刻を持つ構造化ログ行だけをここへ入れる。まとめ出力の時刻はここへ入れない。
create or replace view bench_warning_events as
select
    cast(null as varchar) as run_id,
    cast(null as timestamptz) as occurred_at
where false;

-- 報告時点の累積エラー件数。個々のエラーの発生時刻ではない。
-- elapsed_s は負荷走行開始からの経過秒。
create or replace view bench_error_counts as
select
    cast(null as varchar) as run_id,
    cast(null as bigint) as line_number,
    cast(null as double) as elapsed_s,
    cast(null as bigint) as error_count
where false;
