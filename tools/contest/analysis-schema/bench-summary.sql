-- Completion summaries must survive missing load markers and sit outside the
-- load window. Preserve source line identity, including malformed known lines.
-- Quoted slog names are JSON strings; this handles spaces and escaped quotes.
create or replace macro isuride_log_name(raw_name) as
    case when starts_with(raw_name, '"') then
        json_extract_string(try_cast(raw_name as json), '$')
    else nullif(raw_name, '') end;

create or replace view bench_final_regions as
with parsed as (
    select *, regexp_extract(line,
        'msg=最終地域情報 名前=("(?:[^"\\]|\\.)*"|.*?) ユーザー登録数=([0-9]+) アクティブユーザー数=([0-9]+)$',
        ['name', 'registered', 'active']) as fields
    from bench_log_lines
    where regexp_matches(line, '^time=\S+ level=INFO (?:source=\S+ )?msg=最終地域情報 ')
)
select run_id, source_artifact, line_number, reported_at, elapsed_s,
    'completion_summary' as time_semantics,
    isuride_log_name(fields.name) as region_name,
    try_cast(fields.registered as bigint) as registered_users,
    try_cast(fields.active as bigint) as active_users,
    region_name is not null and registered_users is not null and active_users is not null as parsed,
    line as raw_line
from parsed;

create or replace view bench_final_owners as
with parsed as (
    select *, regexp_extract(line,
        'msg=最終オーナー情報 名前=("(?:[^"\\]|\\.)*"|.*?) 売上=([0-9]+) 椅子数=([0-9]+)$',
        ['name', 'sales', 'chairs']) as fields
    from bench_log_lines
    where regexp_matches(line, '^time=\S+ level=INFO (?:source=\S+ )?msg=最終オーナー情報 ')
)
select run_id, source_artifact, line_number, reported_at, elapsed_s,
    'completion_summary' as time_semantics,
    isuride_log_name(fields.name) as owner_name,
    try_cast(fields.sales as bigint) as sales,
    try_cast(fields.chairs as bigint) as chairs,
    owner_name is not null and sales is not null and chairs is not null as parsed,
    line as raw_line
from parsed;

-- These wide views keep every report, including the last ratios printed after
-- the load period. bench_progress keeps its existing in-window long contract.
create or replace view bench_registrations as
with parsed as (
    select *, regexp_extract(line,
        'msg=これまでに地域内の評判によって([0-9]+)人、既存ユーザーの招待経由で([0-9]+)人が新規登録しました$',
        ['reputation', 'invitation']) as fields
    from bench_log_lines
    where regexp_matches(line, '^time=\S+ level=INFO (?:source=\S+ )?msg=これまでに地域内の評判によって')
)
select run_id, source_artifact, line_number, reported_at, elapsed_s, in_load_window,
    'cumulative' as value_semantics,
    try_cast(fields.reputation as bigint) as registrations_reputation,
    try_cast(fields.invitation as bigint) as registrations_invitation,
    registrations_reputation is not null and registrations_invitation is not null as parsed,
    line as raw_line
from parsed;

create or replace view bench_dissatisfaction as
with parsed as (
    select *, regexp_extract(line,
        'msg=([0-9.]+)%のライドは椅子がマッチされるまでの時間、([0-9.]+)%のライドはマッチされた椅子が乗車地点までに掛かる時間、([0-9.]+)%のライドは椅子の実移動時間に不満がありました$',
        ['matching', 'pickup', 'travel']) as fields
    from bench_log_lines
    where regexp_matches(line, '^time=\S+ level=INFO (?:source=\S+ )?msg=[0-9.]+%のライドは')
)
select run_id, source_artifact, line_number, reported_at, elapsed_s, in_load_window,
    'ratio' as value_semantics, 'percent' as unit,
    try_cast(fields.matching as double) as matching_pct,
    try_cast(fields.pickup as double) as pickup_pct,
    try_cast(fields.travel as double) as travel_pct,
    cast(null as bigint) as denominator,
    coalesce(matching_pct between 0 and 100 and pickup_pct between 0 and 100 and travel_pct between 0 and 100, false) as parsed,
    line as raw_line
from parsed;

-- The most recent reported ratios, without inferring that an incomplete RUN
-- finished successfully. Missing ratios stay absent rather than becoming zero.
create or replace view bench_latest_dissatisfaction as
select * from bench_dissatisfaction
qualify row_number() over (partition by run_id order by line_number desc) = 1;

-- A Go map summary contains reported counts, not individual occurrence times.
-- Validate a complete round trip before interpreting its entries. Unknown or
-- malformed maps stay NULL; only an explicit map[] becomes an empty map/zero.
create or replace view bench_completion as
with parsed as (
    select *, regexp_extract(line,
        'msg=結果 pass=(true|false) スコア=([0-9]+) 種別エラー数=(map\[.*\])$',
        ['passed', 'score', 'errors']) as fields
    from bench_log_lines
    where regexp_matches(line, '^time=\S+ level=INFO (?:source=\S+ )?msg=結果 ')
), entries as (
    select *, regexp_extract(fields.errors, '^map\[(.*)\]$', 1) as error_body,
        regexp_extract_all(error_body, '([^:]+):([0-9]+)', ['name', 'count']) as error_entries
    from parsed
), validated as (
    select *, fields.errors <> ''
        and array_to_string(list_transform(error_entries, lambda e: trim(e.name) || ':' || e.count), ' ') = error_body
        and len(list_filter(error_entries, lambda e: trim(e.name) = '' or try_cast(e.count as bigint) is null)) = 0
        and list_unique(list_transform(error_entries, lambda e: trim(e.name))) = len(error_entries) as errors_parsed
    from entries
)
select run_id, source_artifact, line_number, reported_at, elapsed_s,
    'completion_summary' as time_semantics,
    try_cast(fields.passed as boolean) as passed,
    try_cast(fields.score as bigint) as score,
    nullif(fields.errors, '') as error_types_raw,
    case when errors_parsed then map_from_entries(list_transform(error_entries,
        lambda e: struct_pack(key := trim(e.name), value := try_cast(e.count as bigint))))
        else cast(null as map(varchar, bigint)) end as error_types,
    case when errors_parsed then coalesce(list_sum(list_transform(error_entries,
        lambda e: try_cast(e.count as bigint))), 0)::bigint end as reported_error_count,
    passed is not null and score is not null and errors_parsed as parsed,
    line as raw_line
from validated;

create or replace view bench_error_types as
select c.run_id, c.source_artifact, c.line_number, c.reported_at, c.time_semantics,
    e.entry.key as error_type, e.entry.value as reported_count
from bench_completion c, unnest(map_entries(c.error_types)) e(entry);

-- Identify new output formats and malformed known fields instead of silently
-- treating absent rows as zeros. DEBUG/internal fields remain raw-only.
create or replace view bench_info_coverage as
with decoded as (
    select run_id, line_number, 'final_region' as record_type, parsed from bench_final_regions
    union all select run_id, line_number, 'final_owner', parsed from bench_final_owners
    union all select run_id, line_number, 'registrations', parsed from bench_registrations
    union all select run_id, line_number, 'dissatisfaction', parsed from bench_dissatisfaction
    union all select run_id, line_number, 'completion', parsed from bench_completion
)
select l.run_id, l.source_artifact, l.line_number, l.reported_at,
    coalesce(d.record_type, case
        when regexp_matches(l.line, 'msg=負荷走行を開始します$') then 'load_started'
        when regexp_matches(l.line, 'msg=負荷走行が終了しました$') then 'load_ended'
        else 'unknown' end) as record_type,
    coalesce(d.parsed, regexp_matches(l.line, 'msg=負荷走行(?:を開始します|が終了しました)$'), false) as parsed,
    l.line as raw_line
from bench_log_lines l left join decoded d using (run_id, line_number)
where regexp_matches(l.line, '^time=\S+ level=INFO ');
