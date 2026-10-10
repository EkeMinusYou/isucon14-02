-- Interpret participant INFO progress only; DEBUG ticks/evaluation arrays are
-- internal fields with unknown semantics and are deliberately not decoded.
-- This log uses JST clock times without a date. Resolve the closest date to
-- the load window start, including runs crossing midnight.
create or replace view bench_progress_lines as
with lines as (
    select b.run_id, b.source_artifact, l.line_number, l.line, w.started_at, w.ended_at,
        try_cast(strftime(w.started_at at time zone 'Asia/Tokyo', '%Y-%m-%d') || 'T' ||
            regexp_extract(l.line, '^time=([0-9:.]+)', 1) || '+09:00' as timestamptz) as clock_at
    from bench_error_logs b
    join load_windows w using (run_id),
    unnest(string_split(b.content, chr(10))) with ordinality as l(line, line_number)
    where w.status = 'ok' and regexp_matches(l.line, '^time=\S+ level=INFO ')
), dated as (
    select *, case
        when clock_at < started_at - interval '12 hours' then clock_at + interval '1 day'
        when clock_at > started_at + interval '12 hours' then clock_at - interval '1 day'
        else clock_at end as reported_at
    from lines
)
select run_id, source_artifact, line_number, line, reported_at,
    date_diff('millisecond', started_at, reported_at) / 1000.0 as elapsed_s,
    reported_at >= started_at and reported_at < ended_at as in_load_window
from dated where reported_at is not null;

-- Counts are cumulative people; dissatisfaction values are percentages.
-- The output provides no denominator for these ratios, so it stays unknown.
create or replace view bench_progress as
with registration as (
    select *, regexp_extract(line,
        'msg=これまでに地域内の評判によって([0-9]+)人、既存ユーザーの招待経由で([0-9]+)人が新規登録しました',
        ['reputation', 'invitation']) as fields
    from bench_progress_lines
), dissatisfaction as (
    select *, regexp_extract(line,
        'msg=([0-9.]+)%のライドは椅子がマッチされるまでの時間、([0-9.]+)%のライドはマッチされた椅子が乗車地点までに掛かる時間、([0-9.]+)%のライドは椅子の実移動時間に不満がありました',
        ['matching', 'pickup', 'travel']) as fields
    from bench_progress_lines
), values as (
    select run_id, source_artifact, line_number, reported_at, elapsed_s, in_load_window,
        v.metric, try_cast(v.value as double) as value, 'people' as unit, 'cumulative' as value_semantics
    from registration, lateral (values
        ('registrations_reputation', fields.reputation),
        ('registrations_invitation', fields.invitation)) v(metric, value)
    union all
    select run_id, source_artifact, line_number, reported_at, elapsed_s, in_load_window,
        v.metric, try_cast(v.value as double) as value, 'percent' as unit, 'ratio' as value_semantics
    from dissatisfaction, lateral (values
        ('dissatisfaction_matching', fields.matching),
        ('dissatisfaction_pickup', fields.pickup),
        ('dissatisfaction_travel', fields.travel)) v(metric, value)
)
select *, cast(null as bigint) as denominator
from values where value is not null and in_load_window;
