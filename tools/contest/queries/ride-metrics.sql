-- Set ride_run before executing. Optional ride_window_start/end (TIMESTAMPTZ)
-- choose the exact common window, e.g. participant INFO +30..60 seconds.
-- This temporary view is allowed by task q's read-only database connection.
create or replace temp view ride_comparison_window as
select run_id,
 coalesce(getvariable('ride_window_start')::timestamptz,started_at) as started_at,
 coalesce(getvariable('ride_window_end')::timestamptz,ended_at) as ended_at,
 (epoch_us(coalesce(getvariable('ride_window_end')::timestamptz,ended_at))-
  epoch_us(coalesce(getvariable('ride_window_start')::timestamptz,started_at)))/1000000.0 as duration_seconds,
 status,reason
from load_windows where run_id=getvariable('ride_run');
select name,status,reason,quality_status,quality_window_coverage_pct,quality_max_gap_ms
from artifacts where run_id=getvariable('ride_run') and name like '%sql-pool-metrics.tsv';

select w.*,
       (select count(*) from ride_metric_samples s where s.run_id=w.run_id
         and s.observed_at between w.started_at and w.ended_at and s.ready=1 and s.version in (1,2,3) and s.valid) as ready_samples
from ride_comparison_window w where w.run_id=getvariable('ride_run');

select g.host,g.epoch,g.metric,g.unit,count(g.value) as samples,
       count(*) filter(where g.value is null) as missing_samples,
       avg(g.value) as sample_average,max(g.value) as sample_max
from ride_metric_gauges g join ride_comparison_window w using(run_id)
where g.run_id=getvariable('ride_run') and g.ts between w.started_at and w.ended_at
group by g.host,g.epoch,g.metric,g.unit order by g.host,g.epoch,g.metric;

-- Require the entire interval in the window and all histogram fields present.
-- Count missing intervals separately; never sum NULL as observed zero work.
with h as (
 select h.*,
        events is not null and total is not null and le_b0 is not null
          and le_b1 is not null and le_b2 is not null and le_b3 is not null
          and le_inf is not null as complete
 from ride_metric_histograms h join ride_comparison_window w using(run_id)
 where h.run_id=getvariable('ride_run') and h.interval_start>=w.started_at and h.ts<=w.ended_at
)
select host,epoch,stage,outcome,cohort,observation_unit,
       count(*) filter(where complete) as complete_intervals,
       sum(interval_ms) filter(where complete)/1000.0 as observed_seconds,
       count(*) filter(where not complete) as missing_intervals,
       sum(events) filter(where complete) as events,
       sum(total) filter(where complete) as total,
       sum(total) filter(where complete)/nullif(sum(events) filter(where complete),0) as average,
       sum(le_b0) filter(where complete) as le_b0,
       sum(le_b1) filter(where complete) as le_b1,
       sum(le_b2) filter(where complete) as le_b2,
       sum(le_b3) filter(where complete) as le_b3,
       sum(le_inf) filter(where complete) as le_inf,
       max(b0) as b0,max(b1) as b1,max(b2) as b2,max(b3) as b3
from h group by host,epoch,stage,outcome,cohort,observation_unit
order by host,epoch,stage,outcome,cohort;

select i.host,i.epoch,i.metric,i.unit,count(i.delta) as valid_intervals,
       count(*) filter(where i.delta is null) as missing_intervals,
       sum(i.interval_ms) filter(where i.delta is not null)/1000.0 as observed_seconds,
       sum(i.delta) as total,
       sum(i.delta)/nullif(sum(i.interval_ms) filter(where i.delta is not null)/1000.0,0) as total_per_second
from ride_metric_intervals i join ride_comparison_window w using(run_id)
where i.run_id=getvariable('ride_run') and i.interval_start>=w.started_at and i.ts<=w.ended_at
 and i.kind='counter' and i.suffix is null
group by i.host,i.epoch,i.metric,i.unit order by i.host,i.epoch,i.metric;

-- Rates and stage durations share the same interval, host and epoch.
select i.host,i.epoch,i.ts,i.interval_start,i.interval_ms,
       i.delta as assignments,i.delta*1000.0/nullif(i.interval_ms,0) as assignments_per_second,
       h.events as evaluation_completions,h.total as completion_ms
from ride_metric_intervals i
left join ride_metric_histograms h on h.run_id=i.run_id and h.host=i.host
 and h.ts=i.ts and h.epoch=i.epoch and h.series='eval_complete_ok'
join ride_comparison_window w on w.run_id=i.run_id
where i.run_id=getvariable('ride_run') and i.metric='dispatch_wait_n'
 and i.interval_start>=w.started_at and i.ts<=w.ended_at order by i.host,i.ts;

-- Snapshot failures remain observations; no old cached values are accepted.
select s.host,s.version,s.ready,s.snapshot_status,count(*) as samples,
 min(s.observed_at) as first_observed_at,max(s.observed_at) as last_observed_at,
 max(s.acquire_ms) as max_acquire_ms,max(s.export_ms) as max_export_ms,
 max(s.capture_age_ms) as max_capture_age_ms
from ride_metric_samples s join ride_comparison_window w using(run_id)
where s.observed_at between w.started_at and w.ended_at
group by s.host,s.version,s.ready,s.snapshot_status order by s.host,s.snapshot_status;

-- Integrated stage chair-ms includes unfinished and restored work. Unlike
-- closed-stage histogram averages, this measures occupied capacity in-window.
select host,epoch,metric,unit,
 count(delta) as valid_intervals,count(*) filter(where delta is null) as missing_intervals,
 sum(interval_ms) filter(where delta is not null)/1000.0 as observed_seconds,
 sum(delta)/1000.0 as chair_seconds,
 sum(delta)/nullif(sum(interval_ms) filter(where delta is not null),0) as mean_occupied_chairs
from ride_metric_intervals i join ride_comparison_window w using(run_id)
where starts_with(metric,'flow_') and ends_with(metric,'_total_ms')
 and interval_start>=w.started_at and ts<=w.ended_at
group by host,epoch,metric,unit order by host,epoch,metric;

-- CPU seconds and capacity use proc/service samples in this exact same window.
-- One CPU sample represents one second, as in the existing resource_demand
-- contract. Host and service demand overlap and must never be added together.
with in_window as (
 select m.* from metrics m join ride_comparison_window w using(run_id)
 where m.ts>=w.started_at and m.ts<w.ended_at and m.source in ('proc','service')
), hosts as (
 select host,max(value) filter(where metric='cpu_count') as cpu_count,
 count(*) filter(where metric='cpu_busy_pct') as samples,
 sum(value) filter(where metric='cpu_busy_pct')/100.0 as busy_core_equivalent,
 quantile_cont(value,0.95) filter(where metric='cpu_busy_pct') as busy_p95_pct,
 avg(value) filter(where metric='cpu_pressure_some_avg10') as pressure_avg10
 from in_window where source='proc' group by host
), available as (
 select host,entity,ts from in_window where source='service' and metric='available' and value=1
), services as (
 select m.host,m.entity,count(*) as samples,sum(m.value)/100.0 as cpu_seconds
 from in_window m join available a using(host,entity,ts)
 where m.source='service' and m.metric='cpu_pct' group by m.host,m.entity
)
select h.host,'host' as scope,NULL as service,h.samples,
 h.busy_core_equivalent*h.cpu_count as cpu_seconds,
 h.samples*h.cpu_count as observed_capacity_cpu_seconds,
 w.duration_seconds*h.cpu_count as window_capacity_cpu_seconds,
 h.busy_p95_pct,h.pressure_avg10
from hosts h cross join ride_comparison_window w
union all
select s.host,'service',s.entity,s.samples,s.cpu_seconds,
 s.samples*h.cpu_count,w.duration_seconds*h.cpu_count,NULL,NULL
from services s join hosts h using(host) cross join ride_comparison_window w
order by host,scope,service;

-- HTTP coordinate latency/throughput and evaluation/matching request counts
-- remain distinct from accepted coordinates, assignments and planning attempts.
select t.ingress_host,t.method,t.route,sum(t.requests) as requests,
 sum(t.status_2xx) as success_2xx,sum(t.status_5xx) as failures_5xx,
 sum(t.sum_time_sec)*1000.0/nullif(sum(t.requests),0) as mean_response_ms,
 sum(t.requests)/nullif(max(w.duration_seconds),0) as requests_per_second
from http_traffic t join ride_comparison_window w using(run_id)
where t.ts>=w.started_at and t.ts<w.ended_at
 and (contains(t.route,'coordinate') or contains(t.route,'evaluation')
      or contains(t.route,'matching') or t.route='^/api/app/rides$')
group by t.ingress_host,t.method,t.route order by t.ingress_host,t.method,t.route;

-- Participant invitation/reputation growth and departure reports keep their
-- original report instant and cumulative populations; absent reports are NULL.
select b.reported_at,b.line
from bench_log_lines b join ride_comparison_window w using(run_id)
where b.reported_at>=w.started_at and b.reported_at<w.ended_at
 and (contains(b.line,'招待') or contains(b.line,'評判') or contains(b.line,'低評価なライド'))
order by b.reported_at,b.line_number;

select run_id,score,passed from bench_results where run_id=getvariable('ride_run');
