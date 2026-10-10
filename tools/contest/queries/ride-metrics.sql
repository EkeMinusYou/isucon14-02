-- Set ride_run before executing. Queries stay within the same RUN/load window.
select name,status,reason,quality_status,quality_window_coverage_pct,quality_max_gap_ms
from artifacts where run_id=getvariable('ride_run') and name like '%sql-pool-metrics.tsv';

select w.*,
       (select count(*) from ride_metric_samples s where s.run_id=w.run_id
         and s.ts between w.started_at and w.ended_at and s.ready=1 and s.version=1) as ready_samples
from load_windows w where w.run_id=getvariable('ride_run');

select g.host,g.epoch,g.metric,g.unit,count(g.value) as samples,
       avg(g.value) as sample_average,max(g.value) as sample_max
from ride_metric_gauges g join load_windows w using(run_id)
where g.run_id=getvariable('ride_run') and g.ts between w.started_at and w.ended_at
group by g.host,g.epoch,g.metric,g.unit order by g.host,g.epoch,g.metric;

-- Require the entire interval in the window and all histogram fields present.
-- Count missing intervals separately; never sum NULL as observed zero work.
with h as (
 select h.*,
        events is not null and total is not null and le_b0 is not null
          and le_b1 is not null and le_b2 is not null and le_b3 is not null
          and le_inf is not null as complete
 from ride_metric_histograms h join load_windows w using(run_id)
 where h.run_id=getvariable('ride_run') and h.interval_start>=w.started_at and h.ts<=w.ended_at
)
select host,epoch,stage,outcome,cohort,observation_unit,
       count(*) filter(where complete) as complete_intervals,
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
       count(*) filter(where i.delta is null) as missing_intervals,sum(i.delta) as total
from ride_metric_intervals i join load_windows w using(run_id)
where i.run_id=getvariable('ride_run') and i.interval_start>=w.started_at and i.ts<=w.ended_at
 and i.stage in ('payment_retries','estimated_pickup') and i.suffix is null
group by i.host,i.epoch,i.metric,i.unit order by i.host,i.epoch,i.metric;

-- Rates and stage durations share the same interval, host and epoch.
select i.host,i.epoch,i.ts,i.interval_start,i.interval_ms,
       i.delta as assignments,i.delta*1000.0/nullif(i.interval_ms,0) as assignments_per_second,
       h.events as evaluation_completions,h.total as completion_ms
from ride_metric_intervals i
left join ride_metric_histograms h on h.run_id=i.run_id and h.host=i.host
 and h.ts=i.ts and h.epoch=i.epoch and h.series='eval_complete_ok'
join load_windows w on w.run_id=i.run_id
where i.run_id=getvariable('ride_run') and i.metric='dispatch_wait_n'
 and i.interval_start>=w.started_at and i.ts<=w.ended_at order by i.host,i.ts;
