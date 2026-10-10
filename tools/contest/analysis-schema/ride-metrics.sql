-- Fixed v1/v2 application vocabulary, units and histogram boundaries.
-- v1 pickup_b9_64 covers 9..64; v2 pickup_b9_plus covers every batch >8.
-- The existing dispatch_batch infinity bucket includes plans larger than 64.
create or replace view ride_histogram_definitions as
select * from (values
 ('dispatch_wait', 'created_to_assignment', 'ok', 'all', 'ms', 10,100,1000,3000),
 ('dispatch_batch', 'assignment_batch', 'ok', 'all', 'rides', 1,8,32,64),
 ('pickup_b1', 'estimated_pickup', 'known', '1', 'ms', 10,100,1000,3000),
 ('pickup_b2_8', 'estimated_pickup', 'known', '2_8', 'ms', 10,100,1000,3000),
 ('pickup_b9_64', 'estimated_pickup', 'known', '9_64', 'ms', 10,100,1000,3000),
 ('pickup_b9_plus', 'estimated_pickup', 'known', '9_plus', 'ms', 10,100,1000,3000),
 ('assign_flush_chair', 'assignment_to_flush', 'ok', 'chair', 'ms', 10,100,1000,3000),
 ('assign_flush_app', 'assignment_to_flush', 'ok', 'app', 'ms', 10,100,1000,3000),
 ('eval_total_ok', 'evaluation_total', 'ok', 'all', 'ms', 10,100,1000,3000),
 ('eval_total_fail', 'evaluation_total', 'fail', 'all', 'ms', 10,100,1000,3000),
 ('eval_lock_ok', 'evaluation_user_mutex', 'ok', 'all', 'ms', 10,100,1000,3000),
 ('eval_sql_ok', 'evaluation_sync_sql', 'ok', 'all', 'ms', 10,100,1000,3000),
 ('eval_sql_fail', 'evaluation_sync_sql', 'fail', 'all', 'ms', 10,100,1000,3000),
 ('eval_payment_ok', 'evaluation_payment', 'ok', 'all', 'ms', 10,100,1000,3000),
 ('eval_payment_fail', 'evaluation_payment', 'fail', 'all', 'ms', 10,100,1000,3000),
 ('eval_complete_ok', 'evaluation_completion', 'ok', 'all', 'ms', 10,100,1000,3000),
 ('eval_complete_fail', 'evaluation_completion', 'fail', 'all', 'ms', 10,100,1000,3000)
) as t(series,stage,outcome,cohort,unit,b0,b1,b2,b3);

create or replace view ride_metric_definitions as
select series || suffix as metric, series, stage, outcome, cohort,
       case when suffix='_sum' then unit else 'events' end as unit,
       'counter' as kind, suffix,
       case suffix when '_b0' then b0 when '_b1' then b1 when '_b2' then b2
                   when '_b3' then b3 when '_inf' then 'Infinity'::double end as bucket_upper
from ride_histogram_definitions
cross join (values ('_n'),('_sum'),('_b0'),('_b1'),('_b2'),('_b3'),('_inf')) as s(suffix)
union all
select metric, metric as series, stage, outcome, cohort, unit, kind, NULL, NULL
from (values
 ('dispatch_waiting','unassigned_rides','current','all','rides','gauge'),
 ('dispatch_oldest_ms','oldest_unassigned_age','current','all','ms','gauge'),
 ('dispatch_available','matching_available_chairs','current','all','chairs','gauge'),
 ('dispatch_notify_blocked','notification_blocked_chairs','current','all','chairs','gauge'),
 ('pickup_b1_unknown','estimated_pickup','unknown','1','rides','counter'),
 ('pickup_b2_8_unknown','estimated_pickup','unknown','2_8','rides','counter'),
 ('pickup_b9_64_unknown','estimated_pickup','unknown','9_64','rides','counter'),
 ('pickup_b9_plus_unknown','estimated_pickup','unknown','9_plus','rides','counter'),
 ('eval_payment_ok_retries','payment_retries','ok','all','retries','counter'),
 ('eval_payment_fail_retries','payment_retries','fail','all','retries','counter'),
 ('ride_metrics_epoch','epoch','current','all','unix_us','metadata'),
 ('ride_metrics_ready','ready','current','all','boolean','metadata'),
 ('ride_metrics_version','version','current','all','version','metadata')
) as t(metric,stage,outcome,cohort,unit,kind);

-- This entity is already produced by the generic SQL-pool collector.
create or replace view ride_metric_samples as
with samples as (
 select run_id,host,ts,
        max(value) filter(where metric='ride_metrics_epoch') as epoch,
        max(value) filter(where metric='ride_metrics_ready') as ready,
        max(value) filter(where metric='ride_metrics_version') as version
 from metrics where source='sql_pool' and entity='local:metrics:snapshot:0'
 group by run_id,host,ts
)
select *,
 lag(ts) over w as previous_ts,
 lag(epoch) over w as previous_epoch,
 lag(ready) over w as previous_ready,
 lag(version) over w as previous_version
from samples window w as (partition by run_id,host order by ts);

-- Missing samples/metrics and reset boundaries never become zero increments.
-- A collection gap has its actual elapsed interval, not an assumed one second.
create or replace view ride_metric_intervals as
with observed as (
 select m.run_id,m.host,m.ts,m.value,d.*,
        lag(m.value) over w as previous_value,
        lag(m.ts) over w as metric_previous_ts
 from metrics m join ride_metric_definitions d using(metric)
 where source='sql_pool' and entity='local:metrics:snapshot:0'
 window w as (partition by m.run_id,m.host,m.metric order by m.ts)
)
select o.*,s.epoch,s.previous_ts as interval_start,
       epoch_ms(o.ts)-epoch_ms(s.previous_ts) as interval_ms,
       s.ready=1 and s.version in (1,2) and s.epoch is not null as sample_valid,
       case when kind='counter' and s.ready=1 and s.previous_ready=1
             and s.version in (1,2) and s.previous_version=s.version and s.epoch=s.previous_epoch
             and o.metric_previous_ts=s.previous_ts and o.ts>s.previous_ts
             and o.value>=o.previous_value
            then o.value-o.previous_value end as delta
from observed o join ride_metric_samples s using(run_id,host,ts);

create or replace view ride_metric_gauges as
select run_id,host,ts,epoch,stage,unit,metric,
       case when sample_valid then value end as value
from ride_metric_intervals where kind='gauge';

create or replace view ride_metric_histograms as
select run_id,host,ts,interval_start,interval_ms,epoch,series,stage,outcome,cohort,
       h.unit as observation_unit,
       max(delta) filter(where suffix='_n') as events,
       max(delta) filter(where suffix='_sum') as total,
       max(delta) filter(where suffix='_sum') / nullif(max(delta) filter(where suffix='_n'),0) as average,
       max(delta) filter(where suffix='_b0') as le_b0,
       max(delta) filter(where suffix='_b1') as le_b1,
       max(delta) filter(where suffix='_b2') as le_b2,
       max(delta) filter(where suffix='_b3') as le_b3,
       max(delta) filter(where suffix='_inf') as le_inf,
       h.b0,h.b1,h.b2,h.b3
from ride_metric_intervals i join ride_histogram_definitions h using(series,stage,outcome,cohort)
group by run_id,host,ts,interval_start,interval_ms,epoch,series,stage,outcome,cohort,h.unit,h.b0,h.b1,h.b2,h.b3;
