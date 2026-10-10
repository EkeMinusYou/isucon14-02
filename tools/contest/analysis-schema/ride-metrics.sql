-- Fixed v1/v2/v3 application vocabulary. All durations are elapsed ms, never CPU ms.
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
 ('eval_complete_fail', 'evaluation_completion', 'fail', 'all', 'ms', 10,100,1000,3000),
 ('coord_wait_ok','coordinate_wait','ok','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_wait_fail','coordinate_wait','fail','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_hold_ok','coordinate_hold','ok','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_hold_fail','coordinate_hold','fail','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_json_ok','coordinate_json','ok','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_json_fail','coordinate_json','fail','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_frame_ok','coordinate_frame','ok','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_frame_fail','coordinate_frame','fail','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_write_ok','coordinate_write','ok','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_write_fail','coordinate_write','fail','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_apply_ok','coordinate_apply','ok','sample_1_in_64','ms',0.1,1,10,100),
 ('coord_apply_fail','coordinate_apply','fail','sample_1_in_64','ms',0.1,1,10,100),
 ('flow_assigned','actual_assigned','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_enroute','actual_enroute','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_pickup','actual_pickup','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_carrying','actual_carrying','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_arrived','actual_arrived','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_reuse','actual_reuse','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_unknown','actual_unknown','closed_full_stage','chairs','ms',10,100,1000,3000),
 ('flow_cycle','assignment_to_reusable','closed_full_cycle','chairs','ms',10,100,1000,3000),
 ('flow_dist_enroute','accepted_distance_enroute','ok','accepted_coordinates','coordinate_units',0,1,10,100),
 ('flow_dist_carrying','accepted_distance_carrying','ok','accepted_coordinates','coordinate_units',0,1,10,100),
 ('match_mu','planning_mu','reached','calls','ms',10,100,1000,3000),
 ('match_ready_wait','planning_ready_wait','reached','opportunities','ms',10,100,1000,3000),
 ('match_start_lock','planning_start_lock','reached','attempts','ms',10,100,1000,3000),
 ('match_snapshot_lock','planning_snapshot_lock','reached','attempts','ms',10,100,1000,3000),
 ('match_snapshot','planning_snapshot','reached','attempts','ms',10,100,1000,3000),
 ('match_solver','planning_solver','reached','attempts','ms',10,100,1000,3000),
 ('match_commit_lock','planning_commit_lock','reached','attempts','ms',10,100,1000,3000),
 ('match_commit','planning_commit','reached','attempts','ms',10,100,1000,3000),
 ('match_total_ok','planning_attempt_total','ok','attempts','ms',10,100,1000,3000),
 ('match_total_retry','planning_attempt_total','retry','attempts','ms',10,100,1000,3000),
 ('match_total_empty','planning_attempt_total','empty','attempts','ms',10,100,1000,3000),
 ('match_total_fail','planning_attempt_total','fail','attempts','ms',10,100,1000,3000),
 ('match_rides','planning_rides','nonempty','attempts','rides',1,8,64,256),
 ('match_candidates','planning_candidates','nonempty','attempts','chairs',1,8,64,256)
) as t(series,stage,outcome,cohort,unit,b0,b1,b2,b3);

create or replace view ride_metric_definitions as
select *,
 case when series='pickup_b9_plus' or metric='pickup_b9_plus_unknown' then 2
      when starts_with(metric,'coord_') or starts_with(metric,'flow_')
        or starts_with(metric,'match_') or starts_with(metric,'snapshot_') or metric='ride_state_available' then 3
      else 1 end as min_version,
 case when series='pickup_b9_64' or metric='pickup_b9_64_unknown' then 1 else 3 end as max_version
from (
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
 ('ride_metrics_version','version','current','all','version','metadata'),
 ('coord_calls_ok','coordinate_population','ok','all','coordinates','counter'),
 ('coord_calls_fail','coordinate_population','fail','all','coordinates','counter'),
 ('coord_wal_ok','coordinate_population','ok','all','coordinates','counter'),
 ('coord_wal_fail','coordinate_population','fail','all','coordinates','counter'),
 ('coord_apply_ok','coordinate_population','ok','all','coordinates','counter'),
 ('coord_samples_ok','coordinate_population','ok','sample_1_in_64','coordinates','counter'),
 ('coord_samples_fail','coordinate_population','fail','sample_1_in_64','coordinates','counter'),
 ('coord_sample_bytes','coordinate_population','bytes','sample_1_in_64','bytes','counter'),
 ('coord_sample_every','coordinate_sampling','fixed','all','calls_per_sample','metadata'),
 ('flow_assigned_chairs','actual_assigned','in_progress','chairs','chairs','gauge'),
 ('flow_assigned_age_ms','actual_assigned','in_progress','chairs','chair_ms','gauge'),
 ('flow_assigned_total_ms','actual_assigned','integrated','chairs','chair_ms','counter'),
 ('flow_assigned_partial','actual_assigned','partial','chairs','chairs','counter'),
 ('flow_enroute_chairs','actual_enroute','in_progress','chairs','chairs','gauge'),
 ('flow_enroute_age_ms','actual_enroute','in_progress','chairs','chair_ms','gauge'),
 ('flow_enroute_total_ms','actual_enroute','integrated','chairs','chair_ms','counter'),
 ('flow_enroute_partial','actual_enroute','partial','chairs','chairs','counter'),
 ('flow_pickup_chairs','actual_pickup','in_progress','chairs','chairs','gauge'),
 ('flow_pickup_age_ms','actual_pickup','in_progress','chairs','chair_ms','gauge'),
 ('flow_pickup_total_ms','actual_pickup','integrated','chairs','chair_ms','counter'),
 ('flow_pickup_partial','actual_pickup','partial','chairs','chairs','counter'),
 ('flow_carrying_chairs','actual_carrying','in_progress','chairs','chairs','gauge'),
 ('flow_carrying_age_ms','actual_carrying','in_progress','chairs','chair_ms','gauge'),
 ('flow_carrying_total_ms','actual_carrying','integrated','chairs','chair_ms','counter'),
 ('flow_carrying_partial','actual_carrying','partial','chairs','chairs','counter'),
 ('flow_arrived_chairs','actual_arrived','in_progress','chairs','chairs','gauge'),
 ('flow_arrived_age_ms','actual_arrived','in_progress','chairs','chair_ms','gauge'),
 ('flow_arrived_total_ms','actual_arrived','integrated','chairs','chair_ms','counter'),
 ('flow_arrived_partial','actual_arrived','partial','chairs','chairs','counter'),
 ('flow_reuse_chairs','actual_reuse','in_progress','chairs','chairs','gauge'),
 ('flow_reuse_age_ms','actual_reuse','in_progress','chairs','chair_ms','gauge'),
 ('flow_reuse_total_ms','actual_reuse','integrated','chairs','chair_ms','counter'),
 ('flow_reuse_partial','actual_reuse','partial','chairs','chairs','counter'),
 ('flow_unknown_chairs','actual_unknown','in_progress','chairs','chairs','gauge'),
 ('flow_unknown_age_ms','actual_unknown','in_progress','chairs','chair_ms','gauge'),
 ('flow_unknown_total_ms','actual_unknown','integrated','chairs','chair_ms','counter'),
 ('flow_unknown_partial','actual_unknown','partial','chairs','chairs','counter'),
 ('flow_restored_chairs','flow_boundary','partial','all','chairs','counter'),
 ('flow_cycle_partial','flow_boundary','partial','all','chairs','counter'),
 ('flow_coord_first','flow_boundary','ok','all','coordinates','counter'),
 ('flow_coord_no_ride','flow_boundary','ok','all','coordinates','counter'),
 ('flow_coord_other','flow_boundary','ok','all','coordinates','counter'),
 ('flow_coord_zero','flow_boundary','ok','all','coordinates','counter'),
 ('flow_ongoing_chairs','actual_ongoing','in_progress','chairs','chairs','gauge'),
 ('flow_ongoing_age_ms','actual_ongoing','in_progress','chairs','chair_ms','gauge'),
 ('flow_ongoing_known','actual_ongoing','in_progress','chairs','chairs','gauge'),
 ('flow_ongoing_restored','actual_ongoing','in_progress','chairs','chairs','gauge'),
 ('match_calls','planning_count','calls','all','calls','counter'),
 ('match_attempts','planning_count','attempts','all','attempts','counter'),
 ('match_batches','planning_count','batches','all','batches','counter'),
 ('match_assigned','planning_count','assigned','all','rides','counter'),
 ('match_ready_open','planning_count','ready_open','all','opportunities','counter'),
 ('match_ready_interrupted','planning_count','ready_interrupted','all','opportunities','counter'),
 ('match_ready_generation','planning_count','ready_generation','all','opportunities','counter'),
 ('match_ready_after_start','planning_count','ready_after_start','all','opportunities','counter'),
 ('match_conflict_rides','planning_count','conflict_rides','all','attempts','counter'),
 ('match_conflict_chairs','planning_count','conflict_chairs','all','attempts','counter'),
 ('match_generation','planning_count','generation','all','attempts','counter'),
 ('match_unavailable','planning_count','unavailable','all','attempts','counter'),
 ('match_wal_fail','planning_count','wal_fail','all','attempts','counter'),
 ('match_empty_demand','planning_count','empty_demand','all','attempts','counter'),
 ('match_empty_supply','planning_count','empty_supply','all','attempts','counter'),
 ('match_below_min','planning_count','below_min','all','attempts','counter'),
 ('match_abandon_limit','planning_count','abandon_limit','all','batch_calls','counter'),
 ('match_ready','planning_opportunity','current','all','boolean','gauge'),
 ('match_pending','planning_opportunity','current','all','boolean','gauge'),
 ('match_ready_age_ms','planning_opportunity','current','all','ms','gauge'),
 ('match_pending_age_ms','planning_opportunity','current','all','ms','gauge'),
 ('match_ready_total_ms','planning_opportunity','current','all','ms','counter'),
 ('match_ready_version','planning_opportunity','current','all','transitions','gauge'),
 ('match_planning','planning_opportunity','current','all','attempts','gauge'),
 ('ride_state_available','state_wal_availability','current','all','boolean','gauge'),
 ('snapshot_status','snapshot','current','all','status_code','metadata'),
 ('snapshot_observed_us','snapshot','current','all','unix_us','metadata'),
 ('snapshot_acquire_ms','snapshot','current','all','ms','metadata'),
 ('snapshot_export_ms','snapshot','current','all','ms','metadata')
) as t(metric,stage,outcome,cohort,unit,kind)
) definitions;

-- Raw collection ts and the state observation instant are distinct. Invalid
-- snapshots have no epoch/counters and cannot bridge a counter difference.
create or replace view ride_metric_samples as
with raw_samples as (
 select run_id,host,ts,
        max(value) filter(where metric='ride_metrics_epoch') as epoch,
        max(value) filter(where metric='ride_metrics_ready') as ready,
        max(value) filter(where metric='ride_metrics_version') as version,
        max(value) filter(where metric='snapshot_status') as snapshot_status,
        max(value) filter(where metric='snapshot_observed_us') as observed_us,
        max(value) filter(where metric='snapshot_acquire_ms') as acquire_ms,
        max(value) filter(where metric='snapshot_export_ms') as export_ms
 from metrics where source='sql_pool' and entity='local:metrics:snapshot:0'
 group by run_id,host,ts
), samples as (
 select *, case when version=3 and observed_us>0 then make_timestamptz(observed_us::bigint) else ts end as observed_at,
        ready=1 and version in (1,2,3) and epoch is not null
          and (version<3 or (snapshot_status=0 and observed_us>0 and acquire_ms>=0 and export_ms>=acquire_ms)) as valid
 from raw_samples
)
select *,
 (epoch_us(ts)-epoch_us(observed_at))/1000.0 as capture_age_ms,
 lag(ts) over w as previous_ts,
 lag(observed_at) over w as previous_observed_at,
 lag(epoch) over w as previous_epoch,
 lag(ready) over w as previous_ready,
 lag(version) over w as previous_version,
 lag(valid) over w as previous_valid
from samples window w as (partition by run_id,host order by ts);

-- A fixed version-specific grid exposes absent fields/series. No sample gap
-- is interpolated; real long intervals retain their real observed duration.
create or replace view ride_metric_intervals as
with observed as (
 select s.run_id,s.host,s.ts,m.value,d.*,
        lag(m.value) over w as previous_value
 from ride_metric_samples s
 join ride_metric_definitions d on s.version between d.min_version and d.max_version
 left join metrics m on m.run_id=s.run_id and m.host=s.host and m.ts=s.ts
   and m.metric=d.metric and m.source='sql_pool' and m.entity='local:metrics:snapshot:0'
 window w as (partition by s.run_id,s.host,d.metric order by s.ts)
)
select o.* exclude(ts),s.observed_at as ts,s.ts as captured_at,s.epoch,s.previous_observed_at as interval_start,
       (epoch_us(s.observed_at)-epoch_us(s.previous_observed_at))/1000.0 as interval_ms,
       s.valid as sample_valid,
       case when kind='counter' and s.valid and s.previous_valid
             and s.previous_version=s.version and s.epoch=s.previous_epoch
             and s.observed_at>s.previous_observed_at and o.value>=o.previous_value
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
