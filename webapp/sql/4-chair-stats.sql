INSERT INTO chair_stats (chair_id, total_rides_count, total_evaluation)
SELECT r.chair_id, COUNT(*), SUM(r.evaluation)
FROM rides r
INNER JOIN (
  SELECT ride_id
  FROM ride_statuses
  GROUP BY ride_id
  HAVING SUM(status = 'CARRYING') > 0
     AND SUM(status = 'ARRIVED') > 0
     AND SUM(status = 'COMPLETED') > 0
) completed ON completed.ride_id = r.id
WHERE r.chair_id IS NOT NULL
GROUP BY r.chair_id;
