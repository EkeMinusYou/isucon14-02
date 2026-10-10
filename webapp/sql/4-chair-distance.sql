-- Rebuild persistent distance totals after loading the initial location history.
-- Use the location ID to order records with the same timestamp consistently.
INSERT INTO chair_distance_totals (chair_id, total_distance, last_latitude, last_longitude, updated_at)
SELECT chairs.id,
       IFNULL(locations.total_distance, 0),
       locations.latitude,
       locations.longitude,
       locations.created_at
FROM chairs
LEFT JOIN (
    SELECT chair_id, latitude, longitude, created_at,
           SUM(IFNULL(distance, 0)) OVER (PARTITION BY chair_id) AS total_distance,
           ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY created_at DESC, id DESC) AS latest
    FROM (
        SELECT id, chair_id, latitude, longitude, created_at,
               ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at, id)) +
               ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at, id)) AS distance
        FROM chair_locations
    ) distances
) locations ON locations.chair_id = chairs.id AND locations.latest = 1;
