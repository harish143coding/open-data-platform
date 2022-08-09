-- This query compares the relation between altitude and speed in two locations

SELECT avg(altitude) AS avg_altitude_in_meters,
	avg(velocity) AS average_speed_kmph
FROM public.gps_data
WHERE experiment_location_id = '{exp_location}'