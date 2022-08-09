/*
This query calculates the total distance covered in all the sessions at particular location (in meters)
*/
WITH distance_covered AS
	(SELECT experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr,
			round(cast((max(seconds_of_day) - min(seconds_of_day)) * avg(velocity) / 3.6 AS numeric),
				2) AS distance_travelled
		FROM public.fact_data
		GROUP BY experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr
		ORDER BY experiment_location_id)
SELECT
    round(cast(avg(distance_travelled) AS numeric),
								2) AS avg_distance_travelled,
	sum(distance_travelled) AS total_distance_covered
FROM distance_covered
WHERE experiment_location_id = '{exp_location}'