-- This query calculates the average session duration in minutes at one location.
-- It can be used to compare experiment duration between two locations

WITH session_duration AS
	(SELECT experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr,
			round((max(epoch_timestamp_tst) - min(epoch_timestamp_tst)) % 60,
				2) AS exp_duration_minutes
		FROM public.gps_data
		GROUP BY experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr
		ORDER BY experiment_location_id)
SELECT avg(exp_duration_minutes) AS session_durartion_minutes
FROM session_duration
WHERE exp_duration_minutes < 3600
	AND experiment_location_id = '{exp_location}'