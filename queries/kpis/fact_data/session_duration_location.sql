/* This query calculates the average session duration and cumulative session duration of all the experiments
    in 'minutes' at one location.
-> It can be used to compare experiment duration between two locations
   */

WITH session_duration AS
	(SELECT experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr,
			round(cast((max(seconds_of_day) - min(seconds_of_day)) / 60 AS numeric),
				2) AS exp_duration_minutes
		FROM public.fact_data
		GROUP BY experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr
		ORDER BY experiment_location_id)
SELECT round(cast(avg(exp_duration_minutes) AS numeric),

								2) AS mean_session_duration,
	sum(exp_duration_minutes) AS total_session_durations
FROM session_duration
WHERE exp_duration_minutes < 3600
	AND experiment_location_id = '{exp_location}'