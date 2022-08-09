/*
   This query calculates the average visually distracted per session duration and cumulative visually distracted  duration of all the experiments
    in 'minutes' at one location.
   Note:
-> It can be used to compare experiment duration between two locations
-> currently this KPI can only be applied 'Rytle' location, because annotated visual distraction data only available
*/
WITH session_duration AS
	(SELECT experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr,
            is_visual,
			round(cast((max(seconds_of_day) - min(seconds_of_day)) / 60 AS numeric),
				2) AS exp_duration_minutes
		FROM public.fact_data
		GROUP BY experiment_location_id,
			participant_id,
			experiment_date,
			experiment_nr,
            is_visual
		ORDER BY experiment_location_id)
SELECT round(cast(avg(exp_duration_minutes) AS numeric),

								2) AS mean_visual_distracted_durartion,
	sum(exp_duration_minutes) AS total_visual_distrated_duration
FROM session_duration
WHERE exp_duration_minutes < 3600
    AND is_visual = 'True'
    AND experiment_location_id = '{exp_location}'