/*
 This query calculates the duration of the distraction grouped by experiment details in minutes

 Note:
-> currently this KPI can only be applied 'Rytle' location, because annotated visual distraction data only available
 */
SELECT round(cast((max(seconds_of_day) - min(seconds_of_day)) / 60 AS numeric),

								2) AS distraction_duration
FROM public.fact_data
WHERE is_visual = 'True'
    AND participant_id = '{participant_id}'
	AND experiment_nr = {exp_nr}
	AND experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'