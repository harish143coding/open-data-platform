/*
 This query fetches the geo coordinates where a visual distraction had taken place for a participant in a particular
 session.
 Note:
 -> currently it can also be applied to sessions in 'Rytle' location
 */

 SELECT is_visual,
	experiment_date,
	round(cast(seconds_of_day AS numeric),
		2) AS time_in_seconds,
	latitude,
	longitude
FROM public.fact_data
WHERE is_visual = 'True'
    AND participant_id = '{participant_id}'
	AND experiment_nr = {exp_nr}
	AND experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'
ORDER BY is_visual