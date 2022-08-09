/*
 this query calculates the distance covered by the participant in one experiment
 based upon the 'seconds_of_day' (in metres)
 */

SELECT
    round(cast((max(seconds_of_day) - min(seconds_of_day)) * avg(velocity) / 3.6 AS numeric),
       2) AS distance_travelled
FROM public.fact_data
WHERE participant_id = '{participant_id}'
	AND experiment_nr = {exp_nr}
	AND experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'