-- This query calculates the distance travelled by a participant during a particular session in meters

SELECT
    ROUND(cast((max(seconds_of_day) - min(seconds_of_day)) * avg(velocity) / 3.6 as numeric ) ,2)
        AS distance_travelled
FROM public.fact_data
WHERE participant_id = '{participant_id}'
	AND experiment_nr = {exp_nr}
	AND experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'