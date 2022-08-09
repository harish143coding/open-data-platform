SELECT DISTINCT experiment_nr
FROM public.{table_name}
WHERE experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'
	AND participant_id = '{participant_id}'
ORDER BY experiment_nr ASC