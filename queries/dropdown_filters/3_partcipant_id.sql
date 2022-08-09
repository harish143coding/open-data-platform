SELECT DISTINCT participant_id
FROM public.{table_name}
WHERE experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'
ORDER BY participant_id ASC