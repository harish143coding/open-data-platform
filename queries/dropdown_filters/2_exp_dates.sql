SELECT DISTINCT experiment_date
FROM public.{table_name}
WHERE experiment_location_id = '{exp_location}'
ORDER BY experiment_date ASC