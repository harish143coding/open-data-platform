SELECT *,
	CASE WHEN (EXTRACT(MONTH FROM experiment_date)<11 AND EXTRACT(MONTH FROM experiment_date)>3 ) THEN 3600 + seconds_of_day
			   ELSE 7200 + seconds_of_day
			   END AS local_seconds_of_day
	FROM public.gps_data
	LIMIT 10