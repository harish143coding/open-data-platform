-- This query calculates the maximum and minimum altitude values (in metres)
--  from the sea level at the location where the experiment is conducted

SELECT max(altitude) AS max_altitude_in_meters,
	min(altitude) AS min_altitude_in_meters
FROM public.gps_data
WHERE participant_id = '{participant_id}'
	AND experiment_location_id = '{exp_location}'
	AND experiment_nr = {exp_nr}
	AND experiment_date = '{exp_date}'