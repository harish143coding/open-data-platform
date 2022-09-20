-- this sql query returns all the 'latitude' and 'longitude' values from a  one session
SELECT latitude, longitude
    FROM public.fact_data
    WHERE participant_id = '{participant_id}'
	AND experiment_nr = {exp_nr}
	AND experiment_location_id = '{exp_location}'
	AND experiment_date = '{exp_date}'