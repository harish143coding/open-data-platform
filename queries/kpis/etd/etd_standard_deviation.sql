/*
    This query calculates the standard deviation of eye tracking gaze values filtered per participant, experiment_location
    experiment date and experiment number respectively.
*/

SELECT
    round(cast(stddev(eye_gaze_origin_x) AS numeric), 2) AS sd_etd_ox,
	round(cast(stddev(eye_gaze_origin_y) AS numeric), 2) AS sd_etd_oy,
	round(cast(stddev(eye_gaze_origin_z) AS numeric), 2) AS sd_etd_oz,
	round(cast(stddev(eye_gaze_x) AS numeric), 2) AS sd_etd_x,
	round(cast(stddev(eye_gaze_y) AS numeric), 2) AS sd_etd_y,
	round(cast(stddev(eye_gaze_z) AS numeric), 2) AS sd_etd_z
FROM public.fact_data
WHERE participant_id = '{participant_id}'
	AND experiment_location_id = '{exp_location}'
	AND experiment_nr = {exp_nr}
    AND experiment_date = '{exp_date}'