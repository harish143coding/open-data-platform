/*
 This query presents the average distance of the objects in the experiment viewed by the participant in one particular trip
 */

WITH average_gaze_values AS
	(SELECT participant_id AS participant,
			avg(eye_gaze_origin_x) AS mean_x_o,
			avg(eye_gaze_origin_y) AS mean_y_o,
			avg(eye_gaze_origin_z) AS mean_z_o,
			avg(eye_gaze_x) AS mean_x,
			avg(eye_gaze_y) AS mean_y,
			avg(eye_gaze_z) AS mean_z
		FROM public.etd_data
		WHERE participant_id = '{participant_id}'
			AND experiment_nr = {exp_nr}
			AND experiment_date = '{exp_location}'
		GROUP BY participant_id)
SELECT participant,
	avg_x_o,
	avg_y_o,
	avg_z_o,
	round(cast(sqrt((power((avg_x - avg_x_o), 2) +  (power((avg_y - avg_y_o), 2))
	     + (power((avg_z - avg_z_o), 2)))) AS numeric), 2) AS viewing_distance_from_the_object
FROM average_gaze_values