/*
 This query calculates mean individual electrode signal values from the eeg data
 filtered per participant, experiment location experiment date and experiment number respectively
*/

SELECT avg(eeg_value_1) AS mean_eeg_elctrode_1,
	avg(eeg_value_2) AS mean_eeg_elctrode_2,
	avg(eeg_value_3) AS mean_eeg_elctrode_3,
	avg(eeg_value_4) AS mean_eeg_elctrode_4,
	avg(eeg_value_5) AS mean_eeg_elctrode_5,
	avg(eeg_value_6) AS mean_eeg_elctrode_6,
	avg(eeg_value_7) AS mean_eeg_elctrode_7
FROM public.eeg_data
WHERE participant_id = '{participant_id}'
	AND experiment_location_id = '{exp_location}'
	AND experiment_nr = {exp_nr}
	AND experiment_date = '{exp_date}'