/*
    This query calculates the standard deviation of EEG electrode individual channels filtered per participant, experiment_location
    experiment date and experiment number respectively.
*/

SELECT
    round(cast(stddev(eeg_value_1) AS numeric), 2) AS sd_eeg_1,
	round(cast(stddev(eeg_value_2) AS numeric), 2) AS sd_eeg_2,
	round(cast(stddev(eeg_value_3) AS numeric), 2) AS sd_eeg_3,
	round(cast(stddev(eeg_value_4) AS numeric), 2) AS sd_eeg_4,
	round(cast(stddev(eeg_value_5) AS numeric), 2) AS sd_eeg_5,
	round(cast(stddev(eeg_value_6) AS numeric), 2) AS sd_eeg_6,
	round(cast(stddev(eeg_value_7) AS numeric), 2) AS sd_eeg_7,
FROM public.fact_data
WHERE participant_id = '{participant_id}'
	AND experiment_location_id = '{exp_location}'
	AND experiment_nr = {exp_nr}
    AND experiment_date = '{exp_date}'