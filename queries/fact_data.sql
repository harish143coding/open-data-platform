
WITH
etd_gps_joined_prep AS (
	SELECT
		* ,
		LEAD(lsl_timestamp) OVER(PARTITION BY experiment_date,
									  		   participant_id,
									           experiment_nr,
									           experiment_location_id ORDER BY lsl_timestamp ASC) AS next_lsl_timestamp
	FROM public.gps_data
),

etd_gps_joined AS (
    SELECT
       e.experiment_date,
       e.participant_id,
       e.experiment_nr,
       e.experiment_location_id,
       CAST(e.lsl_timestamp AS numeric) AS lsl_timestamp,
       eye_gaze_origin_x,
       eye_gaze_origin_y,
        eye_gaze_origin_z,
        eye_gaze_x,
        eye_gaze_y,
        eye_gaze_z,
       altitude,
       latitude,
       longitude,
       accuracy,
       velocity
    FROM public.etd_data e
    INNER JOIN etd_gps_joined_prep g
    ON e.lsl_timestamp >= g.lsl_timestamp and e.lsl_timestamp < g.next_lsl_timestamp
    AND e.experiment_date = g.experiment_date
    AND e.participant_id = g.participant_id
    AND e.experiment_location_id = g.experiment_location_id
    AND e.experiment_nr = g.experiment_nr
),

etd_gps_eeg_joined_prep AS (
    SELECT *,
       LEAD(lsl_timestamp) OVER(PARTITION BY experiment_date,
									  		   participant_id,
									           experiment_nr,
									           experiment_location_id ORDER BY lsl_timestamp ASC) AS next_lsl_timestamp
    FROM etd_gps_joined
),

casted_eeg AS (
   SELECT
    experiment_date,
    participant_id,
    experiment_nr,
    experiment_location_id,
    eeg_value_1,
    eeg_value_2,
    eeg_value_3,
    eeg_value_4,
    eeg_value_5,
    eeg_value_6,
    eeg_value_7,
    CAST(lsl_timestamp AS numeric) AS lsl_timestamp

    FROM public.eeg_data
),


final AS (
	SELECT
		e.*,
		eye_gaze_origin_x,
        eye_gaze_origin_y,
        eye_gaze_origin_z,
        eye_gaze_x,
        eye_gaze_y,
        eye_gaze_z,
        altitude,
        latitude,
        longitude,
        accuracy,
        velocity

	FROM casted_eeg e
	LEFT JOIN etd_gps_eeg_joined_prep g
		ON e.experiment_date = g.experiment_date
		AND e.participant_id = g.participant_id
		AND e.experiment_nr = g.experiment_nr
		AND e.experiment_location_id = g.experiment_location_id
		AND ROUND(e.lsl_timestamp, 3)  >= ROUND(g.lsl_timestamp, 2) AND ROUND(e.lsl_timestamp, 3) < ROUND(g.next_lsl_timestamp, 2)
)

SELECT *
FROM final
