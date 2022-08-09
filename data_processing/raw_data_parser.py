import csv
import json
import pandas as pd
import pyxdf
from typing import Tuple
from datetime import date, datetime


def get_experiment_details(remote_filepath: str) -> Tuple[str, str, int]:
    """

    Args:
        remote_filepath: It is the filepath of data stored in the MinIO bucket

    Returns: location where the experiment conducted, participant name and experiment number

    """
    filepath_split = remote_filepath.split("/")
    return filepath_split[0], filepath_split[4], int(filepath_split[5])


def transform_eeg(filepath: str, experiment_date: date, remote_filepath: str) -> pd.DataFrame:
    """
     extracts a single xdf file and the transforms corresponding EEG data into pandas Dataframe xdf --> Dataframe

    Args:
        filepath: local filepath of xdf and csv files stored under the data directory.
        experiment_date: the date on which experiment is conducted.
        remote_filepath: It is the filepath of data stored in the MinIO bucket.

    Returns:
        Dataframe: eeg data transformed into dataframe

    """

    data_streams, header = pyxdf.load_xdf(filepath)

    for x in data_streams:
        if x["info"]["type"][0] == "EEG":
            df_eeg_values = pd.DataFrame(x["time_series"], columns=['eeg_value_1', 'eeg_value_2', 'eeg_value_3',
                                                                    'eeg_value_4', 'eeg_value_5', 'eeg_value_6',
                                                                    'eeg_value_7'])

            df_time_stamps = pd.DataFrame(x["time_stamps"], columns=['lsl_timestamp'])
            eeg_df = pd.concat([df_time_stamps, df_eeg_values], axis=1)
            eeg_df["experiment_date"] = experiment_date
            location, participant, experiment = get_experiment_details(remote_filepath)
            eeg_df["participant_id"] = participant
            eeg_df["experiment_nr"] = experiment
            eeg_df["experiment_location_id"] = location
            # from here generation of seconds of the day takes place with reference to header as the start timestamp
            start_timestamp = header['info']['datetime'][0]
            start_lsl_ts = min(eeg_df["lsl_timestamp"])
            start_timestamp_obj = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S%z")
            eeg_df["raw_delta_lsl_ts"] = (eeg_df["lsl_timestamp"] - start_lsl_ts) * (10 ** 3)
            eeg_df["delta_lsl_ts"] = pd.to_timedelta(eeg_df["raw_delta_lsl_ts"], unit='ms')
            eeg_df["actual_timestamp"] = start_timestamp_obj + eeg_df["delta_lsl_ts"]
            normalized_ts = eeg_df["actual_timestamp"][0].normalize()
            eeg_df["seconds_of_day"] = (eeg_df["actual_timestamp"] - normalized_ts).dt.total_seconds()
            eeg_df.drop(columns=["delta_lsl_ts", "raw_delta_lsl_ts", "actual_timestamp"], inplace=True)

            return eeg_df
        else:
            pass
    return None


def transform_etd(filepath: str, remote_filepath: str) -> Tuple[pd.DataFrame, date]:
    """
    extracts a single xdf file and the transforms corresponding ETD data into pandas Dataframe xdf --> Dataframe

    Args:
        filepath: a file located in the under an experiment in MinIO bucket
        remote_filepath: It is the filepath of data stored in the MinIO bucket.

    Returns: ETD data transformed into Dataframe and the date on which that particular experiment is conducted

    """
    data_streams, header = pyxdf.load_xdf(filepath)
    for x in data_streams:
        if x["info"]["type"][0] == "eye_tracking_stream":
            list_for_df = []
            for i in x['time_series']:
                split = i[0].split(';')
                elapsed_time = split[0].split(',')[0]
                eg_1 = float(split[1].split(',')[0])
                eg_2 = float(split[1].split(',')[1])
                eg_3 = float(split[1].split(',')[2])
                eg_orig_1 = float(split[2].split(',')[0])
                eg_orig_2 = float(split[2].split(',')[1])
                eg_orig_3 = float(split[2].split(',')[2])
                list_for_df.append([elapsed_time, eg_1, eg_2, eg_3, eg_orig_1, eg_orig_2, eg_orig_3])

            df_et_values = pd.DataFrame(list_for_df,
                                        columns=['utc_timestamp', 'eye_gaze_origin_x', 'eye_gaze_origin_y',
                                                 'eye_gaze_origin_z', 'eye_gaze_x', 'eye_gaze_y', 'eye_gaze_z'])
            df_et_values["utc_timestamp"] = pd.to_datetime(df_et_values["utc_timestamp"],
                                                           infer_datetime_format=True)
            df_et_values["experiment_date"] = df_et_values["utc_timestamp"].dt.date
            df_time_stamps = pd.DataFrame(x["time_stamps"], columns=['lsl_timestamp'])
            # df_et_values["seconds_of_day"] = (df_et_values["utc_timestamp"] - pd.to_datetime(df_et_values["experiment_date"])).dt.total_seconds()
            etd_df = pd.concat([df_time_stamps, df_et_values], axis=1)
            location, participant, experiment = get_experiment_details(remote_filepath)
            etd_df["participant_id"] = participant
            etd_df["experiment_nr"] = experiment
            etd_df["experiment_location_id"] = location
            etd_df.drop(columns="utc_timestamp", inplace=True)
            return etd_df, etd_df["experiment_date"].iloc[0]
        else:
            pass
            # print("Eye tracking data to the corresponding file did not find and processing to next ....")
    return None


def transform_gps(filepath: str, remote_filepath: str) -> pd.DataFrame:
    """
    extracts a single csv file and the transforms corresponding GPS data into pandas Dataframe csv --> Dataframe
    Args:

        filepath: a file located in the under an experiment (location directory) in MinIO bucket
        remote_filepath: It is the filepath of data stored in the MinIO bucket.

    Returns: GPS data transformed into Dataframe

    """
    with open(filepath, newline='') as csvfile:
        file = csv.reader(csvfile, delimiter=';')
        is_header_row = True
        list_of_dict = []
        for row in file:
            value_dict = {}
            if is_header_row:
                is_header_row = False
                continue
            else:
                value_dict['utc_timestamp'] = row[3]
                try:
                    msg_dict = json.loads(row[4])
                except json.decoder.JSONDecodeError:
                    return None
                # value_dict['epoch_timestamp_tst'] = msg_dict.get('tst')
                value_dict['altitude'] = msg_dict.get('alt')
                value_dict['latitude'] = msg_dict.get('lat')
                value_dict['longitude'] = msg_dict.get('lon')
                value_dict['accuracy'] = msg_dict.get('acc')
                value_dict['velocity'] = msg_dict.get('vel')
            list_of_dict.append(value_dict)
    gps_df = pd.DataFrame(list_of_dict)
    cols_to_cast = [
        'altitude',
        'latitude',
        'longitude',
        'accuracy',
        'velocity'
    ]
    gps_df[cols_to_cast] = gps_df[cols_to_cast].astype(float)
    gps_df['utc_timestamp'] = pd.to_datetime(gps_df['utc_timestamp'], infer_datetime_format=True)
    gps_df["experiment_date"] = gps_df["utc_timestamp"].dt.date
    location, participant, experiment = get_experiment_details(remote_filepath)
    gps_df["participant_id"] = participant
    gps_df["experiment_nr"] = experiment
    gps_df["experiment_location_id"] = location
    gps_df.drop(columns="utc_timestamp", inplace=True)
    return gps_df


def transform_gps_csv(file_xdf: str, file_csv: str, remote_filepath: str) -> pd.DataFrame:
    """

    Args:
        file_xdf: The xdf filepath of that corresponding experiment to parse lsl timestamps for gps data
        file_csv: The csv filepath of the same experiment to parse the gps information from it
        remote_filepath: remote filepath supplied from scrapping through the function  scrap_dict_from_paths

    Returns: gps data parsed into a dataframe for one experiment

    """
    data_streams, header = pyxdf.load_xdf(file_xdf)
    for x in data_streams:
        if x['info']['name'] == ['GoProCameraStream']:
            df_gps = transform_gps(file_csv, remote_filepath)
            gps_start_lsl = x['time_stamps'][0] + 80
            gps_time_stamps = []
            for x in range(len(df_gps['latitude'])):
                gps_start_lsl = gps_start_lsl + 1
                gps_time_stamps.append(gps_start_lsl)
            df_gps['lsl_timestamp'] = gps_time_stamps
    return df_gps


def transform_gps_xdf(filepath: str, experiment_date: date, remote_filepath: str) -> pd.DataFrame:
    """
    extracts a single csv file and the transforms corresponding GPS data into pandas Dataframe csv --> Dataframe
    Args:
        filepath: a file located in the under an experiment (location directory) in MinIO bucket
        experiment_date: the date on which experiment is conducted.
        remote_filepath:
    Returns: GPS data transformed into Dataframe

    """
    data_streams, header = pyxdf.load_xdf(filepath)

    for x in data_streams:
        if x['info']['name'] == ['Smarthelm-GPS']:
            # check if entry/keys - "time_series" and "time_stamps" exists or not
            if not ("time_series" in x and "time_stamps" in x):
                return None
            coords = x.get('time_series')
            timestamps = list(x.get('time_stamps'))
            if len(coords) and len(timestamps) and len(coords) == len(timestamps):

                lat = []
                long = []
                alt = []
                acc = []
                velo = []
                for ele in coords:
                    info_dict = eval(ele[0])
                    lat.append(info_dict.get('lat'))
                    long.append(info_dict.get('lon'))
                    alt.append(info_dict.get('alt'))
                    acc.append(info_dict.get('acc'))
                    velo.append(info_dict.get('vel'))
                gps_df = pd.DataFrame({
                    "altitude": alt,
                    "latitude": lat,
                    "longitude": long,
                    "accuracy": acc,
                    "velocity": velo,
                    "lsl_timestamp": timestamps
                })
                cols_to_cast = [
                    'altitude',
                    'latitude',
                    'longitude',
                    'accuracy',
                    'velocity'
                ]
                gps_df[cols_to_cast] = gps_df[cols_to_cast].astype(float, errors='ignore')
                location, participant, experiment = get_experiment_details(remote_filepath)
                gps_df["participant_id"] = participant
                gps_df["experiment_nr"] = experiment
                gps_df["experiment_location_id"] = location
                gps_df["experiment_date"] = experiment_date
                return gps_df
            else:
                return None


def transform_gps_null(experiment_date: date, remote_filepath: str) -> pd.DataFrame:
    """

    Args:
        experiment_date: date of the experiment conducted acquired from function get_experiment_details
        remote_filepath: remote filepath supplied from scrapping through the function  scrap_dict_from_paths

    Returns: A pandas Df with one row containing null values and experiment metadata information

    """
    gps_df = pd.DataFrame({
        "altitude": [None],
        "latitude": [None],
        "longitude": [None],
        "accuracy": [None],
        "velocity": [None],
        "lsl_timestamp": [None]
    })
    # gps_df[cols_to_cast] = gps_df[cols_to_cast].astype(float, errors='ignore')
    location, participant, experiment = get_experiment_details(remote_filepath)
    gps_df["participant_id"] = participant
    gps_df["experiment_nr"] = experiment
    gps_df["experiment_location_id"] = location
    gps_df["experiment_date"] = experiment_date
    return gps_df


# currently this fn is only applicable for 'Rytle' location
def transform_distraction_timestamps(filepath: str) -> pd.DataFrame:
    """
    An auxiliary fn in general to create all the distraction dimensional table. Here the main duty of this fn is to
    merge the two consecutive timestamps where there is intersection in the timestamps

    Args:
        filepath: the filepath where the annotated distractions file is downloaded

    Returns: DF rearranged after sorting out the colliding timestamps

    """
    df = pd.read_csv(filepath)
    df = df.rename(columns={"distraction_start": "start_lsl"})
    df["end_lsl"] = df["start_lsl"] + 4

    # sorting the df values w.r.t 'start_lsl' - so that the df is arranged in the ascending order of 'start_lsl'
    df = df.sort_values(by="start_lsl").reset_index(drop=True)

    group_number = 0
    df["group"] = group_number

    # itterating through each row of the input df to divide the rows into groups
    for idx, row in df.iterrows():

        # the row at the zero th index is stored at in 'previous_interval' variable with the follwing condition
        if idx == 0:
            previous_interval = pd.Interval(row["start_lsl"], row["end_lsl"], closed="both")
        else:
            current_interval = pd.Interval(row["start_lsl"], row["end_lsl"], closed="both")

            # if the 'current_interval' does not overlap with the 'previous_interval' then the group_number changes
            # by one.
            if not current_interval.overlaps(previous_interval):
                group_number += 1

            df.loc[idx, "group"] = group_number
            previous_interval = current_interval

    return df.groupby(by="group").agg({"start_lsl": min, "end_lsl": max}).reset_index(drop=True)


def transform_visual_distractions(filepath: str, experiment_date: date, remote_filepath: str) -> pd.DataFrame:
    """
    this fn creates the visual distraction dimension table using the auxiliary "transform_distraction_timestamps" fn. It
    creates the 'is_visual' column and supplies all the other required experimental details

    Args:
        filepath: local filepath of xdf and csv files stored under the data directory.
        experiment_date: the date on which experiment is conducted.
        remote_filepath: It is the filepath of data stored in the MinIO bucket.

    Returns: final visual distraction  dataframe.

    """
    visual_df = transform_distraction_timestamps(filepath)
    visual_df["is_visual"] = True
    experiment_location_id, participant_id, experiment_nr = get_experiment_details(remote_filepath)
    visual_df["experiment_date"] = experiment_date
    visual_df["experiment_location_id"] = experiment_location_id
    visual_df["participant_id"] = participant_id
    visual_df["experiment_nr"] = experiment_nr
    return visual_df



