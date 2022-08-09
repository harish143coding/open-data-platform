import pandas as pd


def get_gps_lsl_tst_join(ts_etd, gps_df: pd.DataFrame) -> pd.DataFrame:
    """
    fetches the joinable from gps lsl_timestamps column in gps dataframe for preparing a temporary column
    to join 2 dataframes
    Args:
        ts_etd: lst_timestamp from etd dataframe column
        gps_df: gps dataframe as input obtained from gps parser

    Returns: a temporary column containing the joinable timestamps from gps lsl timestamps to join with etd

    """
    df = gps_df.loc[(gps_df["lsl_timestamp"] <= ts_etd) & (ts_etd < gps_df["next_lsl_timestamp"])]
    if df.empty:
        return gps_df.loc[gps_df["lsl_timestamp"] == gps_df["lsl_timestamp"].max()]["lsl_timestamp"].values[0]
    else:
        return df["lsl_timestamp"].values[0]


def get_etd_gps_tst_join(eeg_tst, etd_joined_gps: pd.DataFrame) -> pd.DataFrame:
    """

    Args:
        eeg_tst: lst_timestamp from eeg dataframe column
        etd_joined_gps: etd+gps combined dataframe obtained from in between inside the function create_fact_df in

    Returns: a temporary column containing the joinable timestamps from etd+gps lsl timestamps to join with eeg

    """
    df = etd_joined_gps.loc[
        (etd_joined_gps["lsl_timestamp"] <= eeg_tst) & (etd_joined_gps["next_lsl_timestamp"] > eeg_tst)]
    if df.empty:
        return etd_joined_gps.loc[etd_joined_gps["lsl_timestamp"] == etd_joined_gps["lsl_timestamp"].max()][
            "lsl_timestamp"].values[0]
    else:
        return df["lsl_timestamp"].values[0]


def get_etd_lsl_tst_join(ts_eeg, etd_df):
    """
    fetches the joinable from etd lsl_timestamps column in etd dataframe for preparing a temporary column
    to join 2 dataframes
    Args:
        ts_eeg: eeg_tst: lst_timestamp from eeg dataframe column
        etd_df:  etd dataframe as input obtained from etd parser

    Returns: a temporary column containing the joinable timestamps from etd lsl_timestamps to join with eeg

    """
    df = etd_df.loc[(etd_df["lsl_timestamp"] <= ts_eeg) & (ts_eeg < etd_df["next_lsl_timestamp"])]
    if df.empty:
        return etd_df.loc[etd_df["lsl_timestamp"] == etd_df["lsl_timestamp"].max()]["lsl_timestamp"].values[0]
    else:
        return df["lsl_timestamp"].values[0]


def get_visual_joinable_tst_with_fact_df(row: pd.Series, visual_df: pd.DataFrame) -> bool:
    """
    this is the auxiliary fn for True/False generator from the visual distractions dimensional table. It is used
    at the end of the fact dataframe creation, where the remaining 'lsl_timestamps' column in fact dataframe is used as
    the common attribute to fetch the matching visual distraction timestamps from 'visual_df' table
    Args:
        row: each row record from the fact_df will be supplied as the parameter
        visual_df: the generated visual distraction dataframe from raw_data_parser is supplied as parameter

    Returns: Boolean expression at the respective timestamps

    """
    o = visual_df.loc[(visual_df["start_lsl"] <= row["eeg_joinable_column_with_etd_gps"]) & (row["eeg_joinable_column_with_etd_gps"] <= visual_df["end_lsl"])]["is_visual"]
    return True if len(o) else False


def create_fact_df(eeg: pd.DataFrame, etd: pd.DataFrame, gps: pd.DataFrame) -> pd.DataFrame:
    """
    Job: builds a  batch-wise (i.e. each experiment) a fact dataframe which joins eeg, etd and gps from the reference
    as lsl_timestamps and to achieve that in middle a temporary column is created with all the joinable timestamps. In
    this process first gps df is joined with etd then combined etd + gps is joined with eeg

    Args:
        etd: etd dataframe obtained from etd parser fn
        eeg: similarly eeg dataframe obtained from eeg parser fn
        gps: similarly gps dataframe obtained from gps parser fn

    Returns: totally joined fact dataframe with eeg+etd+gps

    """
    gps.sort_values(by="lsl_timestamp")
    gps["next_lsl_timestamp"] = gps["lsl_timestamp"].shift(-1)
    etd["joinable_timestamp_with_gps"] = etd.apply(lambda row: get_gps_lsl_tst_join(row["lsl_timestamp"], gps), axis=1)
    cols_to_select_from_gps_df = [
        "lsl_timestamp",
        "altitude",
        'latitude',
        "longitude",
        "accuracy",
        "velocity"
    ]
    etd_joined_gps_df = etd.merge(
        gps[cols_to_select_from_gps_df],
        left_on="joinable_timestamp_with_gps",
        right_on="lsl_timestamp",
        how="left",
        suffixes=("_etd", "_gps")
    )
    etd_joined_gps_df = etd_joined_gps_df.drop(columns=["lsl_timestamp_gps", "joinable_timestamp_with_gps"])
    etd_joined_gps_df = etd_joined_gps_df.rename(columns={'lsl_timestamp_etd': "lsl_timestamp"})

    # from here forming the one big table
    etd_joined_gps_df = etd_joined_gps_df.sort_values(by="lsl_timestamp")
    etd_joined_gps_df["next_lsl_timestamp"] = etd_joined_gps_df["lsl_timestamp"].shift(-1)
    eeg["eeg_joinable_column_with_etd_gps"] = eeg.apply(
        lambda row: get_etd_gps_tst_join(row["lsl_timestamp"], etd_joined_gps_df), axis=1)
    cols_to_select_from_etd_df = [
        "eye_gaze_origin_x",
        "eye_gaze_origin_y",
        "eye_gaze_origin_z",
        "eye_gaze_x",
        "eye_gaze_y",
        "eye_gaze_z",
        "lsl_timestamp"
    ]
    etd_gps_columns = list(set(cols_to_select_from_gps_df + cols_to_select_from_etd_df))
    eeg_etd_gps = eeg.merge(
        etd_joined_gps_df[etd_gps_columns],
        left_on="eeg_joinable_column_with_etd_gps",
        right_on="lsl_timestamp",
        how="left",
        suffixes=("_eeg", "_etd_gps")
    )
    # "eeg_join able_column_with_etd_gps" should be left to create join able column for fact_df with visual
    eeg_etd_gps = eeg_etd_gps.drop(
                          columns=["lsl_timestamp_etd_gps", "lsl_timestamp_eeg"]
                          )
    return eeg_etd_gps


def create_fact_df_wo_gps(eeg: pd.DataFrame, etd: pd.DataFrame) -> pd.DataFrame:
    """
    Job: Builds a fact datframe as previous function but the only difference is without gps df because few experiments
    does not contain gps values, in that case eeg is joined with etd and at the end empty gps values are added.

    Args:
        eeg: eeg DataFrame obtained from the eeg parser function
        etd: similarly etd DataFrame obtained from the etd parser function

    Returns:
        eeg_etd_gps: returns a merged eeg plus etd data with null gps values, while the original data stream for that
                     experiment contains no gps values
    """
    etd = etd.sort_values(by="lsl_timestamp")
    etd["next_lsl_timestamp"] = etd["lsl_timestamp"].shift(-1)
    eeg["joinable_timestamp_with_etd"] = eeg.apply(lambda row: get_etd_lsl_tst_join(row["lsl_timestamp"], etd), axis=1)
    cols_to_select_from_etd_df = [
        "lsl_timestamp",
        "eye_gaze_origin_x",
        "eye_gaze_origin_y",
        "eye_gaze_origin_z",
        "eye_gaze_x",
        "eye_gaze_y",
        "eye_gaze_z",
    ]

    eeg_joined_etd_df = eeg.merge(
        etd[cols_to_select_from_etd_df],
        left_on="joinable_timestamp_with_etd",
        right_on="lsl_timestamp",
        how="left",
        suffixes=("_eeg", "_etd")
    )
    eeg_joined_etd_df = eeg_joined_etd_df.drop(columns=["lsl_timestamp_etd", "joinable_timestamp_with_etd", "lsl_timestamp_eeg"])
    eeg_joined_etd_df["altitude"] = None
    eeg_joined_etd_df["latitude"] = None
    eeg_joined_etd_df["longitude"] = None
    eeg_joined_etd_df["accuracy"] = None
    eeg_joined_etd_df["velocity"] = None
    cols_to_cast = [
        'altitude',
        'latitude',
        'longitude',
        'accuracy',
        'velocity'
    ]
    eeg_joined_etd_df[cols_to_cast] = eeg_joined_etd_df[cols_to_cast].astype(float, errors='ignore')
    return eeg_joined_etd_df


def fact_df_main(eeg: pd.DataFrame, etd: pd.DataFrame, gps: pd.DataFrame, visual_distraction: pd.DataFrame = None) -> pd.DataFrame:
    """
    Args:

        etd: etd dataframe obtained from etd parser fn
        eeg: similarly eeg dataframe obtained from eeg parser fn
        gps: similarly gps dataframe obtained from gps parser fn

    Returns: totally joined fact dataframe with eeg+etd+gps

    """
    if len(gps) == 1:
        fact_df = create_fact_df_wo_gps(eeg, etd)
    else:
        fact_df = create_fact_df(eeg, etd, gps)
    if visual_distraction:
        fact_df["is_visual"] = fact_df.apply(get_visual_joinable_tst_with_fact_df,args=(visual_distraction,),axis=1)
    else:
        fact_df["is_visual"] = None
    return fact_df
