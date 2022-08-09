import argparse

from data_processing.raw_data_parser import transform_eeg
from data_processing.raw_data_parser import transform_etd
from data_processing.raw_data_parser import transform_gps_xdf
from data_processing.raw_data_parser import transform_gps_csv
from data_processing.raw_data_parser import transform_gps_null
from data_processing.raw_data_parser import transform_visual_distractions
from data_processing.fact_prep import fact_df_main
from tqdm import tqdm
from utilities.data_downloader import download
from utilities.data_loader import load_table_to_db
from utilities.default_params import *
from utilities.scrapper import scrap
from utilities.scrapper import scrap_dict_from_paths


def run_pipeline(args):
    """

    Args:
        args:

    Returns:

    """
    dict_with_filepaths = scrap_dict_from_paths(args.experiment_location, args.bucket_name)
    for k, v in tqdm(dict_with_filepaths.items()):
        raw_data_path = k + "raw_eeg" + v["raw_eeg"]
        local_data_path = download(args.bucket_name, raw_data_path)
        etd_df, exp_date = transform_etd(local_data_path, raw_data_path)
        eeg_df = transform_eeg(local_data_path, exp_date, raw_data_path)
        # load_table_to_db(eeg_df, 'eeg_data')
        # load_table_to_db(etd_df, 'etd_data')
        gps_df = transform_gps_xdf(local_data_path, exp_date, raw_data_path)

        if gps_df is None:
            if "location" in v:
                location_data_path = k + "location" + v["location"]
                local_data_path_gps = download(args.bucket_name, location_data_path)
                gps_df = transform_gps_csv(local_data_path, local_data_path_gps, location_data_path)
            else:
                gps_df = None
        if gps_df is None:
            gps_df = transform_gps_null(exp_date, raw_data_path)
        #load_table_to_db(gps_df, 'gps_data')

        # for only Rytle location if the condition is true then loading the "visual_distraction" table
        if args.experiment_location == "Rytle":
            visual_data_path = k + "annotated_distractions" + v["annotated_distractions"]
            local_data_path_visual = download(args.bucket_name, visual_data_path)
            visual_df = transform_visual_distractions(local_data_path_visual, exp_date, visual_data_path)
            # loading.....
            load_table_to_db(visual_df, "visual_distraction_data")
        else:
            visual_df = None
        fact_df = fact_df_main(eeg_df, etd_df, gps_df, visual_df)
        load_table_to_db(fact_df, 'fact_data')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_location", type=str, required=True)
    parser.add_argument("--bucket_name", type=str, default=BUCKET_NAME)
    args = parser.parse_args()
    run_pipeline(args)
