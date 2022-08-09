import os
from typing import List, Dict
from minio import Minio


smarthelm_client = Minio(
        "smarthelm-nas.vlba.uni-oldenburg.de:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
)

EXCLUDED_FOLDERS = [
    "speech",
    "ui_event",
    "attention",
    "vision"
]


def scrap(folder_path: str, bucket_name: str) -> List[str]:
    """
    This function takes a path to a folder on MinIO bucket and returns the complete paths of the data files

    Args:
        folder_path: a particular location under the bucket  e.g. "CSL/2021"
        bucket_name: name of the bucket which contains the data e.g. "smarthelm-data-accumulation-csl"

    Returns: ["CSL/2021/08/23/KS/01/raw_eeg/test.xdf",
        "CSL/2021/08/23/KS/01/location/test.csv",
        ....,
        "CSL/2021/08/23/KS/02/raw_eeg/test.xdf"]

    """
    objects = smarthelm_client.list_objects(
        bucket_name=bucket_name,
        prefix=folder_path,
        recursive=True
    )
    filepaths = [obj.object_name for obj in objects if obj.size > 10]
    filtered_filepaths = []
    for path in filepaths:
        if not any([ele in path for ele in EXCLUDED_FOLDERS]):
            filtered_filepaths.append(path)
    return filtered_filepaths


def scrap_dict_from_paths(folder_path: str, bucket_name: str) -> Dict[str, Dict[str, str]]:
    """

    Args:
        bucket_name: name of the particular bucket in the MinIO, where the raw data is stored
        folder_path: one experiment location out of the available locations where the data is acquired

    Returns: remote filepaths processed into a list of key, value pairs i.e., dictionary
            {
    "CSL/2021/08/24/MS/01": {
        "location": "smarthelm_gps.csv",
        "raw_eeg": "sub-P001_MS1757_ses-S001_task-Default_run-001_eeg.xdf"
    },
    "CSL/2021/08/26/MS/01": {
        "raw_eeg": "sub-P001_MS1_ses-S001_task-Default_run-001_eeg.xdf",
        "location": "smarthelm_gps.csv"
    },
}

    """
    filtered_filepaths = scrap(folder_path, bucket_name)
    parent_path_dict = {}
    child_keys = [
        "annotated_distractions",
        "raw_eeg",
        "location",
        "vision",
        "attention/",
        "speech/",
        "ui_event"
    ]

    def _get_child_key(path):
        for ck in child_keys:
            if ck in ele:
                return ck

    for ele in filtered_filepaths:

        child_key = _get_child_key(ele)
        parent_path, child_path = ele.split(child_key)
        if parent_path in parent_path_dict:
            parent_path_dict[parent_path][child_key] = child_path
        else:
            parent_path_dict[parent_path] = {child_key: child_path}
    return parent_path_dict
