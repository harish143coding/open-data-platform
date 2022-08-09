import os
from minio import Minio


smarthelm_client = Minio(
        "smarthelm-nas.vlba.uni-oldenburg.de:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


def download(bucket_name: str, filepath: str) -> str:
    """this function takes the filepath of a file kept under MinIO as input and then downloads to the "data" directory
    Args:
        bucket_name:
        filepath:

    Returns:
        It will return the path to the local directory where the file is downloaded

    """

    # to retrieve an object from the client
    if filepath.endswith("xdf"):
        local_path = f"./data/test.xdf"
    elif filepath.endswith("csv"):
        local_path = f"./data/test.csv"
    else:
        raise Exception("file extension not recognised")

    smarthelm_client.fget_object(bucket_name, filepath, file_path=local_path)

    return local_path

