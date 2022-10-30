import os
from io import BytesIO, StringIO
from typing import List, Union
from google.cloud import storage
from ggvlib.logging import logger


def upload_from_string(
    data: Union[StringIO, BytesIO], path: str, content_type: str = "text/plain"
) -> None:
    b = os.environ["BUCKET"]
    logger.info(f"Loading bucket: {b}")
    bucket = storage.Client().bucket(b)
    bucket.blob(path).upload_from_string(data=data, content_type=content_type)
    logger.info(f"data -> gs://{b}/{path}")


def upload_from_file(local_path: str, destination_path: str) -> None:
    b = os.environ["BUCKET"]
    logger.info(f"Loading bucket: {b}")
    bucket = storage.Client().bucket(b)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(local_path)
    logger.info(f"data -> gs://{b}/{destination_path}")


def list_blobs(
    bucket_name: str, directory: str, client: storage.Client = storage.Client()
):
    """_summary_

    Args:
        client (storage.Client): _description_
        bucket_name (str): _description_
        directory (str): _description_

    Returns:
        _type_: _description_
    """
    return list(client.list_blobs(bucket_name, prefix=directory))


def list_files(bucket_name: str, directory: str) -> List[str]:
    """Lists file names in a Google Cloud Storage bucket based on a directory

    Parameters:
        directory (string): The directory to search

    Returns:
        files (List[str]): The resulting list of files
    """
    client = storage.Client()
    files = [
        f.name
        for f in list_blobs(
            client=client, bucket_name=bucket_name, directory=directory
        )
        if not f.name[-1] == "/"
    ]
    return files


def download_to_stream(
    bucket_name, file_path, file_object=BytesIO()
) -> BytesIO:
    """_summary_

    Args:
        bucket_name (_type_): _description_
        file_path (_type_): _description_
        file_object (_type_, optional): _description_. Defaults to io.BytesIO().

    Returns:
        io.BytesIO: _description_
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.download_to_file(file_object)
    file_object.seek(0)
    return file_object
