import os
from io import BytesIO, StringIO
from typing import List, Union
from google.cloud import storage
from google.cloud.storage.acl import ObjectACL
from ggvlib.logging import logger

ALLOWED_ROLES = ["WRITER", "READER", "OWNER"]


def upload_from_string(
    data: Union[StringIO, BytesIO], path: str, content_type: str = "text/plain"
) -> None:
    """_summary_

    Args:
        data (Union[StringIO, BytesIO]): _description_
        path (str): _description_
        content_type (str, optional): _description_. Defaults to "text/plain".
    """
    b = os.environ["BUCKET"]
    logger.info(f"Loading bucket: {b}")
    bucket = storage.Client().bucket(b)
    bucket.blob(path).upload_from_string(data=data, content_type=content_type)
    logger.info(f"data -> gs://{b}/{path}")


def upload_from_file(local_path: str, destination_path: str) -> None:
    """_summary_

    Args:
        local_path (str): _description_
        destination_path (str): _description_
    """
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
    blob = storage.Client().bucket(bucket_name).blob(file_path)
    blob.download_to_file(file_object)
    file_object.seek(0)
    return file_object


def share(
    bucket_name: str,
    cloud_storage_path: str,
    user_email: str,
    role: str = "READER",
) -> None:
    """Share a cloud storage blob with an email or google group

    Args:
        bucket_name (bucket_name): The bucket to read a blob from
        cloud_storage_path (str): The path of the blob to share
        user_email (str): The user to share with
        role (str, optional): The role to grant that user (WRITER, READER or OWNER). Defaults to "READER".
    """
    if role.upper() not in ALLOWED_ROLES:
        raise ValueError(f"Provided role must be one of {ALLOWED_ROLES}")
    blob = storage.Client().bucket(bucket_name).blob(cloud_storage_path)
    blob.acl.reload()
    blob.acl.user(user_email).grant(role)
    blob.acl.save()


def unshare(
    bucket_name: str,
    cloud_storage_path: str,
    user_email: str,
    role: str = None,
) -> None:
    """Unshare a cloud storage blob with an email or google group

    Args:
        bucket_name (bucket_name): The bucket to read a blob from
        cloud_storage_path (str): The path of the blob to unshare
        user_email (str): The user to revoke acl permissions for
        role (str, optional): The role to revoke from that user (WRITER, READER or OWNER). Defaults to None.
    """
    blob = storage.Client().bucket(bucket_name).blob(cloud_storage_path)
    blob.acl.reload()
    if role:
        if role.upper() not in ALLOWED_ROLES:
            raise ValueError(f"Provided role must be one of {ALLOWED_ROLES}")
        blob.acl.user(user_email).revoke(role)
    else:
        blob.acl.user(user_email).revoke_read()
        blob.acl.user(user_email).revoke_write()
        blob.acl.user(user_email).revoke_owner()
    blob.acl.save()


def get_shared(bucket_name: str, cloud_storage_path: str) -> List[ObjectACL]:
    """Returns a list of ObjectACL for a blob

    Args:
        bucket_name (bucket_name): The bucket to read a blob from
        cloud_storage_path (str): The path of the blob to get ObjectACLs for

    Returns:
        List[ObjectACL]: A list of ObjectACL for a blob
    """

    return (
        storage.Client()
        .bucket(bucket_name)
        .blob(cloud_storage_path)
        .acl.get_entities()
    )
