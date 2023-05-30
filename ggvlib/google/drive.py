from io import BytesIO

import google.auth
from typing import Dict, List
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
from ggvlib.logging import logger


def _service() -> Resource:
    """Create and return a Google Drive API service

    Returns:
        Resource: A google drive service
    """
    creds, _ = google.auth.default()
    return build(
        "drive",
        "v3",
        credentials=creds,
    )


def get_service() -> Resource:
    return _service()


def list_files_in_directory(
    drive_folder_id: str, page_size: int = 1000
) -> List[Dict[str, str]]:
    return (
        _service()
        .files()
        .list(
            q=f'"{drive_folder_id}" in parents',
            pageSize=page_size,
            fields="nextPageToken, files(id, name)",
        )
        .execute()
        .get("files", [])
    )


def get_item(item_id: str) -> Dict[str, str]:
    request = (
        _service()
        .files()
        .get(
            fileId=item_id, fields=get_default_fields(), supportsAllDrives=True
        )
    )
    return request.execute()


def download_file(file_id: str) -> BytesIO:
    item = get_item(file_id)
    if "google-apps" in item["mimeType"]:
        request = (
            _service()
            .files()
            .export_media(
                fileId=item["id"],
                mimeType=google_doc_mimetype(item["mimeType"]),
            )
        )
    else:
        request = _service().files().get_media(fileId=item["id"])
    file_handler = io.BytesIO()
    downloader = MediaIoBaseDownload(file_handler, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(
            f"Downloading item '{item['name']}': {str(status.progress() * 100)}%",
        )
    return file_handler


def upload_file(
    name_on_drive: str,
    local_path: str,
    parent_id: str = None,
    mime_type="text/csv",
    fields: str = "id",
) -> Dict[str, str]:
    """Uploads a CSV from a local file

    Args:
        name_on_drive (str): What to name the CSV file on Google Drive
        local_path (str): The local path of the CSV file
        parent_id (str, optional): The parent id (ie folder) to use. Defaults to None.
        mime_type (str, optional): The mimetype to use. Defaults to 'text/csv'.
        fields (List[str], optional): The fields to return. Defaults to ["id"].

    Returns:
        Dict[str, str]: A Google Drive API response including the newly created file id
    """
    meta_data = {"name": name_on_drive, "mimeType": mime_type}
    if parent_id:
        meta_data["parents"] = parent_id
    media = MediaFileUpload(filename=local_path, mimetype=mime_type)
    return (
        _service()
        .files()
        .create(body=meta_data, media_body=media, fields=fields)
        .execute()
    )


def share_file(emails: List[str], item_id: str, role: str = "writer") -> None:
    """Share a file on Google Drive with a list of email addresses

    Args:
        emails (List[str]): A list of emails to share the item with
        item_id (str): The id of the item to share
        role (str, optional): The role to apply to the list of emails. Defaults to "writer".
    """
    service = _service()
    batch = service.new_batch_http_request()
    for user_permission in [
        {"type": "user", "role": role, "emailAddress": email}
        for email in emails
    ]:
        batch.add(
            service.permissions().create(
                fileId=item_id,
                body=user_permission,
                fields="id",
            )
        )
    return batch.execute()


def get_default_fields():
    return "id, name, kind, mimeType, webViewLink, modifiedTime, createdTime"


def google_doc_mimetype(mimetype: str):
    """A Google Docs mimetype string for a given meta mimetype as.
    This is used for making a file, such as a CSV or Excel file interactable with via Google Drive

    Parameters:
        mimetype (string): The mimetype

    Returns:
        mimetype (string): The resulting Google Docs mimetype
    """
    doc_mimetypes = {
        "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    return doc_mimetypes.get(mimetype)
