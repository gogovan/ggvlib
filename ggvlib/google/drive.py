import google.auth
from typing import Dict, List
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaFileUpload


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
