import base64
import os
import mimetypes
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail,
    Attachment,
    FileContent,
    FileName,
    FileType,
    Disposition,
)
from typing import List
from ggvlib.io.files import get_path_if_exists
from ggvlib.logging import logger

logger.info("Initializing mimetypes to determine email attachment file types")
mimetypes.init()


def send_mail(
    html_body: str,
    subject: str,
    to_emails: List[str],
    from_email: str,
    attachment_paths: List[str] = [],
    api_key: str = os.environ["SENDGRID_API_KEY"],
) -> None:
    """Send an email via sendgrid

    Args:
        html_body (str): The html body of the email to send
        subject (str): The subject of the email
        to_emails (List[str]): A list of email to send the email to
        from_email (str): The email to send the message from
        attachment_paths (List[str], optional): A list of paths of files to attach. Defaults to [].
        api_key (str, optional): Your Sendgrid API key. Defaults to os.environ["SENDGRID_API_KEY"].

    >>> from ggvlib.reporting import mail
    >>> mail.send_mail(
    ... html_body="<h1>Hello there</h1>",
    ... subject="An Automated Email Report with an Attachment",
    ...     to_emails=["some.guy@somedomain.com"],
    ...     from_email="noreply@somedomain.com",
    ...     attachment_paths=[
    ...         "data/test.txt",
    ...     ],
    ... )

    """
    message = Mail(
        to_emails=to_emails,
        from_email=from_email,
        subject=subject,
        html_content=html_body,
    )
    if attachment_paths:
        for path in attachment_paths:
            file_path = get_path_if_exists(path)
            with open(path, "rb") as f:
                data = f.read()
            encoded_file = base64.b64encode(data).decode()
            attachedFile = Attachment(
                FileContent(encoded_file),
                FileName(file_path.name),
                FileType(mimetypes.guess_type(file_path)[0]),
                Disposition("attachment"),
            )
            message.add_attachment(attachedFile)
            logger.debug(f"Added attachment: '{file_path}'")
    sg = SendGridAPIClient(api_key)
    return sg.send(message)
