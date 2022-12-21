from pathlib import Path
from typing import List
from ggvlib.logging import logger
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# You can find credentials here: https://console.cloud.google.com/apis/credentials/
def fetch_user_creds(
    scopes: List[str],
    cred_path: str = "token.json",
    secret_path: str = "credentials.json",
    port_no: int = 9000,
) -> Credentials:
    """Fetch authenticated user creds

    Args:
        scopes (List[str]): A list of scopes to authorize for
        cred_path (str, optional): Where to save and read the authorized token from. Defaults to "token.json".
        secret_path (str, optional): The path of the local credentials downloaded from the GCP project. Defaults to "credentials.json".
        port_no (int, optional): The port number to run the login flow on. Defaults to 9000.

    Returns:
        Credentials: Authorized user credentials
    """
    creds = None
    if Path(cred_path).exists():
        try:
            creds = Credentials.from_authorized_user_file(cred_path, scopes)
        except Exception as e:
            logger.warning(
                f"Saved token is not valid. Regenerating token. Details: {e}"
            )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                secret_path, scopes
            )
            creds = flow.run_local_server(port=port_no)
        with open(cred_path, "w") as token:
            token.write(creds.to_json())
    return creds
