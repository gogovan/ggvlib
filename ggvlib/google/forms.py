from typing import Dict
import pandas as pd
from googleapiclient.discovery import build, Resource

from ggvlib.logging import logger

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/forms.body",
    "https://www.googleapis.com/auth/forms.body.readonly",
    "https://www.googleapis.com/auth/forms.responses.readonly",
]
# DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"


def _client() -> Resource:
    """Returns a google forms client

    Returns:
        Resource: A Google Forms client
    """
    credentials, _ = google.auth.default(scopes=DEFAULT_SCOPES)
    return build(
        "forms",
        "v1",
        credentials=credentials,
    )


def get_client() -> Resource:
    """Returns a google forms client

    Returns:
        Resource: A Google Forms client
    """
    return _client()


def get_responses(form_id: str) -> Dict[str, str]:
    """Get responses for a given form_id

    Args:
        form_id (str): A form_id

    Returns:
        Dict[str, str]: A dictionary of the form's responses
    """
    logger.info(f"Getting form responses from form {form_id}")
    return _client().forms().responses().list(formId=form_id).execute()


def get_responses_as_df(form_id: str) -> pd.DataFrame:
    """Get responses for a given form_id as a Pandas DataFrame

    Args:
        form_id (str): A form_id

    Returns:
        pd.DataFrame: A DataFrame composed of the form's responses
    """
    return_df = pd.DataFrame()
    logger.info(f"Getting form responses as df from form {form_id}")
    responses = get_responses(form_id)
    for row in responses["responses"]:
        return_list = list()
        return_list.append(row["responseId"])
        return_list.append(row["createTime"])
        return_list.append(row["lastSubmittedTime"])
        return_list.append(row["answers"])
        for val in row["answers"].values():
            for answer in list(val.values())[1].values():
                if "value" in list(answer[0].keys()):
                    return_list.append(answer[0]["value"])

        return_df = pd.concat(
            [return_df, pd.DataFrame(return_list).T]
        ).reset_index(drop=True)
    return_df = return_df.rename(
        columns={
            0: "response_id",
            1: "created_time",
            2: "last_submitted_time",
            3: "answers",
        }
    )
    return return_df
