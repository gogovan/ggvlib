import itertools
import re
import string
from typing import Dict, List
from ggvlib.logging import logger
import google.auth
from googleapiclient.discovery import build, Resource
from httplib2 import Http
import pandas as pd
import json


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
    credentials, _ = google.auth.default(scopes=DEFAULT_SCOPES)
    return build(
        "forms",
        "v1",
        credentials=credentials,
    )


def get_response(form_id) -> Dict[str, str]:
    logger.info(f"Getting form response from form {form_id}")
    return _client().forms().responses().list(formId=form_id).execute()


def get_response_as_df(form_id) -> pd.DataFrame:
    return_df = pd.DataFrame()
    logger.info(f"Getting form response as df from form {form_id}")
    response = get_response(form_id)
    for row in response["responses"]:
        return_list = list()
        return_list.append(row["responseId"])
        return_list.append(row["createTime"])
        return_list.append(row["lastSubmittedTime"])
        return_list.append(row["answers"])
        for val in row["answers"].values():
            for ans in list(val.values())[1].values():
                if "value" in list(ans[0].keys()):
                    return_list.append(ans[0]["value"])

        return_df = pd.concat([return_df, pd.DataFrame(return_list).T]).reset_index(
            drop=True
        )
    return_df = return_df.rename(
        columns={
            0: "response_id",
            1: "created_time",
            2: "last_submitted_time",
            3: "answers",
        }
    )
    return return_df
