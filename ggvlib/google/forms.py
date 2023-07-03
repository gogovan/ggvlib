from typing import Dict
from ggvlib.logging import logger
import google.auth
from googleapiclient.discovery import build, Resource
import pandas as pd
import numpy as np



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


def get_raw_responses(form_id: str) -> Dict[str, str]:
    """Get responses for a given form_id

    Args:
        form_id (str): A form_id

    Returns:
        Dict[str, str]: A dictionary of the form's responses
    """
    logger.info(f"Getting form responses from form {form_id}")
    return _client().forms().responses().list(formId=form_id).execute()


def get_raw_responses_as_df(form_id: str) -> pd.DataFrame:
    """Get responses for a given form_id as a Pandas DataFrame

    Args:
        form_id (str): A form_id

    Returns:
        pd.DataFrame: A DataFrame composed of the form's responses
    """
    return_df = pd.DataFrame()
    logger.info(f"Getting form responses as df from form {form_id}")
    responses = get_raw_responses(form_id)
    if "responses" in responses.keys():
        for row in responses["responses"]:
            return_list = list()
            return_list.append(row["responseId"])
            return_list.append(row["createTime"])
            return_list.append(row["lastSubmittedTime"])
            return_list.append(row["answers"])
            col_names = ["responseId", "createTime", "LastSubmittedTime", "answer"]
            for val in row["answers"].values():
                for answer in list(val.values())[1].values():
                    if "value" in list(answer[0].keys()):
                        col_names.append(val["questionId"])
                        return_list.append(answer[0]["value"])

            return_df = pd.concat([return_df, pd.DataFrame(return_list).T]).reset_index(
                drop=True
            )
        return_df.columns = col_names
    else:
        logger.info(f"There is no responses in form {form_id}")
    return return_df


def get_questions(form_id: str) -> Dict[str, str]:
    """Get questions for a given form_id

    Args:
        form_id (str): A form_id

    Returns:
        Dict[str, str]: A dictionary of the form's questions
    """
    logger.info(f"Getting questions from form {form_id}")
    return _client().forms().get(formId=form_id).execute()


def get_questions_as_df(form_id: str) -> pd.DataFrame:
    """Get question for a given form_id as a Pandas DataFrame

    Args:
        form_id (str): A form_id

    Returns:
        pd.DataFrame: A DataFrame composed of the form's questions
    """
    return_df = pd.DataFrame()
    data = get_questions(form_id)
    logger.info(f"Getting questions as df from form {form_id}")
    return_df = pd.DataFrame()
    for row in data["items"]:
        if "questionItem" in row:
            pair = list()
            pair.append(row["title"].strip())
            pair.append(row["questionItem"]["question"]["questionId"])
            pair.append("none")
            return_df = pd.concat([return_df, pd.DataFrame(pair).T]).reset_index(
                drop=True
            )
        elif "questionGroupItem" in row:
            for item in row["questionGroupItem"]["questions"]:
                pair = list()
                pair.append(row["title"].strip())
                pair.append(item["questionId"])
                pair.append(item["rowQuestion"]["title"])
                return_df = pd.concat([return_df, pd.DataFrame(pair).T]).reset_index(
                    drop=True
                )
    return_df = return_df.rename(
        columns={0: "question_title", 1: "question_id", 2: "sub_question"}
    )
    return return_df


def get_responses_as_df(form_id: str) -> pd.DataFrame:
    """Get responses for a given form_id as a Pandas DataFrame

    Args:
        form_id (str): A form_id

    Returns:
        pd.DataFrame: A DataFrame composed of the form's responses mapped with questions as column
    """
    question = get_questions_as_df(form_id)
    response = get_raw_responses_as_df(form_id)
    question["name"] = np.where(
        question["sub_question"] == "none",
        question["question_title"],
        question["question_title"] + "-" + question["sub_question"],
    )
    name_dict = dict()
    for id, name in zip(question["question_id"], question["name"]):
        name_dict[id] = name
    return_df = response.rename(columns=name_dict)
    return return_df

