import pandas as pd
import google.auth
from typing import Union
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery.table import _EmptyRowIterator, RowIterator
from ggvlib.logging import logger

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]


def _client() -> bigquery.Client:
    """_summary_

    Returns:
        bigquery.Client: _description_
    """
    credentials, project = google.auth.default(scopes=DEFAULT_SCOPES)
    return bigquery.Client(project, credentials)


def query_to_df(query: str) -> pd.DataFrame:
    """_summary_

    Args:
        query (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    logger.debug(f"Running query: {query}")
    result = _client().query(query).result().to_dataframe()
    logger.debug(f"Result: {len(result)} row(s)")
    return result


def query_to_storage_df(query: str) -> pd.DataFrame:
    """_summary_

    Args:
        query (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    logger.debug(f"Running query: {query}")
    storage_client = bigquery_storage.BigQueryReadClient()
    result = (
        _client()
        .query(query)
        .result()
        .to_dataframe(bqstorage_client=storage_client)
    )
    logger.debug(f"Result: {len(result)} row(s)")
    return result


def query_to_storage(
    query: str, output_path: str, fmt="CSV"
) -> Union[RowIterator, _EmptyRowIterator]:
    logger.debug(f"Running query: {query}")
    if fmt == "CSV":
        query_job = _client().query(
            f"""
            EXPORT DATA OPTIONS(
            uri='{output_path}',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';') AS
            {query}
            ORDER BY 1
            """
        )
    elif fmt == "JSON":
        query_job = _client().query(
            f"""
            EXPORT DATA OPTIONS(
            uri='{output_path}',
            format='JSON',
            overwrite=true) AS
            {query}
            ORDER BY 1
            """
        )
    else:
        raise ValueError("Invalid Big Query export format")
    return query_job.result()
