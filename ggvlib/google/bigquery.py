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
    """Returns an authorized bigquery.Client object with access to Big Query and Google Drive

    Returns:
        bigquery.Client: An authorized bigquery.Client object
    """
    credentials, project = google.auth.default(scopes=DEFAULT_SCOPES)
    return bigquery.Client(project, credentials)


def write_to_table(data: list[dict], table: str) -> None:
    """Upload data to a table in BigQuery

    Args:
        data (List[dict]): The data to upload
        table (str): The table to append

    Raises:
        RuntimeError: Raised when the table is not appended properly
    """
    logger.info(f"Inserting {len(data)} row(s) into {table}")
    if data:
        errors = _client().insert_rows_json(table=table, json_rows=data)
        if errors:
            logger.error(errors)
            raise RuntimeError(
                "Someting went wrong trying to append the table in big query"
            )
        return
    else:
        logger.info("No new data to insert")
        return


def query(query: str) -> list[dict]:
    """Runs a query in Big Query and returns the results as a list of dictionaries
    Args:
        query (str): The query to run
    Returns:
        list[dict]: The results
    """
    result = [dict(row) for row in _client().query(query)]
    logger.debug(f"Result: {len(result)} row(s).")
    return result


def query_to_df(query: str) -> pd.DataFrame:
    """Runs a query and returns the results as a Pandas DataFrame

    Args:
        query (str): The query to run

    Returns:
        pd.DataFrame: _description_
    """
    logger.debug(f"Running query: {query}")
    result = _client().query(query).result().to_dataframe()
    logger.debug(f"Result: {len(result)} row(s)")
    return result


def query_to_storage_df(query: str) -> pd.DataFrame:
    """Runs a query and returns the results as a Pandas DataFrame using Google Cloud Storage

    Args:
        query (str): The query to run

    Returns:
        pd.DataFrame: The results as a Pandas DataFrame
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
    """Runs a query and exports the results to Google Cloud Storage as a CSV or NL JSON file

    Args:
        query (str): The query to run
        output_path (str): The path to output the results to on GCS
        fmt (str, optional): The format to output the results as. Defaults to "CSV".

    Raises:
        ValueError: If the export format is not supported

    Returns:
        Union[RowIterator, _EmptyRowIterator]: Results from the BQ API
    """
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
