import itertools
import re
import string
from typing import Dict, List
from ggvlib.logging import logger
import google.auth
from googleapiclient.discovery import build, Resource
import pandas as pd


DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
]


def _client() -> Resource:
    credentials, _ = google.auth.default(scopes=DEFAULT_SCOPES)
    return build("sheets", "v4", credentials=credentials)


def get_range(sheet_id: str, sheet_range: str) -> Dict[str, str]:
    """Returns a dictionary containing data for a range of a sheet

    Parameters:
        sheet_id (string): The id of sheet to gather data from (can be found in the URL)
        sheet_range (string): The range to gather data from; using only the sheet name will return the entire sheet

    Returns:
        Dict[str, str]: The resulting data

    >>> get_range(sheet_id="example_id", sheet_range="Sheet1!A1:D2")
    {'range': 'Sheet1!A1:D2', 'majorDimension': 'ROWS', 'values': [['a', 'b', '1', '2'], ['4', '5', '3', '4']]}
    """
    logger.info(
        f"Getting cell data from range {sheet_range} in Google Sheet with id {sheet_id}"
    )
    return (
        _client()
        .spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=sheet_range)
        .execute()
    )


def get_range_as_df(
    sheet_id: str, sheet_range: str, header_row=0
) -> pd.DataFrame:
    """Returns a dictionary containing data for an entire sheet

    Parameters:
        sheet_id (string): The id of sheet to gather data from (can be found in the URL)
        sheet_range (string): The range to gather data from; using only the sheet name will return the entire sheet
        header_row (int): The index of the row to use as a header for the DataFrame

    Returns:
        pd.DataFrame: The resulting data
    """
    range_data = get_range(sheet_id, sheet_range)
    if len(range_data.get("values", [])) > header_row:
        return pd.DataFrame(
            range_data["values"][header_row + 1 :],
            columns=range_data["values"][0],
        )
    else:
        raise Exception("Specified range has no data")


def update_range(
    sheet_id: str, sheet_range: str, values: List[list]
) -> Dict[str, str]:
    logger.info(
        f"Updating cell data in range {sheet_range} in Google Sheet with id {sheet_id}"
    )
    """Updates data in a specified sheet range

        Parameters:
            sheet_id (string): The sheet to update data in
            sheet_range (string): The range to update data in
            values (List[list]): A list of lists for each cell in the range to update

        Returns:
            Dict[str, str]: The updated data

        >>> update_range(sheet_id="example_id", sheet_range="Sheet1!C1:D2", values=[[1,2],[3,4]])
        {'spreadsheetId': 'example_id', 'updatedRange': 'Sheet1!C1:D2', 'updatedRows': 2, 'updatedColumns': 2, 'updatedCells': 4}
        """
    data = {"values": values}
    return (
        _client()
        .spreadsheets()
        .values()
        .update(
            spreadsheetId=sheet_id,
            range=sheet_range,
            valueInputOption="USER_ENTERED",
            body=data,
        )
        .execute()
    )


def update_range_from_df(
    sheet_id: str, range_start: str, df: pd.DataFrame
) -> Dict[str, str]:
    """Updates data in a specified sheet range using a DataFrame

    Parameters:
        sheet_id (string): The sheet to update data in
        range_start (string): The upper left corner of the sheet
        df (pd.DataFrame): A DataFrame to update the sheet with

    Returns:
        Dict[str, str]: The updated data
    """
    if "!" in range_start:
        exclm_index = re.match(r"(?s:.*)(!)", range_start).span()[1]
        sheet_name = range_start[: exclm_index - 1]
        start_cell = range_start[exclm_index:]
        start_col = re.search(r"[A-Z]+", start_cell).group()
        start_row = int(re.search(r"[0-9]+", start_cell).group())

        col_range_start = excel_col_value(start_col)
        col_range = list(
            itertools.islice(excel_cols(), col_range_start + len(df.columns))
        )[col_range_start:]

        row_range = list(range(start_row, start_row + len(df)))
        cell_range = f"{sheet_name}!{col_range[0]}{row_range[0]}:{col_range[-1]}{row_range[-1]}"
        return update_range(
            sheet_id=sheet_id,
            sheet_range=cell_range,
            values=df.values.tolist(),
        )
    else:
        raise Exception(
            "Invalid range_start provided. Make sure you include a '!'."
        )


def excel_col_value(column_letter_value: str) -> int:
    """Gets the numerical index of a Google Sheets / Excel column

    Args:
        column_letter_value (str): The column letter ie AB

    Returns:
        int: The index
    """
    if len(column_letter_value) <= 2:
        values = []
        multiplier = 0
        for l in column_letter_value[::-1]:
            values.append(string.ascii_uppercase.index(l) + (multiplier * 26))
            multiplier += 1
        return sum(values)


def excel_cols():
    """Returns alphabetical indexes for Google Sheets / Excel columns

    Yields:
        _type_: _description_
    """
    n = 1
    while True:
        yield from (
            "".join(group)
            for group in itertools.product(string.ascii_uppercase, repeat=n)
        )
        n += 1
