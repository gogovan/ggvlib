import gzip
import json
from io import StringIO
from typing import Any, Dict, List, Union
from ggvlib.logging import logger


def nested_objects_to_string(data: dict) -> dict:
    """Turns the nested objects inside of a dict into JSON strings
    to prevent Big Query from freaking out

    Args:
        data (dict): The dictionary to transform

    Returns:
        dict: The resulting dictionary
    """
    for k, v in data.items():
        if not (isinstance(v, str) or isinstance(v, int)):
            data.update({k: json.dumps(v)})
    return data


def read_compressed_nl_json(file_path: str) -> List[dict]:
    """_summary_

    Args:
        file_path (str): _description_

    Returns:
        List[dict]: _description_
    """
    with gzip.open(file_path, "rt", encoding="UTF-8") as zipfile:
        items = json.load(zipfile)
    return items


def read_json(raw_data: str) -> Union[Dict[Any, Any], List[Any], None]:
    try:
        return json.loads(raw_data, object_hook=numeric_hook)
    except json.decoder.JSONDecodeError:
        logger.debug(f"Json decode error: {raw_data}")


def read_nl_json(s: List[str]) -> List[dict]:
    """Read a list of strings to JSON dict

    Args:
        s (List[str]): A list of strings

    Returns:
        List[dict]: A list of dictionaries
    """
    output = []
    for row in s:
        try:
            output.append(json.loads(row))
        except json.JSONDecodeError:
            pass
    return output


def write_compressed_nl_json(data: List[dict], output_file: str) -> None:
    """Write a list of dictionaries as new line deliminated JSON
    as a text file
    Args:
        data (List[dict]): A list of dictionaries
        output_file (str): The output file path
    """
    with gzip.open(output_file, "wt", encoding="UTF-8") as zipf:
        json.dump(data, zipf)


def write_nl_json_to_stream(data: List[dict]) -> StringIO:
    """Write a list of dictionaries to a StringIO object as
    a new line deliminated JSON

    Args:
        data (List[dict]): A list of dictionaries

    Returns:
        StringIO: The data written to the buffer
    """
    f = StringIO()
    for line in data:
        if line != {}:
            f.write(f"{line}\n")
    return f


def write_json_to_file(
    data: Union[List[dict], dict], output_file: str
) -> None:
    """Write a list of dictionaries or a single dictionary to a JSON
    file

    Args:
        data (Union[List[dict], dict]): A list of dictionaries or a single dictionary
        output_file (str): The output file path
    """
    with open(output_file, "w") as f:
        f.write(json.dumps(data))


def write_nl_json_to_file(data: List[dict], output_file: str) -> None:
    """Write a list of dictionaries as new line deliminated JSON
    as a text file

    Args:
        data (List[dict]): A list of dictionaries
        output_file (str): The output file path
    """
    with open(output_file, "a") as f:
        logger.info(f"Writing to {output_file}")
        for line in data:
            if line != {}:
                json.dump(line, f)
                f.write("\n")


def numeric_hook(d: dict) -> dict:
    """Try to convert all keys to numeric and pass if error

    Args:
        d (dict): Incoming JSON dict

    Returns:
        dict: JSON dict with numeric values
    """
    for k, v in d.items():
        try:
            d[k] = float(v)
        except (ValueError, TypeError):
            pass
    return d
