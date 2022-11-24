from typing import Generator, List
from datetime import datetime


def datetime_to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def flatten(l: list) -> list:
    """Flattens a list of lists

    Args:
        l (list): A list of lists

    Returns:
        list: A flattened list
    """
    return [item for sublist in l for item in sublist]


def node_map(d: dict, parent: str = None) -> List[tuple]:
    """_summary_

    Args:
        d (dict): _description_
        parent (str, optional): _description_. Defaults to None.

    Returns:
        List[tuple]: _description_
    """
    results = []
    for key, v in d.items():
        new_path = f"{parent}.{key}" if parent else key
        if isinstance(v, dict):
            results.extend(node_map(parent=new_path, d=v))
        elif isinstance(v, list):
            for val in v:
                if isinstance(val, dict):
                    results.extend(node_map(parent=new_path, d=val))
                else:
                    results.append((new_path, v))
        else:
            results.append((new_path, v))
    return results


def find(d: dict, path: str) -> List[dict]:
    """_summary_

    Args:
        d (dict): _description_
        path (str): _description_

    Raises:
        ValueError: _description_

    Returns:
        List[dict]: _description_
    """
    results = {
        p: [node[1] for node in node_map(d) if node[0] == p] for p in path
    }
    for k, v in results.items():
        if len(v) == 1:
            results.update({k: v[0]})
        elif not v:
            raise ValueError(f"Path {k} does not exist in data")
    return results


def chunks(lst: list, n: int) -> Generator[list, None, None]:
    """Yield successive n-sized chunks from lst.

    Args:
        lst (list): The list to split into chunks
        n (int): Chunk size

    Yields:
        Generator[list, None, None]: A list of lists
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
