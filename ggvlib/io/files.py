from pathlib import Path


def get_path_if_exists(p: str) -> Path:
    """Returns the pathlib.Path of a file or directory if it exists

    Args:
        p (str): The path of the file to open

    Raises:
        FileNotFoundError: If the file or directory does not exist at the path you provided

    Returns:
        Path: A pathlib.Path object for a file or directory
    """
    if (file_path := Path(p)).exists:
        return file_path
    raise FileNotFoundError(f"File does not exist at '{p}'")


def get_file_bytes(p: str) -> bytes:
    """Returns bytes for a file at a given path if it exists


    Args:
        p (str): The path to read the file bytes from

    Returns:
        bytes: Bytes for a file at a given path

    Raises:
        FileNotFoundError: If the file or directory does not exist at the path you provided
    """
    with open(get_path_if_exists(p), "rb") as f:
        return f.read()
