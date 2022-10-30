import yaml


def from_path(path: str) -> dict:
    with open(path, "r") as f:
        result = yaml.safe_load(f)
    return result
