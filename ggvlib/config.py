from ggvlib.io import yaml


def from_yaml(path: str) -> dict:
    return yaml.from_path(path)
