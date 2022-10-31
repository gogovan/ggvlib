import pytest


@pytest.fixture()
def nested_dict() -> dict:
    return {"a": "b", "c": 1, "d": {"e": 5}}


@pytest.fixture()
def str_nested_dict() -> dict:
    return {"a": "b", "c": 1, "d": '{"e": 5}'}
