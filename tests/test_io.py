from io import StringIO
import json
from ggvlib.io.json import (
    nested_objects_to_string,
    numeric_hook,
    read_json,
    read_nl_json,
    write_nl_json_to_stream,
)


def test_nested_objects_to_string(nested_dict, str_nested_dict):
    assert str_nested_dict == nested_objects_to_string(nested_dict)


def test_read_json(nested_dict):
    json_dict = json.dumps(nested_dict)
    assert read_json(json_dict) == nested_dict


def test_numeric_hook():
    assert numeric_hook({"a": "1"}) == {"a": 1.0}


def test_read_nl_json():
    assert read_nl_json(['{"a":"b"}', '{"c":"d"}']) == [
        {"a": "b"},
        {"c": "d"},
    ]


def test_write_nl_json_to_stream():
    result = write_nl_json_to_stream(
        [
            {"a": "b"},
            {"c": "d"},
        ]
    )
    assert isinstance(result, StringIO)
