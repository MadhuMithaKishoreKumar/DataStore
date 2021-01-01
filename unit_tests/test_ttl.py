import pytest
from yads import yads
import json
import tempfile
from exceptions import *


@pytest.fixture
def tempfile_name():
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="test_file")
    return path


def test_invalid_ttl(tempfile_name):
    data_store = yads(tempfile_name)
    key = "A"
    simple_json = "{}"

    # TTL is not properly formatted
    with pytest.raises(TimeToLiveFormatException):
        data_store.create(key, simple_json, "A")

    # TTL is not a correct value
    with pytest.raises(TimeToLiveFormatException):
        data_store.create(key, simple_json, 0)
