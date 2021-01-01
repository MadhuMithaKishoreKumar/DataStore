import pytest
from yads import yads
import json
import time
import tempfile
from exceptions import *


@pytest.fixture
def tempfile_name():
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="test_file")
    return path


def test_invalid_key_format(tempfile_name):
    data_store = yads(tempfile_name)
    simple_json = "{}"
    time_to_live = 1

    # Key is not of type str
    with pytest.raises(KeyFormatException):
        data_store.create(1, simple_json, time_to_live)

    # Key is longer than 32 characters
    with pytest.raises(KeyFormatException):
        data_store.create("A" * 33, simple_json, time_to_live)


def test_existing_key_creation(tempfile_name):
    data_store = yads(tempfile_name)
    key = "A"
    simple_json = "{}"
    time_to_live = 1

    data_store.create(key, simple_json, time_to_live)

    with pytest.raises(KeyExistsException):
        data_store.create(key, simple_json, time_to_live)


def test_key_access_after_expiration(tempfile_name):
    data_store = yads(tempfile_name)
    key = "A"
    simple_json = "{}"
    time_to_live = 1000

    data_store.create(key, simple_json, time_to_live)

    data_store.data_dict[key] = (time.time() - 100, simple_json)

    with pytest.raises(KeyExpiredException):
        data_store.read(key)
