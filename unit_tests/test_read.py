import pytest
from yads import yads
import json
import tempfile
from exceptions import *


@pytest.fixture
def tempfile_name():
    temp_file = tempfile.NamedTemporaryFile(suffix=".txt", prefix="test_file")
    return temp_file.name

def test_key_value_deletion_is_successful(tempfile_name):
    data_store = yads(tempfile_name)
    key = "A"
    simple_json = "{}"
    time_to_live = 100

    data_store.create(key, simple_json, time_to_live)

    assert data_store.read(key) == simple_json
