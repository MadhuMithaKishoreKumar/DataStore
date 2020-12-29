import pytest
from yads import yads
import json
import tempfile
from exceptions import *


@pytest.fixture
def tempfile_name():
    temp_file = tempfile.NamedTemporaryFile(suffix=".txt", prefix="test_file")
    return temp_file.name


def test_invalid_value_format(tempfile_name):
    data_store = yads(tempfile_name)
    key = "A"
    time_to_live = 1

    # Value is not of type JSON
    with pytest.raises(ValueFormatException):
        data_store.create(key, 1, time_to_live)

    # Value is not a properly formatted JSON
    with pytest.raises(ValueFormatException):
        data_store.create(key, "{/}", time_to_live)
