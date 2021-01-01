import pytest
import os
from yads import yads
import json
from tempfile import mkstemp
from exceptions import *


def test_data_persist_successful():
    fd, path = mkstemp(suffix=".txt", prefix="temp")
    data_store = yads(path)
    key = "A"
    simple_json = "{}"
    time_to_live = 100
    data_store.create(key, simple_json, time_to_live)
    del data_store
    data_store = yads(path)
    assert data_store.read(key) == simple_json
    del data_store
    #os.remove(path)
