# YADS - Yet Another Data Store! 
This is an assignment for Freshworks.
YADS is a library to implement a simple key-value data storage system. The data is persisted in binary format by using python's `pickle` library.

## Prerequisites
```
pip install filelock
pip install pytest
```

## Setup Instructions
```
git clone https://github.com/MadhuMithaKishoreKumar/DataStore.git
export export PYTHONPATH="${PYTHONPATH}:/<path_where_it_was_cloned"
```

## Example usage
```
from yads import yads
data_store = yads()

# Create a key-value pair
data_store.create(key, value, ttl)

# Read key from data_store
data_store.read(key)

# Delete key from data_store
data_store.delete(key)
```

## Running Unit Tests
In the directory where the library was cloned run:
```
python -m pytest
```

## Library characteristics
- Thread Safety: The program ensures thread safety by using `threading.Lock()` to ensure only one thread can create/delete key-value pairs in the data store
- File Safety: The program ensures process safety by locking the file using `FileLock()` to ensure no other process can access the file while the data store is running.
