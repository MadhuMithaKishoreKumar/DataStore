import time
import pickle
import os
import json
import threading

from exceptions import *


class yads:
    '''
    YADS = Yet Another Data Store
    This class implements a key-value data store
    that is persisted on disk. The class can be instantiated
    and used as necessary
    Methods:
            * generate_file_name
            * initialize_data_store
    '''

    def __init__(self, file_path=None):
        self.data_dict = {}
        self.file_path = file_path
        self.file = None
        self.lock = threading.Lock()
        self.initialize_data_store()

    def generate_file_path(self):
        new_file_path = os.getcwd() + "/datastore" + str(int(time.time())) + ".txt"
        print("Creating new file: " + new_file_path)
        return new_file_path

    def initialize_data_store(self):

        if self.file_path is None:
            self.file_path = self.generate_file_path()
            self.file = open(self.file_path, 'wb')
        else:
            self.file = open(self.file_path, 'wb')
            if os.path.getsize(self.file_path) > 0:
                self.data_dict = pickle.load(self.file)

    def check_key_value_format(self, key, value, time_to_live):
        if key in self.data_dict:
            raise KeyExistsException

        if not isinstance(key, type("")) or len(key) > 32:
            raise KeyFormatException

        if not self.is_json(value) or self.utf8len(value) > 160000:
            raise ValueFormatException

        if time_to_live is not None and (
            not isinstance(
                time_to_live,
                int) or time_to_live <= 0):
            raise TimeToLiveFormatException

    def get_expiration_time(self, time_to_live):
        return int(time.time()) + time_to_live - 1

    def create(self, key, value, time_to_live=None):

        self.check_key_value_format(key, value, time_to_live)
        self.lock.acquire()
        self.data_dict[key] = (self.get_expiration_time(time_to_live), value)
        self.lock.release()

    def read(self, key):
        if key not in self.data_dict:
            raise KeyDoesNotExistException

        if int(time.time()) >= self.data_dict[key][0]:
            self.data_dict.pop(key)
            raise KeyExpiredException

        return self.data_dict[key][1]

    def delete(self, key):
        if int(time.time()) >= self.data_dict[key][0]:
            self.lock.acquire()
            self.data_dict.pop(key)
            self.lock.release()
            raise KeyExpiredException

        if key not in self.data_dict:
            raise KeyDoesNotExistException

        self.lock.acquire()
        self.data_dict.pop(key)
        self.lock.release()

    def is_json(self, myjson):
        try:
            json_object = json.loads(myjson)
        except BaseException:
            return False
        return True

    def utf8len(self, s):
        return len(s.encode('utf-8'))

    def __del__(self):
        pickle.dump(self.data_dict, self.file)
        self.file.close()
