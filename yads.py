import time
import pickle
import os
import sys
import json
import threading

from filelock import FileLock
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
            * check_key_value_format
            * get_expiration_time
            * create
            * read
            * delete
            * is_json
            * utf8len
    '''
    def __init__(self, file_path=None):
        '''
        constructor
        :param file_path: file path
        :return: returns nothing
        '''
        self.data_dict = {}
        self.file_path = file_path
        self.file = None
        self.data_thread_lock = threading.Lock()
        self.file_process_lock = None
        self.initialize_data_store()

    def generate_file_path(self):
        '''
        This is to build a file path 
        :return: returns a new file path
        '''
        new_file_path = os.getcwd() + "/datastore" + str(int(
            time.time())) + ".txt"
        print("Creating new file: " + new_file_path)
        return new_file_path

    def initialize_data_store(self):
        '''
        This is to initialize the data store.
        if there is no file path a new file directory is generated 
        and the file is opened for writing in binary mode.
        if a file directory exists,it is opened for writing in binary
        mode and the data_dict is loaded in pickle file.
        The file is locked so that other process cannot be executed.
        
        '''
        if self.file_path is None:
            self.file_path = self.generate_file_path()
            self.file = open(self.file_path, 'wb')
        else:
            self.temp_file = open(self.file_path, 'rb')
            if os.path.getsize(self.file_path) > 0:
                self.data_dict = pickle.load(self.temp_file)
            self.temp_file.close()
            self.file = open(self.file_path, 'wb')

        self.file_process_lock = FileLock(self.file_path)
        self.file_process_lock.acquire()
        if not self.check_file_size():
            print("given file has reached 1 gb limit,Terminating Program...")
            sys.exit()

    def check_key_value_format(self, key, value, time_to_live):
        '''
        checks the format of key value
        :param key: example tina 
        :param value:{}
        :param time_to_value: example 100
        :return: returns nothing
        '''
        if key in self.data_dict:
            '''
            if the key exists in the dictionary it raises 
            the keyExistsException.
            '''
            raise KeyExistsException

        if not isinstance(key, type("")) or len(key) > 32:
            '''
            Checks if key meets required conditions of being 
            a string and capped at 32 chars.
            '''
            raise KeyFormatException

        if not self.is_json(value) or self.utf8len(value) > 160000:
            '''
            Checks if the size of the value exceeds 16 KB.
            '''
            raise ValueFormatException

        if time_to_live is not None and (not isinstance(time_to_live, int)
                                         or time_to_live <= 0):
            '''
            Checks the ttl whether it is in the expected format
            '''
            raise TimeToLiveFormatException

    def get_expiration_time(self, time_to_live):
        '''
        :param time_to_live: example 100
        :return: returns the time in which it should expire
        '''
        if time_to_live is None:
            return None
        return int(time.time()) + time_to_live - 1

    def create(self, key, value, time_to_live=None):
        '''
        This is used to create a key and to check the key value format,
        it make sure the property of thread safety
        :param key: example name
        :param value: {}
        :param time_to_value: example 100
        :return: returns nothing
        '''
        self.check_key_value_format(key, value, time_to_live)
        self.data_thread_lock.acquire()
        self.data_dict[key] = (self.get_expiration_time(time_to_live), value)
        self.data_thread_lock.release()

        if self.check_dict_size_within_limits():
            print("key is created successfully")

        else:
            self.data_thread_lock.acquire()
            self.data_dict.pop(key)
            self.data_thread_lock.release()
            print("key creation unsuccessful! size exceeds 1 gb")

    def read(self, key):
        '''
        finds the key in the data store and displays the value.
        :param key: example name
        :return:returns ttl and value
        '''
        if key not in self.data_dict:
            '''
            checks if the key doesn't exists
            '''
            raise KeyDoesNotExistException

        if self.data_dict[key][0] is not None and int(
                time.time()) >= self.data_dict[key][0]:
            '''
            checks if the key does not expire,
            if the key got expired it raises an Exception
            
            '''
            self.data_thread_lock.acquire()
            self.data_dict.pop(key)
            self.data_thread_lock.release()
            raise KeyExpiredException

        return self.data_dict[key][1]

    def delete(self, key):
        '''
        finds the key and deletes it from the data store 
        :param key: example name
        :return: returns nothing
        '''
        if key not in self.data_dict:
            '''
            checks if the key doesn't exists
            '''
            raise KeyDoesNotExistException

        if self.data_dict[key][0] is not None and int(
                time.time()) >= self.data_dict[key][0]:
            '''
            checks if the key does not expire,
            if the key got expired it raises an Exception.
            '''
            self.data_thread_lock.acquire()
            self.data_dict.pop(key)
            self.data_thread_lock.release()
            raise KeyExpiredException

        self.data_thread_lock.acquire()
        self.data_dict.pop(key)
        self.data_thread_lock.release()
        print("key is deleted")

    def check_dict_size_within_limits(self):
        if sys.getsizeof(self.data_dict) <= 1e+9:
            return True
        else:
            return False

    def check_file_size(self):
        if os.path.getsize(self.file_path) <= 1e+9:
            return True
        else:
            return False

    def is_json(self, myjson):
        '''
        checks if the given value is in json format
        :param myjson: json value
        :return: Boolean
        '''
        try:
            json_object = json.loads(myjson)
        except BaseException:
            return False
        return True

    def utf8len(self, s):
        '''
        :param:s
        :return: length of s
        '''
        return len(s.encode('utf-8'))

    def __del__(self):
        '''
        Destructor
        writes the data to file and closes the file
        '''
        pickle.dump(self.data_dict, self.file)
        self.file_process_lock.release()
        self.file.close()
