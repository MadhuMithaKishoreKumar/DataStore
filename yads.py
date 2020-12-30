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
        :return:
        '''
        self.data_dict = {}
        self.file_path = file_path
        self.file = None
        self.lock = threading.Lock()
        self.initialize_data_store()
        
    def generate_file_path(self):
        '''
        This is to build a file path 
        :return: A new file path
        '''
        new_file_path = os.getcwd() + "/datastore" + str(int(time.time())) + ".txt"
        print("Creating new file: " + new_file_path)
        return new_file_path

    def initialize_data_store(self):
        '''
        This is to initialize the data store.
        if there is no file path a new file directory is generated 
        and the file is opened for writing in binary mode.
        if a file directory exists,it is opened for writing in binary
        mode and the data_dict is loaded in pickle file.
        
        '''
        if self.file_path is None:
            self.file_path = self.generate_file_path()
            self.file = open(self.file_path, 'wb')
        else:
            self.file = open(self.file_path, 'wb')
            if os.path.getsize(self.file_path) > 0:
                self.data_dict = pickle.load(self.file)

    def check_key_value_format(self, key, value, time_to_live):
        '''
        checks the format of key value
        :param key:
        :param value:
        :param time_to_value:
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

        if time_to_live is not None and (
            not isinstance(
                time_to_live,
                int) or time_to_live <= 0):
            '''
            Checks the ttl whether it is in the expected format
            '''
            raise TimeToLiveFormatException

    def get_expiration_time(self, time_to_live):
        '''
        :param time_to_live:
        :return: returns the time in which it should expire
        '''
        return int(time.time()) + time_to_live - 1

    def create(self, key, value, time_to_live=None):
        '''
        This is used to create a key and to check the key value format,
        it make sure the property of thread safety
        :param key:
        :param value:
        :param time_to_value:
        :return: returns nothing
        '''
        self.check_key_value_format(key, value, time_to_live)
        self.lock.acquire()
        self.data_dict[key] = (self.get_expiration_time(time_to_live), value)
        self.lock.release()

    def read(self, key):
        '''
        finds the key in the data store and displays the value.
        :param:key
        :return:returns ttl and value
        '''
        if key not in self.data_dict:
            '''
            checks if the key doesn't exists
            '''
            raise KeyDoesNotExistException

        if int(time.time()) >= self.data_dict[key][0]:
            '''
            checks if the key does not expire,
            if the key got expired it raises an Exception
            
            '''
            self.lock.acquire()
            self.data_dict.pop(key)
            self.lock.release()
            raise KeyExpiredException

        return self.data_dict[key][1]

    def delete(self, key):
        '''
        finds the key and deletes it from the data store 
        :param key:
        :return: returns nothing
        '''
        if key not in self.data_dict:
            '''
            checks if the key doesn't exists
            '''
            raise KeyDoesNotExistException

        if int(time.time()) >= self.data_dict[key][0]:
            '''
            checks if the key does not expire,
            if the key got expired it raises an Exception.
            '''
            self.lock.acquire()
            self.data_dict.pop(key)
            self.lock.release()
            raise KeyExpiredException

        self.lock.acquire()
        self.data_dict.pop(key)
        self.lock.release()

    def is_json(self, myjson):
        '''
        checks if the given value is in json format
        :param myjson:
        :return: Boolean value
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
        self.file.close()
