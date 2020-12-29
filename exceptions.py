class KeyFormatException(Exception):
    '''Raised when Key format is wrong'''
    pass


class KeyExistsException(Exception):
    '''Raised when key already exists
    in data store'''
    pass


class ValueFormatException(Exception):
    '''Raised when value is not in
    the expected format'''
    pass


class TimeToLiveFormatException(Exception):
    '''Raised when ttl is not in
    the expected format'''
    pass


class KeyDoesNotExistException(Exception):
    '''Raised when the key does
    not exists in the datastore'''
    pass


class KeyExpiredException(Exception):
    '''Raised when the key has expired'''
    pass
