'''
Implements a basic future.
Provided that it is accessed by the set and get methods,
this enforces write-once semantics.

TODO: implement a faster C version
'''
import threading

class FutureSetTwiceException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class Future:
    def __init__(self):
        self.__data = None
        self.__isset = False
        self.__cond = threading.Condition()
    
    def get(self):
        """
        Get the value of a future.  If it is
        not available, block until it is
        """
        self.__cond.acquire()
        while not self.__isset:
            self.__cond.wait()
        res = self.__data
        self.__cond.release()
        return res
    
    def set(self,data):
        """
        Sets the value of a future.  This is only allowed
        to be done once
        """
        self.__cond.acquire()
        if self.__isset:
            raise FutureSetTwiceException("A thread attempted to set a filled"
                            + "future a second time")
        self.__isset = True
        self.__data =data
        self.__cond.notify()
        self.__cond.release()

    def isSet(self):
        self.__cond.acquire()
        val = self.__isset
        self.__cond.release()
        return val
