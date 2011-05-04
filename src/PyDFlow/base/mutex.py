'''
@author: Tim Armstrong
'''
from threading import Lock, currentThread
import logging 



# Logging lock for debugging purposes
# from http://stackoverflow.com/questions/5327614/logging-lock-acquire-and-release-calls-in-multi-threaded-application
class LogLock(object):
    def __init__(self):
        self.lock = Lock()

    def acquire(self, blocking=True):
        logging.debug("%d: %s trying to acquire lock" %
            (id(self), currentThread().getName()))
        ret = self.lock.acquire(blocking)
        if ret == True:
            logging.debug("%d: %s acquired lock" % (
                id(self), currentThread().getName()))
        else:
            logging.debug("%d: %s non-blocking aquire of lock failed".format(
                id(self), currentThread().getName()))
        return ret

    def release(self):
        logging.debug("%d: %s releasing  lock" % 
                        (id(self), currentThread().getName()))
        self.lock.release()

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False    # Do not swallow exceptions


graph_mutex = Lock()
#graph_mutex = LogLock()
