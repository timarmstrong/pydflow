'''
@author: Tim Armstrong
'''
import unittest
import time
import logging

from PyDFlow.PyFun import future, func
from PyDFlow.base.states import *
import threading as th

from PyDFlow.types import Multiple, FlTypeError

import PyDFlow.examples.PyFun as ex

import stacktrace

Int = future.subtype()
String = future.subtype()
#logging.basicConfig(level=logging.DEBUG)


@func((Int), ())
def one():
    return 1

@func((Int), (Int))
def inc(x):
    return x + 1

@func((Int), (Int, Int))
def add(x, y):
    return x + y

def double(n):
    return add(n,n)

@func((String), (String, String))
def cat(first, second):
    return first + second 

@func((String), (Multiple(String)))
def cat2(*args):
    return "".join(args)
@func((Int), (None))
def rec_fib(n):
    if n == 0:
        print "got: rec_fib(0) = 0"
        return 0
    elif n == 1:
        print "got: rec_fib(1) = 1"
        return 1
    else:
        print "wait: rec_fib(%d)" % n
        res = rec_fib(n-1).get() + rec_fib(n-2).get()
        print "got: rec_fib(%d) = %d" % (n, res)
        return res

@func((Int), (Int, Int))
def silly_add(x, y):
    if x == 0:
        return y
    elif x < 0:
        return silly_add(Int(x + 1), Int(y - 1)).get()
    else:
        return silly_add(Int(x - 1), Int(y + 1)).get()
    
class MyException(Exception):
    pass
@func((Int), ())
def cause_exception():
    raise MyException()

class TestPyFun(unittest.TestCase):


    def setUp(self):
        pass
        


    def tearDown(self):
        pass

    def testRecurse5(self):
        """
        Check that it works ok if we call a future which is in process of ebing filled
        """
        import stacktrace
        stacktrace.trace_start("trace.html",interval=5,auto=True) # Set auto flag to always update file!
        try:
            @func((Int), (Int, None))
            def sleep(x, dur):
                time.sleep(dur)
                return x
            # Sleep to ensure that x won't be finished ebfore fn launches
            x = sleep(silly_add(Int(30), Int(20)), 2)
            x.force()
            
            @func((Int), ())
            def fn():
                return x.get()        
            self.assertEquals(fn().get(), 50)
        finally:
            stacktrace.trace_stop()
        
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()