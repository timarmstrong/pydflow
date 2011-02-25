'''
Created on 19/02/2011

@author: tim
'''
import unittest
import time
import logging

from PyDFlow.PyFun import future, func
from PyDFlow.base.states import *
import threading as th

from PyDFlow.types import Multiple, FlTypeError

import PyDFlow.examples.PyFun as ex

Int = future.subtype()
String = future.subtype()
logging.basicConfig(level=logging.DEBUG)


@func((Int), ())
def one():
    return 1

@func((Int), (Int))
def inc(x):
    return x + 1

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
        
class TestPyFun(unittest.TestCase):


    def setUp(self):
        pass
        


    def tearDown(self):
        pass


    def testGet1(self):
        x = Int(42)
        self.assertEquals(x.get(), 42)
        self.assertEquals(x.get(), 42)
        self.assertEquals(x.get(), 42)
    
    def testSimpleFun(self):
        x = Int()

        x <<= one()
        self.assertEquals(x.state(), CH_CLOSED)
        self.assertEquals(x.readable(), False)
        self.assertEquals(x.get(), 1)
        self.assertEquals(x.state(), CH_DONE_FILLED)
        self.assertEquals(x.readable(), True)
        
        y = Int() << one()
        self.assertEquals(y.get(), 1)
        
        self.assertEqual(one().get(), 1)
        
        
    
    def testOneArg(self):
        x = Int()
        y = Int(2)
        self.assertEqual((x << inc(y)).get(), 3)
        
    def testTwoArg(self):
        self.assertEqual(cat(String("cow"), String("moo")).get(), "cowmoo")
        
    def testInputTypes(self):
        x = Int(2)
        y = String("sddf")
        self.assertRaises(FlTypeError, cat, x, y)
        
        String2 = String.subtype()
        z = String2("sddf")
        # Check subclass passes test
        cat(y, z).get()
        
    def testOutputTypes(self):
        # Should be ok to assign to superclass
        f = future()
        f <<= one()
        self.assertEquals(f.get(), 1)
        
        x = Int()
        x <<= one()
        self.assertEquals(x.get(), 1)
        
    def testOutputTypes2(self):    
        Int2 = Int.subtype()
        i = Int2()
        self.assertRaises(FlTypeError, lambda : i << one())
        
    def testTypesMulti(self):
        self.assertRaises(FlTypeError, cat2, String("sdf"), Int("sdf"))
        
    def testMultiArg(self):
        args = ["cow", "goes", "moo"]
        res = cat2(*[String(a) for a in args])
        self.assertEquals(res.get(), "cowgoesmoo")
        
        
    def testType(self):
        MagicInt = Int.subtype()
        self.assertTrue(future.isinstance(MagicInt("hello")))

    def testFib(self):
        self.assertEquals(ex.fib(3).get(),2)
        self.assertEquals(ex.fib(49).get(),7778742049) 
        
    def testMergeSort(self):
        import random
        LEN = 15
        xs = [random.randint(0, 10000) for i in range(LEN)]
        sorted = ex.merge_sort(xs).get()
        self.assertEqual(len(sorted), LEN)
        as_str = repr(sorted)
        for i in xrange(len(sorted) - 1):
            self.assertTrue(sorted[i] <= sorted[i+1], 
                    "%d > %d at pos %i in sorted array %s" % (
                            sorted[i], sorted[i+1], i, as_str))
            
    def testWorkerThread(self):
        from PyDFlow.base.LocalExecutor import isWorkerThread
        self.assertFalse(isWorkerThread())
        @func((future), ())
        def isWorker():
            return isWorkerThread()
        self.assertTrue(isWorker().get())
       
    def testRecurse1(self):
        """
        See if recursion works for small number of processes
        """
        self.assertEquals(rec_fib(2).get(), 1)
        
    
    def testZZRecurse2(self):
        """
        See if recursion fails for large number of processes.
        Have this as last test as it ties up lots of threads
        """
        # check that 
        res = rec_fib(49)
        from PyDFlow.futures import Future
        resslot = Future()
        def waiter():
            resslot.set(res.get())
        t = th.Thread(target=waiter)
        t.start()
        
        
        # 10 seconds
        print "waiting for fibonacci result"
        for i in range(10):
        #while True:
            if resslot.isSet():
                self.assertEquals(resslot.get(), 7778742049)
            print ".",
            time.sleep(1)
        self.fail("Ran out of time waiting for recursive fibonacci calc")
        
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()