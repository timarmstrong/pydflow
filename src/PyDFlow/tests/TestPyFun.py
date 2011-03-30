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
from PyDFlow.base import LocalExecutor
from PyDFlow.base.patterns import resultset
from PyDFlow.tests.PyDFlowTest import PyDFlowTest

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

@func((Int), (None))
def just_sleep(dur):
    time.sleep(dur)
    return 0

class MyException(Exception):
    pass
@func((Int), ())
def cause_exception():
    raise MyException()

class TestPyFun(PyDFlowTest):


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
    
    def testSameArg(self):
        self.assertEqual(double(Int(20)).get(), 40)
        
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
        #self.assertEquals(ex.fib(24).get(),46368)
        #self.assertEquals(ex.fib(5).get(),5)
        #self.assertEquals(ex.fib(10).get(),55)
        #self.assertEquals(ex.fib(49).get(),7778742049) 
        
    def testFib2(self):
        @func((Int), (Int, Int))
        def nextfib(f1, f2):
            time.sleep(0.1)
            return f1 + f2
        fib_0, fib_1 = Int(0), Int(1)
        fib_2 = nextfib(fib_0, fib_1)
        fib_3 = nextfib(fib_1, fib_2)
        fib_4 = nextfib(fib_2, fib_3)
        fib_5 = nextfib(fib_3, fib_4)
        fib_5.get()
        
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
    
    def testRecurse2(self):
        """
        Recurse to a largish depth.
        Not that we risk hitting max recursion depth in python if we
        go too far.
        """
        self.assertEquals(silly_add(Int(50), Int(20)).get(), 70)
    
    def testRecurse4(self):
        """
        Check that it works ok if we call a future which has been resolved
        """
        x = silly_add(Int(30), Int(20))
        x.get()
        
        @func((Int), ())
        def fn():
            return x.get()
        
        self.assertEquals(fn().get(), 50)
    
    def testRecurse5(self):
        """
        Check that it works ok if we call a future which is in process of ebing filled
        """
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
    
    def testBalance1(self):
        ts = [just_sleep(0.5) for i in range(LocalExecutor.NUM_THREADS)]
        start = time.time()
        res = resultset(ts)
        for r in res:
            pass
        end = time.time()
        print "Took %f" % (end - start)
        self.assertTrue(end - start < 1.0)
        
    def testBalance2(self):
        @func((Int),(Multiple(Int)))
        def sleep_many(*sleeps):
            return 0
        start = time.time()
        sleep_many(*[just_sleep(0.5) for i in range(LocalExecutor.NUM_THREADS)]).get()
        end = time.time()
        print "Took %f" % (end - start)
        self.assertTrue(end - start < 1.0)
    
    def testZZRecurse2Fail(self):
        """
        Check that max recursion depth failure passed up  ok.
        """
        self.assertExecutionException(RuntimeError, silly_add(Int(100000), Int(20)).get)
    
    def testRecurse3(self):
        """
        See if recursion fails for large number of processes.
        Have this as last test as it ties up lots of threads
        """
        res = rec_fib(15)
        from PyDFlow.futures import Future
        resslot = Future()
        def waiter(resslot):
            print "waiter running"
            f = res.get()
            print "waiter got %d" % f 
            resslot.set(f)
            print "waiter done %s" % repr(resslot)
        t = th.Thread(target=waiter, args=(resslot,))
        t.start()
        
        
        # 10 seconds
        print "waiting for fibonacci result"
        for i in range(10):
        #while True:
            print resslot
            if resslot.isSet():
                self.assertEquals(resslot.get(), 610)
                return
            print ".",
            time.sleep(1)
        print(resslot.get())
        self.fail("Ran out of time waiting for recursive fibonacci calc")
        
    def testSimpleException(self):
        fut = cause_exception()
        self.assertExecutionException(MyException, fut.get)
    
    def testSimpleException2(self):
        fut = inc(cause_exception())
        self.assertExecutionException(MyException, fut.get)
    
    def testSimpleException3(self):
        fut = add(cause_exception(), cause_exception())
        self.assertExecutionException(MyException, fut.get)
    
    def testSimpleException4(self):
        fut = add(one(), cause_exception())
        self.assertExecutionException(MyException, fut.get)
    
    def testSimpleException5(self):
        # Check that exception is propagated ok if we reuse a bad channel
        fut = cause_exception()
        self.assertExecutionException(MyException, fut.get)
        self.assertExecutionException(MyException, fut.get)
        self.assertExecutionException(MyException, add(one(), fut).get)
        self.assertExecutionException(MyException, add(one(), add(one(), fut)).get)
        self.assertExecutionException(MyException, add(one(), add(one(), add(one(), fut))).get)
      
    def testComplexException(self):
        res = add(add(
                    inc(one()), 
                    inc(cause_exception())), one())
        self.assertExecutionException(MyException, res.get)
        res2 = inc(res)
        self.assertExecutionException(MyException, res2.get)
        res3 = add(res2, res)
        self.assertExecutionException(MyException, res3.get)
    
    
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()