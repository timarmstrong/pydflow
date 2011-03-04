'''
Created on Feb 28, 2011
@author: tga
'''
import unittest
from PyDFlow.PyFun import future, func, compound
from PyDFlow.base.states import *

from PyDFlow.types import Multiple

Int = future.subtype()


@func((Int), (Int, Int))
def add(x, y):
    return x + y

@compound((Int), (Int))
def double(n):
    return add(n, n)

@compound((Int), (Int))
def id(n):
    return n

@compound((Int, Int), (Int, Int))
def swap(x, y):
    return (y, x)

@compound((Int), (Multiple(Int)))
def psum_bottomup(*numbers):
    i1 = numbers[0::2]
    i2 = numbers[1::2]
    new_numbers = []
    for a, b in zip(i1, i2):
        new_numbers.append(add(a, b))
    if len(i1) > len(i2):
        new_numbers.append(i1[-1])
    if len(new_numbers) == 0:
        return Int(0)
    elif len(new_numbers) == 1:
        return new_numbers[0]
    else:
        return psum_bottomup(*new_numbers)

@compound((Int), (Multiple(Int)))
def psum_topdown(*numbers):
    if len(numbers) == 1:
        return numbers[0]
    elif len(numbers) == 2:
        return add(numbers[0], numbers[1])
    else:
        split = len(numbers)/2
        i1 = numbers[0:split]
        i2 = numbers[split:]
        return add(psum_topdown(*i1), psum_topdown(*i2))

class Test(unittest.TestCase):


    def setUp(self):
        from stacktrace import trace_start
        import os
        #try:
        #    os.remove("trace.html")
       # except OSError:
        #    pass
        #trace_start("trace.html")

    def tearDown(self):
        from stacktrace import trace_stop
        #trace_stop()
    
    def test1(self):
        Int(None).get()
    
    def testId(self):
        x = id(Int(10))
#        while True:
#            print channel_state_name[x._state]
        self.assertEquals(id(Int(10)).get(), 10)
        
       
    def testId2(self):
        expr = id(id(Int(10)))
        res = expr.get()
        self.assertEquals(res, 10)
    
    def testSimple(self):
        self.assertTrue(double(Int(10)).get(), 20) 

    def testTree1(self):
        ns = [35, 36, 346, 3, 78, 334, 2, 23, 2, 2342, 235, 7745, 6585, 7562, 234]
        self.assertEquals(sum(ns), psum_bottomup(*[Int(n) for n in ns]).get())
        
    def testTree2(self):
        ns = [35, 36, 346, 3, 78, 334, 2, 23, 2, 2342, 235, 7745, 6585, 7562, 234]
        self.assertEquals(sum(ns), psum_topdown(*[Int(n) for n in ns]).get())
    
    def testTwoOutputs(self):
        x1 = Int(10)
        y1 = Int(20)
        
        y2, x2 = swap(x1, y1)
        
        self.assertEquals(x2.get(), 10)
        self.assertEquals(y2.get(), 20)
    
    def testIntermediate(self):
        #raise Exception()
        x1 = Int(10)
        y1 = Int(20)
        
        y2, x2 = swap(x1, y1)
        
        y22 = double(y2)
        x22 = double(x2)
        
        self.assertEquals(x22.get(), 20)
        self.assertEquals(x2.get(), 10)
        self.assertEquals(y2.get(), 20)
        self.assertEquals(y22.get(), 40)    
        
        
    def testFunctionError(self):
        self.assertRaises(TypeError, double(Int(None)).get)

    def testCompoundError(self):
        self.assertRaises(TypeError, double(Int(None)).get)
 

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()