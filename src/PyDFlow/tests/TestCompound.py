'''
Created on Feb 28, 2011
@author: tga
'''
import unittest
from PyDFlow.PyFun import future, func, compound
from PyDFlow.base.states import *

from itertools import islice

from PyDFlow.types import Multiple, FlTypeError

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

@compound((Int), (Multiple(Int)))
def psum(*numbers):
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
        return psum(*new_numbers)

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass
    
    def test1(self):
        Int(None).get()
    
    def testId(self):
        x = id(Int(10))
#        while True:
#            print channel_state_name[x._state]
        self.assertEquals(id(Int(10)).get(), 10)
        
    def testId2(self):
        self.assertEquals(id(id(Int(10))).get(), 10)
    
    def testSimple(self):
        self.assertTrue(double(Int(10)).get(), 20) 

    def testTree(self):
        ns = [35, 36, 346, 3, 78, 334, 2, 23, 2, 2342, 235, 7745, 6585, 7562, 234]
        self.assertEquals(sum(ns), psum(*[Int(n) for n in ns]).get())
                        
    #def testFunctionError(self):
    #    self.assertRaises(TypeError, double(Int(None)).get)

    #def testCompoundError(self):
    #    self.assertRaises(TypeError, double(Int(None)).get)
 

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()