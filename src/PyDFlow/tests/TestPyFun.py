'''
Created on 19/02/2011

@author: tim
'''
import unittest
from PyDFlow.PyFun import future, func
import threading as th

Int = future.subtype()

class Test(unittest.TestCase):


    def setUp(self):
        pass
        


    def tearDown(self):
        pass


    def testGet1(self):
        x = Int.bind(42)
        self.assertEquals(x.get(), 42)
    
    
    def testType(self):
        MagicInt = Int.subtype()
        self.assertTrue(future.isinstance(MagicInt.bind("hello")))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()