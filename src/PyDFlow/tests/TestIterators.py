'''
Created on 19/02/2011

@author: tim
'''
import unittest

from PyDFlow.base.patterns import resultbag, resultlist
from PyDFlow.PyFun import *
import time
import random

@func((future), (None))
def inc(x):
    time.sleep(random.random() * 0.1)
    return x + 1

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass

    def checkSequential(self, resiter, COUNT):
        count = 0
        for i, x in enumerate(resiter):
            # Check that it actually finished
            self.assertTrue(x.readable())
            self.assertEqual(i, x.get())
            count += 1
        self.assertEqual(count, COUNT)
        
    def testResultList(self):
        COUNT = 10
        res1 = (future.bind(i) for i in range(COUNT))
        res1b = (future.bind(i) for i in range(COUNT))
        
        res2 = (inc(i - 1) for i in range(COUNT))
        res2b = (inc(i - 1) for i in range(COUNT))
        
    
        self.checkSequential(resultlist(res1), COUNT)
        self.checkSequential(resultlist(res1b, max_ready=1), COUNT)
        
        self.checkSequential(resultlist(res2, max_ready=20), COUNT)
        self.checkSequential(resultlist(res2b), COUNT)
                
        
    def testResultBag(self):
        COUNT = 10
        res1 = (future.bind(i) for i in range(COUNT))
        res2 = (inc(i - 1) for i in range(COUNT))
        res2b = (inc(i - 1) for i in range(COUNT))

        exp = [(x, x) for x in range(COUNT)]
                
        # with max-running at 1 should be in order
        self.checkSequential((item for i, item in resultbag(res2b, max_ready=1)), COUNT)

        # Check that all of the expected results turn up
        act1 = [(i, x.get()) for i, x in list(resultbag(res1))]
        print act1
        self.assertEquals(sorted(act1), exp)
        
        act2 = [(i, x.get()) for i, x in list(resultbag(res2))]
        print act2
        self.assertEquals(sorted(act2), exp)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testResultList']
    unittest.main()