'''
@author: Tim Armstrong
'''
 
import unittest
from PyDFlow.PyFun import *
import PyDFlow.examples.reduce as red
from PyDFlow.base.patterns import dynreduce, treereduce, foldl, scanl
from itertools import imap
from PyDFlow.tests.PyDFlowTest import PyDFlowTest

Int = future.subtype()
String = future.subtype()
class TestReduce(PyDFlowTest):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testSum(self):
        import random
        nums, boundnums = red.genlist()
        tot = sum(nums)
        print "Total of %s is %d" % (repr(nums), tot)
        
        treeres = treereduce(red.add, boundnums).get()
        print "treereduce(add, bound).get() = %d" % treeres
    
        dynres = dynreduce(red.add, boundnums).get()
        print "dynreduce(add, bound).get() = %d" % dynres
        
        self.assertEqual(treeres, tot)
        self.assertEqual(dynres, tot)

    def testAssociative(self):
        """
        Test reductions which are associative
        """
        import itertools
        str = ["aaa", "bb", "c", "dd", "e", "fff"]
        bound = map(String, str)
        exp = ''.join(str)
        
        
        @func((String),(String, String))
        def concat(a, b):
            return a + b
        foldres = foldl(concat, String(""), bound).get()
        
        treeres = treereduce(concat, bound).get()
        self.assertEquals(exp, foldres)
        self.assertEquals(exp, treeres)
        
    def testScanl(self):
        COUNT = 50
        @func((Int), (Int, Int))
        def add(a, b):
            return a + b
        sums = [c.get() 
                for c in list(scanl(add, Int(0), imap(Int, xrange(1,COUNT))))]
        print sums
        sum = 0
        print "sums", sums
        for i in range(0, len(sums)):
            self.assertEquals(sums[i], sum)
            sum += i + 1

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()