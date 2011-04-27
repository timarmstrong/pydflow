'''
@author: Tim Armstrong
'''
import unittest
from PyDFlow.PyFun import *
from PyDFlow.tests.PyDFlowTest import PyDFlowTest
from PyDFlow.types.check import FlTypeError

Int = future.subtype()

@func((future), (future, Int, int))
def fn(x, y, z):
    return x * (y + z)
        

@func((future), (future, Int, int))
def fn_opt(x, y=Int(3), z=7):
    return x * (y + z)
        
class TestArgHandling(PyDFlowTest):


    def setUp(self):
        pass


    def tearDown(self):
        pass

    def testAllPositional(self):
        
        self.assertEqual(fn(Int(1), Int(2), 3).get(), 5)
        # Wrong number args
        self.assertRaises(FlTypeError, lambda: fn(Int(1), Int(2)).get())
        self.assertRaises(FlTypeError, lambda: fn(Int(1), Int(2), 3, 4).get())
        
        # Wrong types
        self.assertRaises(FlTypeError, lambda: fn(Int(1), Int(2), "hello").get())
        self.assertRaises(FlTypeError, lambda: fn(Int(1), future(2), 2).get())
        
        # reordered
        self.assertRaises(FlTypeError, lambda: fn(Int(1), 3, Int(2)).get())
        
    def testAllKeyword(self):
        # permutations
        self.assertEqual(fn(z=2, x=Int(3), y=Int(1)).get(), 9)
        self.assertEqual(fn(Int(3), z = 2, y=Int(1)).get(), 9)
        self.assertEqual(fn(x=Int(3), z = 2, y=Int(1)).get(), 9)
        self.assertEqual(fn(x=Int(3), y=Int(1), z = 2).get(), 9)
    
    def testWrongNumberKW(self):
        # extra
        self.assertRaises(FlTypeError, lambda: fn(Int(3), z=2, y=Int(1), x=future(1)).get())
        # missing
        self.assertRaises(FlTypeError, lambda: fn(Int(3), y=Int(1)).get())
        #switched
        self.assertRaises(FlTypeError, lambda: fn(Int(3), z=2, j=Int(1)).get())
    
    def testOptionalAllProvided(self):
        # Check works normally with all args present
        self.assertEqual(fn_opt(Int(1), Int(2), 3).get(), 5)
        self.assertEqual(fn_opt(z=2, x=Int(3), y=Int(1)).get(), 9)
        self.assertEqual(fn_opt(Int(3), z = 2, y=Int(1)).get(), 9)
        self.assertEqual(fn_opt(x=Int(3), z = 2, y=Int(1)).get(), 9)
        self.assertEqual(fn_opt(x=Int(3), y=Int(1), z = 2).get(), 9)
    
    def testOptionalErrors(self):
        # Error cases
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(1), Int(2), 3, 4).get())
        
        # Wrong types
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(1), Int(2), "hello").get())
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(1), future(2), 2).get())
        
        # reordered
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(1), 3, Int(2)).get())
        
        # keyword
        # extra
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(3), z=2, y=Int(1), x=future(1)).get())
        # missing non-optional
        self.assertRaises(FlTypeError, lambda: fn_opt().get())
        #switched
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(3), z=2, j=Int(1)).get())
    
    def testOptionalPositional(self):
        # Test positional
        self.assertEqual(fn_opt(Int(1), Int(2)).get(), 9)
        self.assertEqual(fn_opt(Int(2)).get(), 20)
    
    def testOptionalKeyword(self):
        # Test positional
        self.assertEqual(fn_opt(Int(2), z=2).get(), 10)
        self.assertEqual(fn_opt(Int(2), y=Int(2)).get(), 18)    
    
    def testNoArg(self):
        @func((future), ())
        def noarg():
            return 1
        
        self.assertEqual(noarg().get(), 1)
        
        self.assertRaises(FlTypeError, lambda: noarg(1))
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()