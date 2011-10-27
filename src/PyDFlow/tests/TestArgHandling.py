# Copyright 2010-2011 Tim Armstrong <tga@uchicago.edu>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
@author: Tim Armstrong
'''
import unittest
from PyDFlow.PyFun import *
from PyDFlow.tests.PyDFlowTest import PyDFlowTest
from PyDFlow.types.check import FlTypeError, Multiple

Int = py_ivar.subtype()

@func((py_ivar), (py_ivar, Int, int))
def fn(x, y, z):
    return x * (y + z)
        

@func((py_ivar), (py_ivar, Int, int))
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
        self.assertRaises(FlTypeError, lambda: fn(Int(1), py_ivar(2), 2).get())
        
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
        self.assertRaises(FlTypeError, lambda: fn(Int(3), z=2, y=Int(1), x=py_ivar(1)).get())
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
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(1), py_ivar(2), 2).get())
        
        # reordered
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(1), 3, Int(2)).get())
        
        # keyword
        # extra
        self.assertRaises(FlTypeError, lambda: fn_opt(Int(3), z=2, y=Int(1), x=py_ivar(1)).get())
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
        
    def testMulti(self):
        @func((Int), (Multiple(Int)))
        def prod(*ns):
            start = 1
            for n in ns:
                start = start * n
            return start
        
        self.assertEqual(prod(Int(2), Int(3), Int(4)).get(), 24)
    
    def testNoArg(self):
        @func((py_ivar), ())
        def noarg():
            return 1
        
        self.assertEqual(noarg().get(), 1)
        
        self.assertRaises(FlTypeError, lambda: noarg(1))
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()