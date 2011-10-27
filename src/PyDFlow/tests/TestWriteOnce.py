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
from PyDFlow.writeonce import WriteOnceVar
import threading as th
from PyDFlow.tests.PyDFlowTest import PyDFlowTest


class TestWriteOnce(PyDFlowTest):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testName(self):
        pass

    def testGet(self):
        x = WriteOnceVar()
        x.set(32)
        self.assertEquals(x.get(), 32)
        
    def testIsSet(self):
        x = WriteOnceVar()
        self.assertFalse(x.isSet())
        x.set("hello")
        self.assertTrue(x.isSet())
    
    def testSetGet(self):
        x = WriteOnceVar()
        setTh = th.Thread(target=(lambda: x.set(32)))
        setTh.run()
        self.assertEquals(x.get(), 32)
        self.assertEquals(x.get(), 32)
        self.assertEquals(x.get(), 32)
        self.assertEquals(x.get(), 32)
        
    def testSetGet2(self):
        """
        Swap data between three threads
        """
        r1 = WriteOnceVar() 
        r2 = WriteOnceVar()
        x = WriteOnceVar()
        y = WriteOnceVar()
        def t1():
            x.set("one")
            r1.set(y.get())
            
        def t2():
            y.set("two")
            r2.set(x.get())

        th1 = th.Thread(target=t1)
        th1.start()
        
        self.assertEquals(x.get(), "one")
        self.assertTrue(x.isSet())
        
        th2 = th.Thread(target=t2)
        th2.start()
        
        th1.join()
        th2.join()
        self.assertEquals(r1.get(), "two")
        self.assertEquals(r2.get(), "one")
        
    def testReply(self):
        '''
        Send reply ivar down request ivar 
        '''
        requestCh = WriteOnceVar()
        def t():
            reply = requestCh.get()
            reply.set("hello!!")
            
        th.Thread(target=t).start()
        replyCh = WriteOnceVar()
        requestCh.set(replyCh)
        self.assertEquals(replyCh.get(), "hello!!")
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()