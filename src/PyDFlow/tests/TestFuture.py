'''
Created on 19/02/2011

@author: tim
'''
import unittest
from PyDFlow.futures import Future
import threading as th


class TestFuture(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testName(self):
        pass

    def testGet(self):
        x = Future()
        x.set(32)
        self.assertEquals(x.get(), 32)
        
    def testIsSet(self):
        x = Future()
        self.assertFalse(x.isSet())
        x.set("hello")
        self.assertTrue(x.isSet())
    
    def testSetGet(self):
        x = Future()
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
        r1 = Future() 
        r2 = Future()
        x = Future()
        y = Future()
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
        Send reply channel down request channel. 
        '''
        requestCh = Future()
        def t():
            reply = requestCh.get()
            reply.set("hello!!")
            
        th.Thread(target=t).start()
        replyCh = Future()
        requestCh.set(replyCh)
        self.assertEquals(replyCh.get(), "hello!!")
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()