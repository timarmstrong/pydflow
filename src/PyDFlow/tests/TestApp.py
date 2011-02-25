'''
Created on 19/02/2011

@author: tim
'''
import unittest
import PyDFlow.app.paths as app_paths
from PyDFlow.app import *
import os.path
import os
import time

testdir = os.path.dirname(__file__)
app_paths.add_path(os.path.join(testdir, "apps"))

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testCp(self):
        """
        Test a global utility
        """
        @app((localfile), (localfile))
        def cp(src):
            return "cp @src @output_0"
        hw = localfile(os.path.join(testdir, "files/helloworld"))
        x = cp(hw)
        #TODO: for some reason this is a list.
        print x.get()
        self.assertEquals(x.open().readlines(), ["hello world!"])
        self.assertEquals(x.open().readlines(), ["hello world!"])
    
    def testUtil(self):
        @app((localfile), (None))
        def write(str):
            return "myecho @output_0 '%s'" % str
        # Write to a temporary file
        x = write("blah\nblah")
        self.assertEquals(x.open().readlines(), ['blah\n', 'blah\n'])
        xpath = x.get()
        x = None
        time.sleep(1)
        
        
        # write to a bound file
        y = localfile("here")
        y <<= write("sometext")
        self.assertEquals(y.open().readlines(), ['sometext\n'])
        ypath = y.get()
        y = None
        time.sleep(1)
        self.assertTrue(os.path.exists("here"), "bound file deleted accidentally")
        os.remove(ypath)
        time.sleep(1)
        
        self.assertFalse(os.path.exists("here"), "bound file should have been deleted")
        self.assertFalse(os.path.exists(xpath))
        
    def testRedir(self):
        pass
    
    def testMergeSort(self):
        import PyDFlow.examples.mergesort.mergesort as ms
        import random
        import tempfile
        import os
        files = []
        
        try:
            # Make a bunch of files with random integers
            NUM_FILES = 10
            NO_PER_FILE = 100
            for filenum in range(NUM_FILES):
                handle, path = tempfile.mkstemp()
                filehandle = os.fdopen(handle, 'w')
                files.append(path)
                for i in range(NO_PER_FILE):
                    filehandle.write("%d\n" % random.randint(1, 1000))                     
                filehandle.close()
            
            flfiles = map(ms.intfile.bind, files)
            sorted = ms.merge_sort(flfiles)
            results = [int(x) for x in sorted.open().readlines()]
            self.assertEquals(len(results), NUM_FILES*NO_PER_FILE)
            for i in xrange(len(results) - 1):
                self.assertTrue(results[i] <= results[i+1])
            
        finally:
            for f in files:
                os.remove(f)    
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()