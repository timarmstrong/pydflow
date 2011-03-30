'''
Created on Mar 30, 2011

@author: tim armstrong
'''
import unittest

from PyDFlow.base.exceptions import ExecutionException

class PyDFlowTest(unittest.TestCase):
    
    def assertExecutionException(self, cause, fn):
        """
        We wrap exceptions that occur while executing a task in an
        ExecutionExcpetion container.  This function asserts
        that there is only one exception in the container and
        that it is of the above type.
        """
        try:
            fn()
        except ExecutionException, e:
            if len(e.causes) != 1:
                self.fail("ExecutionException had %d causes, expected %d:\n%s"
                          % (len(e.causes), 1, repr(e)))
            else:
                excClass = e.causes[0]
                try:
                    raise excClass
                except cause:
                    return
                except Exception, e:
                    self.fail("Exception %s of wrong class, expected %s"
                              % (repr(e), repr(cause)))
        except Exception, e:
            self.fail("Exception %s of wrong type raised" % repr(e))
        else:
            self.fail("Exception not raised")


        