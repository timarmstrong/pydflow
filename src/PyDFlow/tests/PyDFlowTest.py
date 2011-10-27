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


        