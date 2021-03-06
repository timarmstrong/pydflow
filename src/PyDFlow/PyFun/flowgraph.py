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

from __future__ import with_statement
'''
@author: Tim Armstrong
'''
from PyDFlow.base.atomic import AtomicIvar, AtomicTask
from PyDFlow.base.flowgraph import Unbound
from PyDFlow.base.mutex import graph_mutex
from PyDFlow.base.states import *
from PyDFlow.writeonce import *
import PyDFlow.base.LocalExecutor as LocalExecutor


import logging
import threading
from PyDFlow.types.check import FlTypeError

class PyIvar(AtomicIvar):

    def __init__(self, *args, **kwargs):
        super(PyIvar, self).__init__(*args, **kwargs)
        
        # The bind variable for py ivars will always be
        # either a future, or contents for a future
        if self._bound is not Unbound:
            if not isinstance(self._bound, WriteOnceVar):
                # If a non-future data item is provided as the binding,
                # we should pack it into a future (this avoids users)
                # having to write boilerplate to put things into futures
                self._future.set(self._bound)
            else:
                self._future = self._bound
            if self._future.isSet():
                    self._state = IVAR_DONE_FILLED

    def _has_data(self):
        """
        Check to see if it is possible to start reading from this
        ivar now
        """
        return self._future.isSet()
        
    

class FuncTask(AtomicTask):
    def __init__(self, func, *args, **kwargs):
        super(FuncTask, self).__init__(*args, **kwargs)
        self._func = func



    def _exec(self, continuation, failure_continuation):
        """
        Just run the task in the current thread, 
        assuming it is ready
        """
        logging.debug("%s: Starting a FuncTask %s" % (threading.currentThread().getName(), 
                                                      repr(self)))
        
        #TODO: select execution backend, run me!
        
        with graph_mutex:
            # Set things up
            # Check state again to be sure it is sensible
            if self._state in (T_QUEUED, T_CONTINUATION):
                self._prep_ivars()
                input_values = self._gather_input_values()
                logging.debug("%s: FuncTask %s changing state to T_RUNNING" % (
                                                threading.currentThread().getName(), 
                                              repr(self)))
                self._state = T_RUNNING
                logging.debug("%s: Started a FuncTask %s" % (threading.currentThread().getName(), 
                                              repr(self)))
            else:
                # Bad state
                # TODO: better logic
                raise Exception("Invalid task state %s encountered by worker thread" % 
                                    (repr(self)))
        
        # Update state so we know its running
        #logging.debug("Running %s with inputs %s" %(repr(self), repr(input_values)))
    
        #TODO: tag failed tasks with exception?
        # In general need to work out failure handling logic
        
        # Run the function in this thread.  If the function raises an exception
        # it will be handled by the caller
        return_val = self._func(*(input_values))
            
        continuation(self, return_val)
        
    def isSynchronous(self):
        return True

    
